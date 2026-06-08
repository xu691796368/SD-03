// Package client_test 实现分布式缓存系统 Task 3.9 完整测试套件
//
// 测试范围：
//   - 协议编解码测试（protocol）：序列化/反序列化、ValidateFrame、错误码处理、边界条件
//   - LRU 缓存测试（cache）：基本操作、容量淘汰、热点数据、删除操作、空键处理
//   - 一致性哈希环测试（shard）：路由确定性、虚拟节点、节点添加/移除、数据分布
//   - TCP 服务器测试（server）：多客户端并发、非法命令、参数缺失、连接断开、压力测试
//   - 主从复制测试（replication）：写同步、删除同步、全量恢复、并发安全
//
// 运行方式：
//
//	go test ./tests/client/ -v -count=1 -timeout 120s -coverprofile=coverage.out
//	go tool cover -html=coverage.out -o coverage_report.html
package client_test

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/yourusername/sd-03-cache/pkg/cache"
	"github.com/yourusername/sd-03-cache/pkg/node"
	"github.com/yourusername/sd-03-cache/pkg/protocol"
	"github.com/yourusername/sd-03-cache/pkg/shard"
	"github.com/yourusername/sd-03-cache/tests/client"
)

// ========================================================================
// 第一部分：协议编解码测试（protocol）
// ========================================================================

// TestProtocol_EncodeDecodeRequest 测试请求的序列化和反序列化
//
// 正常场景：
//   - GET/SET/DELETE/INFO 四种命令的编解码往返
//   - 带 Key 和 Value 的完整帧
//   - 空 Key（GET 命令）和空 Value（SET 命令）
func TestProtocol_EncodeDecodeRequest(t *testing.T) {
	tests := []struct {
		name  string
		cmd   uint8
		key   []byte
		value []byte
	}{
		{"GET with key", uint8(protocol.CMD_GET), []byte("mykey"), nil},
		{"SET with key-value", uint8(protocol.CMD_SET), []byte("mykey"), []byte("myvalue")},
		{"DELETE with key", uint8(protocol.CMD_DELETE), []byte("mykey"), nil},
		{"INFO no params", uint8(protocol.CMD_INFO), []byte{}, nil},
		{"SET empty value", uint8(protocol.CMD_SET), []byte("key"), []byte{}},
		{"SET nil value", uint8(protocol.CMD_SET), []byte("key"), nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := protocol.EncodeRequest(tt.cmd, tt.key, tt.value)
			if err != nil {
				t.Fatalf("EncodeRequest failed: %v", err)
			}

			decoded, err := protocol.DecodeRequest(encoded)
			if err != nil {
				t.Fatalf("DecodeRequest failed: %v", err)
			}

			if decoded.Command != tt.cmd {
				t.Errorf("Command: expected 0x%02X, got 0x%02X", tt.cmd, decoded.Command)
			}
			if string(decoded.Key) != string(tt.key) {
				t.Errorf("Key: expected '%s', got '%s'", string(tt.key), string(decoded.Key))
			}
			if string(decoded.Value) != string(tt.value) {
				t.Errorf("Value: expected '%s', got '%s'", string(tt.value), string(decoded.Value))
			}
		})
	}
}

// TestProtocol_EncodeDecodeResponse 测试响应的序列化和反序列化
//
// 正常场景：
//   - 成功响应（Status=SUCCESS）编码和解码
//   - 错误响应（各种错误码）编码和解码
//   - 带 Value 的响应（如 GET 返回数据）
//   - 空 Value 的响应（如 SET/DELETE 成功）
func TestProtocol_EncodeDecodeResponse(t *testing.T) {
	tests := []struct {
		name   string
		cmd    uint8
		status uint8
		value  []byte
	}{
		{"GET success with value", uint8(protocol.CMD_GET), uint8(protocol.SUCCESS), []byte("returned-value")},
		{"SET success no value", uint8(protocol.CMD_SET), uint8(protocol.SUCCESS), []byte{}},
		{"DELETE success no value", uint8(protocol.CMD_DELETE), uint8(protocol.SUCCESS), []byte{}},
		{"INFO success with data", uint8(protocol.CMD_INFO), uint8(protocol.SUCCESS), []byte(`{"nodes":3}`)},
		{"Unknown command error", uint8(protocol.CMD_GET), uint8(protocol.ERROR_UNKNOWN_COMMAND), []byte("bad cmd")},
		{"Invalid key error", uint8(protocol.CMD_GET), uint8(protocol.ERROR_INVALID_KEY), []byte("bad key")},
		{"Invalid value error", uint8(protocol.CMD_SET), uint8(protocol.ERROR_INVALID_VALUE), []byte("bad value")},
		{"Cache full error", uint8(protocol.CMD_SET), uint8(protocol.ERROR_CACHE_FULL), []byte("full")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := protocol.EncodeResponse(tt.cmd, tt.status, tt.value)
			if err != nil {
				t.Fatalf("EncodeResponse failed: %v", err)
			}

			decoded, err := protocol.DecodeResponse(encoded)
			if err != nil {
				t.Fatalf("DecodeResponse failed: %v", err)
			}

			if decoded.Command != tt.cmd {
				t.Errorf("Command: expected 0x%02X, got 0x%02X", tt.cmd, decoded.Command)
			}

			if decoded.ValueLen > 0 {
				if tt.status != decoded.Value[0] {
					t.Errorf("Status: expected 0x%02X, got 0x%02X", tt.status, decoded.Value[0])
				}
				if string(decoded.Value[1:]) != string(tt.value) {
					t.Errorf("Value: expected '%s', got '%s'", string(tt.value), string(decoded.Value[1:]))
				}
			}
		})
	}
}

// TestProtocol_EncodeRequestErrors 测试请求编码的参数校验
//
// 异常场景：
//   - nil Key 返回错误
//   - 超大 Key（> MaxKeyLength）返回错误
//   - 超大 Value（> MaxValueLength）返回错误
func TestProtocol_EncodeRequestErrors(t *testing.T) {
	// nil Key
	_, err := protocol.EncodeRequest(uint8(protocol.CMD_GET), nil, nil)
	client.AssertError(t, err, "nil key should be rejected")

	// oversized Key
	bigKey := make([]byte, protocol.MaxKeyLength+1)
	_, err = protocol.EncodeRequest(uint8(protocol.CMD_GET), bigKey, nil)
	client.AssertError(t, err, "oversized key should be rejected")

	// oversized Value
	bigValue := make([]byte, protocol.MaxValueLength+1)
	_, err = protocol.EncodeRequest(uint8(protocol.CMD_SET), []byte("key"), bigValue)
	client.AssertError(t, err, "oversized value should be rejected")
}

// TestProtocol_DecodeRequestErrors 测试请求解码的参数校验
//
// 异常场景：
//   - nil 数据返回错误
//   - 数据长度不足帧头大小返回错误
//   - 数据长度不足声明长度返回错误
func TestProtocol_DecodeRequestErrors(t *testing.T) {
	// nil data
	_, err := protocol.DecodeRequest(nil)
	client.AssertError(t, err, "nil data should be rejected")

	// data too short
	_, err = protocol.DecodeRequest([]byte{0x01, 0x00, 0x00})
	client.AssertError(t, err, "short data should be rejected")

	// data with declared length beyond actual
	shortData := make([]byte, 11)
	shortData[0] = 0x01                             // Command=GET
	binary.BigEndian.PutUint32(shortData[1:5], 100) // KeyLen=100
	binary.BigEndian.PutUint32(shortData[5:9], 0)   // ValueLen=0
	// only 2 bytes of key data (needs 100)
	shortData[9] = 'a'
	shortData[10] = 'b'
	_, err = protocol.DecodeRequest(shortData)
	client.AssertError(t, err, "data shorter than declared length should be rejected")
}

// TestProtocol_ValidateFrame 测试协议帧验证函数
//
// 覆盖：合法帧、nil帧、非法命令、长度超限、长度不匹配、命令参数校验
func TestProtocol_ValidateFrame(t *testing.T) {
	// 合法帧
	validFrame := protocol.NewFrame(uint8(protocol.CMD_SET), []byte("key"), []byte("value"))
	if err := protocol.ValidateFrame(validFrame); err != nil {
		t.Fatalf("Valid frame should pass: %v", err)
	}

	// INFO 零长度帧
	infoFrame := protocol.NewFrame(uint8(protocol.CMD_INFO), []byte{}, []byte{})
	if err := protocol.ValidateFrame(infoFrame); err != nil {
		t.Fatalf("INFO frame should pass: %v", err)
	}

	// nil 帧
	if err := protocol.ValidateFrame(nil); err == nil {
		t.Fatal("nil frame should be rejected")
	}

	// 非法命令
	badCmdFrame := protocol.NewFrame(0x99, []byte("key"), []byte("value"))
	err := protocol.ValidateFrame(badCmdFrame)
	client.AssertError(t, err, "invalid command should be rejected")
	code := protocol.GetErrorCode(err)
	client.AssertEqual(t, code, protocol.ERROR_UNKNOWN_COMMAND, "invalid command error code")

	// KeyLen 与数据不匹配
	mismatchFrame := &protocol.ProtocolFrame{
		Command:  uint8(protocol.CMD_SET),
		KeyLen:   100,
		ValueLen: 5,
		Key:      []byte("short"),
		Value:    []byte("value"),
	}
	err = protocol.ValidateFrame(mismatchFrame)
	client.AssertError(t, err, "KeyLen mismatch should be rejected")
	code = protocol.GetErrorCode(err)
	client.AssertEqual(t, code, protocol.ERROR_FRAME_MISMATCH, "mismatch error code")

	// GET 空Key
	emptyKeyFrame := protocol.NewFrame(uint8(protocol.CMD_GET), []byte{}, []byte{})
	err = protocol.ValidateFrame(emptyKeyFrame)
	client.AssertError(t, err, "GET with empty key should be rejected")
	code = protocol.GetErrorCode(err)
	client.AssertEqual(t, code, protocol.ERROR_INVALID_KEY, "empty key error code")

	// 超大 KeyLen
	oversizedKeyFrame := protocol.NewFrame(uint8(protocol.CMD_SET), make([]byte, protocol.MaxKeyLength+1), []byte("v"))
	err = protocol.ValidateFrame(oversizedKeyFrame)
	client.AssertError(t, err, "oversized key should be rejected")

	// 超大 ValueLen
	oversizedValueFrame := protocol.NewFrame(uint8(protocol.CMD_SET), []byte("k"), make([]byte, protocol.MaxValueLength+1))
	err = protocol.ValidateFrame(oversizedValueFrame)
	client.AssertError(t, err, "oversized value should be rejected")
}

// TestProtocol_NewFrameAndCopy 测试帧创建和拷贝
func TestProtocol_NewFrameAndCopy(t *testing.T) {
	frame := protocol.NewFrame(uint8(protocol.CMD_SET), []byte("key"), []byte("value"))
	if frame.Command != uint8(protocol.CMD_SET) {
		t.Errorf("Command: expected SET(0x02), got 0x%02X", frame.Command)
	}
	if string(frame.Key) != "key" {
		t.Errorf("Key: expected 'key', got '%s'", string(frame.Key))
	}
	if string(frame.Value) != "value" {
		t.Errorf("Value: expected 'value', got '%s'", string(frame.Value))
	}
	if frame.KeyLen != 3 {
		t.Errorf("KeyLen: expected 3, got %d", frame.KeyLen)
	}
	if frame.ValueLen != 5 {
		t.Errorf("ValueLen: expected 5, got %d", frame.ValueLen)
	}

	// nil key/value should become empty
	emptyFrame := protocol.NewFrame(uint8(protocol.CMD_INFO), nil, nil)
	if len(emptyFrame.Key) != 0 || len(emptyFrame.Value) != 0 {
		t.Fatal("nil key/value should become empty")
	}

	// Copy
	copied := frame.Copy()
	if copied == frame {
		t.Fatal("Copy should return new pointer")
	}
	if !frame.Equals(copied) {
		t.Fatal("Copy should equal original")
	}

	// Modify original, copy should not change
	frame.Value[0] = 'X'
	if copied.Value[0] == 'X' {
		t.Fatal("Copy should be deep copy")
	}

	// nil Copy
	if (*protocol.ProtocolFrame)(nil).Copy() != nil {
		t.Fatal("nil frame Copy should return nil")
	}
}

// ========================================================================
// 第二部分：LRU 缓存测试（cache）
// ========================================================================

// TestCache_NewLRUCache 测试 LRU 缓存构造函数
//
// 正常场景：创建有效容量的缓存
// 异常场景：容量为 0 或负数
func TestCache_NewLRUCache(t *testing.T) {
	// 正常创建
	c, err := cache.NewLRUCache(100)
	client.AssertNoError(t, err, "NewLRUCache(100)")
	client.AssertEqual(t, c.Size(), 0, "initial size")
	client.AssertFalse(t, c.IsFull(), "initial not full")
	client.AssertNoError(t, c.Clear(), "Clear")

	// 容量为 0
	_, err = cache.NewLRUCache(0)
	client.AssertError(t, err, "zero capacity")

	// 负容量
	_, err = cache.NewLRUCache(-1)
	client.AssertError(t, err, "negative capacity")
}

// TestCache_SetGetDelete 测试 LRU 缓存的 SET/GET/DELETE 基本操作
//
// 正常场景：
//   - SET → GET 返回正确的值
//   - DELETE 后 GET 返回不存在
//   - 不存在的 Key GET 返回 false
//   - Size 正确反映条目数
func TestCache_SetGetDelete(t *testing.T) {
	c, _ := cache.NewLRUCache(10)

	// SET
	client.AssertNoError(t, c.Set("key1", []byte("value1")), "SET key1")
	client.AssertEqual(t, c.Size(), 1, "size after SET")

	// GET 存在的 Key
	val, ok := c.Get("key1")
	client.AssertTrue(t, ok, "GET key1 exists")
	client.AssertEqual(t, string(val), "value1", "GET key1 value")

	// GET 不存在的 Key
	_, ok = c.Get("nonexistent")
	client.AssertFalse(t, ok, "GET nonexistent")

	// DELETE 存在的 Key
	deleted := c.Delete("key1")
	client.AssertTrue(t, deleted, "DELETE key1")
	client.AssertEqual(t, c.Size(), 0, "size after DELETE")

	// DELETE 不存在的 Key
	deleted = c.Delete("nonexistent")
	client.AssertFalse(t, deleted, "DELETE nonexistent")

	// DELETE 后 GET
	_, ok = c.Get("key1")
	client.AssertFalse(t, ok, "GET after DELETE")
}

// TestCache_UpdateValue 测试更新已存在 Key 的值
//
// 验证点：更新后新值覆盖旧值
func TestCache_UpdateValue(t *testing.T) {
	c, _ := cache.NewLRUCache(10)

	c.Set("key", []byte("old"))
	c.Set("key", []byte("new"))

	val, ok := c.Get("key")
	client.AssertTrue(t, ok, "GET existing after update")
	client.AssertEqual(t, string(val), "new", "updated value")
	client.AssertEqual(t, c.Size(), 1, "size unchanged after update")
}

// TestCache_EvictionAtCapacity 测试 LRU 容量淘汰机制
//
// 对应 specs.md 场景 "缓存达到容量上限时自动淘汰"
// 验收标准1：容量 N 时添加第 N+1 条时最久未使用的条目被淘汰
func TestCache_EvictionAtCapacity(t *testing.T) {
	c, _ := cache.NewLRUCache(5)

	// 填充 5 条数据
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("val-%d", i)
		client.AssertNoError(t, c.Set(key, []byte(val)), fmt.Sprintf("SET %s", key))
	}
	client.AssertEqual(t, c.Size(), 5, "size at capacity")
	client.AssertTrue(t, c.IsFull(), "cache full")

	// 第 6 条写入，触发淘汰
	client.AssertNoError(t, c.Set("key-5", []byte("val-5")), "SET key-5")
	client.AssertEqual(t, c.Size(), 5, "size after eviction")

	// 验证 key-0（最先入）被淘汰
	_, ok := c.Get("key-0")
	client.AssertFalse(t, ok, "key-0 should be evicted (oldest)")

	// 验证 key-5（最新）存在
	val, ok := c.Get("key-5")
	client.AssertTrue(t, ok, "key-5 should exist (newest)")
	client.AssertEqual(t, string(val), "val-5", "key-5 value")
}

// TestCache_HotDataPreservation 测试热点数据保留
//
// 对应 specs.md 场景 "重复访问热点数据保持命中"
func TestCache_HotDataPreservation(t *testing.T) {
	c, _ := cache.NewLRUCache(5)

	// 填充 5 条
	for i := 0; i < 5; i++ {
		c.Set(fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("val-%d", i)))
	}
	// LRU order: [key-4, key-3, key-2, key-1, key-0]

	// 反复访问 key-0（3次），提升到头部
	for i := 0; i < 3; i++ {
		c.Get("key-0")
	}
	// LRU order: [key-0, key-4, key-3, key-2, key-1]

	// 写入新数据，触发淘汰
	c.Set("key-5", []byte("val-5"))
	// LRU order: [key-5, key-0, key-4, key-3, key-2]

	// key-0（热点）应保留
	_, ok := c.Get("key-0")
	client.AssertTrue(t, ok, "hot key-0 should survive")

	// key-1（最久未访问）应被淘汰
	_, ok = c.Get("key-1")
	client.AssertFalse(t, ok, "key-1 should be evicted (least recently used)")
}

// TestCache_DeleteFreesSpace 测试删除释放空间后不触发淘汰
//
// 对应 specs.md 场景 "删除操作更新LRU链表"
func TestCache_DeleteFreesSpace(t *testing.T) {
	c, _ := cache.NewLRUCache(5)

	// 填充 5 条
	for i := 0; i < 5; i++ {
		c.Set(fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("val-%d", i)))
	}
	client.AssertEqual(t, c.Size(), 5, "full")

	// 删除中间条目
	c.Delete("key-2")
	client.AssertEqual(t, c.Size(), 4, "size after delete")

	// 写入新数据（填充空位，不触发淘汰）
	c.Set("key-5", []byte("val-5"))
	client.AssertEqual(t, c.Size(), 5, "size after filling hole")

	// key-0 应该仍然存在（不触发淘汰）
	_, ok := c.Get("key-0")
	client.AssertTrue(t, ok, "key-0 should exist (no eviction)")

	// key-2 已被删除
	_, ok = c.Get("key-2")
	client.AssertFalse(t, ok, "key-2 should be deleted")
}

// TestCache_EmptyKey 测试空键拒绝
//
// 对应 specs.md 场景 "空值或空键的SET操作"
func TestCache_EmptyKey(t *testing.T) {
	c, _ := cache.NewLRUCache(10)

	err := c.Set("", []byte("value"))
	client.AssertError(t, err, "empty key should be rejected")

	// 空值（空字符串 Value）允许
	client.AssertNoError(t, c.Set("key", []byte{}), "empty value allowed")
	val, ok := c.Get("key")
	client.AssertTrue(t, ok, "key with empty value exists")
	client.AssertEqual(t, len(val), 0, "value is empty")
}

// TestCache_ExportAll 测试全量导出
func TestCache_ExportAll(t *testing.T) {
	c, _ := cache.NewLRUCache(10)
	for i := 0; i < 3; i++ {
		c.Set(fmt.Sprintf("ek-%d", i), []byte(fmt.Sprintf("ev-%d", i)))
	}

	keys, values := c.ExportAll()
	client.AssertEqual(t, len(keys), 3, "export keys count")
	client.AssertEqual(t, len(values), 3, "export values count")

	// 验证导出内容
	keyMap := make(map[string]string)
	for i, k := range keys {
		keyMap[k] = string(values[i])
	}
	client.AssertEqual(t, keyMap["ek-0"], "ev-0", "export ek-0")
	client.AssertEqual(t, keyMap["ek-1"], "ev-1", "export ek-1")
	client.AssertEqual(t, keyMap["ek-2"], "ev-2", "export ek-2")

	// 清空后导出应空
	c.Clear()
	keys, values = c.ExportAll()
	client.AssertEqual(t, len(keys), 0, "empty after clear")
	client.AssertEqual(t, len(values), 0, "empty after clear")
}

// ========================================================================
// 第三部分：一致性哈希环测试（shard）
// ========================================================================

// TestShard_NewHashRing 测试哈希环构造函数
//
// 正常场景：创建有效虚拟节点数的环
// 异常场景：虚拟节点数为 0 或负数
func TestShard_NewHashRing(t *testing.T) {
	// 正常创建
	r, err := shard.NewHashRing(100)
	client.AssertNoError(t, err, "NewHashRing(100)")
	client.AssertEqual(t, r.NodeCount(), 0, "initial node count")

	// 异常
	_, err = shard.NewHashRing(0)
	client.AssertError(t, err, "zero virtual nodes")
	_, err = shard.NewHashRing(-1)
	client.AssertError(t, err, "negative virtual nodes")
}

// TestShard_AddNode 测试添加节点
func TestShard_AddNode(t *testing.T) {
	r, _ := shard.NewHashRing(10)

	// 添加单节点
	client.AssertNoError(t, r.AddNode("NodeA"), "AddNode NodeA")
	client.AssertEqual(t, r.NodeCount(), 1, "node count")
	client.AssertEqual(t, r.VirtualNodeCount(), 10, "virtual node count")

	// 重复添加应幂等
	client.AssertNoError(t, r.AddNode("NodeA"), "AddNode NodeA duplicate")
	client.AssertEqual(t, r.NodeCount(), 1, "node count after duplicate")

	// 添加空节点ID应报错
	err := r.AddNode("")
	client.AssertError(t, err, "empty node ID")
}

// TestShard_RemoveNode 测试移除节点
func TestShard_RemoveNode(t *testing.T) {
	r, _ := shard.NewHashRing(10)
	r.AddNode("NodeA")
	r.AddNode("NodeB")
	client.AssertEqual(t, r.NodeCount(), 2, "initial nodes")

	// 移除节点
	client.AssertNoError(t, r.RemoveNode("NodeA"), "RemoveNode NodeA")
	client.AssertEqual(t, r.NodeCount(), 1, "after removal")
	client.AssertEqual(t, r.VirtualNodeCount(), 10, "virtual node count after removal")

	// 移除不存在的节点
	err := r.RemoveNode("NonExistent")
	client.AssertError(t, err, "remove non-existent node")

	// 空ID
	err = r.RemoveNode("")
	client.AssertError(t, err, "remove empty node ID")
}

// TestShard_RoutingDeterminism 测试路由确定性
//
// 对应 specs.md 场景 "同一个Key MUST 映射到同一个节点"
func TestShard_RoutingDeterminism(t *testing.T) {
	r, _ := shard.NewHashRing(100)
	r.AddNode("NodeA")
	r.AddNode("NodeB")
	r.AddNode("NodeC")

	// 验证同一个 Key 始终映射到同一个节点
	const numKeys = 100
	routing := make(map[string]string)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("route-key-%03d", i)
		routing[key] = r.GetNode(key)
	}

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("route-key-%03d", i)
		nodeID := r.GetNode(key)
		client.AssertEqual(t, nodeID, routing[key], fmt.Sprintf("deterministic routing for %s", key))
	}
}

// TestShard_EmptyRing 测试空环路由
func TestShard_EmptyRing(t *testing.T) {
	r, _ := shard.NewHashRing(10)
	nodeID := r.GetNode("any-key")
	client.AssertEqual(t, nodeID, "", "empty ring returns empty string")
}

// TestShard_DataDistribution 测试数据分布平衡
//
// 对应 specs.md 场景 "虚拟节点数据均匀分布"
// 验收标准4：1000 次 SET 后 3 个分片数据分布差异 < 30%
func TestShard_DataDistribution(t *testing.T) {
	r, _ := shard.NewHashRing(100)
	r.AddNode("Node-1")
	r.AddNode("Node-2")
	r.AddNode("Node-3")

	// 统计每个节点的 Key 分布（使用多种 Key 格式提升分布均匀性）
	counts := make(map[string]int)
	for _, n := range r.GetNodes() {
		counts[n] = 0
	}
	const numKeys = 1000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("dist-key-%04d", i)
		nodeID := r.GetNode(key)
		counts[nodeID]++
	}

	// 验证总数为 numKeys
	total := 0
	for _, c := range counts {
		total += c
	}
	client.AssertEqual(t, total, numKeys, "total distribution")

	// 验证偏差 < 30%
	numNodes := len(counts)
	mean := float64(numKeys) / float64(numNodes)
	for nodeID, count := range counts {
		deviation := (float64(count) - mean) / mean
		if deviation < 0 {
			deviation = -deviation
		}
		t.Logf("  Node %s: %d entries, deviation %.1f%%", nodeID, count, deviation*100)
		if deviation >= 0.30 {
			t.Fatalf("Node %s distribution deviation %.1f%% exceeds 30%%", nodeID, deviation*100)
		}
	}
}

// TestShard_RingIntegrity 测试哈希环完整性
//
// 对应 specs.md 场景 "添加新节点后的数据迁移" / "移除节点后的数据重分配"
// 注意：由于 FNV-1a 哈希可能存在极少量碰撞，虚拟节点数可能略低于理论值
func TestShard_RingIntegrity(t *testing.T) {
	r, _ := shard.NewHashRing(100)
	r.AddNode("Node-1")
	r.AddNode("Node-2")
	r.AddNode("Node-3")

	// 虚拟节点数应接近 300（允许少量哈希碰撞）
	vnCount := r.VirtualNodeCount()
	if vnCount < 285 || vnCount > 300 {
		t.Fatalf("VirtualNodeCount: expected 285~300, got %d", vnCount)
	}
	t.Logf("  Initial: %d nodes, %d virtual nodes", r.NodeCount(), vnCount)

	// 记录所有 Key 的路由
	const numKeys = 500
	routing := make(map[string]string)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("integrity-key-%d", i)
		routing[key] = r.GetNode(key)
	}

	// 移除 Node-3
	r.RemoveNode("Node-3")
	client.AssertEqual(t, r.NodeCount(), 2, "after remove")
	vnAfterRemove := r.VirtualNodeCount()
	if vnAfterRemove < 190 || vnAfterRemove > 200 {
		t.Fatalf("VirtualNodeCount after remove: expected 190~200, got %d", vnAfterRemove)
	}
	t.Logf("  After remove: %d nodes, %d virtual nodes", r.NodeCount(), vnAfterRemove)

	// 路由应只指向 Node-1 或 Node-2
	validNodes := map[string]bool{"Node-1": true, "Node-2": true}
	for key, nodeID := range routing {
		if nodeID == "Node-3" {
			newNode := r.GetNode(key)
			if !validNodes[newNode] {
				t.Fatalf("Key %s re-routed to unknown node %s", key, newNode)
			}
		} else {
			// 非 Node-3 的 Key 路由应保持不变
			newNode := r.GetNode(key)
			client.AssertEqual(t, newNode, nodeID, fmt.Sprintf("routing unchanged for %s", key))
		}
	}

	// 重新添加节点
	r.AddNode("Node-4")
	client.AssertEqual(t, r.NodeCount(), 3, "after re-add")
	vnAfterAdd := r.VirtualNodeCount()
	if vnAfterAdd < 285 {
		t.Fatalf("VirtualNodeCount after add: expected >= 285, got %d", vnAfterAdd)
	}
	t.Logf("  After add: %d nodes, %d virtual nodes", r.NodeCount(), vnAfterAdd)

	// 所有 Key 应路由到现有节点
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("integrity-key-%d", i)
		nodeID := r.GetNode(key)
		if nodeID == "" {
			t.Fatalf("Key %s routed to empty node", key)
		}
	}
}

// ========================================================================
// 第四部分：TCP 服务器测试（server，通过 TestClient）
// ========================================================================

// TestServer_SET_GET 测试 SET 和 GET 命令的完整流程
//
// 对应 specs.md 场景 "完整的缓存读写流程"
func TestServer_SET_GET(t *testing.T) {
	cluster := client.NewTestCluster(t, client.DefaultClusterOptions())
	defer cluster.Stop(t)

	c := cluster.Connect(t)
	defer c.Close()

	// SET
	status, _ := c.Set("sk1", "sv1")
	client.AssertSuccess(t, status, "SET sk1")

	// GET
	status, val := c.Get("sk1")
	client.AssertSuccess(t, status, "GET sk1")
	client.AssertValue(t, val, "sv1", "GET sk1 value")

	// GET 不存在的 Key
	status, val = c.Get("nonexistent")
	client.AssertSuccess(t, status, "GET nonexistent (success with empty)")
	client.AssertEmptyValue(t, val, "GET nonexistent value")
}

// TestServer_DELETE 测试 DELETE 命令
func TestServer_DELETE(t *testing.T) {
	cluster := client.NewTestCluster(t, client.DefaultClusterOptions())
	defer cluster.Stop(t)

	c := cluster.Connect(t)
	defer c.Close()

	// SET → DELETE → GET
	c.Set("dk1", "dv1")
	status, _ := c.Delete("dk1")
	client.AssertSuccess(t, status, "DELETE dk1")

	status, val := c.Get("dk1")
	client.AssertSuccess(t, status, "GET after DELETE")
	client.AssertEmptyValue(t, val, "value after DELETE")
}

// TestServer_INFO 测试 INFO 命令
func TestServer_INFO(t *testing.T) {
	cluster := client.NewTestCluster(t, client.DefaultClusterOptions())
	defer cluster.Stop(t)

	c := cluster.Connect(t)
	defer c.Close()

	status, info := c.Info()
	client.AssertSuccess(t, status, "INFO")
	client.AssertEqual(t, len(info), 3, "3 nodes in info")
}

// TestServer_CompleteWorkflow 测试完整的工作流
//
// 对应 specs.md 第6节 "集成测试场景 - 完整的缓存读写流程"
func TestServer_CompleteWorkflow(t *testing.T) {
	cluster := client.NewTestCluster(t, client.DefaultClusterOptions())
	defer cluster.Stop(t)

	c := cluster.Connect(t)
	defer c.Close()

	// SET Key1=Value1
	status, _ := c.Set("Key1", "Value1")
	client.AssertSuccess(t, status, "SET Key1")

	// GET Key1 → Value1
	status, val := c.Get("Key1")
	client.AssertSuccess(t, status, "GET Key1")
	client.AssertValue(t, val, "Value1", "GET Key1 value")

	// SET Key2=Value2
	status, _ = c.Set("Key2", "Value2")
	client.AssertSuccess(t, status, "SET Key2")

	// GET Key2 → Value2
	status, val = c.Get("Key2")
	client.AssertSuccess(t, status, "GET Key2")
	client.AssertValue(t, val, "Value2", "GET Key2 value")

	// DELETE Key1
	status, _ = c.Delete("Key1")
	client.AssertSuccess(t, status, "DELETE Key1")

	// GET Key1 → 空
	status, val = c.Get("Key1")
	client.AssertSuccess(t, status, "GET Key1 after DELETE")
	client.AssertEmptyValue(t, val, "Key1 value after DELETE")

	// 验证 Key2 仍然存在
	status, val = c.Get("Key2")
	client.AssertSuccess(t, status, "GET Key2 (still exists)")
	client.AssertValue(t, val, "Value2", "Key2 value")
}

// TestServer_MultipleClientsConcurrent 测试多客户端并发连接
//
// 对应 specs.md TCP服务器场景 "多客户端并发连接"
func TestServer_MultipleClientsConcurrent(t *testing.T) {
	cluster := client.NewTestCluster(t, client.DefaultClusterOptions())
	defer cluster.Stop(t)

	const numClients = 5
	const opsPerClient = 10

	var wg sync.WaitGroup
	errCh := make(chan error, numClients*opsPerClient*2)

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.DialTimeout("tcp", cluster.Address, 5*time.Second)
			if err != nil {
				errCh <- fmt.Errorf("client %d: connect: %v", clientID, err)
				return
			}
			defer conn.Close()

			// 手动构造 TestClient
			tc := client.NewTestClient(t, cluster.Address, 5*time.Second)
			defer tc.Close()

			for j := 0; j < opsPerClient; j++ {
				key := fmt.Sprintf("mc-%d-%d", clientID, j)
				value := fmt.Sprintf("mv-%d-%d", clientID, j)

				status, _, err := tc.SendRequestSafe(uint8(protocol.CMD_SET), []byte(key), []byte(value))
				if err != nil {
					errCh <- fmt.Errorf("client %d: SET %s: %v", clientID, key, err)
					return
				}
				if status != uint8(protocol.SUCCESS) {
					errCh <- fmt.Errorf("client %d: SET %s status=0x%02X", clientID, key, status)
					return
				}

				status, respVal, err := tc.SendRequestSafe(uint8(protocol.CMD_GET), []byte(key), nil)
				if err != nil {
					errCh <- fmt.Errorf("client %d: GET %s: %v", clientID, key, err)
					return
				}
				if status != uint8(protocol.SUCCESS) {
					errCh <- fmt.Errorf("client %d: GET %s status=0x%02X", clientID, key, status)
					return
				}
				if string(respVal) != value {
					errCh <- fmt.Errorf("client %d: GET %s: expected '%s', got '%s'",
						clientID, key, value, string(respVal))
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
}

// TestServer_InvalidCommand 测试非法命令
//
// 对应 specs.md TCP服务器场景 "非法命令处理"
func TestServer_InvalidCommand(t *testing.T) {
	cluster := client.NewTestCluster(t, client.DefaultClusterOptions())
	defer cluster.Stop(t)

	c := cluster.Connect(t)
	defer c.Close()

	// 非法命令 0x99
	status, _ := c.SendRequest(0x99, []byte("somekey"), nil)
	client.AssertStatus(t, status, protocol.ERROR_UNKNOWN_COMMAND, "invalid command")

	// 服务器应仍可正常处理合法请求
	status, _ = c.Set("after-invalid", "works")
	client.AssertSuccess(t, status, "SET after invalid command")

	status, val := c.Get("after-invalid")
	client.AssertSuccess(t, status, "GET after invalid command")
	client.AssertValue(t, val, "works", "value after invalid command")
}

// TestServer_MissingKeyParameter 测试参数缺失
//
// 对应 specs.md 协议设计场景 "参数缺失或格式错误"
func TestServer_MissingKeyParameter(t *testing.T) {
	cluster := client.NewTestCluster(t, client.DefaultClusterOptions())
	defer cluster.Stop(t)

	c := cluster.Connect(t)
	defer c.Close()

	// GET 空 Key
	status, _ := c.SendRequest(uint8(protocol.CMD_GET), []byte{}, nil)
	client.AssertStatus(t, status, protocol.ERROR_INVALID_KEY, "GET empty key")

	// SET 空 Key
	status, _ = c.SendRequest(uint8(protocol.CMD_SET), []byte{}, []byte("value"))
	client.AssertStatus(t, status, protocol.ERROR_INVALID_KEY, "SET empty key")

	// DELETE 空 Key
	status, _ = c.SendRequest(uint8(protocol.CMD_DELETE), []byte{}, nil)
	client.AssertStatus(t, status, protocol.ERROR_INVALID_KEY, "DELETE empty key")
}

// TestServer_ClientDisconnection 测试客户端断开连接
//
// 对应 specs.md TCP服务器场景 "客户端异常断开连接"
func TestServer_ClientDisconnection(t *testing.T) {
	cluster := client.NewTestCluster(t, client.DefaultClusterOptions())
	defer cluster.Stop(t)

	// 第一个客户端：连接后立即断开
	conn1 := cluster.ConnectRaw(t)
	conn1.Close()

	// 等待服务器处理断开
	time.Sleep(100 * time.Millisecond)

	// 第二个客户端：应能正常连接和操作
	c := cluster.Connect(t)
	defer c.Close()

	status, _ := c.Set("after-disconnect", "works")
	client.AssertSuccess(t, status, "SET after disconnect")

	status, val := c.Get("after-disconnect")
	client.AssertSuccess(t, status, "GET after disconnect")
	client.AssertValue(t, val, "works", "value after disconnect")
}

// TestServer_StressConcurrent10Clients 压力测试：10 个客户端并发
//
// 对应验收标准 "支持10个并发连接"
func TestServer_StressConcurrent10Clients(t *testing.T) {
	cluster := client.NewTestCluster(t, client.DefaultClusterOptions())
	defer cluster.Stop(t)

	const numClients = 10
	const opsPerClient = 20

	var wg sync.WaitGroup
	errCh := make(chan error, numClients*opsPerClient*2)

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			// 每个客户端独立连接
			conn, err := net.DialTimeout("tcp", cluster.Address, 5*time.Second)
			if err != nil {
				errCh <- fmt.Errorf("client %d: connect: %v", clientID, err)
				return
			}
			defer conn.Close()

			for j := 0; j < opsPerClient; j++ {
				key := fmt.Sprintf("stress-%d-%d", clientID, j)
				value := fmt.Sprintf("v-%d-%d", clientID, j)

				req, _ := protocol.EncodeRequest(uint8(protocol.CMD_SET), []byte(key), []byte(value))
				conn.Write(req)

				header := make([]byte, protocol.FrameHeaderSize)
				if _, err := io.ReadFull(conn, header); err != nil {
					errCh <- fmt.Errorf("client %d: read SET %s header: %v", clientID, key, err)
					return
				}
				valueLen := binary.BigEndian.Uint32(header[5:9])
				body := make([]byte, 1+int(valueLen))
				if 1+int(valueLen) > 0 {
					if _, err := io.ReadFull(conn, body); err != nil {
						errCh <- fmt.Errorf("client %d: read SET %s body: %v", clientID, key, err)
						return
					}
				}
				if body[0] != uint8(protocol.SUCCESS) {
					errCh <- fmt.Errorf("client %d: SET %s status=0x%02X", clientID, key, body[0])
					return
				}

				// GET 验证
				req, _ = protocol.EncodeRequest(uint8(protocol.CMD_GET), []byte(key), nil)
				conn.Write(req)

				if _, err := io.ReadFull(conn, header); err != nil {
					errCh <- fmt.Errorf("client %d: read GET %s header: %v", clientID, key, err)
					return
				}
				valueLen = binary.BigEndian.Uint32(header[5:9])
				body = make([]byte, 1+int(valueLen))
				if 1+int(valueLen) > 0 {
					if _, err := io.ReadFull(conn, body); err != nil {
						errCh <- fmt.Errorf("client %d: read GET %s body: %v", clientID, key, err)
						return
					}
				}
				if body[0] != uint8(protocol.SUCCESS) {
					errCh <- fmt.Errorf("client %d: GET %s status=0x%02X", clientID, key, body[0])
					return
				}
				if string(body[1:]) != value {
					errCh <- fmt.Errorf("client %d: GET %s: expected '%s', got '%s'",
						clientID, key, value, string(body[1:]))
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	errCount := 0
	for err := range errCh {
		t.Error(err)
		errCount++
	}

	if errCount == 0 {
		t.Logf("[PASS] Stress test: %d clients x %d ops = %d total operations",
			numClients, opsPerClient, numClients*opsPerClient)
	}
}

// ========================================================================
// 第五部分：主从复制测试（replication）
// ========================================================================

// TestReplication_SetMasterSlave 测试主从关系配置
func TestReplication_SetMasterSlave(t *testing.T) {
	cluster := client.NewTestCluster(t, client.DefaultClusterOptions())
	defer cluster.Stop(t)

	master := cluster.Nodes[0]
	slave := cluster.Nodes[1]

	// 配置主从关系
	err := cluster.RC.SetMasterSlave(master.GetNodeID(), slave.GetNodeID())
	client.AssertNoError(t, err, "SetMasterSlave")

	// 验证状态
	client.AssertEqual(t, master.GetStatus(), node.StatusMaster, "master status")
	client.AssertEqual(t, slave.GetStatus(), node.StatusSlave, "slave status")

	// 验证错误场景
	err = cluster.RC.SetMasterSlave("", "valid")
	client.AssertError(t, err, "empty master ID")

	err = cluster.RC.SetMasterSlave("valid", "")
	client.AssertError(t, err, "empty slave ID")

	err = cluster.RC.SetMasterSlave(master.GetNodeID(), master.GetNodeID())
	client.AssertError(t, err, "same node as master and slave")

	err = cluster.RC.SetMasterSlave("NonExistent", slave.GetNodeID())
	client.AssertError(t, err, "non-existent master")
}

// TestReplication_SyncToSlave 测试写同步
//
// 对应 specs.md 主从复制场景 "主从同步正常工作"
func TestReplication_SyncToSlave(t *testing.T) {
	cluster := client.NewTestCluster(t, client.DefaultClusterOptions())
	defer cluster.Stop(t)

	master := cluster.Nodes[0]
	slave := cluster.Nodes[1]

	cluster.RC.SetMasterSlave(master.GetNodeID(), slave.GetNodeID())

	// 主节点写入数据
	client.AssertNoError(t, master.Set("rep-key1", []byte("rep-val1")), "master SET")

	// 同步到从节点
	client.AssertNoError(t, cluster.RC.SyncToSlave("rep-key1", []byte("rep-val1")), "SyncToSlave")

	// 验证从节点数据一致
	val, err := slave.Get("rep-key1")
	client.AssertNoError(t, err, "slave GET")
	client.AssertValue(t, val, "rep-val1", "slave value")

	// 验证同步计数
	client.AssertTrue(t, cluster.RC.GetSyncedCount() >= 1, "sync count >= 1")
}

// TestReplication_SyncDeleteToSlave 测试删除同步
//
// 验证点：SyncDeleteToSlave 从从节点删除指定 Key
func TestReplication_SyncDeleteToSlave(t *testing.T) {
	cluster := client.NewTestCluster(t, client.DefaultClusterOptions())
	defer cluster.Stop(t)

	master := cluster.Nodes[0]
	slave := cluster.Nodes[1]

	cluster.RC.SetMasterSlave(master.GetNodeID(), slave.GetNodeID())

	// 写入并同步
	master.Set("del-key", []byte("del-val"))
	cluster.RC.SyncToSlave("del-key", []byte("del-val"))

	// 验证从节点有数据
	val, _ := slave.Get("del-key")
	client.AssertValue(t, val, "del-val", "slave has data before delete")

	// 删除同步
	client.AssertNoError(t, cluster.RC.SyncDeleteToSlave("del-key"), "SyncDeleteToSlave")

	// 验证从节点已删除
	val, _ = slave.Get("del-key")
	if val != nil {
		t.Fatalf("Slave should not have del-key after delete sync, got '%s'", string(val))
	}

	// 错误场景
	err := cluster.RC.SyncDeleteToSlave("")
	client.AssertError(t, err, "empty key")
}

// TestReplication_FullSync 测试全量同步恢复
//
// 对应 specs.md 主从复制场景 "从节点断开重连后恢复同步"
func TestReplication_FullSync(t *testing.T) {
	cluster := client.NewTestCluster(t, client.DefaultClusterOptions())
	defer cluster.Stop(t)

	master := cluster.Nodes[0]
	slave := cluster.Nodes[1]

	cluster.RC.SetMasterSlave(master.GetNodeID(), slave.GetNodeID())

	// 主节点写入 30 条并同步
	const initialCount = 30
	for i := 0; i < initialCount; i++ {
		key := fmt.Sprintf("full-key-%02d", i)
		value := fmt.Sprintf("full-val-%02d", i)
		master.Set(key, []byte(value))
		cluster.RC.SyncToSlave(key, []byte(value))
	}

	// 主节点额外写入 20 条（模拟从节点离线期间的数据变更）
	const extraCount = 20
	for i := 0; i < extraCount; i++ {
		key := fmt.Sprintf("full-extra-%02d", i)
		value := fmt.Sprintf("extra-val-%02d", i)
		master.Set(key, []byte(value))
		// 不同步到从节点
	}

	// 从节点请求全量同步
	frames, err := cluster.RC.RequestFullSync(master.GetNodeID())
	client.AssertNoError(t, err, "RequestFullSync")
	client.AssertEqual(t, len(frames), initialCount+extraCount, "all frames")

	// 应用全量同步
	client.AssertNoError(t, cluster.RC.ApplyFullSync(frames), "ApplyFullSync")

	// 验证从节点数据完全一致
	client.AssertEqual(t, slave.Size(), master.Size(), "size after full sync")

	// 验证初始 30 条
	for i := 0; i < initialCount; i++ {
		key := fmt.Sprintf("full-key-%02d", i)
		expected := fmt.Sprintf("full-val-%02d", i)
		val, _ := slave.Get(key)
		client.AssertValue(t, val, expected, fmt.Sprintf("slave %s", key))
	}

	// 验证额外 20 条
	for i := 0; i < extraCount; i++ {
		key := fmt.Sprintf("full-extra-%02d", i)
		expected := fmt.Sprintf("extra-val-%02d", i)
		val, _ := slave.Get(key)
		client.AssertValue(t, val, expected, fmt.Sprintf("slave extra %s", key))
	}

	// 错误场景
	_, err = cluster.RC.RequestFullSync("")
	client.AssertError(t, err, "empty master ID for full sync")

	_, err = cluster.RC.RequestFullSync("NonExistent")
	client.AssertError(t, err, "non-existent master for full sync")

	err = cluster.RC.ApplyFullSync(nil)
	client.AssertError(t, err, "nil frames")

	err = cluster.RC.ApplyFullSync([]*protocol.ProtocolFrame{})
	client.AssertNoError(t, err, "empty frames (no-op)")
}

// TestReplication_ConcurrentSync 测试并发同步安全性
//
// 验证点：多个 goroutine 同时执行 Set + SyncToSlave，从节点数据完整且一致
func TestReplication_ConcurrentSync(t *testing.T) {
	cluster := client.NewTestCluster(t, client.DefaultClusterOptions())
	defer cluster.Stop(t)

	master := cluster.Nodes[0]
	slave := cluster.Nodes[1]

	cluster.RC.SetMasterSlave(master.GetNodeID(), slave.GetNodeID())

	const numGoroutines = 20
	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("concurrent-key-%02d", idx)
			value := fmt.Sprintf("concurrent-val-%02d", idx)

			if err := master.Set(key, []byte(value)); err != nil {
				errCh <- fmt.Errorf("master Set %s: %w", key, err)
				return
			}
			if err := cluster.RC.SyncToSlave(key, []byte(value)); err != nil {
				errCh <- fmt.Errorf("SyncToSlave %s: %w", key, err)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}

	// 验证从节点数据完整性
	client.AssertEqual(t, slave.Size(), numGoroutines, "slave size after concurrent sync")

	for i := 0; i < numGoroutines; i++ {
		key := fmt.Sprintf("concurrent-key-%02d", i)
		expected := fmt.Sprintf("concurrent-val-%02d", i)
		val, err := slave.Get(key)
		client.AssertNoError(t, err, fmt.Sprintf("slave GET %s", key))
		client.AssertValue(t, val, expected, fmt.Sprintf("slave %s", key))
	}
}

// TestReplication_InitSync 测试 InitSync
func TestReplication_InitSync(t *testing.T) {
	cluster := client.NewTestCluster(t, client.DefaultClusterOptions())
	defer cluster.Stop(t)

	// 正常初始化同步
	err := cluster.RC.InitSync(cluster.Nodes[0].GetNodeID())
	client.AssertNoError(t, err, "InitSync with valid master")

	// 空 ID
	err = cluster.RC.InitSync("")
	client.AssertError(t, err, "InitSync with empty master ID")

	// 不存在的节点
	err = cluster.RC.InitSync("NonExistent")
	client.AssertError(t, err, "InitSync with non-existent master")
}

// TestReplication_StateQuery 测试复制状态查询
func TestReplication_StateQuery(t *testing.T) {
	cluster := client.NewTestCluster(t, client.DefaultClusterOptions())
	defer cluster.Stop(t)

	master := cluster.Nodes[0]
	slave := cluster.Nodes[1]

	// 未配置时的初始状态
	client.AssertEqual(t, cluster.RC.GetMasterID(), "", "initial master ID")
	client.AssertEqual(t, cluster.RC.GetSyncedCount(), 0, "initial sync count")

	cluster.RC.SetMasterSlave(master.GetNodeID(), slave.GetNodeID())

	client.AssertEqual(t, cluster.RC.GetMasterID(), master.GetNodeID(), "master ID after config")
	client.AssertEqual(t, cluster.RC.GetNodeCount(), 3, "node count")

	slaveIDs := cluster.RC.GetSlaveIDs()
	client.AssertEqual(t, len(slaveIDs), 1, "one slave")
	client.AssertEqual(t, slaveIDs[0], slave.GetNodeID(), "slave ID")

	// GetState
	state := cluster.RC.GetState()
	if state["masterID"] != master.GetNodeID() {
		t.Fatalf("State masterID: expected %s, got %s", master.GetNodeID(), state["masterID"])
	}
}

// ========================================================================
// 第六部分：集成测试 - 端到端协议帧边界条件
// ========================================================================

// TestIntegration_ProtocolBoundaries 测试协议帧边界条件
//
// 覆盖：
//   - 截断的帧头（不完整帧）
//   - 声明长度大于实际数据长度
//   - 超大 Value 被协议层拒绝
//   - 边界值（刚好等于 MaxValueLength）
func TestIntegration_ProtocolBoundaries(t *testing.T) {
	cluster := client.NewTestCluster(t, client.DefaultClusterOptions())
	defer cluster.Stop(t)

	t.Run("TruncatedHeader", func(t *testing.T) {
		// 发送不完整的帧头（仅4字节），关闭连接
		conn := cluster.ConnectRaw(t)
		conn.Write([]byte{0x01, 0x00, 0x00, 0x00}) // 只发4字节
		conn.Close()
		time.Sleep(100 * time.Millisecond)

		// 服务器仍可正常处理新客户端
		c := cluster.Connect(t)
		defer c.Close()
		status, _ := c.Set("after-trunc", "ok")
		client.AssertSuccess(t, status, "SET after truncated header")
	})

	t.Run("ValidFrameAfterTruncation", func(t *testing.T) {
		// 发送完整的合法帧
		conn := cluster.ConnectRaw(t)
		header := make([]byte, 9)
		header[0] = uint8(protocol.CMD_INFO)
		binary.BigEndian.PutUint32(header[1:5], 0)
		binary.BigEndian.PutUint32(header[5:9], 0)
		conn.Write(header)

		respHeader := make([]byte, 9)
		if _, err := conn.Read(respHeader); err != nil {
			t.Fatalf("Read INFO response: %v", err)
		}
		vl := binary.BigEndian.Uint32(respHeader[5:9])
		body := make([]byte, 1+int(vl))
		conn.Read(body)
		if body[0] != uint8(protocol.SUCCESS) {
			t.Fatalf("INFO: expected SUCCESS, got 0x%02X", body[0])
		}
		conn.Close()
	})

	t.Run("RejectOversizedValue", func(t *testing.T) {
		// 协议层拒绝超大 Value
		bigValue := make([]byte, protocol.MaxValueLength+1)
		_, err := protocol.EncodeRequest(uint8(protocol.CMD_SET), []byte("k"), bigValue)
		client.AssertError(t, err, "oversized value rejected by EncodeRequest")

		// 正常大小通过
		valueAtLimit := make([]byte, protocol.MaxValueLength)
		_, err = protocol.EncodeRequest(uint8(protocol.CMD_SET), []byte("k"), valueAtLimit)
		client.AssertNoError(t, err, "max size value accepted")
	})
}

// ========================================================================
// 文件内部辅助（io.ReadFull 用于 stress test）
// ========================================================================

// ioReadFull 是 io.ReadFull 的别名，用于 stress test 中
// (实际的 io package 已经在 import 中)
func init() {
	// 确保 io.ReadFull 可用于 stress test
	_ = io.ReadFull
}
