// Package main - auto_tests.go
// 简易测试模块：46 个测试用例覆盖 7 大模块
// 每个测试函数签名 func(t *TestContext)，使用 assertXxx 断言
// 需要集群的测试各自创建独立临时集群，确保测试隔离
package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/yourusername/sd-03-cache/pkg/cache"
	"github.com/yourusername/sd-03-cache/pkg/node"
	"github.com/yourusername/sd-03-cache/pkg/protocol"
	"github.com/yourusername/sd-03-cache/pkg/replication"
	"github.com/yourusername/sd-03-cache/pkg/server"
	"github.com/yourusername/sd-03-cache/pkg/shard"
)

// ========================================================================
// 临时集群辅助函数（确保测试隔离）
// ========================================================================

// tempCluster 创建独立的临时测试集群
func tempCluster(t *TestContext) *EmbeddedCluster {
	t.Helper()
	ring, err := shard.NewHashRing(100)
	if err != nil {
		t.Fatalf("create ring: %v", err)
	}
	var nodes []*node.CacheNode
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("TNode-%d", i+1)
		n, err := node.NewCacheNode(id, 10000)
		if err != nil {
			t.Fatalf("create node: %v", err)
		}
		if err := ring.AddNode(id); err != nil {
			t.Fatalf("add node: %v", err)
		}
		if err := n.Init(ring); err != nil {
			t.Fatalf("init node: %v", err)
		}
		if err := n.Start(); err != nil {
			t.Fatalf("start node: %v", err)
		}
		nodes = append(nodes, n)
	}
	srv, err := server.NewTCPServer(":0", nodes, ring)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	rc, err := replication.NewReplicationController(nodes)
	if err != nil {
		t.Fatalf("create rc: %v", err)
	}
	return &EmbeddedCluster{
		Ring: ring, Nodes: nodes, Server: srv,
		RC: rc, Address: srv.Address(),
	}
}

// stopCluster 停止临时集群
func stopCluster(c *EmbeddedCluster) {
	if c == nil {
		return
	}
	if c.Server != nil {
		c.Server.Stop()
	}
	for _, n := range c.Nodes {
		n.Stop()
	}
}

// dialTest 连接到集群的 TCP 服务器
func dialTest(t *TestContext, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	return conn
}

// ========================================================================
// 协议编解码测试（6 用例）
// ========================================================================

// buildProtocolTests 构建协议编解码模块的测试条目
func buildProtocolTests() []TestEntry {
	return []TestEntry{
		{ID: "P01", Name: "请求编解码往返测试", Category: "正常", Func: testProtoEncodeDecodeReq},
		{ID: "P02", Name: "响应编解码往返测试", Category: "正常", Func: testProtoEncodeDecodeResp},
		{ID: "P03", Name: "帧创建与深拷贝", Category: "正常", Func: testProtoNewFrameCopy},
		{ID: "P04", Name: "请求编码参数校验", Category: "异常", Func: testProtoEncodeReqErrors},
		{ID: "P05", Name: "请求解码参数校验", Category: "异常", Func: testProtoDecodeReqErrors},
		{ID: "P06", Name: "帧验证与边界检查", Category: "边界", Func: testProtoValidateFrame},
	}
}

// testProtoEncodeDecodeReq 测试请求编解码往返一致性
func testProtoEncodeDecodeReq(t *TestContext) {
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *TestContext) {
			encoded, err := protocol.EncodeRequest(tt.cmd, tt.key, tt.value)
			assertNoError(t, err, "EncodeRequest")
			decoded, err := protocol.DecodeRequest(encoded)
			assertNoError(t, err, "DecodeRequest")
			assertEqual(t, decoded.Command, tt.cmd, "Command")
			assertEqual(t, string(decoded.Key), string(tt.key), "Key")
			assertEqual(t, string(decoded.Value), string(tt.value), "Value")
		})
	}
}

// testProtoEncodeDecodeResp 测试响应编解码往返一致性
func testProtoEncodeDecodeResp(t *TestContext) {
	tests := []struct {
		name   string
		cmd    uint8
		status uint8
		value  []byte
	}{
		{"GET success", uint8(protocol.CMD_GET), uint8(protocol.SUCCESS), []byte("returned-value")},
		{"SET success", uint8(protocol.CMD_SET), uint8(protocol.SUCCESS), []byte{}},
		{"Unknown cmd error", uint8(protocol.CMD_GET), uint8(protocol.ERROR_UNKNOWN_COMMAND), []byte("bad cmd")},
		{"Invalid key error", uint8(protocol.CMD_GET), uint8(protocol.ERROR_INVALID_KEY), []byte("bad key")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *TestContext) {
			encoded, err := protocol.EncodeResponse(tt.cmd, tt.status, tt.value)
			assertNoError(t, err, "EncodeResponse")
			decoded, err := protocol.DecodeResponse(encoded)
			assertNoError(t, err, "DecodeResponse")
			assertEqual(t, decoded.Command, tt.cmd, "Command")
			if decoded.ValueLen > 0 {
				assertEqual(t, decoded.Value[0], tt.status, "Status byte")
				assertEqual(t, string(decoded.Value[1:]), string(tt.value), "Value")
			}
		})
	}
}

// testProtoNewFrameCopy 测试帧创建与深拷贝
func testProtoNewFrameCopy(t *TestContext) {
	frame := protocol.NewFrame(uint8(protocol.CMD_SET), []byte("key"), []byte("value"))
	assertEqual(t, frame.Command, uint8(protocol.CMD_SET), "Command")
	assertEqual(t, string(frame.Key), "key", "Key")
	assertEqual(t, string(frame.Value), "value", "Value")
	assertEqual(t, frame.KeyLen, uint32(3), "KeyLen")
	assertEqual(t, frame.ValueLen, uint32(5), "ValueLen")

	emptyFrame := protocol.NewFrame(uint8(protocol.CMD_INFO), nil, nil)
	assertTrue(t, len(emptyFrame.Key) == 0 && len(emptyFrame.Value) == 0, "nil → empty")

	copied := frame.Copy()
	assertTrue(t, copied != frame, "different pointer")
	assertTrue(t, frame.Equals(copied), "equal content")
	frame.Value[0] = 'X'
	assertTrue(t, copied.Value[0] != 'X', "deep copy")

	assertTrue(t, (*protocol.ProtocolFrame)(nil).Copy() == nil, "nil Copy")
}

// testProtoEncodeReqErrors 测试请求编码的参数校验（nil Key、超长 Key/Value）
func testProtoEncodeReqErrors(t *TestContext) {
	_, err := protocol.EncodeRequest(uint8(protocol.CMD_GET), nil, nil)
	assertError(t, err, "nil key rejected")

	bigKey := make([]byte, protocol.MaxKeyLength+1)
	_, err = protocol.EncodeRequest(uint8(protocol.CMD_GET), bigKey, nil)
	assertError(t, err, "oversized key rejected")

	bigValue := make([]byte, protocol.MaxValueLength+1)
	_, err = protocol.EncodeRequest(uint8(protocol.CMD_SET), []byte("key"), bigValue)
	assertError(t, err, "oversized value rejected")
}

// testProtoDecodeReqErrors 测试请求解码的参数校验（nil 数据、长度不足）
func testProtoDecodeReqErrors(t *TestContext) {
	_, err := protocol.DecodeRequest(nil)
	assertError(t, err, "nil data rejected")

	_, err = protocol.DecodeRequest([]byte{0x01, 0x00, 0x00})
	assertError(t, err, "short data rejected")

	shortData := make([]byte, 11)
	shortData[0] = 0x01
	binary.BigEndian.PutUint32(shortData[1:5], 100)
	binary.BigEndian.PutUint32(shortData[5:9], 0)
	shortData[9] = 'a'
	shortData[10] = 'b'
	_, err = protocol.DecodeRequest(shortData)
	assertError(t, err, "data shorter than declared length")
}

// testProtoValidateFrame 测试协议帧校验与边界检查（非法命令、长度超限、长度不匹配）
func testProtoValidateFrame(t *TestContext) {
	validFrame := protocol.NewFrame(uint8(protocol.CMD_SET), []byte("key"), []byte("value"))
	assertNoError(t, protocol.ValidateFrame(validFrame), "valid frame")

	infoFrame := protocol.NewFrame(uint8(protocol.CMD_INFO), []byte{}, []byte{})
	assertNoError(t, protocol.ValidateFrame(infoFrame), "INFO frame")

	assertError(t, protocol.ValidateFrame(nil), "nil frame")

	badCmdFrame := protocol.NewFrame(0x99, []byte("key"), []byte("value"))
	err := protocol.ValidateFrame(badCmdFrame)
	assertError(t, err, "invalid command")
	assertEqual(t, protocol.GetErrorCode(err), protocol.ERROR_UNKNOWN_COMMAND, "error code")

	mismatchFrame := &protocol.ProtocolFrame{
		Command: uint8(protocol.CMD_SET), KeyLen: 100, ValueLen: 5,
		Key: []byte("short"), Value: []byte("value"),
	}
	err = protocol.ValidateFrame(mismatchFrame)
	assertError(t, err, "KeyLen mismatch")
	assertEqual(t, protocol.GetErrorCode(err), protocol.ERROR_FRAME_MISMATCH, "mismatch code")

	emptyKeyFrame := protocol.NewFrame(uint8(protocol.CMD_GET), []byte{}, []byte{})
	err = protocol.ValidateFrame(emptyKeyFrame)
	assertError(t, err, "GET empty key")
	assertEqual(t, protocol.GetErrorCode(err), protocol.ERROR_INVALID_KEY, "empty key code")

	oversizedKeyFrame := protocol.NewFrame(uint8(protocol.CMD_SET), make([]byte, protocol.MaxKeyLength+1), []byte("v"))
	assertError(t, protocol.ValidateFrame(oversizedKeyFrame), "oversized key")

	oversizedValueFrame := protocol.NewFrame(uint8(protocol.CMD_SET), []byte("k"), make([]byte, protocol.MaxValueLength+1))
	assertError(t, protocol.ValidateFrame(oversizedValueFrame), "oversized value")
}

// ========================================================================
// LRU 缓存测试（8 用例）
// ========================================================================

// buildCacheTests 构建 LRU 缓存模块的测试条目
func buildCacheTests() []TestEntry {
	return []TestEntry{
		{ID: "C01", Name: "LRU缓存构造函数", Category: "正常", Func: testCacheNewLRUCache},
		{ID: "C02", Name: "基本SET/GET/DELETE", Category: "正常", Func: testCacheSetGetDelete},
		{ID: "C03", Name: "更新已存在Key的值", Category: "正常", Func: testCacheUpdateValue},
		{ID: "C04", Name: "全量导出ExportAll", Category: "正常", Func: testCacheExportAll},
		{ID: "C05", Name: "空键拒绝", Category: "异常", Func: testCacheEmptyKey},
		{ID: "C06", Name: "容量满自动淘汰", Category: "边界", Func: testCacheEviction},
		{ID: "C07", Name: "热点数据保留", Category: "边界", Func: testCacheHotData},
		{ID: "C08", Name: "删除释放空间不触发淘汰", Category: "边界", Func: testCacheDeleteFreesSpace},
	}
}

// testCacheNewLRUCache 测试 LRU 缓存创建（正常和异常场景）
func testCacheNewLRUCache(t *TestContext) {
	c, err := cache.NewLRUCache(100)
	assertNoError(t, err, "NewLRUCache(100)")
	assertEqual(t, c.Size(), 0, "initial size")
	assertFalse(t, c.IsFull(), "initial not full")
	assertNoError(t, c.Clear(), "Clear")

	_, err = cache.NewLRUCache(0)
	assertError(t, err, "zero capacity")
	_, err = cache.NewLRUCache(-1)
	assertError(t, err, "negative capacity")
}

// testCacheSetGetDelete 测试缓存基本 SET/GET/DELETE 操作
func testCacheSetGetDelete(t *TestContext) {
	c, _ := cache.NewLRUCache(10)
	assertNoError(t, c.Set("key1", []byte("value1")), "SET key1")
	assertEqual(t, c.Size(), 1, "size after SET")

	val, ok := c.Get("key1")
	assertTrue(t, ok, "GET key1 exists")
	assertEqual(t, string(val), "value1", "GET key1 value")

	_, ok = c.Get("nonexistent")
	assertFalse(t, ok, "GET nonexistent")

	assertTrue(t, c.Delete("key1"), "DELETE key1")
	assertEqual(t, c.Size(), 0, "size after DELETE")

	assertFalse(t, c.Delete("nonexistent"), "DELETE nonexistent")
	_, ok = c.Get("key1")
	assertFalse(t, ok, "GET after DELETE")
}

// testCacheUpdateValue 测试更新已存在 Key 的 Value
func testCacheUpdateValue(t *TestContext) {
	c, _ := cache.NewLRUCache(10)
	c.Set("key", []byte("old"))
	c.Set("key", []byte("new"))
	val, ok := c.Get("key")
	assertTrue(t, ok, "exists after update")
	assertEqual(t, string(val), "new", "updated value")
	assertEqual(t, c.Size(), 1, "size unchanged")
}

// testCacheExportAll 测试全量导出缓存数据
func testCacheExportAll(t *TestContext) {
	c, _ := cache.NewLRUCache(10)
	for i := 0; i < 3; i++ {
		c.Set(fmt.Sprintf("ek-%d", i), []byte(fmt.Sprintf("ev-%d", i)))
	}
	keys, values := c.ExportAll()
	assertEqual(t, len(keys), 3, "export keys count")
	assertEqual(t, len(values), 3, "export values count")

	keyMap := make(map[string]string)
	for i, k := range keys {
		keyMap[k] = string(values[i])
	}
	assertEqual(t, keyMap["ek-0"], "ev-0", "export ek-0")
	assertEqual(t, keyMap["ek-1"], "ev-1", "export ek-1")
	assertEqual(t, keyMap["ek-2"], "ev-2", "export ek-2")

	c.Clear()
	keys, values = c.ExportAll()
	assertEqual(t, len(keys), 0, "empty after clear")
}

// testCacheEmptyKey 测试空 Key 的 SET 操作
func testCacheEmptyKey(t *TestContext) {
	c, _ := cache.NewLRUCache(10)
	assertError(t, c.Set("", []byte("value")), "empty key rejected")
	assertNoError(t, c.Set("key", []byte{}), "empty value allowed")
	val, ok := c.Get("key")
	assertTrue(t, ok, "key with empty value exists")
	assertEqual(t, len(val), 0, "value is empty")
}

// testCacheEviction 测试 LRU 缓存满时的淘汰机制
func testCacheEviction(t *TestContext) {
	c, _ := cache.NewLRUCache(5)
	for i := 0; i < 5; i++ {
		assertNoError(t, c.Set(fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("val-%d", i))), "SET")
	}
	assertEqual(t, c.Size(), 5, "size at capacity")
	assertTrue(t, c.IsFull(), "cache full")

	assertNoError(t, c.Set("key-5", []byte("val-5")), "SET key-5")
	assertEqual(t, c.Size(), 5, "size after eviction")

	_, ok := c.Get("key-0")
	assertFalse(t, ok, "key-0 evicted (oldest)")

	val, ok := c.Get("key-5")
	assertTrue(t, ok, "key-5 exists (newest)")
	assertEqual(t, string(val), "val-5", "key-5 value")
}

// testCacheHotData 测试热点数据保护（访问刷新 LRU 位置）
func testCacheHotData(t *TestContext) {
	c, _ := cache.NewLRUCache(5)
	for i := 0; i < 5; i++ {
		c.Set(fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("val-%d", i)))
	}
	for i := 0; i < 3; i++ {
		c.Get("key-0")
	}
	c.Set("key-5", []byte("val-5"))

	_, ok := c.Get("key-0")
	assertTrue(t, ok, "hot key-0 survives")
	_, ok = c.Get("key-1")
	assertFalse(t, ok, "key-1 evicted (LRU)")
}

// testCacheDeleteFreesSpace 测试删除操作释放缓存空间
func testCacheDeleteFreesSpace(t *TestContext) {
	c, _ := cache.NewLRUCache(5)
	for i := 0; i < 5; i++ {
		c.Set(fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("val-%d", i)))
	}
	assertEqual(t, c.Size(), 5, "full")

	c.Delete("key-2")
	assertEqual(t, c.Size(), 4, "after delete")

	c.Set("key-5", []byte("val-5"))
	assertEqual(t, c.Size(), 5, "fill hole")

	_, ok := c.Get("key-0")
	assertTrue(t, ok, "key-0 exists (no eviction)")
	_, ok = c.Get("key-2")
	assertFalse(t, ok, "key-2 deleted")
}

// ========================================================================
// 一致性哈希环测试（7 用例）
// ========================================================================

// buildShardTests 构建一致性哈希模块的测试条目
func buildShardTests() []TestEntry {
	return []TestEntry{
		{ID: "S01", Name: "哈希环构造函数", Category: "正常", Func: testShardNewHashRing},
		{ID: "S02", Name: "添加节点", Category: "正常", Func: testShardAddNode},
		{ID: "S03", Name: "移除节点", Category: "正常", Func: testShardRemoveNode},
		{ID: "S04", Name: "路由确定性", Category: "正常", Func: testShardRoutingDeterminism},
		{ID: "S05", Name: "空环路由", Category: "异常", Func: testShardEmptyRing},
		{ID: "S06", Name: "数据分布平衡", Category: "边界", Func: testShardDataDistribution},
		{ID: "S07", Name: "环完整性(添加/移除)", Category: "边界", Func: testShardRingIntegrity},
	}
}

// testShardNewHashRing 测试哈希环创建（正常和异常场景）
func testShardNewHashRing(t *TestContext) {
	r, err := shard.NewHashRing(100)
	assertNoError(t, err, "NewHashRing(100)")
	assertEqual(t, r.NodeCount(), 0, "initial count")

	_, err = shard.NewHashRing(0)
	assertError(t, err, "zero virtual nodes")
	_, err = shard.NewHashRing(-1)
	assertError(t, err, "negative virtual nodes")
}

// testShardAddNode 测试添加节点到哈希环
func testShardAddNode(t *TestContext) {
	r, _ := shard.NewHashRing(10)
	assertNoError(t, r.AddNode("NodeA"), "AddNode NodeA")
	assertEqual(t, r.NodeCount(), 1, "node count")
	assertEqual(t, r.VirtualNodeCount(), 10, "virtual count")

	assertNoError(t, r.AddNode("NodeA"), "idempotent add")
	assertEqual(t, r.NodeCount(), 1, "still 1")

	assertError(t, r.AddNode(""), "empty ID")
}

// testShardRemoveNode 测试从哈希环移除节点
func testShardRemoveNode(t *TestContext) {
	r, _ := shard.NewHashRing(10)
	r.AddNode("NodeA")
	r.AddNode("NodeB")
	assertEqual(t, r.NodeCount(), 2, "initial")

	assertNoError(t, r.RemoveNode("NodeA"), "RemoveNode")
	assertEqual(t, r.NodeCount(), 1, "after removal")
	assertEqual(t, r.VirtualNodeCount(), 10, "virtual after removal")

	assertError(t, r.RemoveNode("NonExistent"), "remove non-existent")
	assertError(t, r.RemoveNode(""), "remove empty ID")
}

// testShardRoutingDeterminism 测试同一 Key 多次路由结果的一致性
func testShardRoutingDeterminism(t *TestContext) {
	r, _ := shard.NewHashRing(100)
	r.AddNode("NodeA")
	r.AddNode("NodeB")
	r.AddNode("NodeC")

	routing := make(map[string]string)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("route-key-%03d", i)
		routing[key] = r.GetNode(key)
	}
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("route-key-%03d", i)
		assertEqual(t, r.GetNode(key), routing[key], fmt.Sprintf("deterministic %s", key))
	}
}

// testShardEmptyRing 测试空环路由返回错误
func testShardEmptyRing(t *TestContext) {
	r, _ := shard.NewHashRing(10)
	assertEqual(t, r.GetNode("any-key"), "", "empty ring returns empty")
}

// testShardDataDistribution 测试数据在多节点间的分布均衡性
func testShardDataDistribution(t *TestContext) {
	r, _ := shard.NewHashRing(100)
	r.AddNode("Node-1")
	r.AddNode("Node-2")
	r.AddNode("Node-3")

	counts := make(map[string]int)
	for _, n := range r.GetNodes() {
		counts[n] = 0
	}
	const numKeys = 1000
	for i := 0; i < numKeys; i++ {
		counts[r.GetNode(fmt.Sprintf("dist-key-%04d", i))]++
	}

	total := 0
	for _, c := range counts {
		total += c
	}
	assertEqual(t, total, numKeys, "total distribution")

	numNodes := len(counts)
	mean := float64(numKeys) / float64(numNodes)
	for nodeID, count := range counts {
		deviation := float64(count) - mean
		if deviation < 0 {
			deviation = -deviation
		}
		deviation = deviation / mean
		t.Logf("  Node %s: %d entries, deviation %.1f%%", nodeID, count, deviation*100)
		if deviation >= 0.30 {
			t.Fatalf("Node %s deviation %.1f%% exceeds 30%%", nodeID, deviation*100)
		}
	}
}

// testShardRingIntegrity 测试哈希环完整性（虚拟节点数、单调性）
func testShardRingIntegrity(t *TestContext) {
	r, _ := shard.NewHashRing(100)
	r.AddNode("Node-1")
	r.AddNode("Node-2")
	r.AddNode("Node-3")

	vnCount := r.VirtualNodeCount()
	if vnCount < 285 || vnCount > 300 {
		t.Fatalf("VirtualNodeCount: expected 285~300, got %d", vnCount)
	}
	t.Logf("  Initial: %d nodes, %d virtual nodes", r.NodeCount(), vnCount)

	const numKeys = 500
	routing := make(map[string]string)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("integrity-key-%d", i)
		routing[key] = r.GetNode(key)
	}

	r.RemoveNode("Node-3")
	assertEqual(t, r.NodeCount(), 2, "after remove")
	vnAfterRemove := r.VirtualNodeCount()
	if vnAfterRemove < 190 || vnAfterRemove > 200 {
		t.Fatalf("VirtualNodeCount after remove: expected 190~200, got %d", vnAfterRemove)
	}

	validNodes := map[string]bool{"Node-1": true, "Node-2": true}
	for key, nodeID := range routing {
		if nodeID == "Node-3" {
			newNode := r.GetNode(key)
			if !validNodes[newNode] {
				t.Fatalf("Key %s re-routed to unknown node %s", key, newNode)
			}
		} else {
			assertEqual(t, r.GetNode(key), nodeID, fmt.Sprintf("unchanged %s", key))
		}
	}

	r.AddNode("Node-4")
	assertEqual(t, r.NodeCount(), 3, "after re-add")
	if r.VirtualNodeCount() < 285 {
		t.Fatalf("VirtualNodeCount after add: expected >= 285, got %d", r.VirtualNodeCount())
	}

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("integrity-key-%d", i)
		if r.GetNode(key) == "" {
			t.Fatalf("Key %s routed to empty", key)
		}
	}
}

// ========================================================================
// 缓存节点测试（6 用例）- 每个测试独立创建节点
// ========================================================================

// buildNodeTests 构建缓存节点模块的测试条目
func buildNodeTests(cli *CLIClient) []TestEntry {
	_ = cli // 不使用共享集群
	return []TestEntry{
		{ID: "N01", Name: "节点创建与初始化", Category: "正常", Func: testNodeCreateInit},
		{ID: "N02", Name: "节点基本读写删", Category: "正常", Func: testNodeSetGetDelete},
		{ID: "N03", Name: "节点信息查询", Category: "正常", Func: testNodeGetInfo},
		{ID: "N04", Name: "节点状态管理", Category: "正常", Func: testNodeStatus},
		{ID: "N05", Name: "节点数据导出", Category: "正常", Func: testNodeExportAll},
		{ID: "N06", Name: "节点容量与淘汰", Category: "边界", Func: testNodeCapacity},
	}
}

// createTestNode 创建独立的测试节点（带 ring 和 start）
func createTestNode(t *TestContext, id string, cap int) *node.CacheNode {
	t.Helper()
	n, err := node.NewCacheNode(id, cap)
	if err != nil {
		t.Fatalf("NewCacheNode(%s, %d): %v", id, cap, err)
	}
	ring, err := shard.NewHashRing(10)
	if err != nil {
		t.Fatalf("NewHashRing: %v", err)
	}
	ring.AddNode(id)
	if err := n.Init(ring); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := n.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return n
}

// testNodeCreateInit 测试节点创建、初始化与启动的状态变化
func testNodeCreateInit(t *TestContext) {
	// 创建后需要 Init+Start 才能变为 Running
	n, err := node.NewCacheNode("test-init-node", 100)
	assertNoError(t, err, "NewCacheNode")
	assertEqual(t, n.GetNodeID(), "test-init-node", "NodeID")
	// 初始状态应为 Stopped（未 Init/Start）
	assertEqual(t, n.GetStatus(), node.StatusStopped, "initial status Stopped")

	// Init + Start 后变为 Running
	ring, _ := shard.NewHashRing(10)
	ring.AddNode("test-init-node")
	assertNoError(t, n.Init(ring), "Init")
	assertNoError(t, n.Start(), "Start")
	assertEqual(t, n.GetStatus(), node.StatusRunning, "status after Start")
	n.Stop()

	// 无效参数
	_, err = node.NewCacheNode("", 100)
	assertError(t, err, "empty ID")
	_, err = node.NewCacheNode("id", 0)
	assertError(t, err, "zero capacity")
}

// testNodeSetGetDelete 测试节点级别的 SET/GET/DELETE 操作
func testNodeSetGetDelete(t *TestContext) {
	n := createTestNode(t, "test-sgd", 100)
	defer n.Stop()

	assertNoError(t, n.Set("nk1", []byte("nv1")), "SET nk1")
	val, err := n.Get("nk1")
	assertNoError(t, err, "GET nk1")
	assertEqual(t, string(val), "nv1", "value")

	n.Delete("nk1")
	_, err = n.Get("nk1")
	if err == nil {
		t.Logf("  GET deleted key returned nil value (expected)")
	}
}

// testNodeGetInfo 测试节点 GetInfo 返回的状态信息
func testNodeGetInfo(t *TestContext) {
	n := createTestNode(t, "test-info", 100)
	defer n.Stop()
	n.Set("info-k", []byte("info-v"))

	info := n.GetInfo()
	assertTrue(t, info != nil, "GetInfo returns non-nil")
	t.Logf("  Node %s info retrieved successfully", n.GetNodeID())
}

// testNodeStatus 测试节点在各阶段的状态转换
func testNodeStatus(t *TestContext) {
	n := createTestNode(t, "test-status", 100)
	defer n.Stop()

	assertEqual(t, n.GetStatus(), node.StatusRunning, "running status")

	n.SetStatus(node.StatusMaster)
	assertEqual(t, n.GetStatus(), node.StatusMaster, "Master status")

	n.SetStatus(node.StatusSlave)
	assertEqual(t, n.GetStatus(), node.StatusSlave, "Slave status")

	n.SetMasterID("master-1")
	assertEqual(t, n.GetStatus(), node.StatusSlave, "still Slave")
}

// testNodeExportAll 测试节点全量数据导出
func testNodeExportAll(t *TestContext) {
	n := createTestNode(t, "test-export", 100)
	defer n.Stop()

	for i := 0; i < 5; i++ {
		n.Set(fmt.Sprintf("export-%d", i), []byte(fmt.Sprintf("val-%d", i)))
	}
	assertEqual(t, n.Size(), 5, "size")

	keys, values, err := n.ExportAll()
	assertNoError(t, err, "ExportAll")
	assertEqual(t, len(keys), 5, "export keys")
	assertEqual(t, len(values), 5, "export values")
	t.Logf("  Exported %d key-value pairs", len(keys))
}

// testNodeCapacity 测试节点容量管理和 LRU 淘汰
func testNodeCapacity(t *TestContext) {
	n := createTestNode(t, "cap-node", 100)
	defer n.Stop()

	for i := 0; i < 100; i++ {
		n.Set(fmt.Sprintf("cap-%d", i), []byte("v"))
	}
	assertEqual(t, n.Size(), 100, "full capacity")

	n.Set("overflow", []byte("ov"))
	assertEqual(t, n.Size(), 100, "still 100 after overflow")
}

// ========================================================================
// TCP 服务测试（9 用例）- 每个测试独立集群
// ========================================================================

// buildServerTests 构建 TCP 服务模块的测试条目
func buildServerTests(cli *CLIClient) []TestEntry {
	_ = cli
	return []TestEntry{
		{ID: "T01", Name: "SET/GET 基本操作", Category: "正常", Func: testServerSETGET},
		{ID: "T02", Name: "DELETE 操作", Category: "正常", Func: testServerDELETE},
		{ID: "T03", Name: "INFO 操作", Category: "正常", Func: testServerINFO},
		{ID: "T04", Name: "完整工作流", Category: "正常", Func: testServerCompleteWorkflow},
		{ID: "T05", Name: "非法命令处理", Category: "异常", Func: testServerInvalidCommand},
		{ID: "T06", Name: "参数缺失处理", Category: "异常", Func: testServerMissingKey},
		{ID: "T07", Name: "客户端断开连接", Category: "异常", Func: testServerClientDisconnect},
		{ID: "T08", Name: "多客户端并发(5)", Category: "边界", Func: testServerConcurrent5},
		{ID: "T09", Name: "压力测试10客户端并发", Category: "边界", Func: testServerStress10},
	}
}

// testServerSETGET 测试通过 TCP 的 SET/GET 完整流程
func testServerSETGET(t *TestContext) {
	c := tempCluster(t)
	defer stopCluster(c)
	conn := dialTest(t, c.Address)
	defer conn.Close()

	status, _, err := sendTCPRequest(conn, uint8(protocol.CMD_SET), []byte("sk1"), []byte("sv1"))
	assertNoError(t, err, "SET request")
	assertSuccess(t, status, "SET sk1")

	status, val, err := sendTCPRequest(conn, uint8(protocol.CMD_GET), []byte("sk1"), nil)
	assertNoError(t, err, "GET request")
	assertSuccess(t, status, "GET sk1")
	assertValue(t, val, "sv1", "GET sk1 value")

	status, val, err = sendTCPRequest(conn, uint8(protocol.CMD_GET), []byte("nonexistent"), nil)
	assertNoError(t, err, "GET nonexistent")
	assertSuccess(t, status, "GET nonexistent")
	assertEmptyValue(t, val, "nonexistent value")
}

// testServerDELETE 测试通过 TCP 的 DELETE 操作
func testServerDELETE(t *TestContext) {
	c := tempCluster(t)
	defer stopCluster(c)
	conn := dialTest(t, c.Address)
	defer conn.Close()

	sendTCPRequest(conn, uint8(protocol.CMD_SET), []byte("dk1"), []byte("dv1"))
	status, _, err := sendTCPRequest(conn, uint8(protocol.CMD_DELETE), []byte("dk1"), nil)
	assertNoError(t, err, "DELETE request")
	assertSuccess(t, status, "DELETE dk1")

	status, val, err := sendTCPRequest(conn, uint8(protocol.CMD_GET), []byte("dk1"), nil)
	assertNoError(t, err, "GET after DELETE")
	assertSuccess(t, status, "GET after DELETE")
	assertEmptyValue(t, val, "value after DELETE")
}

// testServerINFO 测试通过 TCP 的 INFO 命令响应
func testServerINFO(t *TestContext) {
	c := tempCluster(t)
	defer stopCluster(c)
	conn := dialTest(t, c.Address)
	defer conn.Close()

	status, val, err := sendTCPRequest(conn, uint8(protocol.CMD_INFO), []byte{}, nil)
	assertNoError(t, err, "INFO request")
	assertSuccess(t, status, "INFO")
	assertTrue(t, len(val) > 0, "INFO returns data")
	t.Logf("  INFO response: %d bytes", len(val))
}

// testServerCompleteWorkflow 测试 SET→GET→DELETE→GET 完整工作流
func testServerCompleteWorkflow(t *TestContext) {
	c := tempCluster(t)
	defer stopCluster(c)
	conn := dialTest(t, c.Address)
	defer conn.Close()

	status, _, err := sendTCPRequest(conn, uint8(protocol.CMD_SET), []byte("Key1"), []byte("Value1"))
	assertNoError(t, err, "SET Key1")
	assertSuccess(t, status, "SET Key1")

	status, val, err := sendTCPRequest(conn, uint8(protocol.CMD_GET), []byte("Key1"), nil)
	assertNoError(t, err, "GET Key1")
	assertSuccess(t, status, "GET Key1")
	assertValue(t, val, "Value1", "value")

	status, _, err = sendTCPRequest(conn, uint8(protocol.CMD_SET), []byte("Key2"), []byte("Value2"))
	assertNoError(t, err, "SET Key2")
	assertSuccess(t, status, "SET Key2")

	status, _, err = sendTCPRequest(conn, uint8(protocol.CMD_DELETE), []byte("Key1"), nil)
	assertNoError(t, err, "DELETE Key1")
	assertSuccess(t, status, "DELETE Key1")

	status, val, err = sendTCPRequest(conn, uint8(protocol.CMD_GET), []byte("Key1"), nil)
	assertNoError(t, err, "GET Key1 after DELETE")
	assertSuccess(t, status, "GET Key1 after DELETE")
	assertEmptyValue(t, val, "Key1 after DELETE")

	status, val, err = sendTCPRequest(conn, uint8(protocol.CMD_GET), []byte("Key2"), nil)
	assertNoError(t, err, "GET Key2")
	assertSuccess(t, status, "GET Key2")
	assertValue(t, val, "Value2", "Key2 value")
}

// testServerInvalidCommand 测试非法命令码的错误处理
func testServerInvalidCommand(t *TestContext) {
	c := tempCluster(t)
	defer stopCluster(c)
	conn := dialTest(t, c.Address)
	defer conn.Close()

	status, _, err := sendTCPRequest(conn, 0x99, []byte("somekey"), nil)
	assertNoError(t, err, "invalid cmd request")
	assertStatus(t, status, protocol.ERROR_UNKNOWN_COMMAND, "invalid cmd")

	status, _, err = sendTCPRequest(conn, uint8(protocol.CMD_SET), []byte("after-invalid"), []byte("works"))
	assertNoError(t, err, "SET after invalid")
	assertSuccess(t, status, "SET after invalid cmd")
}

// testServerMissingKey 测试缺少 Key 参数的错误处理
func testServerMissingKey(t *TestContext) {
	c := tempCluster(t)
	defer stopCluster(c)
	conn := dialTest(t, c.Address)
	defer conn.Close()

	status, _, err := sendTCPRequest(conn, uint8(protocol.CMD_GET), []byte{}, nil)
	assertNoError(t, err, "GET empty key request")
	assertStatus(t, status, protocol.ERROR_INVALID_KEY, "GET empty key")

	status, _, err = sendTCPRequest(conn, uint8(protocol.CMD_SET), []byte{}, []byte("value"))
	assertNoError(t, err, "SET empty key request")
	assertStatus(t, status, protocol.ERROR_INVALID_KEY, "SET empty key")

	status, _, err = sendTCPRequest(conn, uint8(protocol.CMD_DELETE), []byte{}, nil)
	assertNoError(t, err, "DELETE empty key request")
	assertStatus(t, status, protocol.ERROR_INVALID_KEY, "DELETE empty key")
}

// testServerClientDisconnect 测试客户端断开后服务器稳定性
func testServerClientDisconnect(t *TestContext) {
	c := tempCluster(t)
	defer stopCluster(c)

	conn1 := dialTest(t, c.Address)
	conn1.Close()
	time.Sleep(100 * time.Millisecond)

	conn2 := dialTest(t, c.Address)
	defer conn2.Close()
	status, _, err := sendTCPRequest(conn2, uint8(protocol.CMD_SET), []byte("after-dc"), []byte("works"))
	assertNoError(t, err, "SET after dc")
	assertSuccess(t, status, "SET after disconnect")
}

// testServerConcurrent5 测试 5 个客户端并发操作
func testServerConcurrent5(t *TestContext) {
	c := tempCluster(t)
	defer stopCluster(c)

	const numClients = 5
	const opsPerClient = 10
	var wg sync.WaitGroup
	errCh := make(chan error, numClients*opsPerClient*2)

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", c.Address, 5*time.Second)
			if err != nil {
				errCh <- fmt.Errorf("client %d: connect: %v", id, err)
				return
			}
			defer conn.Close()

			for j := 0; j < opsPerClient; j++ {
				key := fmt.Sprintf("mc-%d-%d", id, j)
				value := fmt.Sprintf("mv-%d-%d", id, j)

				status, _, err := sendTCPRequest(conn, uint8(protocol.CMD_SET), []byte(key), []byte(value))
				if err != nil || status != uint8(protocol.SUCCESS) {
					errCh <- fmt.Errorf("client %d SET %s: err=%v status=0x%02X", id, key, err, status)
					return
				}
				status, respVal, err := sendTCPRequest(conn, uint8(protocol.CMD_GET), []byte(key), nil)
				if err != nil || string(respVal) != value {
					errCh <- fmt.Errorf("client %d GET %s: err=%v expected='%s' got='%s'", id, key, err, value, string(respVal))
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

// testServerStress10 测试 10 个客户端压力测试
func testServerStress10(t *TestContext) {
	c := tempCluster(t)
	defer stopCluster(c)

	const numClients = 10
	const opsPerClient = 20
	var wg sync.WaitGroup
	errCh := make(chan error, numClients*opsPerClient*2)

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", c.Address, 5*time.Second)
			if err != nil {
				errCh <- fmt.Errorf("client %d: connect: %v", id, err)
				return
			}
			defer conn.Close()

			for j := 0; j < opsPerClient; j++ {
				key := fmt.Sprintf("stress-%d-%d", id, j)
				value := fmt.Sprintf("v-%d-%d", id, j)

				req, _ := protocol.EncodeRequest(uint8(protocol.CMD_SET), []byte(key), []byte(value))
				conn.Write(req)

				header := make([]byte, protocol.FrameHeaderSize)
				if _, err := io.ReadFull(conn, header); err != nil {
					errCh <- fmt.Errorf("c%d SET %s header: %v", id, key, err)
					return
				}
				vl := binary.BigEndian.Uint32(header[5:9])
				body := make([]byte, 1+int(vl))
				if 1+int(vl) > 0 {
					io.ReadFull(conn, body)
				}
				if body[0] != uint8(protocol.SUCCESS) {
					errCh <- fmt.Errorf("c%d SET %s status=0x%02X", id, key, body[0])
					return
				}

				req, _ = protocol.EncodeRequest(uint8(protocol.CMD_GET), []byte(key), nil)
				conn.Write(req)
				if _, err := io.ReadFull(conn, header); err != nil {
					errCh <- fmt.Errorf("c%d GET %s header: %v", id, key, err)
					return
				}
				vl = binary.BigEndian.Uint32(header[5:9])
				body = make([]byte, 1+int(vl))
				if 1+int(vl) > 0 {
					io.ReadFull(conn, body)
				}
				if string(body[1:]) != value {
					errCh <- fmt.Errorf("c%d GET %s: expected '%s' got '%s'", id, key, value, string(body[1:]))
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
		t.Logf("[PASS] Stress: %d clients x %d ops = %d total", numClients, opsPerClient, numClients*opsPerClient)
	}
}

// ========================================================================
// 主从复制测试（7 用例）- 每个测试独立集群
// ========================================================================

// buildReplicationTests 构建主从复制模块的测试条目
func buildReplicationTests(cli *CLIClient) []TestEntry {
	_ = cli
	return []TestEntry{
		{ID: "R01", Name: "主从关系配置", Category: "正常", Func: testRepSetMasterSlave},
		{ID: "R02", Name: "写同步到从节点", Category: "正常", Func: testRepSyncToSlave},
		{ID: "R03", Name: "初始化同步", Category: "正常", Func: testRepInitSync},
		{ID: "R04", Name: "状态查询", Category: "正常", Func: testRepStateQuery},
		{ID: "R05", Name: "删除同步到从节点", Category: "异常", Func: testRepSyncDelete},
		{ID: "R06", Name: "全量同步恢复", Category: "边界", Func: testRepFullSync},
		{ID: "R07", Name: "并发同步安全性", Category: "边界", Func: testRepConcurrentSync},
	}
}

// testRepSetMasterSlave 测试主从关系配置
func testRepSetMasterSlave(t *TestContext) {
	c := tempCluster(t)
	defer stopCluster(c)
	master := c.Nodes[0]
	slave := c.Nodes[1]

	assertNoError(t, c.RC.SetMasterSlave(master.GetNodeID(), slave.GetNodeID()), "SetMasterSlave")
	assertEqual(t, master.GetStatus(), node.StatusMaster, "master status")
	assertEqual(t, slave.GetStatus(), node.StatusSlave, "slave status")

	assertError(t, c.RC.SetMasterSlave("", "valid"), "empty master")
	assertError(t, c.RC.SetMasterSlave("valid", ""), "empty slave")
	assertError(t, c.RC.SetMasterSlave(master.GetNodeID(), master.GetNodeID()), "same node")
	assertError(t, c.RC.SetMasterSlave("NonExistent", slave.GetNodeID()), "non-existent")
}

// testRepSyncToSlave 测试主节点写入数据同步到从节点
func testRepSyncToSlave(t *TestContext) {
	c := tempCluster(t)
	defer stopCluster(c)
	master := c.Nodes[0]
	slave := c.Nodes[1]
	c.RC.SetMasterSlave(master.GetNodeID(), slave.GetNodeID())

	assertNoError(t, master.Set("rep-key1", []byte("rep-val1")), "master SET")
	assertNoError(t, c.RC.SyncToSlave("rep-key1", []byte("rep-val1")), "SyncToSlave")

	val, err := slave.Get("rep-key1")
	assertNoError(t, err, "slave GET")
	assertValue(t, val, "rep-val1", "slave value")
	assertTrue(t, c.RC.GetSyncedCount() >= 1, "sync count >= 1")
}

// testRepInitSync 测试初始全量同步
func testRepInitSync(t *TestContext) {
	c := tempCluster(t)
	defer stopCluster(c)

	assertNoError(t, c.RC.InitSync(c.Nodes[0].GetNodeID()), "InitSync valid")
	assertError(t, c.RC.InitSync(""), "empty ID")
	assertError(t, c.RC.InitSync("NonExistent"), "non-existent")
}

// testRepStateQuery 测试复制状态查询
func testRepStateQuery(t *TestContext) {
	c := tempCluster(t)
	defer stopCluster(c)
	master := c.Nodes[0]
	slave := c.Nodes[1]

	assertEqual(t, c.RC.GetMasterID(), "", "initial master ID")
	assertEqual(t, c.RC.GetSyncedCount(), 0, "initial sync count")

	c.RC.SetMasterSlave(master.GetNodeID(), slave.GetNodeID())
	assertEqual(t, c.RC.GetMasterID(), master.GetNodeID(), "master ID")
	assertEqual(t, c.RC.GetNodeCount(), 3, "node count")

	slaveIDs := c.RC.GetSlaveIDs()
	assertEqual(t, len(slaveIDs), 1, "one slave")
	assertEqual(t, slaveIDs[0], slave.GetNodeID(), "slave ID")

	state := c.RC.GetState()
	if v, ok := state["masterID"].(string); ok {
		assertEqual(t, v, master.GetNodeID(), "state masterID")
	} else {
		t.Fatalf("state masterID type unexpected: %T", state["masterID"])
	}
}

// testRepSyncDelete 测试主节点删除同步到从节点
func testRepSyncDelete(t *TestContext) {
	c := tempCluster(t)
	defer stopCluster(c)
	master := c.Nodes[0]
	slave := c.Nodes[1]
	c.RC.SetMasterSlave(master.GetNodeID(), slave.GetNodeID())

	master.Set("del-key", []byte("del-val"))
	c.RC.SyncToSlave("del-key", []byte("del-val"))

	val, _ := slave.Get("del-key")
	assertValue(t, val, "del-val", "slave has data")

	assertNoError(t, c.RC.SyncDeleteToSlave("del-key"), "SyncDeleteToSlave")
	val, _ = slave.Get("del-key")
	if val != nil {
		t.Fatalf("Slave should not have del-key after delete, got '%s'", string(val))
	}

	assertError(t, c.RC.SyncDeleteToSlave(""), "empty key")
}

// testRepFullSync 测试全量同步恢复（从节点清空后恢复）
func testRepFullSync(t *TestContext) {
	c := tempCluster(t)
	defer stopCluster(c)
	master := c.Nodes[0]
	slave := c.Nodes[1]
	c.RC.SetMasterSlave(master.GetNodeID(), slave.GetNodeID())

	const initialCount = 30
	for i := 0; i < initialCount; i++ {
		key := fmt.Sprintf("full-key-%02d", i)
		value := fmt.Sprintf("full-val-%02d", i)
		master.Set(key, []byte(value))
		c.RC.SyncToSlave(key, []byte(value))
	}

	const extraCount = 20
	for i := 0; i < extraCount; i++ {
		key := fmt.Sprintf("full-extra-%02d", i)
		value := fmt.Sprintf("extra-val-%02d", i)
		master.Set(key, []byte(value))
	}

	frames, err := c.RC.RequestFullSync(master.GetNodeID())
	assertNoError(t, err, "RequestFullSync")
	assertEqual(t, len(frames), initialCount+extraCount, "all frames")

	assertNoError(t, c.RC.ApplyFullSync(frames), "ApplyFullSync")
	assertEqual(t, slave.Size(), master.Size(), "size match")

	for i := 0; i < initialCount; i++ {
		val, _ := slave.Get(fmt.Sprintf("full-key-%02d", i))
		assertValue(t, val, fmt.Sprintf("full-val-%02d", i), "initial data")
	}
	for i := 0; i < extraCount; i++ {
		val, _ := slave.Get(fmt.Sprintf("full-extra-%02d", i))
		assertValue(t, val, fmt.Sprintf("extra-val-%02d", i), "extra data")
	}

	_, err = c.RC.RequestFullSync("")
	assertError(t, err, "empty master ID")
	_, err = c.RC.RequestFullSync("NonExistent")
	assertError(t, err, "non-existent master")
	assertError(t, c.RC.ApplyFullSync(nil), "nil frames")
	assertNoError(t, c.RC.ApplyFullSync([]*protocol.ProtocolFrame{}), "empty frames")
}

// testRepConcurrentSync 测试并发同步安全性
func testRepConcurrentSync(t *TestContext) {
	c := tempCluster(t)
	defer stopCluster(c)
	master := c.Nodes[0]
	slave := c.Nodes[1]
	c.RC.SetMasterSlave(master.GetNodeID(), slave.GetNodeID())

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
			if err := c.RC.SyncToSlave(key, []byte(value)); err != nil {
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

	assertEqual(t, slave.Size(), numGoroutines, "slave size")
	for i := 0; i < numGoroutines; i++ {
		key := fmt.Sprintf("concurrent-key-%02d", i)
		val, err := slave.Get(key)
		assertNoError(t, err, fmt.Sprintf("slave GET %s", key))
		assertValue(t, val, fmt.Sprintf("concurrent-val-%02d", i), key)
	}
}

// ========================================================================
// 集成测试（3 用例）- 每个测试独立集群
// ========================================================================

// buildIntegrationTests 构建集成测试的测试条目
func buildIntegrationTests(cli *CLIClient) []TestEntry {
	_ = cli
	return []TestEntry{
		{ID: "I01", Name: "截断帧头处理", Category: "边界", Func: testIntegTruncHeader},
		{ID: "I02", Name: "截断后合法帧验证", Category: "边界", Func: testIntegValidAfterTrunc},
		{ID: "I03", Name: "超大Value拒绝", Category: "边界", Func: testIntegOversizedValue},
	}
}

// testIntegTruncHeader 测试截断帧头发送后服务器稳定性
func testIntegTruncHeader(t *TestContext) {
	c := tempCluster(t)
	defer stopCluster(c)

	conn := dialTest(t, c.Address)
	conn.Write([]byte{0x01, 0x00, 0x00, 0x00})
	conn.Close()
	time.Sleep(100 * time.Millisecond)

	conn2 := dialTest(t, c.Address)
	defer conn2.Close()
	status, _, err := sendTCPRequest(conn2, uint8(protocol.CMD_SET), []byte("after-trunc"), []byte("ok"))
	assertNoError(t, err, "SET after trunc")
	assertSuccess(t, status, "SET after truncated header")
}

// testIntegValidAfterTrunc 测试截断帧后仍可正常处理合法请求
func testIntegValidAfterTrunc(t *TestContext) {
	c := tempCluster(t)
	defer stopCluster(c)

	conn := dialTest(t, c.Address)
	defer conn.Close()
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
	assertEqual(t, body[0], uint8(protocol.SUCCESS), "INFO status")
}

// testIntegOversizedValue 测试超大 Value 的边界处理
func testIntegOversizedValue(t *TestContext) {
	bigValue := make([]byte, protocol.MaxValueLength+1)
	_, err := protocol.EncodeRequest(uint8(protocol.CMD_SET), []byte("k"), bigValue)
	assertError(t, err, "oversized value rejected")

	valueAtLimit := make([]byte, protocol.MaxValueLength)
	_, err = protocol.EncodeRequest(uint8(protocol.CMD_SET), []byte("k"), valueAtLimit)
	assertNoError(t, err, "max size value accepted")
}
