// Package integration 实现分布式缓存系统 Task 3.8 集成测试
//
// 覆盖范围：
//   - LRU 淘汰机制验证：缓存满淘汰、热点数据保护、删除后腾出空间、更新刷新位置
//   - 一致性哈希节点路由与数据分布：路由确定性、数据均匀分布、环完整性
//   - 主从复制核心逻辑：写+删同步一致性、全量同步恢复、并发同步安全性
//   - 协议帧边界条件：截断帧、超大值、长度校验、校验码不匹配
//
// 集成模块：protocol / cache / shard / node / server / replication
//
// 运行方式：
//
//	go test ./tests/integration/ -v -count=1 -timeout 120s -run "Advanced"
//
// 与 Task 3.7 的区别：本文件专注于边界条件、淘汰机制、路由分布、复制安全，
// 不重复测试基本命令功能（SET/GET/DELETE/INFO）
package integration

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/yourusername/sd-03-cache/pkg/node"
	"github.com/yourusername/sd-03-cache/pkg/protocol"
	"github.com/yourusername/sd-03-cache/pkg/replication"
	"github.com/yourusername/sd-03-cache/pkg/server"
	"github.com/yourusername/sd-03-cache/pkg/shard"
)

// ============ Task 3.8 测试基础设施 ============

// smallCluster 小型测试集群，用于 LRU 淘汰等需要精确控制缓存容量的场景
// 与 testCluster（Task 3.7）的区别：节点数量和缓存容量可自定义
type smallCluster struct {
	ring    *shard.HashRing                    // 一致性哈希环
	nodes   []*node.CacheNode                  // 缓存节点列表
	server  *server.TCPServer                  // TCP 服务器
	rc      *replication.ReplicationController // 主从复制控制器
	address string                             // 服务器实际监听地址
}

// newSmallCluster 创建小型测试集群
// numNodes: 缓存节点数量, capacity: 每个节点的 LRU 缓存容量
// 使用随机端口（:0）避免端口冲突
func newSmallCluster(t *testing.T, numNodes, capacity int) *smallCluster {
	t.Helper()

	ring, err := shard.NewHashRing(100)
	if err != nil {
		t.Fatalf("Failed to create hash ring: %v", err)
	}

	var nodes []*node.CacheNode
	for i := 0; i < numNodes; i++ {
		id := fmt.Sprintf("SNode-%d", i+1)
		n, err := node.NewCacheNode(id, capacity)
		if err != nil {
			t.Fatalf("Failed to create node %s: %v", id, err)
		}
		if err := ring.AddNode(id); err != nil {
			t.Fatalf("Failed to add node %s to ring: %v", id, err)
		}
		if err := n.Init(ring); err != nil {
			t.Fatalf("Failed to init node %s: %v", id, err)
		}
		if err := n.Start(); err != nil {
			t.Fatalf("Failed to start node %s: %v", id, err)
		}
		nodes = append(nodes, n)
	}

	srv, err := server.NewTCPServer(":0", nodes, ring)
	if err != nil {
		t.Fatalf("Failed to create TCP server: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start TCP server: %v", err)
	}

	rc, err := replication.NewReplicationController(nodes)
	if err != nil {
		t.Fatalf("Failed to create replication controller: %v", err)
	}

	return &smallCluster{
		ring:    ring,
		nodes:   nodes,
		server:  srv,
		rc:      rc,
		address: srv.Address(),
	}
}

// stop 关闭小型测试集群，释放所有资源
func (sc *smallCluster) stop(t *testing.T) {
	t.Helper()
	if err := sc.server.Stop(); err != nil {
		t.Logf("Warning: failed to stop server: %v", err)
	}
	for _, n := range sc.nodes {
		if err := n.Stop(); err != nil {
			t.Logf("Warning: failed to stop node: %v", err)
		}
	}
}

// connect 创建到小型测试集群服务器的 TCP 连接
func (sc *smallCluster) connect(t *testing.T) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", sc.address, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to server %s: %v", sc.address, err)
	}
	return conn
}

// ========================================================================
// LRU 淘汰机制验证（对应 specs.md 第1节 LRU缓存淘汰策略）
// ========================================================================

// TestAdvanced_LRUEvictionAtCapacity 验证缓存达到容量上限时 LRU 淘汰最久未使用条目
//
// 对应 specs.md 场景 "缓存达到容量上限时自动淘汰"
// 验收标准1: 容量100时添加第101条时Key1被淘汰
//
// 验证点：
//   - 缓存容量=10，写入10条后缓存满
//   - 写入第11条，触发淘汰，缓存大小保持10
//   - 最久未使用的第1条被淘汰（GET返回空）
//   - 新写入的第11条可正常读取
func TestAdvanced_LRUEvictionAtCapacity(t *testing.T) {
	sc := newSmallCluster(t, 1, 10) // 单节点，容量10，确保所有Key路由到同一节点
	defer sc.stop(t)

	conn := sc.connect(t)
	defer conn.Close()

	// Step 1: 填充缓存至容量上限（10条）
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("evict-key-%02d", i)
		value := fmt.Sprintf("evict-val-%02d", i)
		status, _ := sendRequest(t, conn, uint8(protocol.CMD_SET), []byte(key), []byte(value))
		if status != uint8(protocol.SUCCESS) {
			t.Fatalf("SET %s: expected SUCCESS, got 0x%02X", key, status)
		}
	}

	if sc.nodes[0].Size() != 10 {
		t.Fatalf("Cache size after 10 SETs: expected 10, got %d", sc.nodes[0].Size())
	}
	t.Log("  [Step1] Cache filled to capacity=10")

	// Step 2: 写入第11条，触发 LRU 淘汰
	status, _ := sendRequest(t, conn, uint8(protocol.CMD_SET),
		[]byte("evict-key-10"), []byte("evict-val-10"))
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("SET evict-key-10: expected SUCCESS, got 0x%02X", status)
	}

	if sc.nodes[0].Size() != 10 {
		t.Fatalf("Cache size after eviction: expected 10, got %d", sc.nodes[0].Size())
	}
	t.Log("  [Step2] 11th entry SET, cache size stays at 10 (eviction triggered)")

	// Step 3: 验证 evict-key-00（最久未使用）被淘汰
	status, val := sendRequest(t, conn, uint8(protocol.CMD_GET), []byte("evict-key-00"), nil)
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("GET evict-key-00: expected SUCCESS, got 0x%02X", status)
	}
	if len(val) != 0 {
		t.Fatalf("evict-key-00 should be evicted (oldest), got '%s'", string(val))
	}
	t.Log("  [Step3] evict-key-00 (oldest) correctly evicted")

	// Step 4: 验证 evict-key-10（最新写入）可正常读取
	status, val = sendRequest(t, conn, uint8(protocol.CMD_GET), []byte("evict-key-10"), nil)
	if status != uint8(protocol.SUCCESS) || string(val) != "evict-val-10" {
		t.Fatalf("GET evict-key-10: expected 'evict-val-10', got '%s' (status=0x%02X)",
			string(val), status)
	}
	t.Log("  [Step4] evict-key-10 (newest) readable correctly")

	// Step 5: 验证 evict-key-09（最近使用的）仍保留
	status, val = sendRequest(t, conn, uint8(protocol.CMD_GET), []byte("evict-key-09"), nil)
	if status != uint8(protocol.SUCCESS) || string(val) != "evict-val-09" {
		t.Fatalf("GET evict-key-09: expected 'evict-val-09', got '%s'", string(val))
	}
	t.Log("  [Step5] evict-key-09 (recent) preserved")

	t.Log("[PASS] LRU eviction at capacity")
}

// TestAdvanced_LRUHotDataPreservation 验证热点数据在被反复访问后不会被淘汰
//
// 对应 specs.md 场景 "重复访问热点数据保持命中"
//
// 验证点：
//   - 反复 GET 同一个 Key，该 Key 在 LRU 链表中被提升到头部
//   - 缓存满后写入新数据，热点 Key 不会被淘汰
//   - 最久未访问的非热点数据被淘汰
func TestAdvanced_LRUHotDataPreservation(t *testing.T) {
	sc := newSmallCluster(t, 1, 5) // 单节点，容量5
	defer sc.stop(t)

	conn := sc.connect(t)
	defer conn.Close()

	// Step 1: 填充缓存至容量（5条）
	// LRU order after all SETs: [hot-key-4, hot-key-3, hot-key-2, hot-key-1, hot-key-0]
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("hot-key-%d", i)
		value := fmt.Sprintf("hot-val-%d", i)
		status, _ := sendRequest(t, conn, uint8(protocol.CMD_SET), []byte(key), []byte(value))
		if status != uint8(protocol.SUCCESS) {
			t.Fatalf("SET %s: expected SUCCESS, got 0x%02X", key, status)
		}
	}
	t.Log("  [Step1] Cache filled with 5 entries (hot-key-0 .. hot-key-4)")

	// Step 2: 反复访问 hot-key-0（3次），使其成为热点数据
	for i := 0; i < 3; i++ {
		status, val := sendRequest(t, conn, uint8(protocol.CMD_GET), []byte("hot-key-0"), nil)
		if status != uint8(protocol.SUCCESS) || string(val) != "hot-val-0" {
			t.Fatalf("GET hot-key-0 (access %d): expected 'hot-val-0', got '%s'", i+1, string(val))
		}
	}
	// LRU order: [hot-key-0, hot-key-4, hot-key-3, hot-key-2, hot-key-1]
	// hot-key-1 成为最久未使用
	t.Log("  [Step2] hot-key-0 accessed 3 times, promoted to front")

	// Step 3: 写入新数据，触发淘汰（应淘汰 hot-key-1）
	status, _ := sendRequest(t, conn, uint8(protocol.CMD_SET), []byte("hot-key-5"), []byte("hot-val-5"))
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("SET hot-key-5: expected SUCCESS, got 0x%02X", status)
	}
	// LRU order: [hot-key-5, hot-key-0, hot-key-4, hot-key-3, hot-key-2]
	t.Log("  [Step3] hot-key-5 added, LRU eviction triggered")

	// Step 4: 验证热点数据 hot-key-0 仍然存在
	status, val := sendRequest(t, conn, uint8(protocol.CMD_GET), []byte("hot-key-0"), nil)
	if status != uint8(protocol.SUCCESS) || string(val) != "hot-val-0" {
		t.Fatalf("hot-key-0 should survive as hot data, got '%s'", string(val))
	}
	t.Log("  [Step4] hot-key-0 preserved (hot data not evicted)")

	// Step 5: 验证 hot-key-1 被淘汰（最久未使用）
	status, val = sendRequest(t, conn, uint8(protocol.CMD_GET), []byte("hot-key-1"), nil)
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("GET hot-key-1: expected SUCCESS, got 0x%02X", status)
	}
	if len(val) != 0 {
		t.Fatalf("hot-key-1 should be evicted (oldest), got '%s'", string(val))
	}
	t.Log("  [Step5] hot-key-1 evicted (least recently used)")

	t.Log("[PASS] LRU hot data preservation")
}

// TestAdvanced_LRUDeleteThenEviction 验证删除操作为 LRU 缓存腾出空间，新数据写入不触发淘汰
//
// 对应 specs.md 场景 "删除操作更新LRU链表"
//
// 验证点：
//   - 删除缓存中的条目后，缓存大小减少
//   - 新写入数据填充腾出的空间，不触发淘汰
//   - 再次写满后写入新数据，LRU 淘汰恢复
func TestAdvanced_LRUDeleteThenEviction(t *testing.T) {
	sc := newSmallCluster(t, 1, 5) // 单节点，容量5
	defer sc.stop(t)

	conn := sc.connect(t)
	defer conn.Close()

	// Step 1: 填充缓存至容量（5条）
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("delkey-%d", i)
		value := fmt.Sprintf("delval-%d", i)
		sendRequest(t, conn, uint8(protocol.CMD_SET), []byte(key), []byte(value))
	}
	// LRU order: [delkey-4, delkey-3, delkey-2, delkey-1, delkey-0]
	t.Log("  [Step1] Cache filled with 5 entries")

	// Step 2: 删除 delkey-2
	status, _ := sendRequest(t, conn, uint8(protocol.CMD_DELETE), []byte("delkey-2"), nil)
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("DELETE delkey-2: expected SUCCESS, got 0x%02X", status)
	}
	// LRU order: [delkey-4, delkey-3, delkey-1, delkey-0] (4 entries)
	if sc.nodes[0].Size() != 4 {
		t.Fatalf("Cache size after delete: expected 4, got %d", sc.nodes[0].Size())
	}
	t.Log("  [Step2] delkey-2 deleted, size=4")

	// Step 3: 写入 delkey-5（第5条，填满缓存，不触发淘汰）
	status, _ = sendRequest(t, conn, uint8(protocol.CMD_SET), []byte("delkey-5"), []byte("delval-5"))
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("SET delkey-5: expected SUCCESS, got 0x%02X", status)
	}
	if sc.nodes[0].Size() != 5 {
		t.Fatalf("Cache size after filling: expected 5, got %d", sc.nodes[0].Size())
	}
	t.Log("  [Step3] delkey-5 added (filling slot), size=5, no eviction")

	// Step 4: 写入 delkey-6（第6条，触发 LRU 淘汰 delkey-0）
	status, _ = sendRequest(t, conn, uint8(protocol.CMD_SET), []byte("delkey-6"), []byte("delval-6"))
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("SET delkey-6: expected SUCCESS, got 0x%02X", status)
	}
	if sc.nodes[0].Size() != 5 {
		t.Fatalf("Cache size after eviction: expected 5, got %d", sc.nodes[0].Size())
	}
	t.Log("  [Step4] delkey-6 added, eviction triggered, size=5")

	// Step 5: 验证 delkey-0 被淘汰
	status, val := sendRequest(t, conn, uint8(protocol.CMD_GET), []byte("delkey-0"), nil)
	if len(val) != 0 {
		t.Fatalf("delkey-0 should be evicted, got '%s'", string(val))
	}
	t.Log("  [Step5] delkey-0 (oldest) evicted")

	// Step 6: 验证 delkey-2 已被删除
	status, val = sendRequest(t, conn, uint8(protocol.CMD_GET), []byte("delkey-2"), nil)
	if len(val) != 0 {
		t.Fatalf("delkey-2 should be deleted, got '%s'", string(val))
	}
	t.Log("  [Step6] delkey-2 still deleted")

	// Step 7: 验证 delkey-1、delkey-5、delkey-6 存在
	for _, k := range []string{"delkey-1", "delkey-5", "delkey-6"} {
		status, val = sendRequest(t, conn, uint8(protocol.CMD_GET), []byte(k), nil)
		if status != uint8(protocol.SUCCESS) || len(val) == 0 {
			t.Fatalf("%s should exist, got empty", k)
		}
	}
	t.Log("  [Step7] other entries preserved")

	t.Log("[PASS] LRU delete then eviction")
}

// TestAdvanced_LRUValueUpdateRefreshesPosition 验证更新已有 Key 的值会将其提升到 LRU 链表头部
//
// 验证点：
//   - SET 更新已存在的 Key 时，Value 更新且 LRU 位置移到链表头部
//   - 该 Key 因此不会被立即淘汰（虽然它是最早写入的）
//   - 下一个最久未使用的 Key（非更新目标）被淘汰
func TestAdvanced_LRUValueUpdateRefreshesPosition(t *testing.T) {
	sc := newSmallCluster(t, 1, 5) // 单节点，容量5
	defer sc.stop(t)

	conn := sc.connect(t)
	defer conn.Close()

	// Step 1: 填充缓存至容量（5条）
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("upkey-%d", i)
		value := fmt.Sprintf("upval-%d", i)
		sendRequest(t, conn, uint8(protocol.CMD_SET), []byte(key), []byte(value))
	}
	// LRU order: [upkey-4, upkey-3, upkey-2, upkey-1, upkey-0]
	t.Log("  [Step1] Cache filled with 5 entries")

	// Step 2: 更新 upkey-0（最早写入），应提升到链表头部
	status, _ := sendRequest(t, conn, uint8(protocol.CMD_SET),
		[]byte("upkey-0"), []byte("new-val-0"))
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("SET upkey-0 (update): expected SUCCESS, got 0x%02X", status)
	}
	// LRU order: [upkey-0, upkey-4, upkey-3, upkey-2, upkey-1]
	t.Log("  [Step2] upkey-0 updated, promoted to front")

	// Step 3: 写入 upkey-5（触发淘汰，应淘汰 upkey-1）
	status, _ = sendRequest(t, conn, uint8(protocol.CMD_SET), []byte("upkey-5"), []byte("upval-5"))
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("SET upkey-5: expected SUCCESS, got 0x%02X", status)
	}
	// LRU order: [upkey-5, upkey-0, upkey-4, upkey-3, upkey-2]
	t.Log("  [Step3] upkey-5 added, eviction triggered")

	// Step 4: 验证 upkey-0 返回更新后的值
	status, val := sendRequest(t, conn, uint8(protocol.CMD_GET), []byte("upkey-0"), nil)
	if string(val) != "new-val-0" {
		t.Fatalf("upkey-0: expected 'new-val-0', got '%s'", string(val))
	}
	t.Log("  [Step4] upkey-0 has updated value 'new-val-0'")

	// Step 5: 验证 upkey-1 被淘汰
	status, val = sendRequest(t, conn, uint8(protocol.CMD_GET), []byte("upkey-1"), nil)
	if len(val) != 0 {
		t.Fatalf("upkey-1 should be evicted (oldest), got '%s'", string(val))
	}
	t.Log("  [Step5] upkey-1 evicted (was oldest before update)")

	// Step 6: 验证其他条目存在
	for _, k := range []string{"upkey-2", "upkey-3", "upkey-4", "upkey-5"} {
		status, val = sendRequest(t, conn, uint8(protocol.CMD_GET), []byte(k), nil)
		if status != uint8(protocol.SUCCESS) || len(val) == 0 {
			t.Fatalf("%s should exist, got empty", k)
		}
	}
	t.Log("  [Step6] other entries preserved")

	t.Log("[PASS] LRU value update refreshes position")
}

// ========================================================================
// 一致性哈希节点路由与数据分布测试（对应 specs.md 第4节）
// ========================================================================

// TestAdvanced_ConsistentHashRoutingDeterminism 验证一致性哈希路由确定性
//
// 对应 specs.md 场景 "同一个Key MUST 映射到同一个节点"
//
// 验证点：
//   - 同一个 Key 通过哈希环始终映射到同一个物理节点
//   - 多次 GET 操作返回一致的结果
//   - SET 和 GET 通过 TCP 服务器全链路验证
func TestAdvanced_ConsistentHashRoutingDeterminism(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.stop(t)

	conn := tc.connect(t)
	defer conn.Close()

	// Step 1: 验证哈希环路由确定性——同一个 Key 始终映射到同一个节点
	const numKeys = 100
	routing := make(map[string]string) // key → nodeID

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("route-key-%03d", i)
		nodeID := tc.ring.GetNode(key)
		if nodeID == "" {
			t.Fatalf("Key %s routed to empty node", key)
		}
		routing[key] = nodeID
	}
	t.Log("  [Step1] Initial routing map built for 100 keys")

	// 再次查询，验证路由一致性
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("route-key-%03d", i)
		nodeID := tc.ring.GetNode(key)
		if nodeID != routing[key] {
			t.Fatalf("Routing inconsistency for %s: expected %s, got %s",
				key, routing[key], nodeID)
		}
	}
	t.Log("  [Step2] Routing deterministic: same key → same node (100/100)")

	// Step 2: 通过 TCP 全链路验证 SET + GET
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("route-key-%03d", i)
		value := fmt.Sprintf("route-val-%03d", i)
		status, _ := sendRequest(t, conn, uint8(protocol.CMD_SET), []byte(key), []byte(value))
		if status != uint8(protocol.SUCCESS) {
			t.Fatalf("SET %s: expected SUCCESS, got 0x%02X", key, status)
		}
	}
	t.Log("  [Step3] 100 keys SET through TCP")

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("route-key-%03d", i)
		expected := fmt.Sprintf("route-val-%03d", i)
		status, val := sendRequest(t, conn, uint8(protocol.CMD_GET), []byte(key), nil)
		if status != uint8(protocol.SUCCESS) || string(val) != expected {
			t.Fatalf("GET %s: expected '%s', got '%s' (status=0x%02X)",
				key, expected, string(val), status)
		}
	}
	t.Log("  [Step4] 100 keys GET verified through TCP")

	// Step 3: 验证每个 Key 实际存储在路由到的节点上
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("route-key-%03d", i)
		expectedNodeID := routing[key]

		// 在目标节点上验证数据
		for _, n := range tc.nodes {
			if n.GetNodeID() == expectedNodeID {
				val, err := n.Get(key)
				if err != nil {
					t.Fatalf("Node %s Get %s: %v", expectedNodeID, key, err)
				}
				if string(val) != fmt.Sprintf("route-val-%03d", i) {
					t.Fatalf("Node %s data mismatch for %s", expectedNodeID, key)
				}
				break
			}
		}
	}
	t.Log("  [Step5] All keys stored on correct nodes verified")

	t.Log("[PASS] Consistent hash routing determinism")
}

// TestAdvanced_DataDistributionBalance 验证数据在多个节点间均匀分布
//
// 对应 specs.md 场景 "虚拟节点数据均匀分布"
// 验收标准4: 1000次SET后3个分片数据分布差异<30%
//
// 验证点：
//   - 1000 个不同的 Key 通过哈希环分布在 3 个节点上
//   - 每个节点的数据量与均值的偏差 < 30%
//   - 总数据量 = 1000
func TestAdvanced_DataDistributionBalance(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.stop(t)

	conn := tc.connect(t)
	defer conn.Close()

	const numKeys = 1000

	// Step 1: 通过 TCP 写入 1000 个 Key
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("dist-key-%04d", i)
		value := fmt.Sprintf("dist-val-%04d", i)
		status, _ := sendRequest(t, conn, uint8(protocol.CMD_SET), []byte(key), []byte(value))
		if status != uint8(protocol.SUCCESS) {
			t.Fatalf("SET %s: expected SUCCESS, got 0x%02X (i=%d)", key, status, i)
		}
	}
	t.Log("  [Step1] 1000 keys SET through TCP")

	// Step 2: 检查每个节点的数据量
	sizes := make(map[string]int)
	for _, n := range tc.nodes {
		sizes[n.GetNodeID()] = n.Size()
		t.Logf("  Node %s: %d entries", n.GetNodeID(), n.Size())
	}

	// 验证总数据量
	totalSize := 0
	for _, s := range sizes {
		totalSize += s
	}
	if totalSize != numKeys {
		t.Fatalf("Total data: expected %d, got %d", numKeys, totalSize)
	}

	// Step 3: 计算分布偏差
	mean := float64(totalSize) / float64(len(sizes))
	for nodeID, size := range sizes {
		deviation := (float64(size) - mean) / mean
		if deviation < 0 {
			deviation = -deviation
		}
		t.Logf("  Node %s: size=%d, mean=%.1f, deviation=%.1f%%",
			nodeID, size, mean, deviation*100)
		if deviation >= 0.30 {
			t.Fatalf("Node %s distribution deviation %.1f%% exceeds 30%% threshold",
				nodeID, deviation*100)
		}
	}
	t.Log("  [Step2-3] Distribution balanced: all nodes within 30% deviation")

	// Step 4: 验证所有数据可通过 TCP GET 正确读取
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("dist-key-%04d", i)
		expected := fmt.Sprintf("dist-val-%04d", i)
		status, val := sendRequest(t, conn, uint8(protocol.CMD_GET), []byte(key), nil)
		if status != uint8(protocol.SUCCESS) {
			t.Fatalf("GET %s: expected SUCCESS, got 0x%02X", key, status)
		}
		if string(val) != expected {
			t.Fatalf("GET %s: expected '%s', got '%s'", key, expected, string(val))
		}
	}
	t.Log("  [Step4] All 1000 values verified via TCP GET")

	t.Logf("[PASS] Data distribution balance: %d keys across %d nodes", totalSize, len(sizes))
}

// TestAdvanced_ConsistentHashRingIntegrity 验证哈希环的完整性——添加/移除节点后路由仍正常
//
// 对应 specs.md 场景 "一致性哈希环形成"、"添加新节点后的数据迁移"、"移除节点后的数据重分配"
//
// 验证点：
//   - 添加节点后虚拟节点数和物理节点数正确
//   - 移除节点后虚拟节点数和物理节点数正确
//   - 所有 Key 始终映射到当前存在的物理节点
func TestAdvanced_ConsistentHashRingIntegrity(t *testing.T) {
	ring, err := shard.NewHashRing(100)
	if err != nil {
		t.Fatalf("NewHashRing failed: %v", err)
	}

	// Step 1: 添加3个物理节点
	for _, id := range []string{"Node-A", "Node-B", "Node-C"} {
		if err := ring.AddNode(id); err != nil {
			t.Fatalf("AddNode %s failed: %v", id, err)
		}
	}

	if ring.NodeCount() != 3 {
		t.Fatalf("NodeCount: expected 3, got %d", ring.NodeCount())
	}
	if ring.VirtualNodeCount() != 300 {
		t.Fatalf("VirtualNodeCount: expected 300, got %d", ring.VirtualNodeCount())
	}
	t.Log("  [Step1] Ring: 3 physical nodes, 300 virtual nodes")

	// Step 2: 验证所有 Key 路由到存在的节点
	validNodes := make(map[string]bool)
	for _, n := range ring.GetNodes() {
		validNodes[n] = true
	}

	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("integrity-key-%d", i)
		nodeID := ring.GetNode(key)
		if !validNodes[nodeID] {
			t.Fatalf("Key %s routed to unknown node %s", key, nodeID)
		}
	}
	t.Log("  [Step2] All 500 keys route to valid nodes")

	// Step 3: 移除 Node-C
	if err := ring.RemoveNode("Node-C"); err != nil {
		t.Fatalf("RemoveNode Node-C failed: %v", err)
	}
	if ring.NodeCount() != 2 {
		t.Fatalf("After remove: NodeCount expected 2, got %d", ring.NodeCount())
	}
	if ring.VirtualNodeCount() != 200 {
		t.Fatalf("After remove: VirtualNodeCount expected 200, got %d", ring.VirtualNodeCount())
	}
	t.Log("  [Step3] After removing Node-C: 2 nodes, 200 virtual nodes")

	// Step 4: 验证移除后所有 Key 路由到剩余节点
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("integrity-key-%d", i)
		nodeID := ring.GetNode(key)
		if nodeID != "Node-A" && nodeID != "Node-B" {
			t.Fatalf("After remove: key %s routed to %s (expected A or B)", key, nodeID)
		}
	}
	t.Log("  [Step4] All keys route to remaining nodes after removal")

	// Step 5: 添加 Node-D（替换被移除的）
	if err := ring.AddNode("Node-D"); err != nil {
		t.Fatalf("AddNode Node-D failed: %v", err)
	}
	if ring.NodeCount() != 3 {
		t.Fatalf("After add: NodeCount expected 3, got %d", ring.NodeCount())
	}
	if ring.VirtualNodeCount() != 300 {
		t.Fatalf("After add: VirtualNodeCount expected 300, got %d", ring.VirtualNodeCount())
	}
	t.Log("  [Step5] After adding Node-D: 3 nodes, 300 virtual nodes")

	// Step 6: 最终验证路由
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("integrity-key-%d", i)
		nodeID := ring.GetNode(key)
		if nodeID == "" {
			t.Fatalf("Key %s routed to empty node", key)
		}
	}
	t.Log("  [Step6] All keys route successfully after ring topology changes")

	t.Log("[PASS] Consistent hash ring integrity")
}

// ========================================================================
// 主从复制核心逻辑测试（对应 specs.md 第5节）
// ========================================================================

// TestAdvanced_ReplicationWriteDeleteConsistency 验证主从复制的写+删同步一致性
//
// 对应 specs.md 场景 "主从同步正常工作"
//
// 验证点：
//   - 主节点写入数据后通过 SyncToSlave 同步到从节点
//   - 主节点删除数据后通过 SyncDeleteToSlave 同步到从节点
//   - 从节点数据与主节点保持完全一致
func TestAdvanced_ReplicationWriteDeleteConsistency(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.stop(t)

	// Step 1: 设置主从关系
	if err := tc.rc.SetMasterSlave("Node-1", "Node-2"); err != nil {
		t.Fatalf("SetMasterSlave failed: %v", err)
	}
	masterNode := tc.nodes[0]
	slaveNode := tc.nodes[1]
	t.Log("  [Step1] Master=Node-1, Slave=Node-2")

	// Step 2: 写入20条数据并同步到从节点
	const writeCount = 20
	for i := 0; i < writeCount; i++ {
		key := fmt.Sprintf("sync-key-%02d", i)
		value := fmt.Sprintf("sync-val-%02d", i)

		if err := masterNode.Set(key, []byte(value)); err != nil {
			t.Fatalf("Master Set %s: %v", key, err)
		}
		if err := tc.rc.SyncToSlave(key, []byte(value)); err != nil {
			t.Fatalf("SyncToSlave %s: %v", key, err)
		}
	}
	t.Log("  [Step2] 20 entries written to master and synced to slave")

	// 验证主从数据量一致
	if masterNode.Size() != slaveNode.Size() {
		t.Fatalf("Size mismatch after writes: master=%d, slave=%d",
			masterNode.Size(), slaveNode.Size())
	}

	// 验证每条数据从节点与主节点一致
	for i := 0; i < writeCount; i++ {
		key := fmt.Sprintf("sync-key-%02d", i)
		masterVal, _ := masterNode.Get(key)
		slaveVal, _ := slaveNode.Get(key)
		if string(masterVal) != string(slaveVal) {
			t.Fatalf("Data mismatch for %s: master='%s', slave='%s'",
				key, string(masterVal), string(slaveVal))
		}
	}
	t.Log("  [Step3] All 20 entries verified consistent between master and slave")

	// Step 4: 删除5条数据并同步到从节点
	const deleteCount = 5
	for i := 0; i < deleteCount; i++ {
		key := fmt.Sprintf("sync-key-%02d", i)
		if err := masterNode.Delete(key); err != nil {
			t.Fatalf("Master Delete %s: %v", key, err)
		}
		if err := tc.rc.SyncDeleteToSlave(key); err != nil {
			t.Fatalf("SyncDeleteToSlave %s: %v", key, err)
		}
	}
	t.Log("  [Step4] 5 entries deleted from master and synced to slave")

	// 验证主从数据量一致（20 - 5 = 15）
	if masterNode.Size() != slaveNode.Size() {
		t.Fatalf("Size mismatch after deletes: master=%d, slave=%d",
			masterNode.Size(), slaveNode.Size())
	}
	if masterNode.Size() != writeCount-deleteCount {
		t.Fatalf("Expected %d entries after deletes, got master=%d slave=%d",
			writeCount-deleteCount, masterNode.Size(), slaveNode.Size())
	}

	// 验证已删除的 Key 从节点也不存在
	for i := 0; i < deleteCount; i++ {
		key := fmt.Sprintf("sync-key-%02d", i)
		masterVal, _ := masterNode.Get(key)
		slaveVal, _ := slaveNode.Get(key)
		if masterVal != nil || slaveVal != nil {
			t.Fatalf("Deleted key %s still exists: master=%v, slave=%v",
				key, masterVal, slaveVal)
		}
	}
	t.Log("  [Step5] Deleted entries confirmed absent on both master and slave")

	// 验证剩余 Key 从节点与主节点一致
	for i := deleteCount; i < writeCount; i++ {
		key := fmt.Sprintf("sync-key-%02d", i)
		expected := fmt.Sprintf("sync-val-%02d", i)
		slaveVal, _ := slaveNode.Get(key)
		if string(slaveVal) != expected {
			t.Fatalf("Slave data mismatch for %s: expected '%s', got '%s'",
				key, expected, string(slaveVal))
		}
	}
	t.Log("  [Step6] Remaining 15 entries verified on slave")

	// 验证同步计数
	syncedCount := tc.rc.GetSyncedCount()
	expectedSyncs := writeCount + deleteCount // 20 writes + 5 deletes = 25 syncs
	if syncedCount < expectedSyncs {
		t.Fatalf("SyncedCount: expected >= %d, got %d", expectedSyncs, syncedCount)
	}
	t.Logf("  [Step7] SyncedCount=%d (expected >= %d)", syncedCount, expectedSyncs)

	t.Log("[PASS] Replication write/delete consistency")
}

// TestAdvanced_ReplicationFullSyncRecovery 验证从节点全量同步恢复数据
//
// 对应 specs.md 场景 "从节点断开重连后恢复同步"
//
// 验证点：
//   - 从节点可通过 RequestFullSync 从主节点获取全量数据
//   - ApplyFullSync 可正确应用全量数据到从节点
//   - 全量同步后从节点数据与主节点完全一致
func TestAdvanced_ReplicationFullSyncRecovery(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.stop(t)

	// Step 1: 设置主从关系
	if err := tc.rc.SetMasterSlave("Node-1", "Node-2"); err != nil {
		t.Fatalf("SetMasterSlave failed: %v", err)
	}
	masterNode := tc.nodes[0]
	slaveNode := tc.nodes[1]
	t.Log("  [Step1] Master=Node-1, Slave=Node-2")

	// Step 2: 主节点写入 50 条数据并通过 SyncToSlave 同步
	const initialCount = 50
	for i := 0; i < initialCount; i++ {
		key := fmt.Sprintf("fullsync-key-%02d", i)
		value := fmt.Sprintf("fullsync-val-%02d", i)
		if err := masterNode.Set(key, []byte(value)); err != nil {
			t.Fatalf("Master Set %s: %v", key, err)
		}
		if err := tc.rc.SyncToSlave(key, []byte(value)); err != nil {
			t.Fatalf("SyncToSlave %s: %v", key, err)
		}
	}
	if masterNode.Size() != initialCount || slaveNode.Size() != initialCount {
		t.Fatalf("Initial sync failed: master=%d, slave=%d",
			masterNode.Size(), slaveNode.Size())
	}
	t.Log("  [Step2] 50 entries synced to slave")

	// Step 3: 主节点额外写入 30 条（模拟从节点断开期间的数据变更）
	const extraCount = 30
	for i := 0; i < extraCount; i++ {
		key := fmt.Sprintf("fullsync-extra-%02d", i)
		value := fmt.Sprintf("extra-val-%02d", i)
		if err := masterNode.Set(key, []byte(value)); err != nil {
			t.Fatalf("Master Set extra %s: %v", key, err)
		}
		// 故意不同步到从节点，模拟从节点离线
	}
	if masterNode.Size() != initialCount+extraCount {
		t.Fatalf("Master after extra writes: expected %d, got %d",
			initialCount+extraCount, masterNode.Size())
	}
	t.Log("  [Step3] Master has 30 extra entries (not synced to slave)")

	// Step 4: 从节点请求全量同步
	frames, err := tc.rc.RequestFullSync("Node-1")
	if err != nil {
		t.Fatalf("RequestFullSync failed: %v", err)
	}
	if len(frames) != initialCount+extraCount {
		t.Fatalf("FullSync frames: expected %d, got %d",
			initialCount+extraCount, len(frames))
	}
	t.Logf("  [Step4] Full sync requested: %d frames from master", len(frames))

	// Step 5: 应用全量同步到从节点
	if err := tc.rc.ApplyFullSync(frames); err != nil {
		t.Fatalf("ApplyFullSync failed: %v", err)
	}
	t.Log("  [Step5] Full sync applied to slave")

	// Step 6: 验证从节点数据完全一致
	if slaveNode.Size() != masterNode.Size() {
		t.Fatalf("Size mismatch after full sync: master=%d, slave=%d",
			masterNode.Size(), slaveNode.Size())
	}
	t.Logf("  [Step6] Size match: master=%d, slave=%d",
		masterNode.Size(), slaveNode.Size())

	// 验证初始 50 条数据
	for i := 0; i < initialCount; i++ {
		key := fmt.Sprintf("fullsync-key-%02d", i)
		expected := fmt.Sprintf("fullsync-val-%02d", i)
		val, err := slaveNode.Get(key)
		if err != nil {
			t.Fatalf("Slave Get %s: %v", key, err)
		}
		if string(val) != expected {
			t.Fatalf("Slave data mismatch for %s: expected '%s', got '%s'",
				key, expected, string(val))
		}
	}
	t.Log("  [Step7] Initial 50 entries verified on slave")

	// 验证额外 30 条数据
	for i := 0; i < extraCount; i++ {
		key := fmt.Sprintf("fullsync-extra-%02d", i)
		expected := fmt.Sprintf("extra-val-%02d", i)
		val, err := slaveNode.Get(key)
		if err != nil {
			t.Fatalf("Slave Get %s: %v", key, err)
		}
		if string(val) != expected {
			t.Fatalf("Slave data mismatch for %s: expected '%s', got '%s'",
				key, expected, string(val))
		}
	}
	t.Log("  [Step8] Extra 30 entries verified on slave")

	// 验证同步计数
	t.Logf("  [Step9] Final slave size=%d, syncedCount=%d",
		slaveNode.Size(), tc.rc.GetSyncedCount())

	t.Log("[PASS] Replication full sync recovery")
}

// TestAdvanced_ReplicationConcurrentSync 验证并发场景下主从复制的数据安全性
//
// 验证点：
//   - 多个 goroutine 同时执行 Set + SyncToSlave
//   - 所有同步操作完成后，从节点数据完整且一致
//   - 无数据丢失或错乱
func TestAdvanced_ReplicationConcurrentSync(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.stop(t)

	// Step 1: 设置主从关系
	if err := tc.rc.SetMasterSlave("Node-1", "Node-2"); err != nil {
		t.Fatalf("SetMasterSlave failed: %v", err)
	}
	masterNode := tc.nodes[0]
	slaveNode := tc.nodes[1]
	t.Log("  [Step1] Master=Node-1, Slave=Node-2")

	// Step 2: 20 个 goroutine 并发执行写入和同步
	const numGoroutines = 20
	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("concurrent-key-%02d", idx)
			value := fmt.Sprintf("concurrent-val-%02d", idx)

			if err := masterNode.Set(key, []byte(value)); err != nil {
				errCh <- fmt.Errorf("master Set %s: %w", key, err)
				return
			}
			if err := tc.rc.SyncToSlave(key, []byte(value)); err != nil {
				errCh <- fmt.Errorf("SyncToSlave %s: %w", key, err)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	// Step 3: 检查并发错误
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	t.Log("  [Step2] 20 goroutines completed without errors")

	// Step 4: 验证从节点数据完整性
	if slaveNode.Size() != numGoroutines {
		t.Fatalf("Slave size: expected %d, got %d", numGoroutines, slaveNode.Size())
	}
	t.Logf("  [Step3] Slave has %d entries (expected %d)", slaveNode.Size(), numGoroutines)

	// 验证每条数据的值正确
	for i := 0; i < numGoroutines; i++ {
		key := fmt.Sprintf("concurrent-key-%02d", i)
		expected := fmt.Sprintf("concurrent-val-%02d", i)
		val, err := slaveNode.Get(key)
		if err != nil {
			t.Fatalf("Slave Get %s: %v", key, err)
		}
		if string(val) != expected {
			t.Fatalf("Slave data mismatch for %s: expected '%s', got '%s'",
				key, expected, string(val))
		}
	}
	t.Log("  [Step4] All 20 entries verified on slave with correct values")

	// 验证同步计数
	syncedCount := tc.rc.GetSyncedCount()
	if syncedCount < numGoroutines {
		t.Fatalf("SyncedCount: expected >= %d, got %d", numGoroutines, syncedCount)
	}
	t.Logf("  [Step5] SyncedCount=%d", syncedCount)

	t.Log("[PASS] Replication concurrent sync")
}

// ========================================================================
// 协议帧边界条件测试（对应 specs.md 第3节 + 第5节 缓冲区溢出）
// ========================================================================

// TestAdvanced_ProtocolFrameTruncatedHeader 验证收到不完整的协议帧头时服务器不崩溃
//
// 对应 specs.md 场景 "协议帧长度不足"
//
// 验证点：
//   - 发送不足 9 字节的帧头后断开连接，服务器不崩溃
//   - 新客户端仍可正常连接和操作
func TestAdvanced_ProtocolFrameTruncatedHeader(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.stop(t)

	// Step 1: 发送不完整的帧头（仅4字节）
	conn := tc.connect(t)
	_, err := conn.Write([]byte{0x01, 0x00, 0x00, 0x00}) // 只有4字节
	if err != nil {
		t.Fatalf("Write truncated header: %v", err)
	}
	conn.Close()
	t.Log("  [Step1] Sent 4-byte truncated header, connection closed")

	// 等待服务器处理断开事件
	time.Sleep(100 * time.Millisecond)

	// Step 2: 验证服务器仍可正常处理新客户端请求
	conn2 := tc.connect(t)
	defer conn2.Close()

	status, _ := sendRequest(t, conn2, uint8(protocol.CMD_SET),
		[]byte("after-trunc-hdr"), []byte("works"))
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("SET after truncated header: expected SUCCESS, got 0x%02X", status)
	}

	status, val := sendRequest(t, conn2, uint8(protocol.CMD_GET),
		[]byte("after-trunc-hdr"), nil)
	if status != uint8(protocol.SUCCESS) || string(val) != "works" {
		t.Fatalf("GET after truncated header: expected 'works', got '%s' (0x%02X)",
			string(val), status)
	}
	t.Log("  [Step2] Server still functional after truncated header")

	// Step 3: 发送空帧（仅9字节帧头，无数据）
	conn3 := tc.connect(t)
	emptyHeader := make([]byte, 9)
	emptyHeader[0] = uint8(protocol.CMD_INFO)
	binary.BigEndian.PutUint32(emptyHeader[1:5], 0) // KeyLen=0
	binary.BigEndian.PutUint32(emptyHeader[5:9], 0) // ValueLen=0
	_, err = conn3.Write(emptyHeader)
	if err != nil {
		t.Fatalf("Write empty header: %v", err)
	}

	// 读取 INFO 响应（应成功）
	respHeader := make([]byte, 9)
	_, err = conn3.Read(respHeader)
	if err != nil {
		t.Fatalf("Read response for valid frame after truncated: %v", err)
	}
	valueLen := binary.BigEndian.Uint32(respHeader[5:9])
	body := make([]byte, 1+int(valueLen))
	_, err = conn3.Read(body)
	if err != nil {
		t.Fatalf("Read response body: %v", err)
	}
	if body[0] != uint8(protocol.SUCCESS) {
		t.Fatalf("Valid frame after truncated: expected SUCCESS, got 0x%02X", body[0])
	}
	conn3.Close()
	t.Log("  [Step3] Valid INFO frame processed after truncated header scenario")

	t.Log("[PASS] Protocol frame truncated header")
}

// TestAdvanced_ProtocolFrameTruncatedData 验证收到帧头声称的数据量与实际不符时服务器不崩溃
//
// 对应 specs.md 场景 "校验码错误" + "协议帧长度不足"
//
// 验证点：
//   - 帧头声明 KeyLen=100 但只发送 50 字节数据后断开，服务器不崩溃
//   - 服务器正确清理异常连接资源
//   - 新客户端可正常连接和操作
func TestAdvanced_ProtocolFrameTruncatedData(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.stop(t)

	// Step 1: 发送帧头声明 KeyLen=100，但只发送 50 字节数据
	conn := tc.connect(t)

	header := make([]byte, 9)
	header[0] = uint8(protocol.CMD_GET)
	binary.BigEndian.PutUint32(header[1:5], 100) // KeyLen=100
	binary.BigEndian.PutUint32(header[5:9], 0)   // ValueLen=0

	conn.Write(header)
	conn.Write(make([]byte, 50)) // 只发送 50 字节（不足声明的 100）
	conn.Close()                 // 提前关闭连接
	t.Log("  [Step1] Sent header with KeyLen=100 but only 50 bytes data, then closed")

	time.Sleep(100 * time.Millisecond)

	// Step 2: 验证服务器仍正常
	conn2 := tc.connect(t)
	defer conn2.Close()

	status, _ := sendRequest(t, conn2, uint8(protocol.CMD_SET),
		[]byte("after-trunc-data"), []byte("works"))
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("SET after truncated data: expected SUCCESS, got 0x%02X", status)
	}
	t.Log("  [Step2] Server functional after truncated data")

	// Step 3: 发送帧头声明 KeyLen=0, ValueLen=10000，只发送 5000 字节后断开
	conn3 := tc.connect(t)
	header2 := make([]byte, 9)
	header2[0] = uint8(protocol.CMD_SET)
	binary.BigEndian.PutUint32(header2[1:5], 0)     // KeyLen=0
	binary.BigEndian.PutUint32(header2[5:9], 10000) // ValueLen=10000

	conn3.Write(header2)
	conn3.Write(make([]byte, 5000)) // 只发送 5000 字节
	conn3.Close()
	t.Log("  [Step3] Sent header with ValueLen=10000 but only 5000 bytes data, then closed")

	time.Sleep(100 * time.Millisecond)

	// Step 4: 最终验证服务器仍正常
	conn4 := tc.connect(t)
	defer conn4.Close()

	status, val := sendRequest(t, conn4, uint8(protocol.CMD_GET),
		[]byte("after-trunc-data"), nil)
	if status != uint8(protocol.SUCCESS) || string(val) != "works" {
		t.Fatalf("GET final verification: expected 'works', got '%s' (0x%02X)",
			string(val), status)
	}
	t.Log("  [Step4] Server still fully functional after multiple truncated frames")

	t.Log("[PASS] Protocol frame truncated data")
}

// TestAdvanced_ProtocolOversizedValue 验证协议层正确拒绝超大 Key/Value
//
// 对应 specs.md 场景 "超大值的SET操作" + "协议帧超大导致缓冲区溢出"
//
// 验证点：
//   - EncodeRequest 拒绝 Value 超过 MaxValueLength（1MB）的请求
//   - EncodeRequest 拒绝 Key 超过 MaxKeyLength（1MB）的请求
//   - ValidateFrame 检测到 oversized KeyLen/ValueLen 时返回相应错误码
func TestAdvanced_ProtocolOversizedValue(t *testing.T) {
	// 1. EncodeRequest 拒绝超大 Value（> 1MB）
	bigValue := make([]byte, protocol.MaxValueLength+1)
	_, err := protocol.EncodeRequest(uint8(protocol.CMD_SET), []byte("bigkey"), bigValue)
	if err == nil {
		t.Fatal("EncodeRequest should reject value > MaxValueLength")
	}
	t.Log("  [1] EncodeRequest rejects value > 1MB: ", err)

	// 2. EncodeRequest 拒绝超大 Key（> 1MB）
	bigKey := make([]byte, protocol.MaxKeyLength+1)
	_, err = protocol.EncodeRequest(uint8(protocol.CMD_GET), bigKey, nil)
	if err == nil {
		t.Fatal("EncodeRequest should reject key > MaxKeyLength")
	}
	t.Log("  [2] EncodeRequest rejects key > 1MB: ", err)

	// 3. ValidateFrame 检测 oversized KeyLen
	frame := &protocol.ProtocolFrame{
		Command:  uint8(protocol.CMD_SET),
		KeyLen:   uint32(protocol.MaxKeyLength + 1),
		ValueLen: 0,
		Key:      make([]byte, protocol.MaxKeyLength+1),
		Value:    []byte{},
	}
	err = protocol.ValidateFrame(frame)
	if err == nil {
		t.Fatal("ValidateFrame should reject oversized KeyLen")
	}
	if protocol.GetErrorCode(err) != protocol.ERROR_INVALID_KEY {
		t.Fatalf("Expected ERROR_INVALID_KEY for oversized key, got %v",
			protocol.GetErrorCode(err))
	}
	t.Log("  [3] ValidateFrame rejects oversized KeyLen: ", err)

	// 4. ValidateFrame 检测 oversized ValueLen
	frame2 := &protocol.ProtocolFrame{
		Command:  uint8(protocol.CMD_SET),
		KeyLen:   4,
		ValueLen: uint32(protocol.MaxValueLength + 1),
		Key:      []byte("test"),
		Value:    make([]byte, protocol.MaxValueLength+1),
	}
	err = protocol.ValidateFrame(frame2)
	if err == nil {
		t.Fatal("ValidateFrame should reject oversized ValueLen")
	}
	if protocol.GetErrorCode(err) != protocol.ERROR_INVALID_VALUE {
		t.Fatalf("Expected ERROR_INVALID_VALUE for oversized value, got %v",
			protocol.GetErrorCode(err))
	}
	t.Log("  [4] ValidateFrame rejects oversized ValueLen: ", err)

	// 5. 验证正常大小的 Key/Value 仍然通过（边界条件）
	valueAtLimit := make([]byte, protocol.MaxValueLength)
	_, err = protocol.EncodeRequest(uint8(protocol.CMD_SET), []byte("normal"), valueAtLimit)
	if err != nil {
		t.Fatalf("EncodeRequest should accept value == MaxValueLength: %v", err)
	}
	t.Log("  [5] EncodeRequest accepts value == 1MB (boundary)")

	t.Log("[PASS] Protocol oversized value validation")
}

// TestAdvanced_ProtocolFrameValidationMismatch 验证协议帧校验函数正确检测长度不匹配
//
// 对应 specs.md 场景 "校验码错误"
//
// 验证点：
//   - KeyLen 声明值与实际 Key 数据长度不匹配时返回 ERROR_FRAME_MISMATCH
//   - ValueLen 声明值与实际 Value 数据长度不匹配时返回 ERROR_FRAME_MISMATCH
//   - nil 帧返回非 nil 错误
//   - 合法帧通过验证
func TestAdvanced_ProtocolFrameValidationMismatch(t *testing.T) {
	// 1. KeyLen 声明 100，实际 Key 数据只有 50 字节 → 不匹配
	frame := &protocol.ProtocolFrame{
		Command:  uint8(protocol.CMD_GET),
		KeyLen:   100, // 声明的 Key 长度
		ValueLen: 0,
		Key:      make([]byte, 50), // 实际 Key 数据长度
		Value:    []byte{},
	}
	err := protocol.ValidateFrame(frame)
	if err == nil {
		t.Fatal("ValidateFrame should reject KeyLen mismatch")
	}
	if protocol.GetErrorCode(err) != protocol.ERROR_FRAME_MISMATCH {
		t.Fatalf("Expected ERROR_FRAME_MISMATCH for key length mismatch, got %v",
			protocol.GetErrorCode(err))
	}
	t.Log("  [1] KeyLen mismatch detected: ", err)

	// 2. ValueLen 声明 200，实际 Value 数据只有 100 字节 → 不匹配
	frame2 := &protocol.ProtocolFrame{
		Command:  uint8(protocol.CMD_SET),
		KeyLen:   4,
		ValueLen: 200,
		Key:      []byte("test"),
		Value:    make([]byte, 100),
	}
	err = protocol.ValidateFrame(frame2)
	if err == nil {
		t.Fatal("ValidateFrame should reject ValueLen mismatch")
	}
	if protocol.GetErrorCode(err) != protocol.ERROR_FRAME_MISMATCH {
		t.Fatalf("Expected ERROR_FRAME_MISMATCH for value length mismatch, got %v",
			protocol.GetErrorCode(err))
	}
	t.Log("  [2] ValueLen mismatch detected: ", err)

	// 3. nil 帧
	err = protocol.ValidateFrame(nil)
	if err == nil {
		t.Fatal("ValidateFrame should reject nil frame")
	}
	t.Log("  [3] nil frame rejected: ", err)

	// 4. 合法帧应通过验证
	validFrame := &protocol.ProtocolFrame{
		Command:  uint8(protocol.CMD_SET),
		KeyLen:   4,
		ValueLen: 6,
		Key:      []byte("test"),
		Value:    []byte("value!"),
	}
	err = protocol.ValidateFrame(validFrame)
	if err != nil {
		t.Fatalf("ValidateFrame should accept valid frame: %v", err)
	}
	t.Log("  [4] Valid frame accepted")

	// 5. 零长度帧（INFO 命令）
	infoFrame := &protocol.ProtocolFrame{
		Command:  uint8(protocol.CMD_INFO),
		KeyLen:   0,
		ValueLen: 0,
		Key:      []byte{},
		Value:    []byte{},
	}
	err = protocol.ValidateFrame(infoFrame)
	if err != nil {
		t.Fatalf("ValidateFrame should accept INFO frame: %v", err)
	}
	t.Log("  [5] INFO frame (zero-length) accepted")

	t.Log("[PASS] Protocol frame validation mismatch")
}
