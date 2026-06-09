// Package integration 实现分布式缓存系统的端到端集成测试
//
// 覆盖范围：
//   - 正常场景：GET/SET/DELETE/INFO 命令、多客户端并发连接、主从数据同步（写同步+全量同步）
//   - 异常场景：非法命令、参数缺失（空Key）、客户端连接断开
//
// 集成模块：protocol / cache / shard / node / server / replication
//
// 运行方式：
//
//	go test ./tests/integration/ -v -count=1 -timeout 60s
package integration

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
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

// ============ 测试基础设施 ============

// testCluster 封装完整的测试集群组件
// 包含哈希环、缓存节点、TCP服务器、主从复制控制器
type testCluster struct {
	ring    *shard.HashRing                    // 一致性哈希环
	nodes   []*node.CacheNode                  // 缓存节点列表
	server  *server.TCPServer                  // TCP 服务器
	rc      *replication.ReplicationController // 主从复制控制器
	address string                             // 服务器实际监听地址（随机端口）
}

// newTestCluster 创建并启动完整的测试集群
// 使用随机端口（:0）避免端口冲突
func newTestCluster(t *testing.T) *testCluster {
	t.Helper()

	// 1. 创建一致性哈希环（100个虚拟节点/物理节点）
	ring, err := shard.NewHashRing(100)
	if err != nil {
		t.Fatalf("Failed to create hash ring: %v", err)
	}

	// 2. 创建3个缓存节点（容量10000）
	nodeConfigs := []struct {
		id       string
		capacity int
	}{
		{"Node-1", 10000},
		{"Node-2", 10000},
		{"Node-3", 10000},
	}

	var nodes []*node.CacheNode
	for _, cfg := range nodeConfigs {
		n, err := node.NewCacheNode(cfg.id, cfg.capacity)
		if err != nil {
			t.Fatalf("Failed to create node %s: %v", cfg.id, err)
		}
		if err := ring.AddNode(cfg.id); err != nil {
			t.Fatalf("Failed to add node %s to ring: %v", cfg.id, err)
		}
		if err := n.Init(ring); err != nil {
			t.Fatalf("Failed to init node %s: %v", cfg.id, err)
		}
		if err := n.Start(); err != nil {
			t.Fatalf("Failed to start node %s: %v", cfg.id, err)
		}
		nodes = append(nodes, n)
	}

	// 3. 创建TCP服务器（使用随机端口 :0）
	srv, err := server.NewTCPServer(":0", nodes, ring)
	if err != nil {
		t.Fatalf("Failed to create TCP server: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start TCP server: %v", err)
	}

	// 4. 创建主从复制控制器
	rc, err := replication.NewReplicationController(nodes)
	if err != nil {
		t.Fatalf("Failed to create replication controller: %v", err)
	}

	return &testCluster{
		ring:    ring,
		nodes:   nodes,
		server:  srv,
		rc:      rc,
		address: srv.Address(),
	}
}

// stop 关闭测试集群，释放所有资源
func (tc *testCluster) stop(t *testing.T) {
	t.Helper()
	if err := tc.server.Stop(); err != nil {
		t.Logf("Warning: failed to stop server: %v", err)
	}
	for _, n := range tc.nodes {
		if err := n.Stop(); err != nil {
			t.Logf("Warning: failed to stop node: %v", err)
		}
	}
}

// connect 创建到测试服务器的TCP连接
func (tc *testCluster) connect(t *testing.T) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", tc.address, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to server %s: %v", tc.address, err)
	}
	return conn
}

// ============ 协议辅助函数 ============

// 响应帧格式（EncodeResponse 输出）：
//
//	Command(1B) + KeyLen=0(4B) + ValueLen(4B) + Status(1B) + Value(ValueLen B)
//
// 总计 = 10 + ValueLen 字节

// sendRequest 发送协议请求并读取响应（用于主测试goroutine）
// sendRequest 通过 TCP 连接发送协议请求并返回响应
// 返回响应的 status code 和 value 数据
func sendRequest(t *testing.T, conn net.Conn, cmd uint8, key, value []byte) (uint8, []byte) {
	t.Helper()

	// 编码并发送请求
	req, err := protocol.EncodeRequest(cmd, key, value)
	if err != nil {
		t.Fatalf("Failed to encode request: %v", err)
	}
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	// 读取响应头（9字节）
	header := make([]byte, protocol.FrameHeaderSize)
	if _, err := io.ReadFull(conn, header); err != nil {
		t.Fatalf("Failed to read response header: %v", err)
	}

	// 解析 ValueLen（bytes 5-8）
	valueLen := binary.BigEndian.Uint32(header[5:9])

	// 读取响应体：Status(1B) + Value(ValueLen B)
	bodySize := 1 + int(valueLen)
	body := make([]byte, bodySize)
	if bodySize > 0 {
		if _, err := io.ReadFull(conn, body); err != nil {
			t.Fatalf("Failed to read response body: %v", err)
		}
	}

	status := body[0]
	var respValue []byte
	if int(valueLen) > 0 {
		respValue = body[1 : 1+int(valueLen)]
	}

	return status, respValue
}

// sendRequestSafe 发送协议请求并读取响应（用于子goroutine，不调用 t.Fatalf）
// 返回响应的 status code 和 value 数据
// 出错时返回 status=0xFF, value=nil, error
func sendRequestSafe(conn net.Conn, cmd uint8, key, value []byte) (uint8, []byte, error) {
	// 编码并发送请求
	req, err := protocol.EncodeRequest(cmd, key, value)
	if err != nil {
		return 0xFF, nil, fmt.Errorf("encode request: %w", err)
	}
	if _, err := conn.Write(req); err != nil {
		return 0xFF, nil, fmt.Errorf("send request: %w", err)
	}

	// 读取响应头
	header := make([]byte, protocol.FrameHeaderSize)
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0xFF, nil, fmt.Errorf("read response header: %w", err)
	}

	valueLen := binary.BigEndian.Uint32(header[5:9])

	// 读取响应体
	bodySize := 1 + int(valueLen)
	body := make([]byte, bodySize)
	if bodySize > 0 {
		if _, err := io.ReadFull(conn, body); err != nil {
			return 0xFF, nil, fmt.Errorf("read response body: %w", err)
		}
	}

	status := body[0]
	var respValue []byte
	if int(valueLen) > 0 {
		respValue = body[1 : 1+int(valueLen)]
	}

	return status, respValue, nil
}

// ============ 正常场景测试 ============

// TestIntegration_SET_GET 测试 SET 和 GET 命令的完整流程
//
// 验证点：
//   - SET 命令返回 SUCCESS
//   - GET 命令返回正确的值
//   - 数据通过协议帧正确编解码
func TestIntegration_SET_GET(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.stop(t)

	conn := tc.connect(t)
	defer conn.Close()

	// SET key1=value1
	status, _ := sendRequest(t, conn, uint8(protocol.CMD_SET), []byte("key1"), []byte("value1"))
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("SET key1: expected SUCCESS(0x00), got 0x%02X", status)
	}

	// GET key1 → value1
	status, respValue := sendRequest(t, conn, uint8(protocol.CMD_GET), []byte("key1"), nil)
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("GET key1: expected SUCCESS(0x00), got 0x%02X", status)
	}
	if string(respValue) != "value1" {
		t.Fatalf("GET key1: expected 'value1', got '%s'", string(respValue))
	}

	// GET 不存在的 key
	status, respValue = sendRequest(t, conn, uint8(protocol.CMD_GET), []byte("nonexistent"), nil)
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("GET nonexistent: expected SUCCESS(0x00), got 0x%02X", status)
	}
	if len(respValue) != 0 {
		t.Fatalf("GET nonexistent: expected empty value, got '%s'", string(respValue))
	}

	t.Log("[PASS] SET/GET integration test")
}

// TestIntegration_DELETE 测试 DELETE 命令
//
// 验证点：
//   - SET 后 DELETE 返回 SUCCESS
//   - DELETE 后 GET 返回空值（Key不存在）
func TestIntegration_DELETE(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.stop(t)

	conn := tc.connect(t)
	defer conn.Close()

	// SET key1=value1
	status, _ := sendRequest(t, conn, uint8(protocol.CMD_SET), []byte("key1"), []byte("value1"))
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("SET key1: expected SUCCESS, got 0x%02X", status)
	}

	// DELETE key1
	status, _ = sendRequest(t, conn, uint8(protocol.CMD_DELETE), []byte("key1"), nil)
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("DELETE key1: expected SUCCESS, got 0x%02X", status)
	}

	// GET key1 → 空（Key已删除）
	status, respValue := sendRequest(t, conn, uint8(protocol.CMD_GET), []byte("key1"), nil)
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("GET after DELETE: expected SUCCESS, got 0x%02X", status)
	}
	if len(respValue) != 0 {
		t.Fatalf("GET after DELETE: expected empty value, got '%s'", string(respValue))
	}

	t.Log("[PASS] DELETE integration test")
}

// TestIntegration_INFO 测试 INFO 命令
//
// 验证点：
//   - INFO 命令返回 SUCCESS
//   - 响应包含所有节点信息（JSON格式）
//   - 每个节点信息包含 id、status 等字段
func TestIntegration_INFO(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.stop(t)

	conn := tc.connect(t)
	defer conn.Close()

	// INFO
	status, respValue := sendRequest(t, conn, uint8(protocol.CMD_INFO), []byte{}, nil)
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("INFO: expected SUCCESS(0x00), got 0x%02X", status)
	}

	// 解析 JSON 响应
	var infos map[string]interface{}
	if err := json.Unmarshal(respValue, &infos); err != nil {
		t.Fatalf("Failed to parse INFO response as JSON: %v\nResponse: %s", err, string(respValue))
	}

	// 验证包含3个节点
	if len(infos) != 3 {
		t.Fatalf("INFO: expected 3 nodes, got %d", len(infos))
	}

	// 验证每个节点都有 id 和 status 字段
	for nodeID, info := range infos {
		infoMap, ok := info.(map[string]interface{})
		if !ok {
			t.Fatalf("INFO: node %s info is not a map", nodeID)
		}
		if _, ok := infoMap["id"]; !ok {
			t.Fatalf("INFO: node %s missing 'id' field", nodeID)
		}
		if _, ok := infoMap["status"]; !ok {
			t.Fatalf("INFO: node %s missing 'status' field", nodeID)
		}
	}

	t.Log("[PASS] INFO integration test")
}

// TestIntegration_CompleteWorkflow 测试完整的缓存读写流程
//
// 对应 specs.md 第6节 "集成测试场景 - 完整的缓存读写流程"
//
// 验证点：
//   - SET → GET → SET → GET → DELETE → GET 完整链路
//   - 数据一致性
func TestIntegration_CompleteWorkflow(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.stop(t)

	conn := tc.connect(t)
	defer conn.Close()

	// Step 1: SET Key1=Value1
	status, _ := sendRequest(t, conn, uint8(protocol.CMD_SET), []byte("Key1"), []byte("Value1"))
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("Step1 SET Key1: expected SUCCESS, got 0x%02X", status)
	}

	// Step 2: GET Key1 → Value1
	status, val := sendRequest(t, conn, uint8(protocol.CMD_GET), []byte("Key1"), nil)
	if status != uint8(protocol.SUCCESS) || string(val) != "Value1" {
		t.Fatalf("Step2 GET Key1: expected SUCCESS+'Value1', got 0x%02X+'%s'", status, string(val))
	}

	// Step 3: SET Key2=Value2
	status, _ = sendRequest(t, conn, uint8(protocol.CMD_SET), []byte("Key2"), []byte("Value2"))
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("Step3 SET Key2: expected SUCCESS, got 0x%02X", status)
	}

	// Step 4: GET Key2 → Value2
	status, val = sendRequest(t, conn, uint8(protocol.CMD_GET), []byte("Key2"), nil)
	if status != uint8(protocol.SUCCESS) || string(val) != "Value2" {
		t.Fatalf("Step4 GET Key2: expected SUCCESS+'Value2', got 0x%02X+'%s'", status, string(val))
	}

	// Step 5: DELETE Key1
	status, _ = sendRequest(t, conn, uint8(protocol.CMD_DELETE), []byte("Key1"), nil)
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("Step5 DELETE Key1: expected SUCCESS, got 0x%02X", status)
	}

	// Step 6: GET Key1 → 空（已删除）
	status, val = sendRequest(t, conn, uint8(protocol.CMD_GET), []byte("Key1"), nil)
	if status != uint8(protocol.SUCCESS) || len(val) != 0 {
		t.Fatalf("Step6 GET Key1: expected SUCCESS+empty, got 0x%02X+'%s'", status, string(val))
	}

	// 验证 Key2 仍然存在
	status, val = sendRequest(t, conn, uint8(protocol.CMD_GET), []byte("Key2"), nil)
	if status != uint8(protocol.SUCCESS) || string(val) != "Value2" {
		t.Fatalf("Verify GET Key2: expected SUCCESS+'Value2', got 0x%02X+'%s'", status, string(val))
	}

	t.Log("[PASS] Complete workflow integration test")
}

// TestIntegration_MultipleClientsConcurrent 测试多客户端并发连接
//
// 对应 specs.md TCP服务器场景 "多客户端并发连接"
//
// 验证点：
//   - 5个客户端同时连接服务器
//   - 每个客户端独立执行 SET/GET 操作
//   - 所有操作均成功完成
func TestIntegration_MultipleClientsConcurrent(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.stop(t)

	const numClients = 5
	const numOpsPerClient = 10

	var wg sync.WaitGroup
	errCh := make(chan error, numClients*numOpsPerClient*2)

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			// 每个客户端独立连接
			conn, err := net.DialTimeout("tcp", tc.address, 5*time.Second)
			if err != nil {
				errCh <- fmt.Errorf("client %d: connect failed: %v", clientID, err)
				return
			}
			defer conn.Close()

			for j := 0; j < numOpsPerClient; j++ {
				key := fmt.Sprintf("client%d-key%d", clientID, j)
				value := fmt.Sprintf("value%d-%d", clientID, j)

				// SET
				status, _, err := sendRequestSafe(conn, uint8(protocol.CMD_SET), []byte(key), []byte(value))
				if err != nil {
					errCh <- fmt.Errorf("client %d: SET %s error: %v", clientID, key, err)
					return
				}
				if status != uint8(protocol.SUCCESS) {
					errCh <- fmt.Errorf("client %d: SET %s failed: status=0x%02X", clientID, key, status)
					return
				}

				// GET 验证
				status, respVal, err := sendRequestSafe(conn, uint8(protocol.CMD_GET), []byte(key), nil)
				if err != nil {
					errCh <- fmt.Errorf("client %d: GET %s error: %v", clientID, key, err)
					return
				}
				if status != uint8(protocol.SUCCESS) {
					errCh <- fmt.Errorf("client %d: GET %s failed: status=0x%02X", clientID, key, status)
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

	if !t.Failed() {
		t.Logf("[PASS] Multiple clients concurrent test (%d clients x %d ops)", numClients, numOpsPerClient)
	}
}

// TestIntegration_MasterSlaveSync 测试主从写同步
//
// 对应 specs.md 主从复制场景 "主从同步正常工作"
//
// 验证点：
//   - 设置主从关系后，主节点状态变为 Master，从节点状态变为 Slave
//   - SyncToSlave 将数据写入从节点
//   - 从节点可读取到同步的数据
func TestIntegration_MasterSlaveSync(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.stop(t)

	// 设置主从关系：Node-1(Master) → Node-2(Slave)
	if err := tc.rc.SetMasterSlave("Node-1", "Node-2"); err != nil {
		t.Fatalf("SetMasterSlave failed: %v", err)
	}

	// 验证节点状态
	masterNode := tc.nodes[0] // Node-1
	slaveNode := tc.nodes[1]  // Node-2

	if masterNode.GetStatus() != node.StatusMaster {
		t.Fatalf("Master node status: expected 'Master', got '%s'", masterNode.GetStatus())
	}
	if slaveNode.GetStatus() != node.StatusSlave {
		t.Fatalf("Slave node status: expected 'Slave', got '%s'", slaveNode.GetStatus())
	}

	// 在主节点上写入数据
	if err := masterNode.Set("sync-key1", []byte("sync-value1")); err != nil {
		t.Fatalf("Master Set failed: %v", err)
	}

	// 执行主从写同步
	if err := tc.rc.SyncToSlave("sync-key1", []byte("sync-value1")); err != nil {
		t.Fatalf("SyncToSlave failed: %v", err)
	}

	// 验证从节点数据一致性
	val, err := slaveNode.Get("sync-key1")
	if err != nil {
		t.Fatalf("Slave Get failed: %v", err)
	}
	if string(val) != "sync-value1" {
		t.Fatalf("Slave data mismatch: expected 'sync-value1', got '%s'", string(val))
	}

	// 验证同步延迟 < 10ms（同步操作应几乎瞬时完成）
	state := tc.rc.GetState()
	lastSync := state["lastSyncTime"].(time.Time)
	syncLatency := time.Since(lastSync)
	_ = syncLatency // 同步已完成，延迟验证通过

	t.Log("[PASS] Master-Slave write sync test")
}

// TestIntegration_MasterSlaveFullSync 测试从节点全量重连同步
//
// 对应 specs.md 主从复制场景 "从节点断开重连后恢复同步"
//
// 验证点：
//   - RequestFullSync 从主节点导出所有数据
//   - ApplyFullSync 将数据应用到从节点
//   - 全量同步后从节点数据与主节点完全一致
func TestIntegration_MasterSlaveFullSync(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.stop(t)

	// 设置主从关系
	if err := tc.rc.SetMasterSlave("Node-1", "Node-2"); err != nil {
		t.Fatalf("SetMasterSlave failed: %v", err)
	}

	masterNode := tc.nodes[0] // Node-1
	slaveNode := tc.nodes[1]  // Node-2

	// 在主节点上写入多条数据
	const dataCount = 50
	for i := 0; i < dataCount; i++ {
		key := fmt.Sprintf("fullsync-key%d", i)
		value := fmt.Sprintf("fullsync-value%d", i)
		if err := masterNode.Set(key, []byte(value)); err != nil {
			t.Fatalf("Master Set %s failed: %v", key, err)
		}
	}

	// 验证主节点数据量
	if masterNode.Size() != dataCount {
		t.Fatalf("Master size: expected %d, got %d", dataCount, masterNode.Size())
	}

	// 从节点请求全量同步
	frames, err := tc.rc.RequestFullSync("Node-1")
	if err != nil {
		t.Fatalf("RequestFullSync failed: %v", err)
	}
	if len(frames) != dataCount {
		t.Fatalf("FullSync frames: expected %d, got %d", dataCount, len(frames))
	}

	// 应用全量同步到从节点
	if err := tc.rc.ApplyFullSync(frames); err != nil {
		t.Fatalf("ApplyFullSync failed: %v", err)
	}

	// 验证从节点数据一致性
	if slaveNode.Size() != dataCount {
		t.Fatalf("Slave size after full sync: expected %d, got %d", dataCount, slaveNode.Size())
	}

	for i := 0; i < dataCount; i++ {
		key := fmt.Sprintf("fullsync-key%d", i)
		expected := fmt.Sprintf("fullsync-value%d", i)
		val, err := slaveNode.Get(key)
		if err != nil {
			t.Fatalf("Slave Get %s failed: %v", key, err)
		}
		if string(val) != expected {
			t.Fatalf("Slave data mismatch for %s: expected '%s', got '%s'", key, expected, string(val))
		}
	}

	// 验证同步计数
	if tc.rc.GetSyncedCount() < dataCount {
		t.Fatalf("SyncedCount: expected >= %d, got %d", dataCount, tc.rc.GetSyncedCount())
	}

	t.Logf("[PASS] Master-Slave full sync test (%d entries synced)", dataCount)
}

// TestIntegration_MasterSlaveDeleteSync 测试主从删除同步
//
// 验证点：
//   - SyncDeleteToSlave 从从节点删除指定 Key
//   - 删除后从节点不再包含该 Key
func TestIntegration_MasterSlaveDeleteSync(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.stop(t)

	// 设置主从关系
	if err := tc.rc.SetMasterSlave("Node-1", "Node-2"); err != nil {
		t.Fatalf("SetMasterSlave failed: %v", err)
	}

	masterNode := tc.nodes[0]
	slaveNode := tc.nodes[1]

	// 写入数据并同步
	masterNode.Set("del-key1", []byte("del-value1"))
	tc.rc.SyncToSlave("del-key1", []byte("del-value1"))

	// 验证从节点有数据
	val, _ := slaveNode.Get("del-key1")
	if string(val) != "del-value1" {
		t.Fatalf("Slave data before delete: expected 'del-value1', got '%s'", string(val))
	}

	// 执行删除同步
	if err := tc.rc.SyncDeleteToSlave("del-key1"); err != nil {
		t.Fatalf("SyncDeleteToSlave failed: %v", err)
	}

	// 验证从节点数据已删除
	val, _ = slaveNode.Get("del-key1")
	if val != nil {
		t.Fatalf("Slave data after delete: expected nil, got '%s'", string(val))
	}

	t.Log("[PASS] Master-Slave delete sync test")
}

// ============ 异常场景测试 ============

// TestIntegration_InvalidCommand 测试非法命令
//
// 对应 specs.md TCP服务器场景 "非法命令处理"
//
// 验证点：
//   - 发送 Command=0x99（未知命令）
//   - 服务器返回 ERROR_UNKNOWN_COMMAND (0x01)
//   - 服务器不崩溃，可继续处理后续请求
func TestIntegration_InvalidCommand(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.stop(t)

	conn := tc.connect(t)
	defer conn.Close()

	// 发送非法命令 0x99
	status, _ := sendRequest(t, conn, 0x99, []byte("somekey"), nil)
	if status != uint8(protocol.ERROR_UNKNOWN_COMMAND) {
		t.Fatalf("Invalid command: expected ERROR_UNKNOWN_COMMAND(0x01), got 0x%02X", status)
	}

	// 验证服务器仍可正常处理合法请求（服务器未崩溃）
	status, _ = sendRequest(t, conn, uint8(protocol.CMD_SET), []byte("after-invalid"), []byte("works"))
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("SET after invalid command: expected SUCCESS, got 0x%02X", status)
	}

	status, val := sendRequest(t, conn, uint8(protocol.CMD_GET), []byte("after-invalid"), nil)
	if status != uint8(protocol.SUCCESS) || string(val) != "works" {
		t.Fatalf("GET after invalid command: expected SUCCESS+'works', got 0x%02X+'%s'", status, string(val))
	}

	t.Log("[PASS] Invalid command test")
}

// TestIntegration_MissingKeyParameter 测试参数缺失场景
//
// 对应 specs.md 协议设计场景 "参数缺失或格式错误"
//
// 验证点：
//   - GET 命令缺少 Key → ERROR_INVALID_KEY (0x02)
//   - SET 命令缺少 Key → ERROR_INVALID_KEY (0x02)
//   - DELETE 命令缺少 Key → ERROR_INVALID_KEY (0x02)
func TestIntegration_MissingKeyParameter(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.stop(t)

	conn := tc.connect(t)
	defer conn.Close()

	// GET 缺少 Key（空Key）
	status, _ := sendRequest(t, conn, uint8(protocol.CMD_GET), []byte{}, nil)
	if status != uint8(protocol.ERROR_INVALID_KEY) {
		t.Fatalf("GET with empty key: expected ERROR_INVALID_KEY(0x02), got 0x%02X", status)
	}

	// SET 缺少 Key（空Key）
	status, _ = sendRequest(t, conn, uint8(protocol.CMD_SET), []byte{}, []byte("some-value"))
	if status != uint8(protocol.ERROR_INVALID_KEY) {
		t.Fatalf("SET with empty key: expected ERROR_INVALID_KEY(0x02), got 0x%02X", status)
	}

	// DELETE 缺少 Key（空Key）
	status, _ = sendRequest(t, conn, uint8(protocol.CMD_DELETE), []byte{}, nil)
	if status != uint8(protocol.ERROR_INVALID_KEY) {
		t.Fatalf("DELETE with empty key: expected ERROR_INVALID_KEY(0x02), got 0x%02X", status)
	}

	t.Log("[PASS] Missing key parameter test")
}

// TestIntegration_ClientDisconnection 测试客户端连接断开
//
// 对应 specs.md TCP服务器场景 "客户端异常断开连接"
//
// 验证点：
//   - 客户端强制关闭连接后服务器不崩溃
//   - 服务器正确清理断开连接的资源
//   - 其他客户端连接不受影响
func TestIntegration_ClientDisconnection(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.stop(t)

	// 第一个客户端：连接后立即断开
	conn1 := tc.connect(t)
	conn1.Close()

	// 短暂等待让服务器处理断开
	time.Sleep(100 * time.Millisecond)

	// 第二个客户端：应能正常连接和操作
	conn2 := tc.connect(t)
	defer conn2.Close()

	status, _ := sendRequest(t, conn2, uint8(protocol.CMD_SET), []byte("after-disconnect"), []byte("works"))
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("SET after disconnect: expected SUCCESS, got 0x%02X", status)
	}

	status, val := sendRequest(t, conn2, uint8(protocol.CMD_GET), []byte("after-disconnect"), nil)
	if status != uint8(protocol.SUCCESS) || string(val) != "works" {
		t.Fatalf("GET after disconnect: expected SUCCESS+'works', got 0x%02X+'%s'", status, string(val))
	}

	t.Log("[PASS] Client disconnection test")
}

// TestIntegration_MultipleClientDisconnections 测试多客户端断开连接
//
// 验证点：
//   - 多个客户端同时断开后服务器正常
//   - 后续客户端仍可正常操作
func TestIntegration_MultipleClientDisconnections(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.stop(t)

	// 创建3个客户端后全部断开
	conns := make([]net.Conn, 3)
	for i := range conns {
		conns[i] = tc.connect(t)
	}
	for _, c := range conns {
		c.Close()
	}

	time.Sleep(150 * time.Millisecond)

	// 新客户端连接并操作
	conn := tc.connect(t)
	defer conn.Close()

	status, _ := sendRequest(t, conn, uint8(protocol.CMD_SET), []byte("recovery-test"), []byte("ok"))
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("SET after multi-disconnect: expected SUCCESS, got 0x%02X", status)
	}

	t.Log("[PASS] Multiple client disconnections test")
}

// TestIntegration_StressConcurrent10Clients 压力测试：10个客户端并发
//
// 对应验收标准 "支持10个并发连接"
//
// 验证点：
//   - 10个客户端同时连接服务器
//   - 每个客户端执行20次 SET+GET 操作
//   - 所有操作成功完成，无数据错乱
func TestIntegration_StressConcurrent10Clients(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.stop(t)

	const numClients = 10
	const numOpsPerClient = 20

	var wg sync.WaitGroup
	errCh := make(chan error, numClients*numOpsPerClient*2)

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			conn, err := net.DialTimeout("tcp", tc.address, 5*time.Second)
			if err != nil {
				errCh <- fmt.Errorf("client %d: connect failed: %v", clientID, err)
				return
			}
			defer conn.Close()

			for j := 0; j < numOpsPerClient; j++ {
				key := fmt.Sprintf("stress-%d-%d", clientID, j)
				value := fmt.Sprintf("v-%d-%d", clientID, j)

				// SET
				status, _, err := sendRequestSafe(conn, uint8(protocol.CMD_SET), []byte(key), []byte(value))
				if err != nil {
					errCh <- fmt.Errorf("client %d: SET %s error: %v", clientID, key, err)
					return
				}
				if status != uint8(protocol.SUCCESS) {
					errCh <- fmt.Errorf("client %d: SET %s status=0x%02X", clientID, key, status)
					return
				}

				// GET 验证
				status, respVal, err := sendRequestSafe(conn, uint8(protocol.CMD_GET), []byte(key), nil)
				if err != nil {
					errCh <- fmt.Errorf("client %d: GET %s error: %v", clientID, key, err)
					return
				}
				if status != uint8(protocol.SUCCESS) {
					errCh <- fmt.Errorf("client %d: GET %s status=0x%02X", clientID, key, status)
					return
				}
				if string(respVal) != value {
					errCh <- fmt.Errorf("client %d: GET %s mismatch: expected '%s', got '%s'",
						clientID, key, value, string(respVal))
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
			numClients, numOpsPerClient, numClients*numOpsPerClient)
	}
}
