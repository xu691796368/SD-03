// Package server 单元测试
// 覆盖命令分发逻辑、网络异常处理、多客户端并发等场景
package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/yourusername/sd-03-cache/pkg/node"
	"github.com/yourusername/sd-03-cache/pkg/protocol"
	"github.com/yourusername/sd-03-cache/pkg/shard"
)

// ============ 测试辅助函数 ============

// newTestServer 创建测试用TCP服务器（使用随机端口）
// 返回服务器实例、节点列表和哈希环
func newTestServer(t *testing.T) (*TCPServer, []*node.CacheNode, *shard.HashRing) {
	t.Helper()

	// 创建哈希环（3个虚拟节点，减少测试时间）
	ring, err := shard.NewHashRing(3)
	if err != nil {
		t.Fatalf("Failed to create hash ring: %v", err)
	}

	// 创建缓存节点
	nodes := make([]*node.CacheNode, 3)
	for i := 0; i < 3; i++ {
		nodeID := fmt.Sprintf("Node%d", i)
		n, err := node.NewCacheNode(nodeID, 100)
		if err != nil {
			t.Fatalf("Failed to create node %s: %v", nodeID, err)
		}
		if err := n.Init(ring); err != nil {
			t.Fatalf("Failed to init node %s: %v", nodeID, err)
		}
		if err := n.Start(); err != nil {
			t.Fatalf("Failed to start node %s: %v", nodeID, err)
		}
		if err := ring.AddNode(nodeID); err != nil {
			t.Fatalf("Failed to add node %s to ring: %v", nodeID, err)
		}
		nodes[i] = n
	}

	// 创建服务器（端口0表示随机可用端口）
	srv, err := NewTCPServer("127.0.0.1:0", nodes, ring)
	if err != nil {
		t.Fatalf("Failed to create TCP server: %v", err)
	}

	return srv, nodes, ring
}

// startServer 启动测试服务器并注册清理函数
func startServer(t *testing.T, srv *TCPServer) {
	t.Helper()
	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	t.Cleanup(func() {
		srv.Stop()
	})
	// 等待服务器启动
	time.Sleep(50 * time.Millisecond)
}

// sendRequest 发送请求并读取响应
func sendRequest(t *testing.T, addr string, reqData []byte) ([]byte, error) {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(5 * time.Second))

	// 发送请求
	if _, err := conn.Write(reqData); err != nil {
		return nil, err
	}

	// 读取响应头（9字节）
	header := make([]byte, protocol.FrameHeaderSize)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}

	// 解析响应头
	valueLen := binary.BigEndian.Uint32(header[5:9])

	// 读取剩余数据（Status(1B) + Value）
	remaining := make([]byte, 1+int(valueLen))
	if _, err := io.ReadFull(conn, remaining); err != nil {
		return nil, err
	}

	// 拼接完整响应
	return append(header, remaining...), nil
}

// parseResponse 解析响应，返回命令码、状态码和值
func parseResponse(t *testing.T, data []byte) (cmd uint8, status uint8, value []byte) {
	t.Helper()

	frame, err := protocol.DecodeResponse(data)
	if err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	cmd = frame.Command
	if len(frame.Value) > 0 {
		status = frame.Value[0]
		value = frame.Value[1:]
	}
	return cmd, status, value
}

// buildRequest 构建请求帧数据
func buildRequest(t *testing.T, cmd uint8, key, value []byte) []byte {
	t.Helper()
	data, err := protocol.EncodeRequest(cmd, key, value)
	if err != nil {
		t.Fatalf("Failed to encode request: %v", err)
	}
	return data
}

// ============ fmt import（测试辅助函数需要） ============

// ============ 构造函数测试 ============

// TestNewTCPServer_Valid 测试正常创建服务器
func TestNewTCPServer_Valid(t *testing.T) {
	ring, _ := shard.NewHashRing(10)
	n, _ := node.NewCacheNode("test", 100)
	n.Init(ring)
	n.Start()
	ring.AddNode("test")

	srv, err := NewTCPServer(":0", []*node.CacheNode{n}, ring)
	if err != nil {
		t.Fatalf("NewTCPServer should succeed, got error: %v", err)
	}
	if srv == nil {
		t.Fatal("Server should not be nil")
	}
	if srv.GetNodeCount() != 1 {
		t.Errorf("Expected 1 node, got %d", srv.GetNodeCount())
	}
}

// TestNewTCPServer_EmptyAddress 测试空地址
func TestNewTCPServer_EmptyAddress(t *testing.T) {
	ring, _ := shard.NewHashRing(10)
	n, _ := node.NewCacheNode("test", 100)

	_, err := NewTCPServer("", []*node.CacheNode{n}, ring)
	if err != ErrInvalidAddress {
		t.Errorf("Expected ErrInvalidAddress, got: %v", err)
	}
}

// TestNewTCPServer_NoNodes 测试空节点列表
func TestNewTCPServer_NoNodes(t *testing.T) {
	ring, _ := shard.NewHashRing(10)

	_, err := NewTCPServer(":0", []*node.CacheNode{}, ring)
	if err != ErrNoNodes {
		t.Errorf("Expected ErrNoNodes, got: %v", err)
	}
}

// TestNewTCPServer_NilRing 测试空哈希环
func TestNewTCPServer_NilRing(t *testing.T) {
	n, _ := node.NewCacheNode("test", 100)

	_, err := NewTCPServer(":0", []*node.CacheNode{n}, nil)
	if err != ErrNilRing {
		t.Errorf("Expected ErrNilRing, got: %v", err)
	}
}

// TestNewTCPServer_NilNodesInList 测试节点列表中有nil节点
func TestNewTCPServer_NilNodesInList(t *testing.T) {
	ring, _ := shard.NewHashRing(10)

	_, err := NewTCPServer(":0", []*node.CacheNode{nil}, ring)
	if err != ErrNoNodes {
		t.Errorf("Expected ErrNoNodes (all nil nodes), got: %v", err)
	}
}

// ============ 生命周期测试 ============

// TestServerStartStop 测试服务器启动和停止
func TestServerStartStop(t *testing.T) {
	srv, _, _ := newTestServer(t)

	// 启动
	if err := srv.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if !srv.IsRunning() {
		t.Error("Server should be running after Start()")
	}

	addr := srv.Address()
	if addr == "" {
		t.Error("Address should not be empty after Start()")
	}

	// 停止
	if err := srv.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if srv.IsRunning() {
		t.Error("Server should not be running after Stop()")
	}
}

// TestServerStartTwice 测试重复启动
func TestServerStartTwice(t *testing.T) {
	srv, _, _ := newTestServer(t)
	startServer(t, srv)

	err := srv.Start()
	if err != ErrServerRunning {
		t.Errorf("Expected ErrServerRunning, got: %v", err)
	}
}

// TestServerStopNotStarted 测试停止未启动的服务器
func TestServerStopNotStarted(t *testing.T) {
	srv, _, _ := newTestServer(t)

	err := srv.Stop()
	if err != nil {
		t.Errorf("Stop on non-started server should succeed (idempotent), got: %v", err)
	}
}

// ============ 命令分发测试 ============

// TestCommandDispatch_SET_AND_GET 测试SET和GET命令分发
func TestCommandDispatch_SET_AND_GET(t *testing.T) {
	srv, _, _ := newTestServer(t)
	startServer(t, srv)
	addr := srv.Address()

	key := []byte("TestKey1")
	value := []byte("TestValue1")

	// SET
	setReq := buildRequest(t, uint8(protocol.CMD_SET), key, value)
	respData, err := sendRequest(t, addr, setReq)
	if err != nil {
		t.Fatalf("SET request failed: %v", err)
	}

	cmd, status, _ := parseResponse(t, respData)
	if cmd != uint8(protocol.CMD_SET) {
		t.Errorf("Expected cmd SET(0x%02X), got 0x%02X", protocol.CMD_SET, cmd)
	}
	if status != uint8(protocol.SUCCESS) {
		t.Errorf("Expected status SUCCESS(0x%02X), got 0x%02X", protocol.SUCCESS, status)
	}

	// GET
	getReq := buildRequest(t, uint8(protocol.CMD_GET), key, nil)
	respData, err = sendRequest(t, addr, getReq)
	if err != nil {
		t.Fatalf("GET request failed: %v", err)
	}

	cmd, status, respValue := parseResponse(t, respData)
	if cmd != uint8(protocol.CMD_GET) {
		t.Errorf("Expected cmd GET(0x%02X), got 0x%02X", protocol.CMD_GET, cmd)
	}
	if status != uint8(protocol.SUCCESS) {
		t.Errorf("Expected status SUCCESS(0x%02X), got 0x%02X", protocol.SUCCESS, status)
	}
	if string(respValue) != string(value) {
		t.Errorf("Expected value %q, got %q", string(value), string(respValue))
	}
}

// TestCommandDispatch_SET_GET_Multiple 测试多次SET/GET
func TestCommandDispatch_SET_GET_Multiple(t *testing.T) {
	srv, _, _ := newTestServer(t)
	startServer(t, srv)
	addr := srv.Address()

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))

		// SET
		setReq := buildRequest(t, uint8(protocol.CMD_SET), key, value)
		respData, err := sendRequest(t, addr, setReq)
		if err != nil {
			t.Fatalf("SET %s failed: %v", string(key), err)
		}
		_, status, _ := parseResponse(t, respData)
		if status != uint8(protocol.SUCCESS) {
			t.Errorf("SET %s: expected SUCCESS, got 0x%02X", string(key), status)
		}

		// GET
		getReq := buildRequest(t, uint8(protocol.CMD_GET), key, nil)
		respData, err = sendRequest(t, addr, getReq)
		if err != nil {
			t.Fatalf("GET %s failed: %v", string(key), err)
		}
		_, status, respValue := parseResponse(t, respData)
		if status != uint8(protocol.SUCCESS) {
			t.Errorf("GET %s: expected SUCCESS, got 0x%02X", string(key), status)
		}
		if string(respValue) != string(value) {
			t.Errorf("GET %s: expected %q, got %q", string(key), string(value), string(respValue))
		}
	}
}

// TestCommandDispatch_DELETE 测试DELETE命令分发
func TestCommandDispatch_DELETE(t *testing.T) {
	srv, _, _ := newTestServer(t)
	startServer(t, srv)
	addr := srv.Address()

	key := []byte("DeleteKey")
	value := []byte("DeleteValue")

	// 先SET
	setReq := buildRequest(t, uint8(protocol.CMD_SET), key, value)
	sendRequest(t, addr, setReq)

	// DELETE
	delReq := buildRequest(t, uint8(protocol.CMD_DELETE), key, nil)
	respData, err := sendRequest(t, addr, delReq)
	if err != nil {
		t.Fatalf("DELETE request failed: %v", err)
	}

	cmd, status, _ := parseResponse(t, respData)
	if cmd != uint8(protocol.CMD_DELETE) {
		t.Errorf("Expected cmd DELETE(0x%02X), got 0x%02X", protocol.CMD_DELETE, cmd)
	}
	if status != uint8(protocol.SUCCESS) {
		t.Errorf("Expected status SUCCESS, got 0x%02X", status)
	}

	// GET应返回空值（Key已被删除）
	getReq := buildRequest(t, uint8(protocol.CMD_GET), key, nil)
	respData, err = sendRequest(t, addr, getReq)
	if err != nil {
		t.Fatalf("GET after DELETE failed: %v", err)
	}

	_, status, respValue := parseResponse(t, respData)
	if status != uint8(protocol.SUCCESS) {
		t.Errorf("Expected SUCCESS for GET after DELETE, got 0x%02X", status)
	}
	if len(respValue) != 0 {
		t.Errorf("Expected empty value after DELETE, got %q", string(respValue))
	}
}

// TestCommandDispatch_INFO 测试INFO命令分发
func TestCommandDispatch_INFO(t *testing.T) {
	srv, _, _ := newTestServer(t)
	startServer(t, srv)
	addr := srv.Address()

	// INFO
	infoReq := buildRequest(t, uint8(protocol.CMD_INFO), []byte{}, []byte{})
	respData, err := sendRequest(t, addr, infoReq)
	if err != nil {
		t.Fatalf("INFO request failed: %v", err)
	}

	cmd, status, respValue := parseResponse(t, respData)
	if cmd != uint8(protocol.CMD_INFO) {
		t.Errorf("Expected cmd INFO(0x%02X), got 0x%02X", protocol.CMD_INFO, cmd)
	}
	if status != uint8(protocol.SUCCESS) {
		t.Errorf("Expected status SUCCESS, got 0x%02X", status)
	}
	if len(respValue) == 0 {
		t.Error("INFO response should contain data")
	}
}

// ============ 异常处理测试 ============

// TestUnknownCommand 测试非法命令处理
// 对应spec: 非法命令处理场景 - Command=0x99
func TestUnknownCommand(t *testing.T) {
	srv, _, _ := newTestServer(t)
	startServer(t, srv)
	addr := srv.Address()

	// 构建非法命令帧 (Command=0x99)
	reqData := buildRequest(t, 0x99, []byte("somekey"), nil)
	respData, err := sendRequest(t, addr, reqData)
	if err != nil {
		t.Fatalf("Unknown command request failed: %v", err)
	}

	_, status, _ := parseResponse(t, respData)
	if status != uint8(protocol.ERROR_UNKNOWN_COMMAND) {
		t.Errorf("Expected ERROR_UNKNOWN_COMMAND(0x%02X), got 0x%02X", protocol.ERROR_UNKNOWN_COMMAND, status)
	}
}

// TestEmptyKeyGET 测试GET命令缺少Key参数
// 对应spec: 参数缺失或格式错误场景
func TestEmptyKeyGET(t *testing.T) {
	srv, _, _ := newTestServer(t)
	startServer(t, srv)
	addr := srv.Address()

	// GET with empty key
	reqData := buildRequest(t, uint8(protocol.CMD_GET), []byte{}, nil)
	respData, err := sendRequest(t, addr, reqData)
	if err != nil {
		t.Fatalf("Empty key GET request failed: %v", err)
	}

	_, status, _ := parseResponse(t, respData)
	if status != uint8(protocol.ERROR_INVALID_KEY) {
		t.Errorf("Expected ERROR_INVALID_KEY(0x%02X), got 0x%02X", protocol.ERROR_INVALID_KEY, status)
	}
}

// TestEmptyKeySET 测试SET命令缺少Key参数
func TestEmptyKeySET(t *testing.T) {
	srv, _, _ := newTestServer(t)
	startServer(t, srv)
	addr := srv.Address()

	// SET with empty key
	reqData := buildRequest(t, uint8(protocol.CMD_SET), []byte{}, []byte("value"))
	respData, err := sendRequest(t, addr, reqData)
	if err != nil {
		t.Fatalf("Empty key SET request failed: %v", err)
	}

	_, status, _ := parseResponse(t, respData)
	if status != uint8(protocol.ERROR_INVALID_KEY) {
		t.Errorf("Expected ERROR_INVALID_KEY(0x%02X), got 0x%02X", protocol.ERROR_INVALID_KEY, status)
	}
}

// TestEmptyKeyDELETE 测试DELETE命令缺少Key参数
func TestEmptyKeyDELETE(t *testing.T) {
	srv, _, _ := newTestServer(t)
	startServer(t, srv)
	addr := srv.Address()

	// DELETE with empty key
	reqData := buildRequest(t, uint8(protocol.CMD_DELETE), []byte{}, nil)
	respData, err := sendRequest(t, addr, reqData)
	if err != nil {
		t.Fatalf("Empty key DELETE request failed: %v", err)
	}

	_, status, _ := parseResponse(t, respData)
	if status != uint8(protocol.ERROR_INVALID_KEY) {
		t.Errorf("Expected ERROR_INVALID_KEY(0x%02X), got 0x%02X", protocol.ERROR_INVALID_KEY, status)
	}
}

// TestGETNonExistentKey 测试查询不存在的键
// 对应spec: 查询不存在的键值场景
func TestGETNonExistentKey(t *testing.T) {
	srv, _, _ := newTestServer(t)
	startServer(t, srv)
	addr := srv.Address()

	key := []byte("NonExistentKey999")
	reqData := buildRequest(t, uint8(protocol.CMD_GET), key, nil)
	respData, err := sendRequest(t, addr, reqData)
	if err != nil {
		t.Fatalf("GET non-existent key failed: %v", err)
	}

	_, status, respValue := parseResponse(t, respData)
	if status != uint8(protocol.SUCCESS) {
		t.Errorf("Expected SUCCESS for non-existent key, got 0x%02X", status)
	}
	if len(respValue) != 0 {
		t.Errorf("Expected empty value for non-existent key, got %q", string(respValue))
	}
}

// TestIncompleteFrame 测试协议帧长度不足
// 对应spec: 协议帧长度不足场景 - 只发送帧头，不发送数据
func TestIncompleteFrame(t *testing.T) {
	srv, _, _ := newTestServer(t)
	startServer(t, srv)
	addr := srv.Address()

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(3 * time.Second))

	// 发送一个声称有KeyLen=10但实际不发送Key数据的帧头
	buf := new(bytes.Buffer)
	buf.WriteByte(uint8(protocol.CMD_GET))          // Command
	binary.Write(buf, binary.BigEndian, uint32(10)) // KeyLen=10
	binary.Write(buf, binary.BigEndian, uint32(0))  // ValueLen=0
	conn.Write(buf.Bytes())

	// 服务器应等待数据，读取可能超时或连接关闭
	// 不应该崩溃 - 这是主要测试点
	// 发送不完整数据后等一下，然后关闭连接
	time.Sleep(100 * time.Millisecond)
	conn.Close()

	// 确保服务器仍然在运行
	if !srv.IsRunning() {
		t.Error("Server should still be running after incomplete frame")
	}
}

// TestConnectionClose 测试客户端异常断开
// 对应spec: 客户端异常断开连接场景
func TestConnectionClose(t *testing.T) {
	srv, _, _ := newTestServer(t)
	startServer(t, srv)
	addr := srv.Address()

	// 建立连接后立即关闭
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	conn.Close()

	// 服务器应该仍然在运行
	time.Sleep(100 * time.Millisecond)
	if !srv.IsRunning() {
		t.Error("Server should still be running after client disconnect")
	}

	// 确保可以建立新连接
	conn2, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect after previous disconnect: %v", err)
	}
	conn2.Close()
}

// TestClientDisconnectDuringRequest 测试客户端在请求中途断开
func TestClientDisconnectDuringRequest(t *testing.T) {
	srv, _, _ := newTestServer(t)
	startServer(t, srv)
	addr := srv.Address()

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// 发送部分数据后关闭连接
	conn.Write([]byte{uint8(protocol.CMD_GET)})
	conn.Close()

	// 服务器应该仍然在运行
	time.Sleep(100 * time.Millisecond)
	if !srv.IsRunning() {
		t.Error("Server should still be running after client disconnect during request")
	}
}

// ============ 多客户端并发测试 ============

// TestMultipleClients 测试多客户端并发连接
// 对应spec: 多客户端并发连接场景
func TestMultipleClients(t *testing.T) {
	srv, _, _ := newTestServer(t)
	startServer(t, srv)
	addr := srv.Address()

	numClients := 5
	done := make(chan error, numClients)

	for i := 0; i < numClients; i++ {
		go func(idx int) {
			key := []byte(fmt.Sprintf("concurrent_key_%d", idx))
			value := []byte(fmt.Sprintf("concurrent_value_%d", idx))

			// SET
			setReq := buildRequest(t, uint8(protocol.CMD_SET), key, value)
			setResp, err := sendRequest(t, addr, setReq)
			if err != nil {
				done <- fmt.Errorf("client %d SET failed: %w", idx, err)
				return
			}
			_, status, _ := parseResponse(t, setResp)
			if status != uint8(protocol.SUCCESS) {
				done <- fmt.Errorf("client %d SET status=0x%02X, want SUCCESS", idx, status)
				return
			}

			// GET
			getReq := buildRequest(t, uint8(protocol.CMD_GET), key, nil)
			getResp, err := sendRequest(t, addr, getReq)
			if err != nil {
				done <- fmt.Errorf("client %d GET failed: %w", idx, err)
				return
			}
			_, status, respValue := parseResponse(t, getResp)
			if status != uint8(protocol.SUCCESS) {
				done <- fmt.Errorf("client %d GET status=0x%02X, want SUCCESS", idx, status)
				return
			}
			if string(respValue) != string(value) {
				done <- fmt.Errorf("client %d GET value=%q, want %q", idx, string(respValue), string(value))
				return
			}

			done <- nil
		}(i)
	}

	// 等待所有客户端完成
	for i := 0; i < numClients; i++ {
		if err := <-done; err != nil {
			t.Errorf("Concurrent client error: %v", err)
		}
	}
}

// TestMultipleClientsSequence 测试同一连接上的多次请求
func TestMultipleClientsSequence(t *testing.T) {
	srv, _, _ := newTestServer(t)
	startServer(t, srv)
	addr := srv.Address()

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	// 在同一连接上连续发送多个SET请求
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("seq_key_%d", i))
		value := []byte(fmt.Sprintf("seq_value_%d", i))

		reqData := buildRequest(t, uint8(protocol.CMD_SET), key, value)
		if _, err := conn.Write(reqData); err != nil {
			t.Fatalf("Failed to write request %d: %v", i, err)
		}

		// 读取响应
		header := make([]byte, protocol.FrameHeaderSize)
		if _, err := io.ReadFull(conn, header); err != nil {
			t.Fatalf("Failed to read response header %d: %v", i, err)
		}
		valueLen := binary.BigEndian.Uint32(header[5:9])
		remaining := make([]byte, 1+int(valueLen))
		if _, err := io.ReadFull(conn, remaining); err != nil {
			t.Fatalf("Failed to read response data %d: %v", i, err)
		}

		respData := append(header, remaining...)
		_, status, _ := parseResponse(t, respData)
		if status != uint8(protocol.SUCCESS) {
			t.Errorf("Request %d: expected SUCCESS, got 0x%02X", i, status)
		}
	}
}

// ============ 集成验证测试 ============

// TestFullWorkflow 测试完整工作流：SET → GET → DELETE → GET
// 对应spec: 完整的缓存读写流程
func TestFullWorkflow(t *testing.T) {
	srv, _, _ := newTestServer(t)
	startServer(t, srv)
	addr := srv.Address()

	key := []byte("workflow_key")
	value := []byte("workflow_value")

	// Step 1: SET
	setReq := buildRequest(t, uint8(protocol.CMD_SET), key, value)
	respData, err := sendRequest(t, addr, setReq)
	if err != nil {
		t.Fatalf("SET failed: %v", err)
	}
	_, status, _ := parseResponse(t, respData)
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("SET: expected SUCCESS, got 0x%02X", status)
	}

	// Step 2: GET（应返回之前设置的值）
	getReq := buildRequest(t, uint8(protocol.CMD_GET), key, nil)
	respData, err = sendRequest(t, addr, getReq)
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	_, status, respValue := parseResponse(t, respData)
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("GET: expected SUCCESS, got 0x%02X", status)
	}
	if string(respValue) != string(value) {
		t.Fatalf("GET: expected %q, got %q", string(value), string(respValue))
	}

	// Step 3: DELETE
	delReq := buildRequest(t, uint8(protocol.CMD_DELETE), key, nil)
	respData, err = sendRequest(t, addr, delReq)
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}
	_, status, _ = parseResponse(t, respData)
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("DELETE: expected SUCCESS, got 0x%02X", status)
	}

	// Step 4: GET（应返回空值）
	getReq = buildRequest(t, uint8(protocol.CMD_GET), key, nil)
	respData, err = sendRequest(t, addr, getReq)
	if err != nil {
		t.Fatalf("GET after DELETE failed: %v", err)
	}
	_, status, respValue = parseResponse(t, respData)
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("GET after DELETE: expected SUCCESS, got 0x%02X", status)
	}
	if len(respValue) != 0 {
		t.Fatalf("GET after DELETE: expected empty value, got %q", string(respValue))
	}
}

// TestINFOContainsNodes 测试INFO响应包含节点信息
func TestINFOContainsNodes(t *testing.T) {
	srv, _, _ := newTestServer(t)
	startServer(t, srv)
	addr := srv.Address()

	infoReq := buildRequest(t, uint8(protocol.CMD_INFO), []byte{}, []byte{})
	respData, err := sendRequest(t, addr, infoReq)
	if err != nil {
		t.Fatalf("INFO request failed: %v", err)
	}

	_, status, respValue := parseResponse(t, respData)
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("INFO: expected SUCCESS, got 0x%02X", status)
	}

	// 验证INFO响应包含节点数据
	infoStr := string(respValue)
	for i := 0; i < 3; i++ {
		nodeID := fmt.Sprintf("Node%d", i)
		if !bytes.Contains(respValue, []byte(nodeID)) {
			t.Errorf("INFO response should contain node %q, got: %s", nodeID, infoStr)
		}
	}
}

// TestSETOverwrite 测试SET覆盖已有值
func TestSETOverwrite(t *testing.T) {
	srv, _, _ := newTestServer(t)
	startServer(t, srv)
	addr := srv.Address()

	key := []byte("overwrite_key")
	value1 := []byte("value_v1")
	value2 := []byte("value_v2")

	// SET v1
	setReq := buildRequest(t, uint8(protocol.CMD_SET), key, value1)
	sendRequest(t, addr, setReq)

	// SET v2（覆盖）
	setReq = buildRequest(t, uint8(protocol.CMD_SET), key, value2)
	respData, err := sendRequest(t, addr, setReq)
	if err != nil {
		t.Fatalf("SET overwrite failed: %v", err)
	}
	_, status, _ := parseResponse(t, respData)
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("SET overwrite: expected SUCCESS, got 0x%02X", status)
	}

	// GET（应返回v2）
	getReq := buildRequest(t, uint8(protocol.CMD_GET), key, nil)
	respData, err = sendRequest(t, addr, getReq)
	if err != nil {
		t.Fatalf("GET after overwrite failed: %v", err)
	}
	_, status, respValue := parseResponse(t, respData)
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("GET after overwrite: expected SUCCESS, got 0x%02X", status)
	}
	if string(respValue) != string(value2) {
		t.Errorf("GET after overwrite: expected %q, got %q", string(value2), string(respValue))
	}
}

// TestServerStopCleanup 测试服务器停止后资源清理
func TestServerStopCleanup(t *testing.T) {
	srv, _, _ := newTestServer(t)
	startServer(t, srv)

	// 停止服务器
	if err := srv.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if srv.IsRunning() {
		t.Error("Server should not be running after Stop()")
	}
}

// TestLargeValue 测试较大的Value值
func TestLargeValue(t *testing.T) {
	srv, _, _ := newTestServer(t)
	startServer(t, srv)
	addr := srv.Address()

	key := []byte("large_key")
	// 10KB value
	value := make([]byte, 10*1024)
	for i := range value {
		value[i] = byte(i % 256)
	}

	// SET
	setReq := buildRequest(t, uint8(protocol.CMD_SET), key, value)
	respData, err := sendRequest(t, addr, setReq)
	if err != nil {
		t.Fatalf("SET large value failed: %v", err)
	}
	_, status, _ := parseResponse(t, respData)
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("SET large value: expected SUCCESS, got 0x%02X", status)
	}

	// GET
	getReq := buildRequest(t, uint8(protocol.CMD_GET), key, nil)
	respData, err = sendRequest(t, addr, getReq)
	if err != nil {
		t.Fatalf("GET large value failed: %v", err)
	}
	_, status, respValue := parseResponse(t, respData)
	if status != uint8(protocol.SUCCESS) {
		t.Fatalf("GET large value: expected SUCCESS, got 0x%02X", status)
	}
	if !bytes.Equal(respValue, value) {
		t.Errorf("GET large value: data mismatch, expected %d bytes, got %d bytes", len(value), len(respValue))
	}
}
