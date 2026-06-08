// Package client 实现分布式缓存系统的 TCP 客户端测试工具
//
// 提供功能：
//   - TestClient：封装 TCP 连接和协议帧收发
//   - TestCluster：一键创建完整测试集群（哈希环 + 缓存节点 + TCP 服务器 + 主从复制）
//   - TestReport：测试报告生成器（覆盖率统计、测试结果）
//   - AssertXxx：断言辅助函数
//
// 运行方式：
//
//	go test ./tests/client/ -v -count=1 -timeout 120s -coverprofile=coverage.out
//	go tool cover -html=coverage.out -o coverage_report.html
package client

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

// ========================================================================
// TCP 客户端测试工具
// ========================================================================

// TestClient TCP 客户端测试工具
// 封装 TCP 连接和协议帧的发送/接收操作，提供类型安全的 API
type TestClient struct {
	conn    net.Conn
	t       *testing.T
	timeout time.Duration
}

// NewTestClient 创建并连接到指定地址的 TCP 测试客户端
// timeout 为读写超时时间，0 表示不设超时
func NewTestClient(t *testing.T, address string, timeout time.Duration) *TestClient {
	t.Helper()

	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		t.Fatalf("TestClient: failed to connect to %s: %v", address, err)
	}

	if timeout > 0 {
		conn.SetDeadline(time.Now().Add(timeout))
	}

	return &TestClient{
		conn:    conn,
		t:       t,
		timeout: timeout,
	}
}

// Close 关闭客户端连接
func (tc *TestClient) Close() error {
	return tc.conn.Close()
}

// RemoteAddr 返回客户端的远程地址
func (tc *TestClient) RemoteAddr() string {
	if tc.conn != nil {
		return tc.conn.RemoteAddr().String()
	}
	return ""
}

// SendRequest 发送协议请求并读取响应
// 返回响应的 status code 和 value 数据
// 适用于主测试 goroutine（出错时调用 t.Fatalf）
func (tc *TestClient) SendRequest(cmd uint8, key, value []byte) (uint8, []byte) {
	tc.t.Helper()

	// 编码并发送请求
	req, err := protocol.EncodeRequest(cmd, key, value)
	if err != nil {
		tc.t.Fatalf("TestClient.EncodeRequest: %v", err)
	}

	if _, err := tc.conn.Write(req); err != nil {
		tc.t.Fatalf("TestClient.Write: %v", err)
	}

	// 读取响应头（9字节）
	header := make([]byte, protocol.FrameHeaderSize)
	if _, err := io.ReadFull(tc.conn, header); err != nil {
		tc.t.Fatalf("TestClient.ReadHeader: %v", err)
	}

	// 解析 ValueLen（bytes 5-8）
	valueLen := binary.BigEndian.Uint32(header[5:9])

	// 读取响应体：Status(1B) + Value(ValueLen B)
	bodySize := 1 + int(valueLen)
	body := make([]byte, bodySize)
	if bodySize > 0 {
		if _, err := io.ReadFull(tc.conn, body); err != nil {
			tc.t.Fatalf("TestClient.ReadBody: %v", err)
		}
	}

	status := body[0]
	var respValue []byte
	if int(valueLen) > 0 {
		respValue = body[1 : 1+int(valueLen)]
	}

	return status, respValue
}

// SendRequestSafe 发送协议请求并读取响应（用于子 goroutine）
// 不调用 t.Fatalf，返回 error 供调用方处理
func (tc *TestClient) SendRequestSafe(cmd uint8, key, value []byte) (uint8, []byte, error) {
	req, err := protocol.EncodeRequest(cmd, key, value)
	if err != nil {
		return 0xFF, nil, fmt.Errorf("encode request: %w", err)
	}

	if _, err := tc.conn.Write(req); err != nil {
		return 0xFF, nil, fmt.Errorf("write request: %w", err)
	}

	header := make([]byte, protocol.FrameHeaderSize)
	if _, err := io.ReadFull(tc.conn, header); err != nil {
		return 0xFF, nil, fmt.Errorf("read response header: %w", err)
	}

	valueLen := binary.BigEndian.Uint32(header[5:9])
	bodySize := 1 + int(valueLen)
	body := make([]byte, bodySize)
	if bodySize > 0 {
		if _, err := io.ReadFull(tc.conn, body); err != nil {
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

// SendRaw 发送原始字节数据到服务器（不经过协议编码）
// 用于测试异常帧、截断帧等场景
func (tc *TestClient) SendRaw(data []byte) (int, error) {
	return tc.conn.Write(data)
}

// ReadResponse 读取服务器响应（返回原始字节）
// 用于测试异常场景下服务器的响应
func (tc *TestClient) ReadResponse(bufSize int) ([]byte, error) {
	buf := make([]byte, bufSize)
	n, err := tc.conn.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

// ---- 高级封装方法 ----

// Set 发送 SET 命令并验证返回 SUCCESS
func (tc *TestClient) Set(key, value string) (uint8, []byte) {
	tc.t.Helper()
	return tc.SendRequest(uint8(protocol.CMD_SET), []byte(key), []byte(value))
}

// Get 发送 GET 命令并返回结果
func (tc *TestClient) Get(key string) (uint8, []byte) {
	tc.t.Helper()
	return tc.SendRequest(uint8(protocol.CMD_GET), []byte(key), nil)
}

// Delete 发送 DELETE 命令并返回结果
func (tc *TestClient) Delete(key string) (uint8, []byte) {
	tc.t.Helper()
	return tc.SendRequest(uint8(protocol.CMD_DELETE), []byte(key), nil)
}

// Info 发送 INFO 命令并解析 JSON 响应
func (tc *TestClient) Info() (uint8, map[string]interface{}) {
	tc.t.Helper()
	status, raw := tc.SendRequest(uint8(protocol.CMD_INFO), []byte{}, nil)
	if status != uint8(protocol.SUCCESS) {
		return status, nil
	}
	var result map[string]interface{}
	if err := json.Unmarshal(raw, &result); err != nil {
		tc.t.Fatalf("TestClient.Info: failed to parse JSON: %v", err)
	}
	return status, result
}

// ========================================================================
// 测试集群管理
// ========================================================================

// TestClusterOption 测试集群配置选项
type TestClusterOption struct {
	NumNodes     int    // 缓存节点数量（默认3）
	Capacity     int    // 每个节点的 LRU 缓存容量（默认10000）
	VirtualNodes int    // 每个物理节点的虚拟节点数（默认100）
	Address      string // 监听地址（默认 ":0" 使用随机端口）
}

// DefaultClusterOptions 返回默认集群配置
func DefaultClusterOptions() TestClusterOption {
	return TestClusterOption{
		NumNodes:     3,
		Capacity:     10000,
		VirtualNodes: 100,
		Address:      ":0",
	}
}

// TestCluster 完整的测试集群
// 包含哈希环、缓存节点、TCP 服务器、主从复制控制器
type TestCluster struct {
	Ring    *shard.HashRing
	Nodes   []*node.CacheNode
	Server  *server.TCPServer
	RC      *replication.ReplicationController
	Address string // 服务器实际监听地址
	opts    TestClusterOption
}

// NewTestCluster 创建并启动完整的测试集群
// 使用随机端口（:0）避免端口冲突
func NewTestCluster(t *testing.T, opts TestClusterOption) *TestCluster {
	t.Helper()

	if opts.NumNodes <= 0 {
		opts.NumNodes = 3
	}
	if opts.Capacity <= 0 {
		opts.Capacity = 10000
	}
	if opts.VirtualNodes <= 0 {
		opts.VirtualNodes = 100
	}
	if opts.Address == "" {
		opts.Address = ":0"
	}

	// 1. 创建一致性哈希环
	ring, err := shard.NewHashRing(opts.VirtualNodes)
	if err != nil {
		t.Fatalf("NewTestCluster: failed to create hash ring: %v", err)
	}

	// 2. 创建缓存节点
	var nodes []*node.CacheNode
	for i := 0; i < opts.NumNodes; i++ {
		id := fmt.Sprintf("TestNode-%d", i+1)
		n, err := node.NewCacheNode(id, opts.Capacity)
		if err != nil {
			t.Fatalf("NewTestCluster: failed to create node %s: %v", id, err)
		}
		if err := ring.AddNode(id); err != nil {
			t.Fatalf("NewTestCluster: failed to add node %s to ring: %v", id, err)
		}
		if err := n.Init(ring); err != nil {
			t.Fatalf("NewTestCluster: failed to init node %s: %v", id, err)
		}
		if err := n.Start(); err != nil {
			t.Fatalf("NewTestCluster: failed to start node %s: %v", id, err)
		}
		nodes = append(nodes, n)
	}

	// 3. 创建 TCP 服务器
	srv, err := server.NewTCPServer(opts.Address, nodes, ring)
	if err != nil {
		t.Fatalf("NewTestCluster: failed to create TCP server: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("NewTestCluster: failed to start TCP server: %v", err)
	}

	// 4. 创建主从复制控制器
	rc, err := replication.NewReplicationController(nodes)
	if err != nil {
		t.Fatalf("NewTestCluster: failed to create replication controller: %v", err)
	}

	return &TestCluster{
		Ring:    ring,
		Nodes:   nodes,
		Server:  srv,
		RC:      rc,
		Address: srv.Address(),
		opts:    opts,
	}
}

// Stop 关闭测试集群，释放所有资源
func (tc *TestCluster) Stop(t *testing.T) {
	t.Helper()
	if err := tc.Server.Stop(); err != nil {
		t.Logf("Warning: failed to stop server: %v", err)
	}
	for _, n := range tc.Nodes {
		if err := n.Stop(); err != nil {
			t.Logf("Warning: failed to stop node: %v", err)
		}
	}
}

// Connect 创建新的 TCP 测试客户端连接
func (tc *TestCluster) Connect(t *testing.T) *TestClient {
	t.Helper()
	return NewTestClient(t, tc.Address, 5*time.Second)
}

// ConnectRaw 创建原始 TCP 连接（不封装 TestClient）
func (tc *TestCluster) ConnectRaw(t *testing.T) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", tc.Address, 5*time.Second)
	if err != nil {
		t.Fatalf("ConnectRaw: failed to connect to %s: %v", tc.Address, err)
	}
	return conn
}

// NodeByID 根据 ID 查找缓存节点
func (tc *TestCluster) NodeByID(id string) *node.CacheNode {
	for _, n := range tc.Nodes {
		if n.GetNodeID() == id {
			return n
		}
	}
	return nil
}

// ========================================================================
// 断言辅助函数
// ========================================================================

// AssertStatus 断言响应状态码等于期望值
func AssertStatus(t *testing.T, actual uint8, expected protocol.ErrorCode, msg string) {
	t.Helper()
	if actual != uint8(expected) {
		t.Fatalf("%s: expected status %s(0x%02X), got 0x%02X",
			msg, expected.String(), uint8(expected), actual)
	}
}

// AssertSuccess 断言响应状态码为 SUCCESS
func AssertSuccess(t *testing.T, status uint8, msg string) {
	t.Helper()
	AssertStatus(t, status, protocol.SUCCESS, msg)
}

// AssertValue 断言响应值等于期望值
func AssertValue(t *testing.T, actual []byte, expected string, msg string) {
	t.Helper()
	if string(actual) != expected {
		t.Fatalf("%s: expected '%s', got '%s'", msg, expected, string(actual))
	}
}

// AssertEmptyValue 断言响应值为空
func AssertEmptyValue(t *testing.T, actual []byte, msg string) {
	t.Helper()
	if len(actual) != 0 {
		t.Fatalf("%s: expected empty value, got '%s'", msg, string(actual))
	}
}

// AssertError 断言函数返回非 nil 错误
func AssertError(t *testing.T, err error, msg string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected error, got nil", msg)
	}
}

// AssertNoError 断言函数返回 nil 错误
func AssertNoError(t *testing.T, err error, msg string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: expected no error, got %v", msg, err)
	}
}

// AssertEqual 断言两个值相等（泛型风格，用于 int/string 等）
func AssertEqual[T comparable](t *testing.T, actual, expected T, msg string) {
	t.Helper()
	if actual != expected {
		t.Fatalf("%s: expected %v, got %v", msg, expected, actual)
	}
}

// AssertTrue 断言条件为 true
func AssertTrue(t *testing.T, condition bool, msg string) {
	t.Helper()
	if !condition {
		t.Fatalf("%s: expected true, got false", msg)
	}
}

// AssertFalse 断言条件为 false
func AssertFalse(t *testing.T, condition bool, msg string) {
	t.Helper()
	if condition {
		t.Fatalf("%s: expected false, got true", msg)
	}
}

// ========================================================================
// 并发测试辅助
// ========================================================================

// ConcurrentResult 并发测试结果
type ConcurrentResult struct {
	ClientID int
	OpIndex  int
	Err      error
}

// RunConcurrentTest 并发测试执行器
// numClients: 客户端数量
// opsPerClient: 每个客户端的操作数
// fn: 测试函数（clientID, opIndex）
func RunConcurrentTest(t *testing.T, numClients, opsPerClient int,
	fn func(clientID, opIndex int, conn net.Conn) error) {

	t.Helper()

	var wg sync.WaitGroup
	errCh := make(chan *ConcurrentResult, numClients*opsPerClient)

	// 获取集群地址（从测试上下文中）
	// 注意：调用方需要确保在创建集群后调用此函数
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			// 注意：fn 负责创建/关闭连接
			for j := 0; j < opsPerClient; j++ {
				// fn 接收 nil conn，由 fn 自行管理连接
				if err := fn(clientID, j, nil); err != nil {
					errCh <- &ConcurrentResult{ClientID: clientID, OpIndex: j, Err: err}
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for result := range errCh {
		t.Errorf("Client %d Op %d: %v", result.ClientID, result.OpIndex, result.Err)
	}
}

// ========================================================================
// 测试报告生成器
// ========================================================================

// TestReport 测试报告数据
type TestReport struct {
	TotalTests   int                // 总测试数
	PassedTests  int                // 通过数
	FailedTests  int                // 失败数
	SkippedTests int                // 跳过数
	Coverage     map[string]float64 // 各包覆盖率
	Results      []TestResult       // 详细测试结果
	StartTime    time.Time          // 开始时间
	EndTime      time.Time          // 结束时间
}

// TestResult 单个测试结果
type TestResult struct {
	Name     string        // 测试名称
	Category string        // 测试分类（Protocol/Cache/Shard/Server/Replication）
	Duration time.Duration // 执行时长
	Status   string        // PASS/FAIL/SKIP
	Error    string        // 失败原因
}

// ReportWriter 报告写入器
type ReportWriter struct {
	report TestReport
	mu     sync.Mutex
}

// NewReportWriter 创建新的报告写入器
func NewReportWriter() *ReportWriter {
	return &ReportWriter{
		report: TestReport{
			Coverage:  make(map[string]float64),
			Results:   make([]TestResult, 0),
			StartTime: time.Now(),
		},
	}
}

// Record 记录单个测试结果
func (rw *ReportWriter) Record(name, category, status string, duration time.Duration, errMsg string) {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	rw.report.TotalTests++
	rw.report.Results = append(rw.report.Results, TestResult{
		Name:     name,
		Category: category,
		Duration: duration,
		Status:   status,
		Error:    errMsg,
	})

	switch status {
	case "PASS":
		rw.report.PassedTests++
	case "FAIL":
		rw.report.FailedTests++
	case "SKIP":
		rw.report.SkippedTests++
	}
}

// SetCoverage 设置包覆盖率
func (rw *ReportWriter) SetCoverage(pkg string, pct float64) {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	rw.report.Coverage[pkg] = pct
}

// Finalize 完成报告
func (rw *ReportWriter) Finalize() TestReport {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	rw.report.EndTime = time.Now()
	return rw.report
}

// FormatText 将测试报告格式化为文本
func (r *TestReport) FormatText() string {
	totalDuration := r.EndTime.Sub(r.StartTime)
	overallCoverage := 0.0
	for _, pct := range r.Coverage {
		overallCoverage += pct
	}
	if len(r.Coverage) > 0 {
		overallCoverage /= float64(len(r.Coverage))
	}

	text := fmt.Sprintf(`
============================================================
  SD-03 分布式缓存系统 - 测试报告
============================================================

  测试时间: %s
  总耗时:   %v

------------------------------------------------------------
  测试结果汇总
------------------------------------------------------------
  总测试数:  %d
  通过:      %d
  失败:      %d
  跳过:      %d
  通过率:    %.1f%%
------------------------------------------------------------
  覆盖率统计
------------------------------------------------------------
`,
		r.StartTime.Format("2006-01-02 15:04:05"),
		totalDuration,
		r.TotalTests, r.PassedTests, r.FailedTests, r.SkippedTests,
		float64(r.PassedTests)/float64(r.TotalTests)*100,
	)

	if len(r.Coverage) > 0 {
		for pkg, pct := range r.Coverage {
			text += fmt.Sprintf("  %-30s %.1f%%\n", pkg, pct)
		}
		text += fmt.Sprintf("  %-30s %.1f%%\n", "整体覆盖率", overallCoverage)
	} else {
		text += "  （覆盖率数据未提供，请使用 -coverprofile 参数生成）\n"
	}

	// 按分类输出结果
	categories := []string{"Protocol", "Cache", "Shard", "Server", "Replication"}
	for _, cat := range categories {
		text += fmt.Sprintf("\n------------------------------------------------------------\n")
		text += fmt.Sprintf("  %s 测试详情\n", cat)
		text += fmt.Sprintf("------------------------------------------------------------\n")

		count := 0
		for _, result := range r.Results {
			if result.Category == cat {
				count++
				statusMark := "✓"
				if result.Status == "FAIL" {
					statusMark = "✗"
				} else if result.Status == "SKIP" {
					statusMark = "○"
				}
				text += fmt.Sprintf("  %s %-50s %s (%v)\n",
					statusMark, result.Name, result.Status, result.Duration.Round(time.Microsecond))
				if result.Error != "" {
					text += fmt.Sprintf("      错误: %s\n", result.Error)
				}
			}
		}
		if count == 0 {
			text += "  （无测试用例）\n"
		}
	}

	text += fmt.Sprintf(`
============================================================
  报告结束
============================================================
`)

	return text
}
