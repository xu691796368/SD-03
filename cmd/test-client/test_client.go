// Package main 实现 SD-03 分布式缓存系统的 CLI 交互式 TCP 测试客户端
//
// 功能模块：
//   - 简易测试：三级菜单自动执行（模块→场景→用例）
//   - 自由测试：动态指令支持 SET/GET/DELETE/INFO/主从同步/批量操作等
//   - 客户端设置：自动保存、输出配置、超时设置
//
// 运行方式：
//
//	go run ./cmd/test-client/
package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/yourusername/sd-03-cache/pkg/node"
	"github.com/yourusername/sd-03-cache/pkg/protocol"
	"github.com/yourusername/sd-03-cache/pkg/replication"
	"github.com/yourusername/sd-03-cache/pkg/server"
	"github.com/yourusername/sd-03-cache/pkg/shard"
)

// ========================================================================
// TestContext - testing.T 的 CLI 替代品
// ========================================================================

// testPanic 用于 Fatalf/Fatal 触发的 panic recovery
type testPanic struct {
	message string
}

// TestContext 替代 testing.T，提供兼容 API 供测试函数使用
type TestContext struct {
	name     string
	failed   bool
	logs     []string
	errors   []string
	subTests []SubTestResult
}

// SubTestResult 子测试执行结果
type SubTestResult struct {
	Name    string
	Passed  bool
	Message string
}

// NewTestContext 创建测试上下文
func NewTestContext(name string) *TestContext {
	return &TestContext{name: name}
}

func (tc *TestContext) Helper() {} // 兼容 testing.T.Helper()

func (tc *TestContext) Name() string { return tc.name }

func (tc *TestContext) Fatalf(format string, args ...interface{}) {
	tc.failed = true
	panic(testPanic{message: fmt.Sprintf(format, args...)})
}

func (tc *TestContext) Fatal(args ...interface{}) {
	tc.failed = true
	panic(testPanic{message: fmt.Sprint(args...)})
}

func (tc *TestContext) Logf(format string, args ...interface{}) {
	tc.logs = append(tc.logs, fmt.Sprintf(format, args...))
}

func (tc *TestContext) Log(args ...interface{}) {
	tc.logs = append(tc.logs, fmt.Sprint(args...))
}

func (tc *TestContext) Errorf(format string, args ...interface{}) {
	tc.errors = append(tc.errors, fmt.Sprintf(format, args...))
	tc.failed = true
}

func (tc *TestContext) Error(args ...interface{}) {
	tc.errors = append(tc.errors, fmt.Sprint(args...))
	tc.failed = true
}

func (tc *TestContext) Failed() bool { return tc.failed }

func (tc *TestContext) FailNow() {
	tc.failed = true
	panic(testPanic{message: "FailNow"})
}

// Run 执行子测试
func (tc *TestContext) Run(name string, f func(t *TestContext)) bool {
	sub := NewTestContext(name)
	passed := runWithRecovery(sub, func() { f(sub) })
	tc.subTests = append(tc.subTests, SubTestResult{
		Name: name, Passed: passed, Message: sub.ErrorMessage(),
	})
	if !passed {
		tc.failed = true
	}
	return passed
}

func (tc *TestContext) ErrorMessage() string {
	if len(tc.errors) > 0 {
		return strings.Join(tc.errors, "; ")
	}
	return ""
}

func (tc *TestContext) LogMessages() []string           { return tc.logs }
func (tc *TestContext) SubTestResults() []SubTestResult { return tc.subTests }

// runWithRecovery 带有 panic recovery 的测试执行器
func runWithRecovery(tc *TestContext, f func()) (passed bool) {
	defer func() {
		if r := recover(); r != nil {
			if tp, ok := r.(testPanic); ok {
				tc.errors = append(tc.errors, tp.message)
			} else {
				tc.errors = append(tc.errors, fmt.Sprintf("panic: %v", r))
			}
			tc.failed = true
			passed = false
		}
	}()
	f()
	return !tc.failed
}

// ========================================================================
// 测试结果类型
// ========================================================================

// TestCaseResult 测试用例执行结果
type TestCaseResult struct {
	Name     string
	Category string
	Module   string
	Status   string // PASS, FAIL, SKIP
	Message  string
	Duration time.Duration
	Logs     []string
	SubTests []SubTestResult
}

// ========================================================================
// 测试菜单类型
// ========================================================================

// TestCaseFunc 测试函数签名
type TestCaseFunc func(t *TestContext)

// TestEntry 菜单中的测试条目
type TestEntry struct {
	ID       string
	Name     string
	Category string // "正常", "异常", "边界"
	Desc     string // 测试过程描述（前置场景、测试步骤、验证方式）
	Func     TestCaseFunc
}

// TestModule 测试模块
type TestModule struct {
	ID      string
	Name    string
	Entries []TestEntry
}

// ========================================================================
// 断言辅助函数（与 TestContext 兼容）
// ========================================================================

func assertStatus(t *TestContext, actual uint8, expected protocol.ErrorCode, msg string) {
	t.Helper()
	if actual != uint8(expected) {
		t.Fatalf("%s: expected status %s(0x%02X), got 0x%02X",
			msg, expected.String(), uint8(expected), actual)
	}
}

func assertSuccess(t *TestContext, status uint8, msg string) {
	t.Helper()
	assertStatus(t, status, protocol.SUCCESS, msg)
}

func assertValue(t *TestContext, actual []byte, expected string, msg string) {
	t.Helper()
	if string(actual) != expected {
		t.Fatalf("%s: expected '%s', got '%s'", msg, expected, string(actual))
	}
}

func assertEmptyValue(t *TestContext, actual []byte, msg string) {
	t.Helper()
	if len(actual) != 0 {
		t.Fatalf("%s: expected empty value, got '%s'", msg, string(actual))
	}
}

func assertError(t *TestContext, err error, msg string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected error, got nil", msg)
	}
}

func assertNoError(t *TestContext, err error, msg string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: expected no error, got %v", msg, err)
	}
}

func assertEqual[T comparable](t *TestContext, actual, expected T, msg string) {
	t.Helper()
	if actual != expected {
		t.Fatalf("%s: expected %v, got %v", msg, expected, actual)
	}
}

func assertTrue(t *TestContext, condition bool, msg string) {
	t.Helper()
	if !condition {
		t.Fatalf("%s: expected true, got false", msg)
	}
}

func assertFalse(t *TestContext, condition bool, msg string) {
	t.Helper()
	if condition {
		t.Fatalf("%s: expected false, got true", msg)
	}
}

// ========================================================================
// CLIClient - 核心 CLI 测试客户端
// ========================================================================

// TCPConn 自由模式的 TCP 连接
type TCPConn struct {
	ID      int
	Conn    net.Conn
	Address string
}

// Settings 客户端设置
type Settings struct {
	AutoSave    bool
	OutputDir   string
	Timeout     time.Duration
	Verbose     bool // 详细输出模式
	DisplayMode int  // 显示模式: 1=追加 2=清屏 3=滚动窗口
	ScrollLines int  // 滚动窗口保留条数（模式3生效）
}

// ClusterOptions 嵌入式集群配置
type ClusterOptions struct {
	NumNodes     int
	Capacity     int
	VirtualNodes int
	Address      string
}

// DefaultClusterOptions 默认集群配置
func DefaultClusterOptions() ClusterOptions {
	return ClusterOptions{
		NumNodes:     3,
		Capacity:     10000,
		VirtualNodes: 100,
		Address:      ":0",
	}
}

// EmbeddedCluster 嵌入式测试集群
type EmbeddedCluster struct {
	Ring    *shard.HashRing
	Nodes   []*node.CacheNode
	Server  *server.TCPServer
	RC      *replication.ReplicationController
	Address string
	opts    ClusterOptions
}

// CLIClient CLI 测试客户端
type CLIClient struct {
	cluster     *EmbeddedCluster
	connections map[int]*TCPConn
	activeConn  int
	nextConnID  int
	settings    Settings
	report      []TestCaseResult
	mu          sync.Mutex
	modules     []TestModule
	reader      *bufio.Reader
}

// NewCLIClient 创建 CLI 测试客户端
func NewCLIClient(reader *bufio.Reader) *CLIClient {
	cli := &CLIClient{
		connections: make(map[int]*TCPConn),
		activeConn:  0,
		nextConnID:  1,
		settings: Settings{
			AutoSave:    false,
			OutputDir:   "test_results",
			Timeout:     5 * time.Second,
			Verbose:     true,
			DisplayMode: 2, // 默认清屏模式
			ScrollLines: 5, // 默认保留5条
		},
		report: make([]TestCaseResult, 0),
		reader: reader,
	}
	cli.initModules()
	return cli
}

// initModules 初始化测试模块菜单（由 auto_tests.go 提供 builder 函数）
func (cli *CLIClient) initModules() {
	cli.modules = []TestModule{
		{ID: "1", Name: "协议编解码", Entries: buildProtocolTests()},
		{ID: "2", Name: "LRU缓存", Entries: buildCacheTests()},
		{ID: "3", Name: "一致性哈希", Entries: buildShardTests()},
		{ID: "4", Name: "缓存节点", Entries: buildNodeTests(cli)},
		{ID: "5", Name: "TCP服务", Entries: buildServerTests(cli)},
		{ID: "6", Name: "主从复制", Entries: buildReplicationTests(cli)},
		{ID: "7", Name: "集成测试", Entries: buildIntegrationTests(cli)},
	}
}

// ========================================================================
// 集群管理
// ========================================================================

// EnsureCluster 确保嵌入式集群已启动
func (cli *CLIClient) EnsureCluster() error {
	if cli.cluster != nil {
		return nil
	}
	return cli.CreateCluster(DefaultClusterOptions())
}

// CreateCluster 创建并启动嵌入式测试集群
func (cli *CLIClient) CreateCluster(opts ClusterOptions) error {
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

	ring, err := shard.NewHashRing(opts.VirtualNodes)
	if err != nil {
		return fmt.Errorf("创建哈希环失败: %v", err)
	}

	var nodes []*node.CacheNode
	for i := 0; i < opts.NumNodes; i++ {
		id := fmt.Sprintf("TestNode-%d", i+1)
		n, err := node.NewCacheNode(id, opts.Capacity)
		if err != nil {
			return fmt.Errorf("创建节点 %s 失败: %v", id, err)
		}
		if err := ring.AddNode(id); err != nil {
			return fmt.Errorf("添加节点到环失败: %v", err)
		}
		if err := n.Init(ring); err != nil {
			return fmt.Errorf("初始化节点失败: %v", err)
		}
		if err := n.Start(); err != nil {
			return fmt.Errorf("启动节点失败: %v", err)
		}
		nodes = append(nodes, n)
	}

	srv, err := server.NewTCPServer(opts.Address, nodes, ring)
	if err != nil {
		return fmt.Errorf("创建TCP服务器失败: %v", err)
	}
	if err := srv.Start(); err != nil {
		return fmt.Errorf("启动TCP服务器失败: %v", err)
	}

	rc, err := replication.NewReplicationController(nodes)
	if err != nil {
		return fmt.Errorf("创建复制控制器失败: %v", err)
	}

	cli.cluster = &EmbeddedCluster{
		Ring: ring, Nodes: nodes, Server: srv,
		RC: rc, Address: srv.Address(), opts: opts,
	}

	fmt.Printf("  [+] 测试集群已启动: %s (%d 节点, %d 虚拟节点/物理节点)\n",
		srv.Address(), len(nodes), opts.VirtualNodes)
	return nil
}

// Cleanup 清理所有资源
func (cli *CLIClient) Cleanup() {
	for _, conn := range cli.connections {
		conn.Conn.Close()
	}
	cli.connections = make(map[int]*TCPConn)
	cli.activeConn = 0
	if cli.cluster != nil {
		if cli.cluster.Server != nil {
			cli.cluster.Server.Stop()
		}
		for _, n := range cli.cluster.Nodes {
			n.Stop()
		}
		cli.cluster = nil
	}
}

// ========================================================================
// TCP 连接管理（自由模式）
// ========================================================================

// CreateConnection 创建新的 TCP 连接
func (cli *CLIClient) CreateConnection(addr string) (int, error) {
	if addr == "" {
		if err := cli.EnsureCluster(); err != nil {
			return 0, err
		}
		addr = cli.cluster.Address
	}

	conn, err := net.DialTimeout("tcp", addr, cli.settings.Timeout)
	if err != nil {
		return 0, fmt.Errorf("连接 %s 失败: %v", addr, err)
	}

	id := cli.nextConnID
	cli.nextConnID++
	tcpConn := &TCPConn{ID: id, Conn: conn, Address: addr}
	cli.connections[id] = tcpConn
	if cli.activeConn == 0 {
		cli.activeConn = id
	}
	return id, nil
}

// GetActiveConn 获取当前活跃连接
func (cli *CLIClient) GetActiveConn() (*TCPConn, error) {
	conn, ok := cli.connections[cli.activeConn]
	if !ok {
		return nil, fmt.Errorf("没有活跃连接，请先使用 connect 命令创建连接")
	}
	return conn, nil
}

// CloseConnection 关闭指定连接
func (cli *CLIClient) CloseConnection(id int) error {
	conn, ok := cli.connections[id]
	if !ok {
		return fmt.Errorf("连接 %d 不存在", id)
	}
	conn.Conn.Close()
	delete(cli.connections, id)
	if cli.activeConn == id {
		cli.activeConn = 0
		for k := range cli.connections {
			cli.activeConn = k
			break
		}
	}
	return nil
}

// ========================================================================
// TCP 请求发送（通用）
// ========================================================================

// sendTCPRequest 通过 TCP 连接发送协议请求并读取响应
func sendTCPRequest(conn net.Conn, cmd uint8, key, value []byte) (uint8, []byte, error) {
	req, err := protocol.EncodeRequest(cmd, key, value)
	if err != nil {
		return 0xFF, nil, fmt.Errorf("编码请求失败: %v", err)
	}
	if _, err := conn.Write(req); err != nil {
		return 0xFF, nil, fmt.Errorf("发送请求失败: %v", err)
	}

	header := make([]byte, protocol.FrameHeaderSize)
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0xFF, nil, fmt.Errorf("读取响应头失败: %v", err)
	}

	valueLen := binary.BigEndian.Uint32(header[5:9])
	bodySize := 1 + int(valueLen)
	body := make([]byte, bodySize)
	if bodySize > 0 {
		if _, err := io.ReadFull(conn, body); err != nil {
			return 0xFF, nil, fmt.Errorf("读取响应体失败: %v", err)
		}
	}

	status := body[0]
	var respValue []byte
	if int(valueLen) > 0 {
		respValue = body[1 : 1+int(valueLen)]
	}
	return status, respValue, nil
}

// ========================================================================
// 测试执行
// ========================================================================

// RunTestCase 执行单个测试用例并收集结果
func RunTestCase(name, category, module string, f TestCaseFunc) TestCaseResult {
	tc := NewTestContext(name)
	start := time.Now()
	passed := runWithRecovery(tc, func() { f(tc) })
	duration := time.Since(start)

	status := "PASS"
	msg := ""
	if !passed {
		status = "FAIL"
		msg = tc.ErrorMessage()
	}

	return TestCaseResult{
		Name: name, Category: category, Module: module,
		Status: status, Message: msg, Duration: duration,
		Logs: tc.LogMessages(), SubTests: tc.SubTestResults(),
	}
}

// RunAndPrintResult 执行测试并打印结果
// RunAndPrintResult 执行测试并打印结果
func (cli *CLIClient) RunAndPrintResult(entry TestEntry, module string) TestCaseResult {
	// 打印测试过程描述（优先使用 entry.Desc，否则从描述库查找）
	desc := entry.Desc
	if desc == "" {
		desc = getTestDescription(entry.ID)
	}
	if desc != "" {
		fmt.Printf("  ┌─ 📋 测试说明 ─────────────────────────────────\n")
		for _, line := range strings.Split(desc, "\n") {
			fmt.Printf("  │  %s\n", line)
		}
		fmt.Printf("  └───────────────────────────────────────────────\n")
	}

	result := RunTestCase(entry.Name, entry.Category, module, entry.Func)

	mark := "[PASS]"
	if result.Status == "FAIL" {
		mark = "[FAIL]"
	}
	fmt.Printf("  %s %-45s %v\n", mark, result.Name, result.Duration.Round(time.Microsecond))
	if result.Message != "" {
		fmt.Printf("       -> 错误: %s\n", result.Message)
	}
	for _, sub := range result.SubTests {
		subMark := "[PASS]"
		if !sub.Passed {
			subMark = "[FAIL]"
		}
		fmt.Printf("    %s %s\n", subMark, sub.Name)
		if sub.Message != "" {
			fmt.Printf("       -> %s\n", sub.Message)
		}
	}
	if cli.settings.Verbose {
		for _, log := range result.Logs {
			fmt.Printf("       LOG: %s\n", log)
		}
	}

	cli.mu.Lock()
	cli.report = append(cli.report, result)
	cli.mu.Unlock()

	return result
}

// RunEntries 执行一组测试条目
func (cli *CLIClient) RunEntries(entries []TestEntry, module string) (passed, failed int) {
	fmt.Printf("\n  >>> 开始执行 %s 测试 (%d 用例)\n\n", module, len(entries))
	start := time.Now()

	for _, entry := range entries {
		result := cli.RunAndPrintResult(entry, module)
		if result.Status == "PASS" {
			passed++
		} else {
			failed++
		}
	}

	duration := time.Since(start)
	fmt.Printf("\n  >>> 执行完毕: %d 通过, %d 失败, 耗时 %v\n", passed, failed, duration.Round(time.Millisecond))

	if cli.settings.AutoSave {
		filename := fmt.Sprintf("auto_test_%s.md", time.Now().Format("20060102_150405"))
		if err := cli.SaveReport(filename); err != nil {
			fmt.Printf("  [!] 保存报告失败: %v\n", err)
		} else {
			fmt.Printf("  [+] 报告已保存: %s/%s\n", cli.settings.OutputDir, filename)
		}
	}

	return passed, failed
}

// ========================================================================
// 菜单显示
// ========================================================================

// ShowMainMenu 显示主菜单
func (cli *CLIClient) ShowMainMenu() {
	fmt.Println()
	fmt.Println("============================================================")
	fmt.Println("  SD-03 分布式缓存系统 - TCP 测试客户端")
	fmt.Println("============================================================")
	fmt.Println("  1. 简易测试（全自动菜单，覆盖全系统）")
	fmt.Println("  2. 自由测试（动态指令，全功能覆盖）")
	fmt.Println("  3. 客户端设置")
	fmt.Println("  0. 退出")
	fmt.Println("============================================================")
}

// ShowAutoTestMenu 显示简易测试一级菜单
func (cli *CLIClient) ShowAutoTestMenu() {
	fmt.Println()
	fmt.Println("------------------------------------------------------------")
	fmt.Println("  简易测试 - 模块选择")
	fmt.Println("------------------------------------------------------------")
	for _, m := range cli.modules {
		fmt.Printf("  %s. %-12s (%d 用例)\n", m.ID, m.Name, len(m.Entries))
	}
	fmt.Println("  A. 全部执行")
	fmt.Println("  0. 返回主菜单")
	fmt.Println("------------------------------------------------------------")
}

// ShowCategoryMenu 显示二级菜单（测试场景分类）
func (cli *CLIClient) ShowCategoryMenu(module *TestModule) {
	categories := []string{"正常", "异常", "边界"}
	fmt.Println()
	fmt.Println("------------------------------------------------------------")
	fmt.Printf("  %s - 测试场景选择\n", module.Name)
	fmt.Println("------------------------------------------------------------")

	for i, cat := range categories {
		entries := filterByCategory(module.Entries, cat)
		if len(entries) > 0 {
			fmt.Printf("  %d. %s测试 (%d 用例)\n", i+1, cat, len(entries))
		}
	}
	fmt.Println("  A. 执行全部")
	fmt.Println("  0. 返回上级")
	fmt.Println("------------------------------------------------------------")
}

// ShowTestCaseList 显示三级菜单（具体测试用例）
func (cli *CLIClient) ShowTestCaseList(entries []TestEntry) {
	fmt.Println()
	fmt.Println("------------------------------------------------------------")
	fmt.Println("  测试用例列表")
	fmt.Println("------------------------------------------------------------")
	for i, e := range entries {
		fmt.Printf("  %d. [%s] %s\n", i+1, e.Category, e.Name)
	}
	fmt.Println("  A. 执行全部")
	fmt.Println("  0. 返回上级")
	fmt.Println("------------------------------------------------------------")
}

// ShowFreeModeMenu 显示自由测试帮助
func ShowFreeModeHelp() {
	fmt.Println()
	fmt.Println("------------------------------------------------------------")
	fmt.Println("  自由测试 - 可用命令")
	fmt.Println("------------------------------------------------------------")
	fmt.Println("  连接管理:")
	fmt.Println("    connect [addr]              创建新连接(默认连接本地集群)")
	fmt.Println("    disconnect [id]             断开连接(默认当前活跃)")
	fmt.Println("    use <id>                    切换活跃连接")
	fmt.Println("    list                        列出所有连接")
	fmt.Println()
	fmt.Println("  缓存操作:")
	fmt.Println("    set <key> <value>           SET 操作")
	fmt.Println("    get <key>                   GET 操作")
	fmt.Println("    delete <key>                DELETE 操作")
	fmt.Println("    info                        INFO 操作")
	fmt.Println()
	fmt.Println("  主从同步:")
	fmt.Println("    sync <master> <slave>       配置主从关系")
	fmt.Println("    sync-set <key> <value>      主节点SET并同步到从节点")
	fmt.Println("    sync-del <key>              主节点DELETE并同步到从节点")
	fmt.Println("    full-sync <master>          全量同步")
	fmt.Println("    sync-status                 查看同步状态")
	fmt.Println()
	fmt.Println("  批量操作:")
	fmt.Println("    batch-set <n> [prefix]      批量SET n条数据")
	fmt.Println("    batch-get <n> [prefix]      批量GET验证")
	fmt.Println("    batch-del <n> [prefix]      批量DELETE")
	fmt.Println()
	fmt.Println("  路由与节点:")
	fmt.Println("    route <key>                 查看Key路由到的节点")
	fmt.Println("    nodes                       查看所有节点信息")
	fmt.Println("    ring-info                   查看哈希环信息")
	fmt.Println()
	fmt.Println("  测试场景:")
	fmt.Println("    lru-evict <cap>             测试LRU淘汰(capacity=cap)")
	fmt.Println("    stress <clients> <ops>      并发压力测试")
	fmt.Println("    raw <hex>                   发送原始十六进制字节")
	fmt.Println()
	fmt.Println("  其他:")
	fmt.Println("    help                        显示此帮助")
	fmt.Println("    usage                       显示具体使用示例")
	fmt.Println("    back                        返回主菜单")
	fmt.Println("------------------------------------------------------------")
	fmt.Println("  提示: 输入 usage 查看完整的使用示例和测试流程")
	fmt.Println("------------------------------------------------------------")
}

// ShowSettingsMenu 显示设置菜单
func (cli *CLIClient) ShowSettingsMenu() {
	fmt.Println()
	fmt.Println("------------------------------------------------------------")
	fmt.Println("  客户端设置")
	fmt.Println("------------------------------------------------------------")
	fmt.Printf("  1. 自动保存测试结果:  %v\n", cli.settings.AutoSave)
	fmt.Printf("  2. 输出目录:          %s\n", cli.settings.OutputDir)
	fmt.Printf("  3. 连接超时:          %v\n", cli.settings.Timeout)
	fmt.Printf("  4. 详细日志:          %v\n", cli.settings.Verbose)
	fmt.Printf("  5. 显示模式:          %s\n", displayModeName(cli.settings.DisplayMode))
	if cli.settings.DisplayMode == 3 {
		fmt.Printf("  6. 滚动行数:          %d\n", cli.settings.ScrollLines)
	} else {
		fmt.Printf("  6. 滚动行数:          %d (仅模式3生效)\n", cli.settings.ScrollLines)
	}
	fmt.Println("  7. 查看当前完整配置")
	fmt.Println("  8. 清空测试报告记录")
	fmt.Println("  0. 返回主菜单")
	fmt.Println("------------------------------------------------------------")
}

func filterByCategory(entries []TestEntry, category string) []TestEntry {
	var result []TestEntry
	for _, e := range entries {
		if e.Category == category {
			result = append(result, e)
		}
	}
	return result
}

// ========================================================================
// 输入处理
// ========================================================================

func readInput(reader *bufio.Reader, prompt string) string {
	fmt.Print(prompt)
	line, _ := reader.ReadString('\n')
	return strings.TrimSpace(line)
}

// clearScreen 清除终端屏幕（使用 ANSI 转义码）
func clearScreen() {
	fmt.Print("\x1b[2J\x1b[1;1H")
}

// captureOutput 捕获函数执行期间的 stdout 输出（不显示到终端）
func captureOutput(f func()) string {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	var buf bytes.Buffer
	done := make(chan struct{})
	go func() {
		io.Copy(&buf, r)
		close(done)
	}()

	f()
	w.Close()
	<-done
	os.Stdout = oldStdout

	return buf.String()
}

// displayModeName 返回显示模式的中文名称
func displayModeName(mode int) string {
	switch mode {
	case 1:
		return "追加模式（持续追加）"
	case 2:
		return "清屏模式（仅显示菜单）"
	case 3:
		return "滚动窗口模式"
	default:
		return "未知"
	}
}

// ========================================================================
// 报告生成
// ========================================================================

// SaveReport 将测试报告保存为 Markdown 文件
func (cli *CLIClient) SaveReport(filename string) error {
	if err := os.MkdirAll(cli.settings.OutputDir, 0755); err != nil {
		return fmt.Errorf("创建输出目录失败: %v", err)
	}

	if filename == "" {
		filename = fmt.Sprintf("test_report_%s.md", time.Now().Format("20060102_150405"))
	}

	path := cli.settings.OutputDir + string(os.PathSeparator) + filename
	var sb strings.Builder

	sb.WriteString("# SD-03 分布式缓存系统 - 测试报告\n\n")
	sb.WriteString(fmt.Sprintf("**生成时间**: %s\n\n", time.Now().Format("2006-01-02 15:04:05")))

	total := len(cli.report)
	passed := 0
	failed := 0
	for _, r := range cli.report {
		if r.Status == "PASS" {
			passed++
		} else {
			failed++
		}
	}

	sb.WriteString("## 汇总\n\n")
	sb.WriteString("| 指标 | 值 |\n")
	sb.WriteString("|------|----|\n")
	sb.WriteString(fmt.Sprintf("| 总测试数 | %d |\n", total))
	sb.WriteString(fmt.Sprintf("| 通过 | %d |\n", passed))
	sb.WriteString(fmt.Sprintf("| 失败 | %d |\n", failed))
	if total > 0 {
		sb.WriteString(fmt.Sprintf("| 通过率 | %.1f%% |\n", float64(passed)/float64(total)*100))
	}
	sb.WriteString("\n")

	moduleOrder := []string{"协议编解码", "LRU缓存", "一致性哈希", "缓存节点", "TCP服务", "主从复制", "集成测试"}
	for _, mod := range moduleOrder {
		var modResults []TestCaseResult
		for _, r := range cli.report {
			if r.Module == mod {
				modResults = append(modResults, r)
			}
		}
		if len(modResults) == 0 {
			continue
		}

		sb.WriteString(fmt.Sprintf("## %s\n\n", mod))
		sb.WriteString("| 状态 | 用例名称 | 类别 | 耗时 |\n")
		sb.WriteString("|------|----------|------|------|\n")
		for _, r := range modResults {
			mark := "[PASS]"
			if r.Status == "FAIL" {
				mark = "[FAIL]"
			}
			sb.WriteString(fmt.Sprintf("| %s | %s | %s | %v |\n",
				mark, r.Name, r.Category, r.Duration.Round(time.Microsecond)))
			if r.Message != "" {
				sb.WriteString(fmt.Sprintf("| | *错误: %s* | | |\n", r.Message))
			}
		}
		sb.WriteString("\n")
	}

	return os.WriteFile(path, []byte(sb.String()), 0644)
}

// ShowCurrentConfig 显示当前完整配置
func (cli *CLIClient) ShowCurrentConfig() {
	fmt.Println()
	fmt.Println("------------------------------------------------------------")
	fmt.Println("  当前客户端配置")
	fmt.Println("------------------------------------------------------------")
	fmt.Printf("  自动保存:     %v\n", cli.settings.AutoSave)
	fmt.Printf("  输出目录:     %s\n", cli.settings.OutputDir)
	fmt.Printf("  连接超时:     %v\n", cli.settings.Timeout)
	fmt.Printf("  详细日志:     %v\n", cli.settings.Verbose)
	fmt.Printf("  显示模式:     %s (%d)\n", displayModeName(cli.settings.DisplayMode), cli.settings.DisplayMode)
	fmt.Printf("  滚动行数:     %d\n", cli.settings.ScrollLines)

	if cli.cluster != nil {
		fmt.Printf("  集群状态:     已启动\n")
		fmt.Printf("  集群地址:     %s\n", cli.cluster.Address)
		fmt.Printf("  节点数量:     %d\n", len(cli.cluster.Nodes))
		fmt.Printf("  虚拟节点数:   %d\n", cli.cluster.Ring.VirtualNodeCount())
		fmt.Printf("  复制控制器:   已初始化\n")
	} else {
		fmt.Printf("  集群状态:     未启动\n")
	}

	fmt.Printf("  TCP 连接数:   %d\n", len(cli.connections))
	fmt.Printf("  活跃连接ID:   %d\n", cli.activeConn)
	fmt.Printf("  已记录结果:   %d 条\n", len(cli.report))
	fmt.Println("------------------------------------------------------------")
}

// ========================================================================
// TestConnect - 测试用 TCP 连接（auto_tests 用）
// ========================================================================

// TestConnect 创建测试用的 TCP 连接（内部使用，不计入自由模式连接表）
func (cli *CLIClient) TestConnect(t *TestContext) net.Conn {
	t.Helper()
	if err := cli.EnsureCluster(); err != nil {
		t.Fatalf("启动集群失败: %v", err)
	}
	conn, err := net.DialTimeout("tcp", cli.cluster.Address, cli.settings.Timeout)
	if err != nil {
		t.Fatalf("连接 %s 失败: %v", cli.cluster.Address, err)
	}
	return conn
}

// TestConnectRaw 创建原始 TCP 连接（不断言）
func (cli *CLIClient) TestConnectRaw(t *TestContext) net.Conn {
	t.Helper()
	if err := cli.EnsureCluster(); err != nil {
		t.Fatalf("启动集群失败: %v", err)
	}
	conn, err := net.DialTimeout("tcp", cli.cluster.Address, cli.settings.Timeout)
	if err != nil {
		t.Fatalf("连接 %s 失败: %v", cli.cluster.Address, err)
	}
	return conn
}
