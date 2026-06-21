// Package main - free_mode.go
// 自由测试模式：动态指令支持多客户端、缓存操作、主从同步、批量操作等
// 客户端设置：自动保存、配置查看、超时设置等
package main

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yourusername/sd-03-cache/pkg/node"
	"github.com/yourusername/sd-03-cache/pkg/protocol"
)

// ========================================================================
// 自由测试模式
// ========================================================================

// RunFreeMode 自由测试主循环
func (cli *CLIClient) RunFreeMode() {
	if err := cli.EnsureCluster(); err != nil {
		fmt.Printf("  [!] 启动集群失败: %v\n", err)
		return
	}

	// 自动创建第一个连接
	connID, err := cli.CreateConnection("")
	if err != nil {
		fmt.Printf("  [!] 创建连接失败: %v\n", err)
		return
	}
	cli.activeConn = connID
	fmt.Printf("  [+] 已自动创建连接 #%d -> %s\n", connID, cli.cluster.Address)

	// 进入时根据显示模式显示初始帮助
	if cli.settings.DisplayMode >= 2 {
		clearScreen()
	}
	ShowFreeModeHelp()

	var outputHistory []string

	for {
		prompt := fmt.Sprintf("free:%d> ", cli.activeConn)
		input := readInput(cli.reader, prompt)
		if input == "" {
			continue
		}

		parts := strings.Fields(input)
		cmd := strings.ToLower(parts[0])
		args := parts[1:]

		// 执行命令（模式3时捕获输出用于历史回显）
		shouldReturn := false
		skipCapture := cmd == "help" || cmd == "h" || cmd == "?" || cmd == "usage"
		if cli.settings.DisplayMode == 3 && !skipCapture {
			output := captureOutput(func() {
				shouldReturn = cli.executeFreeModeCommand(cmd, args)
			})
			if !shouldReturn {
				entry := fmt.Sprintf("  > %s\n%s\n", input, output)
				outputHistory = append(outputHistory, entry)

				// 模式3：每次命令后重绘，只保留最近N条历史 + 帮助
				clearScreen()
				start := 0
				if len(outputHistory) > cli.settings.ScrollLines {
					start = len(outputHistory) - cli.settings.ScrollLines
				}
				for _, e := range outputHistory[start:] {
					fmt.Print(e)
				}
				ShowFreeModeHelp()
			}
		} else {
			shouldReturn = cli.executeFreeModeCommand(cmd, args)
		}

		if shouldReturn {
			return
		}
	}
}

// executeFreeModeCommand 执行自由测试模式命令，返回是否应退出
func (cli *CLIClient) executeFreeModeCommand(cmd string, args []string) bool {
	switch cmd {
	case "back", "0":
		return true
	case "help", "h", "?":
		ShowFreeModeHelp()
	case "usage":
		ShowUsageExamples()

	// ---- 连接管理 ----
	case "connect":
		cli.handleConnect(args)
	case "disconnect":
		cli.handleDisconnect(args)
	case "use":
		cli.handleUse(args)
	case "list":
		cli.handleList()

	// ---- 缓存操作 ----
	case "set":
		cli.handleSet(args)
	case "get":
		cli.handleGet(args)
	case "delete", "del":
		cli.handleDelete(args)
	case "info":
		cli.handleInfo()

	// ---- 主从同步 ----
	case "sync":
		cli.handleSync(args)
	case "sync-set":
		cli.handleSyncSet(args)
	case "sync-del":
		cli.handleSyncDel(args)
	case "full-sync":
		cli.handleFullSync(args)
	case "sync-status":
		cli.handleSyncStatus()

	// ---- 批量操作 ----
	case "batch-set":
		cli.handleBatchSet(args)
	case "batch-get":
		cli.handleBatchGet(args)
	case "batch-del":
		cli.handleBatchDel(args)

	// ---- 路由与节点 ----
	case "route":
		cli.handleRoute(args)
	case "nodes":
		cli.handleNodes()
	case "ring-info":
		cli.handleRingInfo()

	// ---- 测试场景 ----
	case "lru-evict":
		cli.handleLRUEvict(args)
	case "stress":
		cli.handleStress(args)
	case "raw":
		cli.handleRaw(args)

	default:
		fmt.Printf("  [!] 未知命令: %s (输入 help 查看帮助)\n", cmd)
	}
	return false
}

// ========================================================================
// 连接管理命令
// ========================================================================

// handleConnect 处理 connect 命令，创建新的 TCP 连接
func (cli *CLIClient) handleConnect(args []string) {
	addr := ""
	if len(args) > 0 {
		addr = args[0]
	}
	connID, err := cli.CreateConnection(addr)
	if err != nil {
		fmt.Printf("  [!] 连接失败: %v\n", err)
		return
	}
	cli.activeConn = connID
	actual := cli.cluster.Address
	if conn, ok := cli.connections[connID]; ok {
		actual = conn.Address
	}
	fmt.Printf("  [+] 连接 #%d 已创建 -> %s (当前活跃)\n", connID, actual)
}

// handleDisconnect 处理 disconnect 命令，关闭指定或当前活跃连接
func (cli *CLIClient) handleDisconnect(args []string) {
	id := cli.activeConn
	if len(args) > 0 {
		n, err := strconv.Atoi(args[0])
		if err != nil {
			fmt.Printf("  [!] 无效连接ID: %s\n", args[0])
			return
		}
		id = n
	}
	if err := cli.CloseConnection(id); err != nil {
		fmt.Printf("  [!] %v\n", err)
		return
	}
	fmt.Printf("  [+] 连接 #%d 已断开\n", id)
}

// handleUse 处理 use 命令，切换活跃连接
func (cli *CLIClient) handleUse(args []string) {
	if len(args) == 0 {
		fmt.Println("  [!] 用法: use <连接ID>")
		return
	}
	id, err := strconv.Atoi(args[0])
	if err != nil {
		fmt.Printf("  [!] 无效ID: %s\n", args[0])
		return
	}
	if _, ok := cli.connections[id]; !ok {
		fmt.Printf("  [!] 连接 %d 不存在\n", id)
		return
	}
	cli.activeConn = id
	fmt.Printf("  [+] 已切换到连接 #%d\n", id)
}

// handleList 处理 list 命令，列出所有 TCP 连接状态
func (cli *CLIClient) handleList() {
	if len(cli.connections) == 0 {
		fmt.Println("  (无活跃连接)")
		return
	}
	fmt.Println("  ID  | 本地地址              | 远程地址              | 状态")
	fmt.Println("  ----|------------------------|------------------------|---------")
	for id, conn := range cli.connections {
		active := ""
		if id == cli.activeConn {
			active = " <-- 活跃"
		}
		localAddr := conn.Conn.LocalAddr().String()
		remoteAddr := conn.Address
		fmt.Printf("  %-4d| %-22s | %-22s | %s\n", id, localAddr, remoteAddr, active)
	}
}

// ========================================================================
// 缓存操作命令
// ========================================================================

// handleSet 处理 set 命令，向服务器发送 SET 请求
func (cli *CLIClient) handleSet(args []string) {
	if len(args) < 2 {
		fmt.Println("  [!] 用法: set <key> <value>")
		return
	}
	key, value := args[0], strings.Join(args[1:], " ")
	conn, err := cli.GetActiveConn()
	if err != nil {
		fmt.Printf("  [!] %v\n", err)
		return
	}

	status, val, err := sendTCPRequest(conn.Conn, uint8(protocol.CMD_SET), []byte(key), []byte(value))
	if err != nil {
		fmt.Printf("  [!] 请求失败: %v\n", err)
		return
	}
	if status == uint8(protocol.SUCCESS) {
		fmt.Printf("  [+] SET OK: %s = %s\n", key, value)
	} else {
		fmt.Printf("  [-] SET FAILED: status=0x%02X (%s)\n", status, protocol.ErrorCode(status).String())
		if len(val) > 0 {
			fmt.Printf("      响应: %s\n", string(val))
		}
	}
}

// handleGet 处理 get 命令，向服务器发送 GET 请求
func (cli *CLIClient) handleGet(args []string) {
	if len(args) < 1 {
		fmt.Println("  [!] 用法: get <key>")
		return
	}
	key := args[0]
	conn, err := cli.GetActiveConn()
	if err != nil {
		fmt.Printf("  [!] %v\n", err)
		return
	}

	status, val, err := sendTCPRequest(conn.Conn, uint8(protocol.CMD_GET), []byte(key), nil)
	if err != nil {
		fmt.Printf("  [!] 请求失败: %v\n", err)
		return
	}
	if status == uint8(protocol.SUCCESS) {
		if len(val) > 0 {
			fmt.Printf("  [+] GET OK: %s = %s\n", key, string(val))
		} else {
			fmt.Printf("  [o] GET OK: %s = (nil)\n", key)
		}
	} else {
		fmt.Printf("  [-] GET FAILED: status=0x%02X (%s)\n", status, protocol.ErrorCode(status).String())
	}
}

// handleDelete 处理 delete 命令，向服务器发送 DELETE 请求
func (cli *CLIClient) handleDelete(args []string) {
	if len(args) < 1 {
		fmt.Println("  [!] 用法: delete <key>")
		return
	}
	key := args[0]
	conn, err := cli.GetActiveConn()
	if err != nil {
		fmt.Printf("  [!] %v\n", err)
		return
	}

	status, val, err := sendTCPRequest(conn.Conn, uint8(protocol.CMD_DELETE), []byte(key), nil)
	if err != nil {
		fmt.Printf("  [!] 请求失败: %v\n", err)
		return
	}
	if status == uint8(protocol.SUCCESS) {
		fmt.Printf("  [+] DELETE OK: %s\n", key)
	} else {
		fmt.Printf("  [-] DELETE FAILED: status=0x%02X (%s)\n", status, protocol.ErrorCode(status).String())
		if len(val) > 0 {
			fmt.Printf("      响应: %s\n", string(val))
		}
	}
}

// handleInfo 处理 info 命令，获取并展示服务器信息
func (cli *CLIClient) handleInfo() {
	conn, err := cli.GetActiveConn()
	if err != nil {
		fmt.Printf("  [!] %v\n", err)
		return
	}

	status, val, err := sendTCPRequest(conn.Conn, uint8(protocol.CMD_INFO), []byte{}, nil)
	if err != nil {
		fmt.Printf("  [!] 请求失败: %v\n", err)
		return
	}
	if status == uint8(protocol.SUCCESS) && len(val) > 0 {
		var info map[string]interface{}
		if err := json.Unmarshal(val, &info); err != nil {
			fmt.Printf("  [+] INFO (raw): %s\n", string(val))
		} else {
			fmt.Println("  [+] INFO:")
			if b, err := json.MarshalIndent(info, "      ", "  "); err == nil {
				fmt.Printf("      %s\n", string(b))
			}
		}
	} else {
		fmt.Printf("  [-] INFO FAILED: status=0x%02X\n", status)
	}
}

// ========================================================================
// 主从同步命令
// ========================================================================

// handleSync 处理 sync 命令，配置主从复制关系
func (cli *CLIClient) handleSync(args []string) {
	if len(args) < 2 {
		fmt.Println("  [!] 用法: sync <masterID> <slaveID>")
		fmt.Println("      可用节点ID:")
		if cli.cluster != nil {
			for _, n := range cli.cluster.Nodes {
				fmt.Printf("        %s\n", n.GetNodeID())
			}
		}
		return
	}
	masterID, slaveID := args[0], args[1]
	if err := cli.cluster.RC.SetMasterSlave(masterID, slaveID); err != nil {
		fmt.Printf("  [-] 配置主从失败: %v\n", err)
		return
	}
	fmt.Printf("  [+] 主从关系已配置: Master=%s, Slave=%s\n", masterID, slaveID)
}

// handleSyncSet 处理 sync-set 命令，主节点 SET 并同步到从节点
func (cli *CLIClient) handleSyncSet(args []string) {
	if len(args) < 2 {
		fmt.Println("  [!] 用法: sync-set <key> <value>")
		return
	}
	if cli.cluster == nil || cli.cluster.RC.GetMasterID() == "" {
		fmt.Println("  [!] 请先使用 sync 命令配置主从关系")
		return
	}
	key, value := args[0], strings.Join(args[1:], " ")
	masterID := cli.cluster.RC.GetMasterID()
	var masterNode *node.CacheNode
	for _, n := range cli.cluster.Nodes {
		if n.GetNodeID() == masterID {
			masterNode = n
			break
		}
	}
	if masterNode == nil {
		fmt.Printf("  [!] 主节点 %s 不存在\n", masterID)
		return
	}

	if err := masterNode.Set(key, []byte(value)); err != nil {
		fmt.Printf("  [-] 主节点SET失败: %v\n", err)
		return
	}
	if err := cli.cluster.RC.SyncToSlave(key, []byte(value)); err != nil {
		fmt.Printf("  [-] 同步失败: %v\n", err)
		return
	}
	fmt.Printf("  [+] sync-set OK: %s = %s (已同步到从节点)\n", key, value)
}

// handleSyncDel 处理 sync-del 命令，主节点 DELETE 并同步到从节点
func (cli *CLIClient) handleSyncDel(args []string) {
	if len(args) < 1 {
		fmt.Println("  [!] 用法: sync-del <key>")
		return
	}
	if cli.cluster == nil || cli.cluster.RC.GetMasterID() == "" {
		fmt.Println("  [!] 请先使用 sync 命令配置主从关系")
		return
	}
	key := args[0]
	if err := cli.cluster.RC.SyncDeleteToSlave(key); err != nil {
		fmt.Printf("  [-] 删除同步失败: %v\n", err)
		return
	}
	fmt.Printf("  [+] sync-del OK: %s (已同步删除到从节点)\n", key)
}

// handleFullSync 处理 full-sync 命令，执行全量同步
func (cli *CLIClient) handleFullSync(args []string) {
	if len(args) < 1 {
		fmt.Println("  [!] 用法: full-sync <masterID>")
		return
	}
	masterID := args[0]
	frames, err := cli.cluster.RC.RequestFullSync(masterID)
	if err != nil {
		fmt.Printf("  [-] 全量同步请求失败: %v\n", err)
		return
	}
	fmt.Printf("  [+] 获取主节点数据: %d 条\n", len(frames))

	if err := cli.cluster.RC.ApplyFullSync(frames); err != nil {
		fmt.Printf("  [-] 应用全量同步失败: %v\n", err)
		return
	}
	fmt.Printf("  [+] 全量同步完成: %d 条数据已同步\n", len(frames))
}

// handleSyncStatus 处理 sync-status 命令，查询并展示复制状态
func (cli *CLIClient) handleSyncStatus() {
	if cli.cluster == nil {
		fmt.Println("  [!] 集群未启动")
		return
	}
	rc := cli.cluster.RC
	fmt.Println("  同步状态:")
	fmt.Printf("    MasterID:    %s\n", rc.GetMasterID())
	fmt.Printf("    同步次数:     %d\n", rc.GetSyncedCount())
	fmt.Printf("    从节点列表:   %v\n", rc.GetSlaveIDs())
	fmt.Printf("    节点总数:     %d\n", rc.GetNodeCount())

	state := rc.GetState()
	fmt.Println("    详细状态:")
	for k, v := range state {
		fmt.Printf("      %s: %v\n", k, v)
	}
}

// ========================================================================
// 批量操作命令
// ========================================================================

// handleBatchSet 处理 batch-set 命令，批量 SET 数据
func (cli *CLIClient) handleBatchSet(args []string) {
	if len(args) < 1 {
		fmt.Println("  [!] 用法: batch-set <count> [prefix]")
		return
	}
	count, err := strconv.Atoi(args[0])
	if err != nil || count <= 0 {
		fmt.Printf("  [!] 无效数量: %s\n", args[0])
		return
	}
	prefix := "batch"
	if len(args) > 1 {
		prefix = args[1]
	}

	conn, err2 := cli.GetActiveConn()
	if err2 != nil {
		fmt.Printf("  [!] %v\n", err2)
		return
	}

	start := time.Now()
	success := 0
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		value := fmt.Sprintf("%s-val-%d", prefix, i)
		status, _, err := sendTCPRequest(conn.Conn, uint8(protocol.CMD_SET), []byte(key), []byte(value))
		if err != nil {
			fmt.Printf("  [!] 第 %d 条失败: %v\n", i+1, err)
			break
		}
		if status == uint8(protocol.SUCCESS) {
			success++
		}
	}
	duration := time.Since(start)
	fmt.Printf("  [+] batch-set 完成: %d/%d 成功, 耗时 %v (%.0f ops/s)\n",
		success, count, duration.Round(time.Millisecond), float64(success)/duration.Seconds())
}

// handleBatchGet 处理 batch-get 命令，批量 GET 验证数据
func (cli *CLIClient) handleBatchGet(args []string) {
	if len(args) < 1 {
		fmt.Println("  [!] 用法: batch-get <count> [prefix]")
		return
	}
	count, err := strconv.Atoi(args[0])
	if err != nil || count <= 0 {
		fmt.Printf("  [!] 无效数量: %s\n", args[0])
		return
	}
	prefix := "batch"
	if len(args) > 1 {
		prefix = args[1]
	}

	conn, err2 := cli.GetActiveConn()
	if err2 != nil {
		fmt.Printf("  [!] %v\n", err2)
		return
	}

	start := time.Now()
	hit := 0
	miss := 0
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		status, val, err := sendTCPRequest(conn.Conn, uint8(protocol.CMD_GET), []byte(key), nil)
		if err != nil {
			fmt.Printf("  [!] 第 %d 条失败: %v\n", i+1, err)
			break
		}
		if status == uint8(protocol.SUCCESS) && len(val) > 0 {
			hit++
		} else {
			miss++
		}
	}
	duration := time.Since(start)
	fmt.Printf("  [+] batch-get 完成: %d 命中, %d 未命中, 耗时 %v (%.0f ops/s)\n",
		hit, miss, duration.Round(time.Millisecond), float64(count)/duration.Seconds())
}

// handleBatchDel 处理 batch-del 命令，批量 DELETE 数据
func (cli *CLIClient) handleBatchDel(args []string) {
	if len(args) < 1 {
		fmt.Println("  [!] 用法: batch-del <count> [prefix]")
		return
	}
	count, err := strconv.Atoi(args[0])
	if err != nil || count <= 0 {
		fmt.Printf("  [!] 无效数量: %s\n", args[0])
		return
	}
	prefix := "batch"
	if len(args) > 1 {
		prefix = args[1]
	}

	conn, err2 := cli.GetActiveConn()
	if err2 != nil {
		fmt.Printf("  [!] %v\n", err2)
		return
	}

	start := time.Now()
	success := 0
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		status, _, err := sendTCPRequest(conn.Conn, uint8(protocol.CMD_DELETE), []byte(key), nil)
		if err != nil {
			fmt.Printf("  [!] 第 %d 条失败: %v\n", i+1, err)
			break
		}
		if status == uint8(protocol.SUCCESS) {
			success++
		}
	}
	duration := time.Since(start)
	fmt.Printf("  [+] batch-del 完成: %d/%d 成功, 耗时 %v\n",
		success, count, duration.Round(time.Millisecond))
}

// ========================================================================
// 路由与节点信息
// ========================================================================

// handleRoute 处理 route 命令，查看 Key 路由到的目标节点
func (cli *CLIClient) handleRoute(args []string) {
	if len(args) < 1 {
		fmt.Println("  [!] 用法: route <key>")
		return
	}
	if cli.cluster == nil {
		fmt.Println("  [!] 集群未启动")
		return
	}
	key := args[0]
	nodeID := cli.cluster.Ring.GetNode(key)
	fmt.Printf("  Key '%s' -> 路由到节点: %s\n", key, nodeID)
}

// handleNodes 处理 nodes 命令，查看所有缓存节点信息
func (cli *CLIClient) handleNodes() {
	if cli.cluster == nil {
		fmt.Println("  [!] 集群未启动")
		return
	}
	fmt.Println("  节点列表:")
	fmt.Println("  ID               | 状态     | 缓存大小")
	fmt.Println("  -----------------|----------|----------")
	for _, n := range cli.cluster.Nodes {
		fmt.Printf("  %-16s | %-8s | %d\n", n.GetNodeID(), n.GetStatus(), n.Size())
	}
}

// handleRingInfo 处理 ring-info 命令，查看哈希环详细信息
func (cli *CLIClient) handleRingInfo() {
	if cli.cluster == nil {
		fmt.Println("  [!] 集群未启动")
		return
	}
	ring := cli.cluster.Ring
	fmt.Println("  哈希环信息:")
	fmt.Printf("    物理节点数:   %d\n", ring.NodeCount())
	fmt.Printf("    虚拟节点数:   %d\n", ring.VirtualNodeCount())
	nodes := ring.GetNodes()
	if len(nodes) > 0 {
		fmt.Printf("    节点列表:     %v\n", nodes)
	}
}

// ========================================================================
// 测试场景命令
// ========================================================================

// handleLRUEvict 处理 lru-evict 命令，创建指定容量的集群测试 LRU 淘汰
func (cli *CLIClient) handleLRUEvict(args []string) {
	if len(args) < 1 {
		fmt.Println("  [!] 用法: lru-evict <capacity>")
		return
	}
	capacity, err := strconv.Atoi(args[0])
	if err != nil || capacity <= 0 {
		fmt.Printf("  [!] 无效容量: %s\n", args[0])
		return
	}

	// 创建小容量节点进行 LRU 淘汰演示
	if cli.cluster == nil {
		fmt.Println("  [!] 集群未启动")
		return
	}

	n := cli.cluster.Nodes[0]
	// 清空节点数据
	n.Stop()
	n2, _ := node.NewCacheNode("lru-test", capacity)
	n2.Init(cli.cluster.Ring)
	n2.Start()

	// 填充到满
	for i := 0; i < capacity; i++ {
		n2.Set(fmt.Sprintf("lru-%d", i), []byte(fmt.Sprintf("v-%d", i)))
	}
	fmt.Printf("  [+] 已填充 %d 条数据（容量=%d）\n", capacity, capacity)

	// 写入超量数据触发淘汰
	n2.Set("overflow", []byte("ov"))
	fmt.Printf("  [+] 写入 overflow 触发淘汰，当前大小: %d\n", n2.Size())

	// 检查最老数据是否被淘汰
	_, err = n2.Get("lru-0")
	if err != nil {
		fmt.Println("  [+] lru-0 已被淘汰（符合预期）")
	} else {
		fmt.Println("  [o] lru-0 仍存在")
	}

	n2.Stop()
}

// handleStress 处理 stress 命令，执行多客户端并发压力测试
func (cli *CLIClient) handleStress(args []string) {
	if len(args) < 2 {
		fmt.Println("  [!] 用法: stress <clients> <ops>")
		return
	}
	numClients, err1 := strconv.Atoi(args[0])
	opsPerClient, err2 := strconv.Atoi(args[1])
	if err1 != nil || err2 != nil || numClients <= 0 || opsPerClient <= 0 {
		fmt.Printf("  [!] 无效参数: %s %s\n", args[0], args[1])
		return
	}
	if numClients > 50 {
		fmt.Println("  [!] 最大支持 50 个并发客户端")
		return
	}
	if err := cli.EnsureCluster(); err != nil {
		fmt.Printf("  [!] 集群启动失败: %v\n", err)
		return
	}

	fmt.Printf("  [ ] 开始压力测试: %d 客户端 x %d 操作 = %d 总操作\n",
		numClients, opsPerClient, numClients*opsPerClient)

	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, numClients)
	successCount := int64(0)

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", cli.cluster.Address, 5*time.Second)
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
					errCh <- fmt.Errorf("c%d SET %s: %v", id, key, err)
					return
				}
				vl := binary.BigEndian.Uint32(header[5:9])
				body := make([]byte, 1+int(vl))
				if 1+int(vl) > 0 {
					io.ReadFull(conn, body)
				}
				if body[0] == uint8(protocol.SUCCESS) {
					successCount++
				}
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	errCount := 0
	for range errCh {
		errCount++
	}

	duration := time.Since(start)
	total := numClients * opsPerClient
	fmt.Printf("  [+] 压力测试完成: 成功 %d/%d, 错误 %d, 耗时 %v\n",
		successCount, total, errCount, duration.Round(time.Millisecond))
	fmt.Printf("      吞吐量: %.0f ops/s\n", float64(successCount)/duration.Seconds())
}

// handleRaw 处理 raw 命令，发送原始十六进制字节到服务器
// 帧格式: Command(1B) + KeyLen(4B big-endian) + ValueLen(4B big-endian) + Key + Value
func (cli *CLIClient) handleRaw(args []string) {
	if len(args) < 1 {
		fmt.Println("  [!] 用法: raw <hex_bytes>")
		fmt.Println("      帧格式: Command(1B) + KeyLen(4B) + ValueLen(4B) + Key + Value")
		fmt.Println("      例: raw 0100000003000000006b6579              (GET key, 12字节)")
		fmt.Println("      例: raw 0200000003000000036b657976616c        (SET key val, 15字节)")
		fmt.Println("      例: raw 0300000003000000006b6579              (DELETE key, 12字节)")
		fmt.Println("      例: raw 040000000000000000                    (INFO, 9字节)")
		return
	}
	hexStr := strings.Join(args, "")
	data, err := hex.DecodeString(hexStr)
	if err != nil {
		fmt.Printf("  [!] 十六进制解码失败: %v\n", err)
		return
	}

	// ---- 帧格式校验 ----
	if len(data) < protocol.FrameHeaderSize {
		fmt.Printf("  [!] 数据过短: %d 字节，帧头至少需要 %d 字节 (Command+KeyLen+ValueLen)\n",
			len(data), protocol.FrameHeaderSize)
		return
	}

	cmd := data[0]
	keyLen := binary.BigEndian.Uint32(data[1:5])
	valLen := binary.BigEndian.Uint32(data[5:9])
	expectedSize := int(protocol.FrameHeaderSize) + int(keyLen) + int(valLen)
	cmdName := protocol.Command(cmd).String()

	fmt.Printf("  [i] 帧解析: Cmd=0x%02X(%s)  KeyLen=%d  ValueLen=%d  期望总长=%d字节\n",
		cmd, cmdName, keyLen, valLen, expectedSize)

	if len(data) != expectedSize {
		fmt.Printf("  [!] 帧长度不匹配: 实际 %d 字节 ≠ 期望 %d 字节 (头部9 + Key(%d) + Value(%d))\n",
			len(data), expectedSize, keyLen, valLen)
		fmt.Println("      服务端会因等待剩余数据而阻塞，请检查 hex 是否完整")
		return
	}

	// ---- 发送帧 ----
	conn, err2 := cli.GetActiveConn()
	if err2 != nil {
		fmt.Printf("  [!] %v\n", err2)
		return
	}

	n, err := conn.Conn.Write(data)
	if err != nil {
		fmt.Printf("  [!] 发送失败: %v\n", err)
		return
	}
	fmt.Printf("  [+] 已发送 %d 字节: %x\n", n, data)

	// ---- 读取响应帧 ----
	// 响应帧格式: Command(1B) + KeyLen(4B,恒为0) + ValueLen(4B) + Status(1B) + Value
	conn.Conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	defer conn.Conn.SetReadDeadline(time.Time{})

	// 先读9字节响应头部
	respHeader := make([]byte, protocol.FrameHeaderSize)
	if _, err := io.ReadFull(conn.Conn, respHeader); err != nil {
		fmt.Printf("  [!] 读取响应头失败: %v\n", err)
		return
	}
	respValLen := binary.BigEndian.Uint32(respHeader[5:9])

	// 读响应体: Status(1B) + Value(ValueLen)
	bodySize := 1 + int(respValLen)
	if bodySize > 10*1024*1024 { // 安全限制 10MB
		fmt.Printf("  [!] 响应体过大: %d 字节，拒绝读取\n", bodySize)
		return
	}
	respBody := make([]byte, bodySize)
	if bodySize > 0 {
		if _, err := io.ReadFull(conn.Conn, respBody); err != nil {
			fmt.Printf("  [!] 读取响应体失败: %v\n", err)
			return
		}
	}

	// 解析并展示响应
	respCmd := respHeader[0]
	status := uint8(0)
	value := []byte{}
	if len(respBody) > 0 {
		status = respBody[0]
		value = respBody[1:]
	}

	totalResp := protocol.FrameHeaderSize + bodySize
	fmt.Printf("  [+] 响应 %d 字节: %x\n", totalResp, append(respHeader, respBody...))
	fmt.Printf("      Cmd=0x%02X(%s)  Status=0x%02X(%s)\n",
		respCmd, protocol.Command(respCmd).String(), status, protocol.ErrorCode(status).String())
	if len(value) > 0 {
		// 尝试以可读文本展示
		printable := true
		for _, b := range value {
			if b < 0x20 || b > 0x7e {
				printable = false
				break
			}
		}
		if printable {
			fmt.Printf("      Value(%d字节): %s\n", len(value), string(value))
		} else {
			fmt.Printf("      Value(%d字节): %x\n", len(value), value)
		}
	}
}

// ========================================================================
// 客户端设置模式
// ========================================================================

// RunSettings 设置主循环
func (cli *CLIClient) RunSettings() {
	// 进入时清屏并显示菜单
	if cli.settings.DisplayMode >= 2 {
		clearScreen()
	}
	cli.ShowSettingsMenu()

	for {
		choice := readInput(cli.reader, "请选择: ")

		switch choice {
		case "0", "back":
			return
		case "1":
			cli.settings.AutoSave = !cli.settings.AutoSave
			fmt.Printf("  [+] 自动保存已%v\n", map[bool]string{true: "开启", false: "关闭"}[cli.settings.AutoSave])
		case "2":
			dir := readInput(cli.reader, "  输入输出目录路径: ")
			if dir != "" {
				cli.settings.OutputDir = dir
				fmt.Printf("  [+] 输出目录已设为: %s\n", dir)
			}
		case "3":
			s := readInput(cli.reader, "  输入超时秒数(当前 "+fmt.Sprintf("%v", cli.settings.Timeout)+"): ")
			if s != "" {
				if sec, err := strconv.Atoi(s); err == nil && sec > 0 {
					cli.settings.Timeout = time.Duration(sec) * time.Second
					fmt.Printf("  [+] 连接超时已设为: %v\n", cli.settings.Timeout)
				} else {
					fmt.Println("  [!] 无效秒数")
				}
			}
		case "4":
			cli.settings.Verbose = !cli.settings.Verbose
			fmt.Printf("  [+] 详细日志已%v\n", map[bool]string{true: "开启", false: "关闭"}[cli.settings.Verbose])
		case "5":
			fmt.Println("  显示模式选项:")
			fmt.Println("    1 - 追加模式（文字持续追加，窗口越来越长）")
			fmt.Println("    2 - 清屏模式（每次只显示当前菜单）")
			fmt.Println("    3 - 滚动窗口（保留最近N条指令输出）")
			m := readInput(cli.reader, "  选择显示模式(1/2/3): ")
			if mode, err := strconv.Atoi(m); err == nil && mode >= 1 && mode <= 3 {
				cli.settings.DisplayMode = mode
				fmt.Printf("  [+] 显示模式已设为: %s\n", displayModeName(mode))
			} else {
				fmt.Println("  [!] 无效选择，请输入 1/2/3")
			}
		case "6":
			if cli.settings.DisplayMode == 3 {
				s := readInput(cli.reader, fmt.Sprintf("  输入滚动行数(当前 %d): ", cli.settings.ScrollLines))
				if s != "" {
					if n, err := strconv.Atoi(s); err == nil && n > 0 {
						cli.settings.ScrollLines = n
						fmt.Printf("  [+] 滚动行数已设为: %d\n", n)
					} else {
						fmt.Println("  [!] 无效行数，请输入正整数")
					}
				}
			} else {
				fmt.Println("  [!] 滚动行数仅在模式3（滚动窗口）下可设置")
			}
		case "7":
			cli.ShowCurrentConfig()
		case "8":
			cli.report = make([]TestCaseResult, 0)
			fmt.Println("  [+] 测试报告记录已清空")
		default:
			fmt.Println("  [!] 无效选择")
		}

		// 自动保存选项提示
		if choice == "1" && cli.settings.AutoSave {
			fmt.Println("  提示: 测试结果将在每次执行后自动保存为 Markdown 文件")
		}
	}
}

// ========================================================================
// 使用示例
// ========================================================================

// ShowUsageExamples 显示自由测试的具体使用示例
func ShowUsageExamples() {
	fmt.Println()
	fmt.Println("============================================================")
	fmt.Println("  自由测试 - 使用示例")
	fmt.Println("============================================================")
	fmt.Println()
	fmt.Println("  ┌─────────────────────────────────────────────────────────┐")
	fmt.Println("  │ 示例1: 基本缓存读写流程                                 │")
	fmt.Println("  └─────────────────────────────────────────────────────────┘")
	fmt.Println("    set mykey hello           # 写入 key=mykey, value=hello")
	fmt.Println("    get mykey                 # 读取 -> 应返回 hello")
	fmt.Println("    delete mykey              # 删除")
	fmt.Println("    get mykey                 # 再读 -> 应返回 (nil)")
	fmt.Println()
	fmt.Println("  ┌─────────────────────────────────────────────────────────┐")
	fmt.Println("  │ 示例2: 批量操作与验证                                   │")
	fmt.Println("  └─────────────────────────────────────────────────────────┘")
	fmt.Println("    batch-set 100             # 批量写入100条 (batch-0~batch-99)")
	fmt.Println("    batch-get 100             # 批量读取 -> 应100命中")
	fmt.Println("    batch-del 50              # 批量删除前50条")
	fmt.Println("    batch-get 100             # 再读 -> 应50命中, 50未命中")
	fmt.Println()
	fmt.Println("  ┌─────────────────────────────────────────────────────────┐")
	fmt.Println("  │ 示例3: 查看服务器信息和节点状态                         │")
	fmt.Println("  └─────────────────────────────────────────────────────────┘")
	fmt.Println("    info                      # 查看服务器INFO")
	fmt.Println("    nodes                     # 查看所有节点信息")
	fmt.Println("    ring-info                 # 查看哈希环信息")
	fmt.Println("    route mykey               # 查看key路由到哪个节点")
	fmt.Println()
	fmt.Println("  ┌─────────────────────────────────────────────────────────┐")
	fmt.Println("  │ 示例4: 主从复制完整流程                                 │")
	fmt.Println("  └─────────────────────────────────────────────────────────┘")
	fmt.Println("    nodes                     # 先查看可用节点ID")
	fmt.Println("    sync TestNode-1 TestNode-2   # 配置主从关系")
	fmt.Println("    sync-set user1 Alice      # 主节点写入并同步")
	fmt.Println("    sync-set user2 Bob        # 主节点写入并同步")
	fmt.Println("    sync-status               # 查看同步状态")
	fmt.Println("    sync-del user1            # 同步删除")
	fmt.Println("    full-sync TestNode-1      # 全量同步")
	fmt.Println()
	fmt.Println("  ┌─────────────────────────────────────────────────────────┐")
	fmt.Println("  │ 示例5: 多客户端并发测试                                 │")
	fmt.Println("  └─────────────────────────────────────────────────────────┘")
	fmt.Println("    connect                   # 创建第2个连接")
	fmt.Println("    list                      # 查看所有连接")
	fmt.Println("    use 1                     # 切换到连接1")
	fmt.Println("    set key1 value1           # 在连接1上写入")
	fmt.Println("    use 2                     # 切换到连接2")
	fmt.Println("    get key1                  # 在连接2上读取(同一集群)")
	fmt.Println("    disconnect 1              # 断开连接1")
	fmt.Println("    stress 5 20               # 5客户端x20操作压力测试")
	fmt.Println()
	fmt.Println("  ┌─────────────────────────────────────────────────────────┐")
	fmt.Println("  │ 示例6: LRU淘汰与边界测试                                │")
	fmt.Println("  └─────────────────────────────────────────────────────────┘")
	fmt.Println("    lru-evict 5               # 容量5的LRU淘汰演示")
	fmt.Println("    set \"\" value              # 空key应报错")
	fmt.Println("    batch-set 200             # 大量写入测试")
	fmt.Println("    batch-get 200             # 验证数据一致性")
	fmt.Println()
	fmt.Println("  ┌─────────────────────────────────────────────────────────┐")
	fmt.Println("  │ 示例7: 原始协议帧测试                                   │")
	fmt.Println("  └─────────────────────────────────────────────────────────┘")
	fmt.Println("    raw 0100000003000000006b6579            # GET key (12字节)")
	fmt.Println("    raw 0200000003000000036b657976616c      # SET key val (15字节)")
	fmt.Println("    raw 040000000000000000                  # INFO (9字节)")
	fmt.Println("    # 帧格式: Command(1B)+KeyLen(4B big-endian)+ValueLen(4B big-endian)+Key+Value")
	fmt.Println("    # 01=GET 02=SET 03=DELETE 04=INFO")
	fmt.Println("    # GET key: 01 | 00000003(keylen=3) | 00000000(vallen=0) | 6b6579(\"key\")")
	fmt.Println()
	fmt.Println("  提示: 所有命令不区分大小写（命令部分），参数区分大小写")
	fmt.Println("============================================================")
}
