# Task 3.9 CLI 交互式测试客户端功能总结

## 任务信息
- **任务编号**: Task 3.9
- **任务描述**: 编写 CLI 交互式 TCP 测试客户端，提供简易测试（46 个用例自动执行）和自由测试（动态指令全功能覆盖）两大模式
- **源码目录**: `cmd/test-client/`
- **完成日期**: 2026-06-09
- **测试结果**: ✅ **46/46 PASS**（简易测试全部通过）
- **原有测试**: ✅ **31/31 PASS**（`go test ./...` 未受影响）

---

## 测试环境
- **操作系统**: Windows 10/11
- **Go 版本**: go 1.26.4
- **依赖**: 仅 Go 标准库 + 项目内部包（cache、node、protocol、replication、server、shard）
- **运行命令**: `go run ./cmd/test-client/`
- **启动方式**: 无需预先启动服务器，客户端内嵌完整缓存集群

---

## 架构设计

### 核心设计理念：嵌入式集群

测试客户端在进程内嵌入了完整的缓存集群，无需启动外部服务器：

```
┌───────────────────────────────────────────────────────┐
│                  CLI 测试客户端进程                      │
│                                                       │
│  ┌─────────────┐    ┌─────────────────────────────┐   │
│  │  用户交互层   │    │       嵌入式集群              │   │
│  │  (菜单/指令)  │───▶│  HashRing + 3×CacheNode    │   │
│  │             │    │  + TCPServer + Replication   │   │
│  └─────────────┘    └─────────────────────────────┘   │
│         │                      │                       │
│         │           TCP (localhost:0)                  │
│         └──────────────────────┘                       │
└───────────────────────────────────────────────────────┘
```

> 服务器监听 `:0`（操作系统随机分配端口），客户端通过 TCP 连接到本进程内的服务器，实现完整的网络通信测试。

### 模块架构

```
cmd/test-client/
├── main.go              (196行)  入口程序 + 三级菜单导航
├── test_client.go       (955行)  核心框架（数据结构、集群管理、连接管理、报告生成）
├── auto_tests.go       (1207行)  简易测试模块（46个测试用例 + 7大模块）
├── test_descriptions.go (269行)  测试用例详细描述（前置场景/测试步骤/验证方式）
├── free_mode.go        (962行)   自由测试模块（动态指令 + 设置管理）
└── USAGE.md            (346行)   使用指南文档
                    合计 ~3935行
```

---

## 功能模块详情

### 模块1：简易测试模式

三级菜单导航的自动测试执行器，覆盖系统全部 7 大模块：

| 序号 | 模块 | 用例数 | 正常 | 异常 | 边界 |
|------|------|--------|------|------|------|
| 1 | 协议编解码 | 6 | 2 | 2 | 2 |
| 2 | LRU缓存 | 8 | 3 | 2 | 3 |
| 3 | 一致性哈希 | 7 | 3 | 1 | 3 |
| 4 | 缓存节点 | 6 | 2 | 2 | 2 |
| 5 | TCP服务 | 9 | 3 | 2 | 4 |
| 6 | 主从复制 | 7 | 3 | 2 | 2 |
| 7 | 集成测试 | 3 | 1 | 1 | 1 |
| **合计** | | **46** | **17** | **12** | **17** |

#### 菜单层级

```
一级菜单 → 模块选择（1-7选模块，A执行全部）
  └─ 二级菜单 → 场景分类（1正常/2异常/3边界，A执行全部）
       └─ 三级菜单 → 具体用例（输入编号执行单个，A执行全部）
```

#### 测试描述系统

每个测试用例执行前，显示详细的三段式描述（[`test_descriptions.go`](cmd/test-client/test_descriptions.go)）：
- **前置场景**：测试所依赖的初始状态
- **测试过程**：具体的操作步骤
- **验证方式**：断言检查点

#### 测试隔离机制

每个需要集群的测试通过 [`tempCluster()`](cmd/test-client/auto_tests.go:28) 创建独立的临时集群，测试结束后调用 `stopCluster()` 清理，确保用例之间完全隔离。

---

### 模块2：自由测试模式

支持 20+ 种动态指令，可实时操作缓存集群。

#### 支持的命令列表

| 分类 | 命令 | 说明 |
|------|------|------|
| **连接管理** | `connect [addr]` | 创建新的 TCP 连接 |
| | `disconnect [id]` | 断开指定连接 |
| | `use <id>` | 切换活跃连接 |
| | `list` | 列出所有连接（显示本地端口+远程地址） |
| **缓存操作** | `set <key> <value>` | 写入数据 |
| | `get <key>` | 读取数据 |
| | `delete <key>` | 删除数据 |
| | `info` | 查看服务器信息 |
| **主从同步** | `sync <master> <slave>` | 配置主从关系 |
| | `sync-set <key> <value>` | 主节点SET并同步 |
| | `sync-del <key>` | 主节点DELETE并同步 |
| | `full-sync <master>` | 全量同步 |
| | `sync-status` | 查看同步状态 |
| **批量操作** | `batch-set <n> [prefix]` | 批量写入 |
| | `batch-get <n> [prefix]` | 批量读取验证 |
| | `batch-del <n> [prefix]` | 批量删除 |
| **路由与节点** | `route <key>` | 查看Key路由节点 |
| | `nodes` | 查看所有节点信息 |
| | `ring-info` | 查看哈希环信息 |
| **测试场景** | `lru-evict <cap>` | LRU淘汰测试 |
| | `stress <clients> <ops>` | 并发压力测试 |
| | `raw <hex>` | 发送原始十六进制字节 |
| **辅助** | `help` | 显示帮助 |
| | `usage` | 显示7个完整使用示例 |
| | `back` | 返回主菜单 |

#### 多客户端连接

自由测试模式支持同时管理多个 TCP 连接，可模拟多客户端并发场景：
- 启动时自动创建第一个连接
- 通过 `connect` 添加更多连接
- 通过 `use` 切换活跃连接
- `list` 命令显示每条连接的真实本地端口（通过 `net.Conn.LocalAddr()` 获取）

---

### 模块3：客户端设置

| 序号 | 设置项 | 默认值 | 说明 |
|------|--------|--------|------|
| 1 | 自动保存测试结果 | false | 自动将测试报告保存为 Markdown 文件 |
| 2 | 输出目录 | test_results | 报告文件保存路径 |
| 3 | 连接超时 | 5s | TCP 连接超时时间 |
| 4 | 详细日志 | false | 显示更详细的测试过程信息 |
| 5 | 显示模式 | 清屏模式 | 三种输出显示模式（见下文） |
| 6 | 滚动行数 | 5 | 模式3下的保留行数 |
| 7 | 查看完整配置 | - | 显示当前所有配置项 |
| 8 | 清空报告记录 | - | 清除内存中的测试报告数据 |

#### 输出显示模式

| 模式 | 名称 | 行为 |
|------|------|------|
| 1 | 追加模式 | 所有输出依次追加显示，窗口持续增长 |
| 2 | **清屏模式（默认）** | 每次操作前清屏，只显示当前菜单层级 |
| 3 | 滚动窗口模式 | 保留最近 N 条命令输出，旧内容自动清除 |

---

## 核心数据结构

### TestContext（testing.T 替代品）

为 CLI 环境设计的测试上下文，兼容 `testing.T` 的常用 API：

```go
type TestContext struct {
    name     string           // 测试名称
    failed   bool             // 是否失败
    logs     []string         // 日志记录
    errors   []string         // 错误记录
    subTests []SubTestResult  // 子测试结果
}
```

支持的方法：`Logf`、`Log`、`Errorf`、`Error`、`Fatalf`、`Fatal`、`Failed`、`FailNow`、`Run`、`Helper`

### EmbeddedCluster（嵌入式集群）

```go
type EmbeddedCluster struct {
    Ring    *shard.HashRing                   // 一致性哈希环
    Nodes   []*node.CacheNode                 // 缓存节点（默认3个）
    Server  *server.TCPServer                 // TCP服务器
    RC      *replication.ReplicationController // 主从复制控制器
    Address string                            // 服务器监听地址
    opts    ClusterOptions                    // 集群配置
}
```

### Settings（客户端设置）

```go
type Settings struct {
    AutoSave    bool            // 自动保存
    OutputDir   string          // 输出目录
    Timeout     time.Duration   // 连接超时
    Verbose     bool            // 详细日志
    DisplayMode int             // 显示模式: 1=追加 2=清屏 3=滚动窗口
    ScrollLines int             // 滚动窗口保留条数
}
```

---

## 断言工具函数

提供 8 个类型安全的断言函数（[`test_client.go`](cmd/test-client/test_client.go:197)）：

| 函数 | 用途 |
|------|------|
| `assertEqual[T comparable]` | 泛型相等断言 |
| `assertTrue` | 布尔值为真断言 |
| `assertFalse` | 布尔值为假断言 |
| `assertValue` | 字节切片值匹配断言 |
| `assertEmptyValue` | 字节切片为空断言 |
| `assertError` | 期望返回错误 |
| `assertNoError` | 期望无错误 |
| `assertContains` | 字符串包含断言 |

---

## 测试报告生成

简易测试执行完成后，可自动生成 Markdown 格式的测试报告（[`SaveReport()`](cmd/test-client/test_client.go:828)）：

- 按 7 大模块分组展示
- 每个用例显示：名称、状态（✅/❌）、耗时、错误信息
- 包含汇总统计：通过数、失败数、总耗时
- 支持 `AutoSave` 自动保存和手动保存

---

## 测试验证结果

### 1. 简易测试全量执行

```
============================================================
  全部测试完成: 46 通过, 0 失败, 总计 46, 耗时 xxxms
  *** ALL TESTS PASSED ***
============================================================
```

### 2. 自由测试模式验证

| 测试操作 | 结果 |
|----------|------|
| 多客户端连接（connect × 3） | ✅ 成功创建 3 条连接 |
| 连接列表（list） | ✅ 显示唯一本地端口（56668/56669/56670） |
| 缓存读写（set/get） | ✅ 数据正确读写 |
| 断开连接（disconnect） | ✅ 正确断开并显示端口 |
| 帮助与示例（help/usage） | ✅ 完整显示 |
| 批量操作（batch-set/get/del） | ✅ 批量数据正确处理 |
| 主从同步（sync/sync-set/sync-status） | ✅ 同步数据一致 |
| 路由查询（route/nodes/ring-info） | ✅ 路由信息正确 |
| 压力测试（stress） | ✅ 并发安全 |

### 3. 原有测试未受影响

```
go test ./...  →  31/31 PASS
```

覆盖所有原有测试包：cache、node、protocol、replication、server、shard、client、integration。

---

## 优化迭代记录

### 迭代1：核心功能实现
- 实现完整的 CLI 框架（[`test_client.go`](cmd/test-client/test_client.go)、[`main.go`](cmd/test-client/main.go)）
- 46 个测试用例覆盖 7 大模块（[`auto_tests.go`](cmd/test-client/auto_tests.go)）
- 自由测试 20+ 指令（[`free_mode.go`](cmd/test-client/free_mode.go)）
- 嵌入式集群，无需外部服务器

### 迭代2：用户体验优化
- 添加详细的测试过程描述（[`test_descriptions.go`](cmd/test-client/test_descriptions.go)）
- 添加 `usage` 命令，提供 7 个完整使用示例
- 生成使用指南文档（[`USAGE.md`](cmd/test-client/USAGE.md)）

### 迭代3：缺陷修复 + 显示模式
- **修复**：`list` 命令多客户端端口号显示一致的问题
  - 原因：只显示 `conn.Address`（服务器地址），所有连接相同
  - 修复：使用 `conn.Conn.LocalAddr().String()` 获取真实本地端口
- **新增**：输出显示模式设置（追加/清屏/滚动窗口）
  - 默认清屏模式，每次操作只显示当前菜单
  - 滚动窗口模式使用 `os.Pipe()` 捕获输出实现历史管理

---

## 覆盖的测试场景映射

| specs.md 场景 | 客户端覆盖方式 |
|--------------|---------------|
| LRU缓存基本读写操作 | 简易测试 C01 + 自由测试 set/get |
| 缓存达到容量上限时自动淘汰 | 简易测试 C02 + 自由测试 lru-evict |
| 重复访问热点数据保持命中 | 简易测试 C03 |
| 删除操作更新LRU链表 | 简易测试 C04 + 自由测试 delete |
| 查询不存在的键值 | 简易测试 C05 |
| 服务器正常启动和监听 | 嵌入式集群启动验证 |
| 多客户端并发连接 | 简易测试 S03/S06/S07 + 自由测试 stress |
| 协议帧编解码一致性 | 简易测试 P01-P06 |
| 一致性哈希路由确定性 | 简易测试 H01-H07 + 自由测试 route |
| 主从同步正常工作 | 简易测试 R01-R07 + 自由测试 sync-* |
| 完整的缓存读写流程 | 简易测试 I01-I03 + 自由测试完整工作流 |

---

## 使用方式

```bash
# 进入项目目录
cd SD-03

# 启动测试客户端
go run ./cmd/test-client/

# 简易测试：输入 1 → 选择模块 → 选择场景 → 选择/执行用例
# 自由测试：输入 2 → 直接输入指令操作
# 客户端设置：输入 3 → 配置显示模式、超时等
```

详细使用说明见 [`USAGE.md`](cmd/test-client/USAGE.md)。
