# SD-03 分布式缓存系统 - 快速使用指南

本文档提供 SD-03 分布式缓存系统的快速上手指南，涵盖环境准备、服务启动、功能验证和测试操作。

---

## 1. 环境准备

### 1.1 前置条件

| 项目 | 要求 |
|------|------|
| Go 版本 | 1.26.4+（最低 1.21） |
| 操作系统 | Windows / Linux / macOS |
| 外部依赖 | 无（纯标准库实现） |

### 1.2 获取项目

```bash
cd SD-03
```

### 1.3 验证环境

```bash
go version          # 确认Go版本
go mod verify       # 验证模块完整性
```

---

## 2. 启动缓存服务器

### 2.1 编译并启动

```bash
# 编译
go build -o cache-server.exe ./cmd/cache-server/

# 启动（默认监听端口 7000）
./cache-server.exe

# 自定义端口
./cache-server.exe -port 8080
```

### 2.2 服务器启动参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-port` | 7000 | TCP监听端口 |
| `-nodes` | 3 | 缓存节点数量 |
| `-capacity` | 10000 | 每个节点的缓存容量 |
| `-vnode` | 100 | 每个物理节点的虚拟节点数 |

---

## 3. 使用 CLI 测试客户端（推荐）

CLI 测试客户端是系统的主要交互工具，内置完整的嵌入式缓存集群，无需单独启动服务器。

### 3.1 启动客户端

```bash
go run ./cmd/test-client/
```

### 3.2 三大操作模式

启动后看到主菜单：

```
1. 简易测试（全自动菜单，覆盖全系统）
2. 自由测试（动态指令，全功能覆盖）
3. 客户端设置
0. 退出
```

### 3.3 模式一：简易测试（推荐新手）

一键运行全部测试用例：

```
1 → 选择"简易测试"
A → 全部执行（46个用例一键完成）
```

也可按模块逐个测试：

| 选项 | 模块 | 用例数 | 说明 |
|------|------|--------|------|
| 1 | 协议编解码 | 6 | 帧编解码、校验 |
| 2 | LRU缓存 | 8 | 读写、淘汰、边界 |
| 3 | 一致性哈希 | 7 | 路由、分布、环完整性 |
| 4 | 缓存节点 | 6 | 节点操作、状态管理 |
| 5 | TCP服务 | 9 | 并发、异常、压力 |
| 6 | 主从复制 | 7 | 同步、全量、并发 |
| 7 | 集成测试 | 3 | 端到端场景 |

每个模块支持按场景类型分类执行：
- **正常测试**：核心功能验证
- **异常测试**：非法输入防御
- **边界测试**：容量、并发、极端情况

### 3.4 模式二：自由测试

进入自由模式后可直接输入命令：

```bash
free:1> set mykey hello           # 写入
free:1> get mykey                 # 读取
free:1> delete mykey              # 删除
free:1> info                      # 查看服务器信息
```

#### 常用操作示例

**批量操作**：
```bash
free:1> batch-set 100             # 批量写入100条
free:1> batch-get 100             # 批量读取验证
free:1> batch-del 50              # 批量删除
```

**主从复制**：
```bash
free:1> sync TestNode-1 TestNode-2    # 配置主从
free:1> sync-set user1 Alice          # 写入并同步
free:1> sync-status                   # 查看同步状态
free:1> full-sync TestNode-1          # 全量同步
```

**集群信息**：
```bash
free:1> nodes                     # 查看节点列表
free:1> ring-info                 # 查看哈希环信息
free:1> route mykey               # 查看Key路由目标
```

**多客户端并发**：
```bash
free:1> connect                   # 创建新连接
free:1> list                      # 列出所有连接
free:1> use 2                     # 切换连接
free:1> stress 5 20               # 5客户端 x 20操作
```

**LRU淘汰测试**：
```bash
free:1> lru-evict 5               # 容量5的LRU淘汰演示
```

#### 完整命令速查

| 命令 | 格式 | 说明 |
|------|------|------|
| `set` | `set <key> <value>` | 写入缓存 |
| `get` | `get <key>` | 读取缓存 |
| `delete` | `delete <key>` | 删除缓存 |
| `info` | `info` | 服务器信息 |
| `connect` | `connect [addr]` | 创建TCP连接 |
| `disconnect` | `disconnect [id]` | 断开连接 |
| `use` | `use <id>` | 切换连接 |
| `list` | `list` | 列出连接 |
| `sync` | `sync <master> <slave>` | 配置主从 |
| `sync-set` | `sync-set <key> <value>` | 同步写入 |
| `sync-del` | `sync-del <key>` | 同步删除 |
| `full-sync` | `full-sync <master>` | 全量同步 |
| `sync-status` | `sync-status` | 同步状态 |
| `batch-set` | `batch-set <n> [prefix]` | 批量写入 |
| `batch-get` | `batch-get <n> [prefix]` | 批量读取 |
| `batch-del` | `batch-del <n> [prefix]` | 批量删除 |
| `route` | `route <key>` | Key路由查询 |
| `nodes` | `nodes` | 节点列表 |
| `ring-info` | `ring-info` | 哈希环信息 |
| `lru-evict` | `lru-evict <capacity>` | LRU淘汰测试 |
| `stress` | `stress <clients> <ops>` | 压力测试 |
| `raw` | `raw <hex>` | 发送原始字节 |
| `help` | `help` | 命令帮助 |
| `back` | `back` | 返回主菜单 |

---

## 4. 运行自动化测试

### 4.1 单元测试

```bash
# 运行所有 pkg 模块单元测试
go test ./pkg/...

# 运行指定模块测试（详细输出）
go test -v ./pkg/cache/...
go test -v ./pkg/protocol/...
go test -v ./pkg/shard/...
go test -v ./pkg/node/...
go test -v ./pkg/server/...
go test -v ./pkg/replication/...
```

### 4.2 集成测试

```bash
# 运行端到端集成测试
go test -v ./tests/integration/...

# 运行测试客户端工具库测试
go test -v ./tests/client/...
```

### 4.3 全量测试

```bash
# 运行项目中所有测试
go test -v ./...

# 带覆盖率
go test -cover ./...
```

---

## 5. 项目结构概览

```
SD-03/
├── cmd/
│   ├── cache-server/           # 缓存服务器主程序
│   └── test-client/            # CLI交互式测试客户端
├── pkg/                        # 核心实现（6个模块）
│   ├── cache/                  #   LRU缓存（双向链表+哈希表）
│   ├── protocol/               #   二进制协议编解码
│   ├── shard/                  #   一致性哈希（FNV-1a+虚拟节点）
│   ├── node/                   #   缓存节点（集成LRU+哈希环）
│   ├── server/                 #   TCP并发服务器
│   └── replication/            #   主从复制控制器
├── tests/                      # 测试文件
│   ├── client/                 #   测试客户端工具库
│   └── integration/            #   集成测试
├── docs/                       # 项目文档
└── test_results/               # 测试结果报告
```

### 核心模块说明

| 模块 | 包路径 | 核心功能 |
|------|--------|---------|
| LRU缓存 | `pkg/cache` | 双向链表+哈希表实现，O(1)操作，容量限制自动淘汰 |
| 协议编解码 | `pkg/protocol` | 大端字节序，Command+KeyLen+ValueLen+Data帧格式 |
| 一致性哈希 | `pkg/shard` | FNV-1a哈希，虚拟节点（默认100个/物理节点），二分查找路由 |
| 缓存节点 | `pkg/node` | 集成LRU和哈希环，统一Get/Set/Delete接口，线程安全 |
| TCP服务器 | `pkg/server` | goroutine-per-connection，优雅关闭，命令分发 |
| 主从复制 | `pkg/replication` | 写操作同步，全量重连同步，故障切换 |

---

## 6. 协议格式参考

### 帧结构

```
+----------+----------+-----------+---------+
| Command  | KeyLen   | ValueLen  | Data    |
| (1 byte) | (4 byte) | (4 byte)  | (变长)   |
+----------+----------+-----------+---------+
```

### 命令码

| 命令 | 代码 | 说明 |
|------|------|------|
| GET | `0x01` | 读取缓存 |
| SET | `0x02` | 写入缓存 |
| DELETE | `0x03` | 删除缓存 |
| INFO | `0x04` | 查询信息 |

### 响应状态码

| 状态 | 代码 | 说明 |
|------|------|------|
| SUCCESS | `0x00` | 操作成功 |
| ERROR_UNKNOWN_COMMAND | `0x01` | 未知命令 |
| ERROR_INVALID_KEY | `0x02` | 无效Key |
| ERROR_INVALID_VALUE | `0x03` | 无效Value |
| ERROR_CACHE_FULL | `0x04` | 缓存已满 |

### 原始帧测试示例

```bash
# 在自由模式下发送原始帧
# 帧格式: Command(1B) + KeyLen(4B big-endian) + ValueLen(4B big-endian) + Key + Value
free:1> raw 0100000003000000006b6579              # GET key (12字节)
free:1> raw 0200000003000000036b657976616c        # SET key val (15字节)
free:1> raw 040000000000000000                    # INFO (9字节)
```

---

## 7. 常见问题

### Q: 测试客户端和缓存服务器有什么区别？

**缓存服务器**（`cmd/cache-server/`）是独立的 TCP 服务进程，监听端口接受客户端连接。

**测试客户端**（`cmd/test-client/`）内置了完整的嵌入式集群（哈希环+3节点+TCP服务器+主从复制），无需启动独立服务器即可测试所有功能。

### Q: 为什么单元测试在 pkg/ 目录下？

Go 语言的惯例是将单元测试文件（`*_test.go`）与源码放在同一目录下。这使得测试可以直接访问包内未导出的函数和结构体，方便进行白盒测试。

### Q: 如何查看测试结果报告？

测试结果报告按开发阶段组织在 `test_results/` 目录下：
- `phase1_basic/`：基础架构阶段测试结果
- `phase2_implementation/`：核心功能实现阶段测试结果
- `phase3_testing/`：测试验证阶段测试结果

---

**文档版本**: v1.0
**创建日期**: 2026-06-09
**适用项目**: SD-03 分布式缓存系统
