# SD-03 分布式缓存系统

## 项目简介

SD-03 是一个轻量级的分布式内存缓存系统，专注于学习分布式缓存的核心原理和实践TCP网络编程。项目实现了一个完整的分布式缓存系统，涵盖LRU缓存淘汰算法、TCP服务器、自定义二进制协议、一致性哈希分片和简化版主从复制。

## 核心功能

- ✅ **LRU缓存淘汰算法**：使用双向链表+哈希表实现标准LRU算法
- ✅ **TCP服务器**：提供基于TCP的缓存服务器，支持多客户端并发连接
- ✅ **自定义二进制协议**：简洁高效的二进制协议设计（大端字节序）
- ✅ **一致性哈希分片**：虚拟节点机制实现数据分片和负载均衡
- ✅ **简化版主从复制**：主节点写操作同步到从节点，支持故障切换
- ✅ **CLI测试客户端**：内置嵌入式集群的交互式测试工具（51个自动测试用例，含8个集成测试）

## 技术栈

- **编程语言**：Go 1.26.4
- **网络通信**：Go标准库 `net`
- **数据结构**：`container/list` + `hash/fnv`
- **并发控制**：`sync` 包（Mutex/RWMutex）
- **序列化**：`encoding/binary`

## 项目结构

```
SD-03/
├── AI工具使用/                     # AI工具使用相关文档
│   └── readme.md
├── cmd/                             # 主程序入口
│   ├── cache-server/                # 缓存服务器
│   │   └── main.go                  # 服务器启动入口
│   └── test-client/                 # CLI交互式测试客户端
│       ├── main.go                  # 主程序入口与菜单系统
│       ├── test_client.go           # 客户端核心逻辑与嵌入式集群
│       ├── auto_tests.go            # 自动化测试套件（51个用例）
│       ├── free_mode.go             # 自由测试模式（22条交互命令）
│       ├── test_descriptions.go     # 测试用例描述定义
│       └── USAGE.md                 # 客户端详细使用指南
├── pkg/                             # 核心代码实现
│   ├── cache/                       # LRU缓存实现
│   │   ├── cache.go                 # LRU缓存（双向链表+哈希表）
│   │   └── cache_test.go            # 单元测试
│   ├── protocol/                    # 协议编解码
│   │   ├── protocol.go              # 二进制协议定义与编解码
│   │   ├── protocol_encoding_test.go # 编解码测试
│   │   ├── protocol_test.go         # 协议测试
│   │   └── validate_frame_test.go   # 帧校验测试
│   ├── shard/                       # 一致性哈希分片
│   │   ├── shard.go                 # 哈希环与虚拟节点
│   │   └── shard_test.go            # 单元测试
│   ├── node/                        # 缓存节点模块
│   │   ├── node.go                  # 节点管理（集成LRU+哈希环）
│   │   └── node_test.go             # 单元测试
│   ├── server/                      # TCP服务器
│   │   ├── server.go                # TCP服务器核心实现
│   │   └── server_test.go           # 单元测试
│   └── replication/                 # 主从复制
│       ├── replication.go           # 复制控制器实现
│       └── replication_test.go      # 单元测试
├── tests/                           # 测试文件
│   ├── client/                      # 测试客户端工具库
│   │   ├── test_client.go           # TCP客户端封装
│   │   └── test_client_test.go      # Task 3.9 完整测试套件
│   └── integration/                 # 集成测试
│       ├── integration_test.go      # 端到端集成测试
│       └── advanced_test.go         # 高级集成测试
├── test_results/                    # 测试结果报告（按阶段组织）
│   ├── phase1_basic/                # Phase 1 基础架构测试结果
│   ├── phase2_implementation/       # Phase 2 核心功能测试结果
│   ├── phase3_testing/              # Phase 3 测试验证结果
|   └── 客户端测试结果截图             # 客户端CLI工具测试结果截图
├── docs/                            # 项目文档
│   ├── README.md                    # 文档索引
│   ├── design/                      # 设计文档
│   │   └── design.md                # 系统设计文档
│   ├── proposal/                    # 提案文档
│   │   └── proposal.md              # 项目提案
│   ├── specs/                       # 需求规格
│   │   └── specs.md                 # 需求规格说明
│   ├── tasks/                       # 任务分解
│   │   └── tasks.md                 # 任务分解文档
│   ├── sdd_review.md                # SDD回顾文档
│   └── QUICK_START.md               # 快速开始指南
├── go.mod
├── QUICK_START.md
└── README.md
```

## 快速开始

### 环境要求

- Go 1.26.4+
- Windows / Linux / macOS

### 启动测试客户端

```bash
cd SD-03

# 启动CLI测试客户端（内置嵌入式集群，无需单独启动服务器）
go run ./cmd/test-client/
```

### 启动缓存服务器

```bash
cd SD-03

# 编译并启动缓存服务器
go build -o cache-server.exe ./cmd/cache-server/
./cache-server.exe
```

### 运行测试

```bash
# 运行所有单元测试
go test ./pkg/...

# 运行集成测试
go test ./tests/...

# 运行所有测试
go test ./...
```

## 开发进度

- [x] **Phase 1: 基础架构**
  - [x] Task 1.1: 创建项目目录结构
  - [x] Task 1.2: 初始化Go模块
  - [x] Task 1.3: 创建README.md
  - [x] Task 1.4: 定义协议常量和类型
  - [x] Task 1.5: 实现协议编解码
  - [x] Task 1.6: 实现帧校验函数

- [x] **Phase 2: 核心功能**
  - [x] Task 2.1: LRU缓存实现
  - [x] Task 2.2: 一致性哈希分片
  - [x] Task 2.3: 缓存节点模块
  - [x] Task 2.4: TCP服务器核心
  - [x] Task 2.5: 命令处理器
  - [x] Task 2.6: 主从复制
  - [x] Task 2.7: 主程序入口

- [x] **Phase 3: 测试与优化**
  - [x] Task 3.1~3.6: 各模块单元测试
  - [x] Task 3.7~3.8: 集成测试与高级测试
  - [x] Task 3.9: CLI测试客户端
  - [x] Task 3.10: 代码注释完善
  - [x] Task 3.11: 项目文档对齐

- [ ] **Phase 4: 验收与交付**
  - [ ] Task 4.1: 最终验收与交付

## 阅读顺序

1. [项目提案](./docs/proposal/proposal.md) - 项目背景、目标和范围
2. [需求规格](./docs/specs/specs.md) - 功能场景和验收标准
3. [设计文档](./docs/design/design.md) - 架构和接口设计
4. [任务列表](./docs/tasks/tasks.md) - 开发任务分解
5. [SDD回顾文档](./docs/sdd_review.md) - 系统设计回顾

## 代码规范

- 遵循Go官方代码规范
- 使用标准库 `container/list` 实现LRU算法
- 所有多字节字段使用大端字节序（Big-Endian）
- 函数和类型命名采用驼峰命名法
- 完整的godoc注释覆盖

## 验收标准

### 核心功能验收

- [x] **LRU缓存淘汰算法正确性**
  - 使用双向链表+哈希表实现
  - 容量100时，添加第101条数据后Key1被淘汰
  - 重复访问的Key保持高频使用
  - DELETE操作正确更新LRU链表

- [x] **TCP服务器基本功能**
  - 服务器监听端口7000
  - 支持10个客户端并发连接
  - 客户端连接后能正常读写数据
  - 客户端断开后服务器正确清理资源

- [x] **自定义二进制协议**
  - 协议格式：Command (1B) + KeyLen (4B) + ValueLen (4B) + Data
  - GET/SET/DELETE/INFO命令正确处理
  - 返回ERROR_UNKNOWN_COMMAND、ERROR_INVALID_KEY等错误码

- [x] **一致性哈希分片**
  - 虚拟节点机制（默认每个节点100个虚拟节点）
  - 1000次SET后，3个分片数据分布差异<30%
  - 同一个Key总是路由到同一个节点

- [x] **主从复制基本功能**
  - Master的SET操作同步到Slave
  - Slave断开重连后请求全量同步
  - Master故障后Slave提升为新Master
  - 原Master重启后连接到新Master并同步

## 性能指标

- 单节点支持最多10万条缓存数据
- 支持1000+并发连接
- 缓存命中响应延迟 < 1ms
- 主从复制同步延迟 < 10ms
- 单进程内存使用不超过2GB

## 文档索引

- [QUICK_START.md](./QUICK_START.md) - 系统快速使用指南
- [docs/README.md](./docs/README.md) - 文档索引和导航
- [docs/proposal/proposal.md](./docs/proposal/proposal.md) - 项目提案
- [docs/design/design.md](./docs/design/design.md) - 系统设计文档
- [docs/specs/specs.md](./docs/specs/specs.md) - 需求规格说明
- [docs/tasks/tasks.md](./docs/tasks/tasks.md) - 任务分解文档
- [docs/sdd_review.md](./docs/sdd_review.md) - SDD回顾文档
- [cmd/test-client/USAGE.md](./cmd/test-client/USAGE.md) - CLI测试客户端详细使用指南

## 许可证

本项目仅用于学习和研究目的。

---

**项目版本**: v1.1
**最后更新**: 2026-06-22
**适用Go版本**: 1.26.4+
