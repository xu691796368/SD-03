# SD-03分布式缓存系统

## 项目简介

SD-03是一个轻量级的分布式内存缓存系统，专注于学习分布式缓存的核心原理和实践TCP网络编程。项目实现了一个完整的分布式缓存系统，涵盖LRU缓存淘汰算法、TCP服务器、自定义二进制协议、一致性哈希分片和简化版主从复制。

## 核心功能

- ⏳ **LRU缓存淘汰算法**：使用自定义双向链表+哈希表实现标准LRU算法
- ⏳ **TCP服务器**：提供基于TCP的缓存服务器，支持多客户端并发连接
- ⏳ **自定义二进制协议**：简洁高效的二进制协议设计（大端字节序）
- ⏳ **一致性哈希分片**：虚拟节点机制实现数据分片和负载均衡
- ⏳ **简化版主从复制**：主节点写操作同步到从节点，支持故障切换

## 技术栈

- **编程语言**：Go 1.26.4
- **网络通信**：Go标准库 `net`
- **数据结构**：container/list + `hash/fnv`
- **并发控制**：`sync` 包（Mutex/RWMutex）
- **序列化**：`encoding/binary`

## 项目结构

```
SD-03/
├── cmd/                         # 主程序入口
│   └── cache-server/
│       └── main.go
├── pkg/                         # 核心代码实现（Phase 2 实现）
│   ├── cache/                   # LRU缓存实现
│   ├── shard/                   # 一致性哈希分片
│   ├── protocol/                # 协议编解码
│   ├── server/                  # TCP服务器
│   └── replication/             # 主从复制
├── tests/                       # 测试文件（Phase 3 实现）
│   ├── cache_test.go
│   ├── shard_test.go
│   ├── server_test.go
│   ├── replication_test.go
│   └── protocol_test.go
├── docs/                        # 项目文档
│   ├── specs/                   # 需求规格
│   ├── design/                  # 设计文档
│   └── tasks/                   # 任务列表
├── go.mod
├── go.sum
└── README.md                    # 项目主README
```

详细文档请参考：[docs/README.md](./docs/README.md)

## 快速开始

### 环境要求

- Go 1.26.4+
- Windows / Linux / macOS

### 安装步骤

1. **克隆或下载项目**
```bash
git clone <repository-url>
cd SD-03
```

2. **安装依赖**
```bash
go mod download
```

3. **验证目录结构**
```bash
# 检查目录结构
tree -L 2  # Linux/Mac
dir /s /b   # Windows
```

4. **构建项目**（Phase 2 完成后）
```bash
go build -o cache-server cmd/cache-server/main.go
```

5. **运行服务器**（Phase 2 完成后）
```bash
./cache-server
```

## 开发指南

### 阶段说明

- **Phase 1（当前）**: 基础架构初始化（Task 1.1-1.3）✅ 已完成
- **Phase 2**: 核心功能实现（Task 2.1-2.7）
- **Phase 3**: 测试与优化（Task 3.1-3.12）
- **Phase 4**: 验收与交付（Task 4.1-4.4）

### 阅读顺序

1. [任务列表](./docs/tasks/tasks.md) - 了解开发任务分解
2. [项目提案](./proposal.md) - 了解项目背景和目标
3. [需求规格](./docs/specs/specs.md) - 了解功能场景和验收标准
4. [设计文档](./docs/design/design.md) - 了解架构和接口设计

### 开发进度

- [x] **Phase 1: 基础架构**（Day0）
  - [x] Task 1.1: 创建项目目录结构
  - [x] Task 1.2: 初始化Go模块
  - [x] Task 1.3: 创建README.md

- [ ] **Phase 2: 核心功能**（Day1-2）
  - [ ] Task 2.1-2.7: 实现核心功能

- [ ] **Phase 3: 测试与优化**（Day3-4）
  - [ ] Task 3.1-3.12: 编写测试和优化代码

- [ ] **Phase 4: 验收与交付**（Day5）
  - [ ] Task 4.1-4.4: 验收检查和Demo准备

### 代码规范

- 遵循Go官方代码规范
- 接口定义在go文件中，实现也要在同一文件或对应实现文件中
- 使用标准库 `container/list` 的双向链表实现LRU算法
- 所有多字节字段使用大端字节序（Big-Endian）
- 函数和类型命名采用驼峰命名法

### 测试要求

- 单元测试覆盖率 > 60%
- 集成测试覆盖所有核心场景（至少20个场景）
- 测试文件命名：`*_test.go`
- 测试函数命名：`Test<TargetFunction>`

### 代码规范

- 遵循Go官方代码规范
- 接口定义在go文件中，实现也要在同一文件或对应实现文件中
- 使用标准库 `container/list` 的双向链表实现LRU算法
- 所有多字节字段使用大端字节序（Big-Endian）
- 函数和类型命名采用驼峰命名法

## 验收标准

### 核心功能验收

- [ ] **LRU缓存淘汰算法正确性**
  - 使用双向链表+哈希表实现
  - 容量100时，添加第101条数据后Key1被淘汰
  - 重复访问的Key保持高频使用
  - DELETE操作正确更新LRU链表

- [ ] **TCP服务器基本功能**
  - 服务器监听端口7000
  - 支持10个客户端并发连接
  - 客户端连接后能正常读写数据
  - 客户端断开后服务器正确清理资源

- [ ] **自定义二进制协议**
  - 协议格式：Command (1B) + KeyLen (4B) + ValueLen (4B) + Data
  - GET/SET/DELETE/INFO命令正确处理
  - 返回ERROR_UNKNOWN_COMMAND、ERROR_INVALID_KEY等错误码

- [ ] **一致性哈希分片**
  - 虚拟节点机制（默认每个节点100个虚拟节点）
  - 1000次SET后，3个分片数据分布差异<30%
  - 同一个Key总是路由到同一个节点

- [ ] **主从复制基本功能**
  - Master的SET操作在10ms内同步到Slave
  - Slave断开重连后请求全量同步
  - Master故障后Slave提升为新Master
  - 原Master重启后连接到新Master并同步

## 性能指标

- 单节点支持最多10万条缓存数据
- 支持1000+并发连接
- 缓存命中响应延迟 < 1ms
- 主从复制同步延迟 < 10ms
- 单进程内存使用不超过2GB

## 验收标准

### 核心功能验收

- [ ] **LRU缓存淘汰算法正确性**
  - 使用双向链表+哈希表实现
  - 容量100时，添加第101条数据后Key1被淘汰
  - 重复访问的Key保持高频使用
  - DELETE操作正确更新LRU链表

- [ ] **TCP服务器基本功能**
  - 服务器监听端口7000
  - 支持10个客户端并发连接
  - 客户端连接后能正常读写数据
  - 客户端断开后服务器正确清理资源

- [ ] **自定义二进制协议**
  - 协议格式：Command (1B) + KeyLen (4B) + ValueLen (4B) + Data
  - GET/SET/DELETE/INFO命令正确处理
  - 返回ERROR_UNKNOWN_COMMAND、ERROR_INVALID_KEY等错误码

- [ ] **一致性哈希分片**
  - 虚拟节点机制（默认每个节点100个虚拟节点）
  - 1000次SET后，3个分片数据分布差异<30%
  - 同一个Key总是路由到同一个节点

- [ ] **主从复制基本功能**
  - Master的SET操作在10ms内同步到Slave
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

- [docs/README.md](./docs/README.md) - 文档索引和导航

## 许可证

本项目仅用于学习和研究目的。

## 联系方式

- 项目地址：[GitHub Repository]
- 问题反馈：[Issues]

---

**项目版本**: v1.0
**最后更新**: 2026-06-07
**适用Go版本**: 1.26.4+

## 文档索引

- [docs/README.md](./docs/README.md) - 文档索引和导航

## 许可证

本项目仅用于学习和研究目的。

## 联系方式

- 项目地址：[GitHub Repository]
- 问题反馈：[Issues]

---

**项目版本**: v1.0
**最后更新**: 2026-06-06
**适用Go版本**: 1.26.4+
