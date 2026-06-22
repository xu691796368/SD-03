# SD-03 分布式缓存系统 - 文档索引

本文档索引列出了SD-03项目的所有相关文档及其链接。

## 文档目录

### 1. 项目提案（Proposal）

**[proposal.md](./proposal/proposal.md)** - 分布式缓存系统项目提案
- 项目背景和学习目标
- 总体目标和具体功能目标
- 功能范围和技术约束
- 验收标准和测试结果
- 技术栈和项目结构
- 风险评估和进度计划
- 参考资料

---

### 2. 需求规格（Specifications）

**[specs.md](./specs/specs.md)** - 缓存系统功能场景规格说明
- 定义了5个核心功能模块的功能场景
- 包含LRU缓存淘汰算法、TCP服务器、自定义协议、一致性哈希分片、主从复制
- 使用RFC 2119关键词（MUST/MAY/MUST NOT）定义验收标准
- 适用范围：仅包含核心功能模块的基础实现和验证

**对应设计文档**：[design/design.md](./design/design.md)

**对应任务文档**：[tasks/tasks.md](./tasks/tasks.md)

---

### 3. 设计文档（Design）

**[design.md](./design/design.md)** - 分布式缓存系统设计文档
- 架构概览：客户端→协议编解码→TCP服务器→一致性哈希→缓存节点→主从复制
- 模块划分：6个核心模块的详细设计和接口定义
- 数据模型：协议帧、LRU缓存、哈希环、缓存节点等数据结构
- 技术选型：Go 1.26.4、大端字节序、双向链表等
- 约束要求：功能、性能、测试、代码质量约束

**关键设计决策**：
- 大端字节序（Big-Endian）用于协议编解码
- 使用 Go 标准库 container/list 实现LRU算法
- Go 1.26.4作为开发环境要求

**对应任务文档**：[tasks/tasks.md](./tasks/tasks.md)

---

### 4. 任务文档（Tasks）

**[tasks.md](./tasks/tasks.md)** - 开发任务分解
- Phase 1：基础架构（6个任务）
- Phase 2：核心功能（7个任务）
- Phase 3：测试与优化（11个任务）
- Phase 4：验收与交付（1个任务）

**任务特点**：
- 任务粒度细化到单个Go源码文件或函数级别
- 每条任务关联design.md接口定义
- 每条任务绑定specs/specs.md测试场景
- 确保测试覆盖率>60%

**对应设计文档**：[design/design.md](./design/design.md)

---

### 5. 过程回顾（Review）

**[sdd_review.md](./sdd_review.md)** - SDD 完整过程回顾文档
- SDD（场景驱动开发）全流程回顾
- 遇到的挑战与解决方案（协议编解码、LRU并发、哈希分布、TCP并发、测试隔离）
- 技术决策与设计权衡分析
- AI 使用心得与最佳实践
- 经验教训与改进方向

---

### 6. 快速开始指南

**[QUICK_START.md](../QUICK_START.md)** - 系统快速使用指南
- 快速启动缓存服务器
- 快速启动CLI测试客户端
- 常用测试命令说明
- 性能指标参考

---

## 项目代码结构

### 核心代码（pkg/）

| 模块 | 目录 | 源码文件 | 说明 |
|------|------|---------|------|
| LRU缓存 | `pkg/cache/` | `cache.go` | LRU缓存实现（双向链表+哈希表） |
| 协议编解码 | `pkg/protocol/` | `protocol.go` | 二进制协议定义与编解码 |
| 一致性哈希 | `pkg/shard/` | `shard.go` | 哈希环与虚拟节点 |
| 缓存节点 | `pkg/node/` | `node.go` | 节点管理（集成LRU+哈希环） |
| TCP服务器 | `pkg/server/` | `server.go` | TCP服务器核心实现 |
| 主从复制 | `pkg/replication/` | `replication.go` | 复制控制器实现 |

> 每个模块的单元测试（`*_test.go`）遵循Go惯例，与源码放在同一目录下。

### 主程序入口（cmd/）

| 程序 | 目录 | 入口文件 | 说明 |
|------|------|---------|------|
| 缓存服务器 | `cmd/cache-server/` | `main.go` | 服务器启动入口 |
| CLI测试客户端 | `cmd/test-client/` | `main.go` | 交互式测试工具（嵌入式集群） |

### 测试目录（tests/）

| 目录 | 文件 | 说明 |
|------|------|------|
| `tests/client/` | `test_client.go`, `test_client_test.go` | 测试客户端工具库与完整测试套件 |
| `tests/integration/` | `integration_test.go`, `advanced_test.go` | 端到端集成测试与高级测试 |

---

## 文档关系图

```
README.md (项目主README)
    │
    ├── proposal.md (项目提案)
    │   ├── 关联文档 ───────> docs/specs/specs.md (需求规格)
    │   │                    │
    │   │                    ├── 对应 design/design.md (设计文档)
    │   │                    │   ├── 架构概览
    │   │                    │   ├── 模块划分
    │   │                    │   ├── 数据模型
    │   │                    │   └── 接口定义
    │   │                    │
    │   │                    └── 对应 tasks/tasks.md (任务文档)
    │   │                        ├── Phase 1: 基础架构 (6任务)
    │   │                        ├── Phase 2: 核心功能 (7任务)
    │   │                        ├── Phase 3: 测试与优化 (11任务)
    │   │                        └── Phase 4: 验收与交付 (1任务)
    │   │
    │   ├── sdd_review.md (SDD回顾文档)
    │   │    └── SDD全流程回顾与经验总结
    │   │
    │   ├── QUICK_START.md (快速开始指南)
    │   │    └── 快速启动与使用说明
    │
    ├── docs/specs/specs.md (需求规格)
    │
    ├── docs/design/design.md (设计文档)
    │
    ├── docs/tasks/tasks.md (任务文档)
    │
    ├── docs/sdd_review.md (SDD回顾文档)
    │
    ├── cmd/test-client/USAGE.md (CLI客户端使用指南)
    │
    └── AI工具使用/ (AI工具使用记录)
```

**项目代码结构图**：

```
SD-03/
├── cmd/ (主程序入口)
│   ├── cache-server/ (缓存服务器)
│   └── test-client/ (CLI测试客户端)
├── pkg/ (核心代码)
│   ├── cache/ (LRU缓存)
│   ├── protocol/ (协议编解码)
│   ├── shard/ (一致性哈希)
│   ├── node/ (缓存节点)
│   ├── server/ (TCP服务器)
│   └── replication/ (主从复制)
├── tests/ (测试文件)
│   ├── client/ (测试客户端工具)
│   └── integration/ (集成测试)
└── test_results/ (测试结果报告)
```

---

## 文档阅读顺序

### 初次了解项目
1. 阅读 [README.md](../README.md) - 项目主README
2. 阅读 [docs/proposal/proposal.md](./proposal/proposal.md) - 项目提案（背景、目标、范围）

### 快速上手
3. 阅读 [QUICK_START.md](../QUICK_START.md) - 系统快速使用指南
4. 阅读 [cmd/test-client/USAGE.md](../cmd/test-client/USAGE.md) - CLI测试客户端使用指南

### 深入了解需求
5. 阅读 [docs/specs/specs.md](./specs/specs.md) - 功能场景规格
6. 对照 [docs/design/design.md](./design/design.md) - 设计文档

### 开始开发
7. 阅读 [docs/design/design.md](./design/design.md) - 架构和接口设计
8. 阅读 [docs/tasks/tasks.md](./tasks/tasks.md) - 任务分解和验收标准
9. 参考 [pkg/](../pkg/) - 代码实现

### 完成开发
10. 编写代码（遵循 [docs/design/design.md](./design/design.md) 的接口定义）
11. 编写测试（遵循 [docs/tasks/tasks.md](./tasks/tasks.md) 的测试任务）
12. 执行验收（对照 [docs/specs/specs.md](./specs/specs.md) 的验收标准）

### 学习回顾
13. 阅读 [docs/sdd_review.md](./sdd_review.md) - SDD完整过程回顾
14. 参考 [AI工具使用/](../AI工具使用/) - AI使用记录与经验总结

---

## 文档版本

- **proposal.md**: v4.1
- **specs.md**: v2.0
- **design.md**: v2.0
- **tasks.md**: v3.0
- **docs/README.md**: v3.0
- **README.md**: v1.1

**创建日期**: 2026-06-06
**最后更新**: 2026-06-22
**维护者**: SD-03项目组
