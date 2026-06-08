# Tasks: SD-03分布式缓存系统

## Phase 1: 基础架构
- [ ] Task 1.1: 创建项目目录结构（cmd/cache-server、pkg/cache、pkg/shard、pkg/protocol、pkg/server、pkg/replication、tests）
- [ ] Task 1.2: 初始化Go模块，编写go.mod配置文件
- [ ] Task 1.3: 创建根目录README.md，包含项目介绍、快速开始、技术栈说明
- [ ] Task 1.4: 创建protocol包目录，定义协议常量和类型（CMD_GET/SET/DELETE/INFO、错误码、ProtocolFrame结构体）
- [ ] Task 1.5: 实现ProtocolFrame序列化/反序列化函数（EncodeRequest、DecodeRequest、EncodeResponse、DecodeRequest）
- [ ] Task 1.6: 实现ProtocolFrame验证函数（ValidateFrame），检查帧长度、KeyLen、ValueLen一致性

## Phase 2: 核心功能
- [ ] Task 2.1: 实现LRU缓存核心功能，使用container.list标准库+哈希表，包含NewLRUCache、Get/Set/Delete/Size/IsFull/Clear方法
- [ ] Task 2.2: 实现一致性哈希分片核心功能，使用FNV-1a算法、虚拟节点机制，包含NewHashRing、AddNode、RemoveNode、Rebuild、GetNode方法
- [ ] Task 2.3: 实现缓存节点模块，集成LRU缓存和哈希环，包含NewCacheNode、Init、Get/Set/Delete、GetInfo、Start/Stop方法
- [ ] Task 2.4: 实现TCP服务器核心功能，包含NewTCPServer、Start、Stop、handleConnection、handleRequest方法，实现多客户端并发连接处理
- [ ] Task 2.5: 实现TCP服务器命令处理器，合并handleGet、handleSet、handleDelete、handleInfo方法，根据命令码分发请求
- [ ] Task 2.6: 实现主从复制核心功能（P0），包含写同步（SyncToSlave）和从节点全量重连同步（InitSync、RequestFullSync、ApplyFullSync）
- [ ] Task 2.7: 实现主程序入口，创建cmd/cache-server/main.go，初始化TCPServer、CacheNode、HashRing并启动服务

## Phase 3: 测试与优化
### 3.1 单元测试
- [ ] Task 3.1: 编写protocol包单元测试，测试序列化/反序列化、ValidateFrame、错误码处理
- [ ] Task 3.2: 编写cache包单元测试，测试LRU缓存所有方法、容量淘汰机制、热点数据保持、删除操作、空键处理
- [ ] Task 3.3: 编写shard包单元测试，测试哈希环初始化、虚拟节点添加/移除、GetNode方法、数据分布（偏差公式：abs(单节点数量-均值)/均值）
- [ ] Task 3.4: 编写node包单元测试，测试缓存节点基本操作、状态管理
- [ ] Task 3.5: 编写server包单元测试，测试命令分发逻辑、网络异常处理
- [ ] Task 3.6: 编写replication包单元测试，测试主从关系配置、写同步、从节点全量重连同步、并发安全性


### 3.2 集成测试
- [ ] Task 3.7: 编写集成测试，覆盖所有正常场景（GET/SET/DELETE/INFO命令、多客户端连接、主从同步）和主要异常场景（非法命令、参数缺失、连接断开）
- [ ] Task 3.8: 编写集成测试，覆盖LRU淘汰、一致性哈希路由、主从复制、协议帧边界条件（长度不足、超大值、缓冲区溢出）

### 3.3 优化与文档
### 质量保证（1个任务）
- [ ] Task 3.9: 编写TCP客户端测试工具，执行完整测试套件
  - 包含：协议编解码测试、LRU缓存测试、哈希环测试、TCP服务器测试、主从复制测试
  - 要求：所有测试通过，覆盖正常、异常、边界场景
  - 输出：测试报告（覆盖率统计、测试结果）

### 文档完善（3个任务）
- [ ] Task 3.10: 补充代码注释，确保遵循Go规范
  - 要求：关键算法注释、复杂逻辑注释、结构体字段注释
  - 输出：代码注释覆盖率报告

- [ ] Task 3.11: 编写使用示例和Demo演示
  - 要求：快速开始指南、典型场景示例（5个核心场景）
  - 输出：demo示例代码、README更新

- [ ] Task 3.12: 编写测试工具和集成测试脚本
  - 要求：测试工具代码、自动化测试脚本、测试覆盖率报告
  - 输出：test_client.go、test_suite.go、coverage_report.html

## Phase 4: 验收与交付
### 验收检查（1个任务）
- [ ] Task 4.1: 执行所有验收标准检查
  - 检查项：验收标准1-5、约束要求（功能/性能/测试/质量）
  - 输出：验收检查表（Pass/Fail/需改进）

### 交付准备（1个任务）
- [ ] Task 4.2: 整理项目交付材料
  - 包含：代码、文档、测试报告、演示视频、测试截图
  - 输出：交付清单

### 最终审查（1个任务）
- [ ] Task 4.3: 项目最终审查和交付
  - 要求：代码审查、文档完整性、项目打包
  - 输出：最终项目包、交付文档

---

## 任务说明

### 任务粒度优化说明
- **合并同类任务**：LRU的Get/Set/Delete/Size等方法合并为1条任务；TCP各类handler统一合并
- **精简主从复制**：只保留写同步和从全量重连（P0），自动故障提升改为可选优化任务
- **单元测试合并**：每个包只1条测试任务，覆盖该模块全部spec场景
- **集成测试合并**：合并为1-2条任务，统一覆盖所有正常、异常场景

### 关联关系
- **Protocol模块**：Task 1.4-1.6 → 关联design.md Module 1，对应spec场景：命令处理、错误码、校验码
- **LRU缓存模块**：Task 2.1 → 关联design.md Module 2，对应spec场景：基本读写、容量淘汰、热点数据、删除操作、空键/超大值
- **一致性哈希模块**：Task 2.2 → 关联design.md Module 3，对应spec场景：单分片、虚拟节点、哈希环、数据迁移、节点移除
- **缓存节点模块**：Task 2.3 → 关联design.md Module 4，集成LRU和HashRing
- **TCP服务器模块**：Task 2.4-2.5 → 关联design.md Module 5，对应spec场景：服务器启动、多客户端、异常断开、协议帧长度不足、非法命令、参数缺失
- **主从复制模块**：Task 2.6 → 关联design.md Module 6，对应spec场景：写同步、从全量重连
- **主程序**：Task 2.7 → 关联整体架构

### 测试场景映射
- **LRU场景**：Task 3.2 → cache.md LRU缓存场景（基本读写、容量淘汰、热点数据、删除操作、空键处理）
- **哈希环场景**：Task 3.3 → cache.md 一致性哈希场景（单分片、虚拟节点、哈希环形成、数据迁移、节点移除）
- **TCP服务器场景**：Task 3.5 → cache.md TCP服务器场景（启动、多客户端、异常断开、协议帧长度不足、非法命令、参数缺失、校验码错误）
- **主从复制场景**：Task 3.6-3.7 → cache.md 主从复制场景（写同步、断开重连、故障切换（可选））

### 验收标准映射
- **验收标准1（LRU）**：Task 2.1、3.2，要求使用container/list+哈希表，容量100时添加第101条时Key1被淘汰
- **验收标准2（TCP服务器）**：Task 2.4-2.5，要求监听端口7000，支持10个并发连接
- **验收标准3（协议编解码）**：Task 1.4-1.6，要求帧格式正确、命令码/错误码符合定义
- **验收标准4（一致性哈希）**：Task 2.2、3.3，要求虚拟节点100个，1000次SET后3个分片数据分布差异<30%（偏差公式）
- **验收标准5（主从复制）**：Task 2.6、3.6-3.7，要求写同步、从全量重连（同步延迟<10ms），故障切换为可选

### 关键设计变更说明
1. **LRU实现调整**：使用Go标准库`container/list`实现双向链表（非自定义），与design.md最新版本一致
2. **偏差计算公式**：偏差 = abs(单节点数量-均值) / 均值，确保数据分布差异<30%
3. **主从复制简化**：只保留P0功能（写同步、从全量重连），自动故障提升降级为可选优化任务


---

**文档版本**: v3.0
**创建日期**: 2026-06-06
**更新日期**: 2026-06-09
**状态**: 待评审
**适用范围**: SD-03分布式缓存系统任务分解（v3.0精简版）
