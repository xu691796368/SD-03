# 分布式缓存系统 - 功能场景规格说明

本文档定义了分布式缓存系统的核心功能场景，遵循场景驱动开发（Scenario-Driven Development）规范。

**适用范围**：仅包含核心功能模块的基础实现和验证，聚焦LRU淘汰算法、TCP服务器、自定义协议、一致性哈希分片、简化版主从复制。

---

## 1. LRU缓存淘汰策略

### Requirement: LRU缓存淘汰算法
The system SHALL implement a Least Recently Used (LRU) cache eviction policy using a combination of doubly-linked list and hash table, maintaining the most recently used items at the front and evicting the least recently used items when the cache reaches its capacity.

#### Scenario: LRU缓存基本读写操作
- GIVEN 缓存系统已初始化，初始容量为100条数据
- AND 缓存中已存在数据：Key1=Value1, Key2=Value2, Key3=Value3
- WHEN 执行GET请求查询Key1
- AND 执行SET操作添加Key4=Value4
- AND 执行GET请求查询Key4
- AND 执行GET请求查询Key2
- THEN 缓存系统 MUST 返回 Value1, Value4, Value2
- AND 缓存系统 MUST 保持数据一致性

#### Scenario: 缓存达到容量上限时自动淘汰
- GIVEN 缓存系统已初始化，初始容量为100条数据
- AND 缓存中已存在100条数据（Key1~Key100）
- WHEN 执行SET操作添加Key101=Value101
- AND 执行GET请求查询Key101
- AND 执行GET请求查询Key1
- THEN 缓存系统 MUST 返回 Value101
- AND 缓存系统 MUST NOT 返回 Key1（被LRU淘汰）
- AND 缓存系统 MUST 确保缓存大小保持为100条

#### Scenario: 重复访问热点数据保持命中
- GIVEN 缓存系统已初始化，容量为100条数据
- AND 缓存中已存在Key1=Value1, Key2=Value2, Key3=Value3
- WHEN 执行GET请求查询Key1，重复3次
- AND 执行SET操作添加Key4=Value4
- AND 再次执行GET请求查询Key1
- THEN 缓存系统 MUST 返回 Value1 每次查询
- AND 缓存系统 MUST 成功添加Key4

#### Scenario: 删除操作更新LRU链表
- GIVEN 缓存系统已初始化，容量为100条数据
- AND 缓存中已存在Key1~Key100
- WHEN 执行DELETE操作删除Key50
- AND 执行GET请求查询Key50
- AND 执行GET请求查询Key51
- AND 执行SET操作添加Key101=Value101
- THEN 缓存系统 MUST NOT 返回 Key50
- AND 缓存系统 MUST 返回 Key51

#### Scenario: 查询不存在的键值
- GIVEN 缓存系统已初始化，容量为100条数据
- AND 缓存中已存在Key1~Key50
- WHEN 执行GET请求查询Key999
- THEN 缓存系统 MUST 返回 null
- AND 缓存系统 MUST NOT 改变缓存大小

#### Scenario: 空值或空键的SET操作
- GIVEN 缓存系统已初始化，容量为100条数据
- WHEN 执行SET操作添加KeyEmpty=""
- AND 执行SET操作添加""=ValueEmpty
- THEN 缓存系统 MAY 返回成功（空值允许）
- AND 缓存系统 MUST NOT 允许空键（返回错误）

#### Scenario: 超大值的SET操作
- GIVEN 缓存系统已初始化，容量为100条数据
- WHEN 执行SET操作添加KeyLarge="data_large"（长度>1MB）
- THEN 缓存系统 MAY 返回错误（可选）
- AND 缓存系统 MUST NOT 分配超过缓冲区大小的内存

---

## 2. TCP服务器实现

### Requirement: TCP服务器基本功能
The system SHALL provide a TCP server that supports multiple concurrent client connections and handles network exceptions gracefully.

#### Scenario: 服务器正常启动和监听
- GIVEN 缓存系统启动
- WHEN 执行服务器启动命令
- THEN 服务器 MUST 成功监听指定端口（默认7000）
- AND 服务器 MUST 保持运行状态

#### Scenario: 多客户端并发连接
- GIVEN 缓存服务器已启动并监听端口7000
- WHEN 执行客户端连接请求5次
- AND 每个客户端发送独立的GET请求
- THEN 服务器 MUST 成功处理所有5个连接
- AND 每个客户端 MUST 能够正常读写数据

#### Scenario: 客户端异常断开连接
- GIVEN 缓存服务器已启动并连接了3个客户端
- WHEN 执行客户端强制关闭网络连接
- THEN 服务器 MUST 捕获连接异常
- AND 服务器 MUST 清理该客户端的资源
- AND 其他客户端连接 MUST 不受影响

#### Scenario: 协议帧长度不足
- GIVEN 缓存服务器已启动
- WHEN 发送一个不完整的帧（只发送帧头，不发送数据）
- THEN 服务器 MAY 等待直到超时或发送完整帧
- AND 服务器 MUST NOT 崩溃

#### Scenario: 非法命令处理
- GIVEN 缓存服务器已启动
- WHEN 发送二进制协议请求：Command=0x99（未知命令）
- THEN 服务器 MUST 返回错误码 ERROR_UNKNOWN_COMMAND
- AND 服务器 MUST NOT 崩溃

---

## 3. 自定义协议设计

### Requirement: 自定义二进制协议
The system SHALL define a simple binary protocol for communication between cache server and clients, supporting basic commands like GET, SET, DELETE, and INFO.

#### Scenario: GET命令正常处理
- GIVEN 缓存服务器已启动，缓存中包含Key1=Value1
- WHEN 发送二进制协议请求：Command=GET, Key="Key1"
- THEN 服务器 MUST 返回：Command=GET, Status=SUCCESS, Value="Value1"
- AND 服务器 MUST 验证协议帧长度正确

#### Scenario: SET命令正常处理
- GIVEN 缓存服务器已启动，容量为10000
- WHEN 发送二进制协议请求：Command=SET, Key="TestKey", Value="TestValue"
- THEN 服务器 MUST 返回：Command=SET, Status=SUCCESS
- AND 缓存系统 MUST 存储Key和Value
- AND 缓存大小 MUST 增加1

#### Scenario: DELETE命令正常处理
- GIVEN 缓存服务器已启动，缓存中包含Key1=Value1
- WHEN 发送二进制协议请求：Command=DELETE, Key="Key1"
- THEN 服务器 MUST 返回：Command=DELETE, Status=SUCCESS
- AND 缓存系统 MUST 不再包含Key1
- AND 缓存大小 MUST 减少1

#### Scenario: INFO命令返回服务器信息
- GIVEN 缓存服务器已启动
- WHEN 发送二进制协议请求：Command=INFO
- THEN 服务器 MUST 返回：Command=INFO, Status=SUCCESS
- AND 响应 MUST 包含服务器ID和版本号

#### Scenario: 无效命令返回错误码
- GIVEN 缓存服务器已启动
- WHEN 发送二进制协议请求：Command=0x99（未知命令）
- THEN 服务器 MUST 返回：Status=ERROR_UNKNOWN_COMMAND
- AND 错误码 MUST 为0x01

#### Scenario: 参数缺失或格式错误
- GIVEN 缓存服务器已启动
- WHEN 发送二进制协议请求：Command=GET（缺少Key参数）
- THEN 服务器 MUST 返回：Status=ERROR_INVALID_KEY
- AND 错误码 MUST 为0x02

#### Scenario: 校验码错误
- GIVEN 缓存服务器已启动
- WHEN 发送二进制协议请求，Key长度字段为100但实际Key长度为50
- THEN 服务器 MAY 返回错误（可选）
- AND 服务器 MUST NOT 崩溃

---

## 4. 一致性哈希分片

### Requirement: 一致性哈希分片
The system SHALL implement consistent hashing to distribute cache data across multiple shards, using virtual nodes to achieve basic load balancing.

#### Scenario: 单分片基础功能
- GIVEN 缓存系统初始化，仅包含一个分片节点（Node1）
- WHEN 执行GET请求Key1
- AND 执行SET操作Key1=Value1
- THEN 数据 MUST 存储在Node1的分片上
- AND 所有操作 MUST 在Node1上执行

#### Scenario: 虚拟节点数据均匀分布
- GIVEN 缓存系统初始化，包含3个物理节点（NodeA、NodeB、NodeC）
- AND 每个物理节点有100个虚拟节点
- WHEN 执行1000次SET操作，随机生成1000个不同的Key
- THEN 节点间的数据分布差异 MAY < 30%
- AND 总数据量 MUST 为1000条

#### Scenario: 一致性哈希环环形成
- GIVEN 缓存系统初始化，添加3个物理节点
- WHEN 执行查询每个Key所属的分片节点
- THEN 所有Key MUST 映射到哈希环上的某个节点
- AND 同一个Key MUST 映射到同一个节点

#### Scenario: 添加新节点后的数据迁移
- GIVEN 缓存系统已包含NodeA、NodeB、NodeC
- AND NodeA上已存储500条数据
- WHEN 添加新节点NodeD（虚拟节点数100）
- THEN 约10-20%的NodeA数据 MAY 迁移到NodeD
- AND 其余数据 MUST 保持在NodeA
- AND NodeD MUST 接收约100-200条数据

#### Scenario: 移除节点后的数据重分配
- GIVEN 缓存系统包含NodeA、NodeB、NodeC、NodeD
- AND NodeD上存储约150条数据
- WHEN 移除NodeD
- THEN 约150条数据 MAY 重新分配到其他3个节点
- AND 重分配后系统 MUST 仍然可以正常读写

#### Scenario: Key的哈希冲突处理
- GIVEN 缓存系统初始化
- WHEN 执行SET操作Key1=Value1，再执行SET操作Key2=Value2
- THEN 两个Key MUST 能够正确存储
- AND 读取Key1 MUST 返回Value1，读取Key2 MUST 返回Value2

---

## 5. 主从复制

### Requirement: 简化版主从复制
The system SHALL implement a simplified primary-secondary replication mechanism where write operations on the primary are synchronized to one or more secondary nodes.

#### Scenario: 主从同步正常工作
- GIVEN 缓存系统包含1个主节点（Master）和1个从节点（Slave）
- AND 主从节点已建立连接
- WHEN 在Master上执行SET操作Key1=Value1
- THEN Master MUST 立即返回成功
- AND Slave MUST 在10ms内接收到Key1=Value1
- AND Slave MUST 存储Key1=Value1

#### Scenario: 从节点断开重连后恢复同步
- GIVEN 缓存系统包含1个Master和1个Slave
- AND Master上有1000条数据
- WHEN 在Slave断开连接10秒后重新连接
- THEN Slave MUST 与Master建立连接
- AND Slave MUST 请求同步Master的所有1000条数据
- AND Slave MUST 完成同步

#### Scenario: 主节点故障后从节点提升
- GIVEN 缓存系统包含1个主节点（Master）和1个从节点（Slave）
- AND 主从节点已同步所有数据
- WHEN Master进程崩溃
- AND Slave检测到Master不可达
- THEN Slave状态 MUST 变为"Master"
- AND Slave MUST 开始接受写操作

#### Scenario: 主节点恢复后成为从节点
- GIVEN 缓存系统包含1个主节点（Master）和1个从节点（Slave）
- AND Master故障后Slave提升为新Master
- AND Slave上有500条新数据
- WHEN 原Master重启并连接到Slave
- THEN 原Master状态 MUST 变为"Slave"
- AND 原Master MUST 请求同步新Master的数据

#### Scenario: 协议帧超大导致缓冲区溢出
- GIVEN 缓存服务器已启动
- WHEN 发送一个超过缓冲区大小的帧（例如Key长度为1GB）
- THEN 服务器 MUST 返回错误：ERROR_INVALID_VALUE
- AND 服务器 MUST NOT 分配1GB内存

---

## 6. 集成测试场景

### Requirement: 端到端集成验证
The system SHALL support end-to-end integration testing to verify the complete workflow from client request to server response.

#### Scenario: 完整的缓存读写流程
- GIVEN 缓存系统已启动，包含3个分片节点
- AND 缓存容量为100条数据
- WHEN 客户端执行SET操作Key1=Value1
- AND 执行GET操作Key1，期望返回Value1
- AND 执行SET操作Key2=Value2
- AND 执行GET操作Key2，期望返回Value2
- AND 执行DELETE操作Key1
- AND 执行GET操作Key1，期望返回null
- THEN 所有操作 MUST 成功执行
- AND 缓存数据 MUST 一致
- AND LRU链表 MUST 正确更新

---

## 7. 测试场景追溯矩阵

> 以下矩阵将每一条 Spec 场景映射到客户端测试工具（`cmd/test-client`）中的测试 ID 及测试描述。
> 测试 ID 命名规则：P=协议编解码、C=LRU缓存、S=一致性哈希、N=缓存节点、T=TCP服务、R=主从复制、I=集成测试。
> 标注 ⚠️ 的场景为简化版实现（可选/降级），核心功能已在对应测试中覆盖。

### 7.1 LRU 缓存淘汰策略（Spec 第1节）

| Spec 场景 | 测试 ID | 测试描述 | 覆盖说明 |
|-----------|---------|---------|---------|
| LRU缓存基本读写操作 | C02 | 基本 SET/GET/DELETE | GET 查询、SET 添加、GET 验证值、DELETE 删除 |
| 缓存达到容量上限时自动淘汰 | C06 | 容量满自动淘汰 | 写满后再写入触发淘汰，Size 不超过容量 |
| 重复访问热点数据保持命中 | C07 | 热点数据保留 | 频繁访问的 Key 在淘汰中存活 |
| 删除操作更新LRU链表 | C08 | 删除释放空间不触发淘汰 | 删除后写入不触发淘汰 |
| 查询不存在的键值 | C02 | 基本 SET/GET/DELETE | GET 不存在 Key 返回 ok=false |
| 空值或空键的SET操作 | C05 | 空键拒绝 | 空 Key 被拒绝，空 Value 被接受 |
| 超大值的SET操作 | P04, I03 | 请求编码参数校验 / 超大Value拒绝 | 超过 MaxValueLength 被拒绝 |

### 7.2 TCP 服务器实现（Spec 第2节）

| Spec 场景 | 测试 ID | 测试描述 | 覆盖说明 |
|-----------|---------|---------|---------|
| 服务器正常启动和监听 | T01~T09 | TCP服务全模块 | 每个测试创建独立集群验证服务器正常启动 |
| 多客户端并发连接 | T08, I06 | 多客户端并发(5) / 多客户端并发集成 | 5 客户端×10操作无数据串扰 |
| 客户端异常断开连接 | T07 | 客户端断开连接 | 断开后新客户端仍可正常操作 |
| 协议帧长度不足 | I01 | 截断帧头处理 | 4字节不完整帧头，服务器不崩溃 |
| 非法命令处理 | T05 | 非法命令处理 | 0x99 命令返回 ERROR_UNKNOWN_COMMAND |

### 7.3 自定义协议设计（Spec 第3节）

| Spec 场景 | 测试 ID | 测试描述 | 覆盖说明 |
|-----------|---------|---------|---------|
| GET命令正常处理 | P01, T01 | 请求编解码往返测试 / SET/GET 基本操作 | GET 编解码一致性 + TCP 端到端 |
| SET命令正常处理 | P01, T01 | 请求编解码往返测试 / SET/GET 基本操作 | SET 编解码一致性 + TCP 端到端 |
| DELETE命令正常处理 | P01, T02 | 请求编解码往返测试 / DELETE 操作 | DELETE 编解码一致性 + TCP 端到端 |
| INFO命令返回服务器信息 | P01, T03 | 请求编解码往返测试 / INFO 操作 | INFO 返回非空服务器状态数据 |
| 无效命令返回错误码 | P06, T05 | 帧验证与边界检查 / 非法命令处理 | 0x99 → ERROR_UNKNOWN_COMMAND(0x01) |
| 参数缺失或格式错误 | P06, T06 | 帧验证与边界检查 / 参数缺失处理 | 空 Key → ERROR_INVALID_KEY(0x02) |
| 校验码错误 | P06 | 帧验证与边界检查 | KeyLen 声明与实际不符 → ERROR_FRAME_MISMATCH(0x06) |

### 7.4 一致性哈希分片（Spec 第4节）

| Spec 场景 | 测试 ID | 测试描述 | 覆盖说明 |
|-----------|---------|---------|---------|
| 单分片基础功能 | S02 | 添加节点 | 单节点路由验证 |
| 虚拟节点数据均匀分布 | S06 | 数据分布平衡 | 3节点×1000Key，偏差<30% |
| 一致性哈希环环形成 | S04 | 路由确定性 | 同一 Key 多次路由结果一致 |
| 添加新节点后的数据迁移 | S07 | 环完整性(添加/移除) | 添加 Node-4 后所有 Key 仍可路由 |
| 移除节点后的数据重分配 | S03, S07 | 移除节点 / 环完整性(添加/移除) | 移除 Node-3 后其 Key 重路由 |
| Key的哈希冲突处理 | S04 | 路由确定性 | 不同 Key 可正确存储和读取 |

### 7.5 主从复制（Spec 第5节）

| Spec 场景 | 测试 ID | 测试描述 | 覆盖说明 |
|-----------|---------|---------|---------|
| 主从同步正常工作 | R02, I07 | 写同步到从节点 / 主从写同步集成 | Master SET → SyncToSlave → Slave GET 一致 |
| 从节点断开重连后恢复同步 | R06, I08 | 全量同步恢复 / 主从全量同步集成 | RequestFullSync + ApplyFullSync 全量恢复 |
| 主节点故障后从节点提升 ⚠️ | N04 | 节点状态管理 | SetStatus(Slave→Master) 状态切换（简化版） |
| 主节点恢复后成为从节点 ⚠️ | N04 | 节点状态管理 | SetStatus 状态可逆切换（简化版） |
| 协议帧超大导致缓冲区溢出 | I03, P04 | 超大Value拒绝 / 请求编码参数校验 | 超大 Value 在编码阶段被拒绝 |

### 7.6 集成测试场景（Spec 第6节）

| Spec 场景 | 测试 ID | 测试描述 | 覆盖说明 |
|-----------|---------|---------|---------|
| 完整的缓存读写流程 | T04, I04, I05 | 完整工作流 / SET/GET端到端集成 / 完整读写删工作流 | SET→GET→SET→DELETE→GET 全链路验证 |

### 7.7 测试覆盖率统计

| 模块 | 测试 ID 范围 | 正常用例 | 异常用例 | 边界用例 | 合计 |
|------|-------------|---------|---------|---------|------|
| 协议编解码 | P01~P06 | 3 | 2 | 1 | 6 |
| LRU缓存 | C01~C08 | 4 | 1 | 3 | 8 |
| 一致性哈希 | S01~S07 | 4 | 1 | 2 | 7 |
| 缓存节点 | N01~N06 | 5 | 0 | 1 | 6 |
| TCP服务 | T01~T09 | 4 | 3 | 2 | 9 |
| 主从复制 | R01~R07 | 4 | 1 | 2 | 7 |
| 集成测试 | I01~I08 | 5 | 0 | 3 | 8 |
| **合计** | — | **29** | **8** | **14** | **51** |

> Spec 全部 31 条场景均已覆盖对应测试 ID 与测试描述。标注 ⚠️ 的 2 条主从故障切换场景为简化版实现（见 tasks.md 关键设计变更说明第3条），核心状态切换逻辑已由 N04 覆盖。

---

**文档版本**: v2.2
**创建日期**: 2026-06-06
**更新日期**: 2026-06-09
**作者**: SD-03项目组
**状态**: 已验证
**适用范围**: 核心功能模块基础实现和验证，不包含性能测试、错误恢复、安全性等进阶功能
