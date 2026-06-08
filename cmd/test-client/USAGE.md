# SD-03 分布式缓存系统 - TCP 测试客户端使用指南

## 快速开始

```bash
# 进入项目目录
cd SD-03

# 启动测试客户端（无需预先启动服务器，客户端内置嵌入式集群）
go run ./cmd/test-client/
```

> **为什么不需要启动服务器？** 测试客户端在进程内嵌入了完整的缓存集群（哈希环 + 3个缓存节点 + TCP服务器 + 主从复制控制器），启动即用，关闭即清理。

---

## 主菜单

启动后看到三级主菜单：

```
1. 简易测试（全自动菜单，覆盖全系统）
2. 自由测试（动态指令，全功能覆盖）
3. 客户端设置
0. 退出
```

---

## 模式1：简易测试

### 一级菜单：模块选择

| 选项 | 模块 | 用例数 |
|------|------|--------|
| 1 | 协议编解码 | 6 |
| 2 | LRU缓存 | 8 |
| 3 | 一致性哈希 | 7 |
| 4 | 缓存节点 | 6 |
| 5 | TCP服务 | 9 |
| 6 | 主从复制 | 7 |
| 7 | 集成测试 | 3 |
| A | 全部执行 | 46 |

### 二级菜单：场景分类

进入模块后可选择：
- **1. 正常测试** - 验证核心功能正常工作
- **2. 异常测试** - 验证非法输入的防御性处理
- **3. 边界测试** - 验证容量限制、并发、极端情况
- **A. 执行全部** - 运行该模块所有用例

### 三级菜单：具体用例

显示具体测试用例列表，输入编号执行单个用例，或输入 `A` 执行全部。

### 示例输出

```
  ┌─ 📋 测试说明 ─────────────────────────────────
  │  【前置场景】协议帧是客户端与服务器通信的基础...
  │  【测试过程】构造5种不同类型的请求帧...
  │  【验证方式】解码后的每个字段必须与编码前的输入完全一致...
  └───────────────────────────────────────────────
  [PASS] 请求编解码往返测试                                     0s
```

### 推荐操作流程

```
1 → 选择"简易测试"
A → 全部执行（46个用例一键完成）
```

---

## 模式2：自由测试

### 进入方式

主菜单输入 `2` 即进入自由测试模式。系统会自动：
1. 启动嵌入式集群（3节点 + TCP服务器）
2. 创建第一个TCP连接

### 基本缓存操作

```bash
free:1> set mykey hello           # 写入数据
  [+] SET OK: mykey = hello

free:1> get mykey                 # 读取数据
  [+] GET OK: mykey = hello

free:1> delete mykey              # 删除数据
  [+] DELETE OK: mykey

free:1> get mykey                 # 确认已删除
  [o] GET OK: mykey = (nil)
```

### 批量操作

```bash
free:1> batch-set 100             # 批量写入100条 (batch-0~batch-99)
  [+] batch-set 完成: 100/100 成功, 耗时 1ms (100000 ops/s)

free:1> batch-get 100             # 批量读取验证
  [+] batch-get 完成: 100 命中, 0 未命中

free:1> batch-del 50              # 批量删除前50条
  [+] batch-del 完成: 50/50 成功

free:1> batch-get 100             # 再次验证
  [+] batch-get 完成: 50 命中, 50 未命中

# 使用自定义前缀
free:1> batch-set 20 custom       # 写入 custom-0~custom-19
free:1> batch-get 20 custom       # 读取对应前缀
```

### 服务器信息查询

```bash
free:1> info                      # 查看服务器INFO
  [+] INFO:
      {
        "TestNode-1": { "capacity": 10000, "size": 0, "status": "Running", ... },
        "TestNode-2": { ... },
        "TestNode-3": { ... }
      }

free:1> nodes                     # 查看所有节点
  节点列表:
  ID               | 状态     | 缓存大小
  TestNode-1       | Running  | 0
  TestNode-2       | Running  | 0
  TestNode-3       | Running  | 0

free:1> ring-info                 # 查看哈希环
  哈希环信息:
    物理节点数:   3
    虚拟节点数:   300
    节点列表:     [TestNode-1 TestNode-2 TestNode-3]

free:1> route mykey               # 查看Key路由
  Key 'mykey' -> 路由到节点: TestNode-2
```

### 主从复制流程

```bash
# 步骤1: 查看可用节点
free:1> nodes
  TestNode-1       | Running  | 0
  TestNode-2       | Running  | 0
  TestNode-3       | Running  | 0

# 步骤2: 配置主从关系
free:1> sync TestNode-1 TestNode-2
  [+] 主从关系已配置: Master=TestNode-1, Slave=TestNode-2

# 步骤3: 写入数据并同步
free:1> sync-set user1 Alice
  [+] sync-set OK: user1 = Alice (已同步到从节点)

free:1> sync-set user2 Bob
  [+] sync-set OK: user2 = Bob (已同步到从节点)

# 步骤4: 查看同步状态
free:1> sync-status
  同步状态:
    MasterID:    TestNode-1
    同步次数:     2
    从节点列表:   [TestNode-2]
    节点总数:     3
    详细状态:
      nodeCount: 3
      masterID: TestNode-1
      slaveIDs: [TestNode-2]
      syncedCount: 2

# 步骤5: 同步删除
free:1> sync-del user1
  [+] sync-del OK: user1 (已同步删除到从节点)

# 步骤6: 全量同步
free:1> full-sync TestNode-1
  [+] 获取主节点数据: 2 条
  [+] 全量同步完成: 2 条数据已同步
```

### 多客户端并发

```bash
# 创建多个连接
free:1> connect                   # 创建第2个连接
  [+] 连接 #2 已创建 -> [::]:xxxxx (当前活跃)

free:1> list                      # 查看所有连接
  ID  | 地址              | 状态
  1   | [::]:xxxxx        | 
  2   | [::]:xxxxx        |  <-- 活跃

# 切换连接
free:2> use 1                     # 切换到连接1
  [+] 已切换到连接 #1

free:1> set key1 value1           # 在连接1上写入
free:1> use 2                     # 切换到连接2
free:2> get key1                  # 在连接2上读取(同一集群，数据共享)
  [+] GET OK: key1 = value1

# 断开连接
free:2> disconnect 1              # 断开连接1
  [+] 连接 #1 已断开

# 压力测试
free:2> stress 5 20               # 5客户端 x 20操作
  [ ] 开始压力测试: 5 客户端 x 20 操作 = 100 总操作
  [+] 压力测试完成: 成功 100/100, 错误 0, 耗时 1ms
      吞吐量: 100000 ops/s
```

### LRU淘汰测试

```bash
free:1> lru-evict 5               # 容量5的LRU淘汰演示
  [+] 已填充 5 条数据（容量=5）
  [+] 写入 overflow 触发淘汰，当前大小: 5
  [+] lru-0 已被淘汰（符合预期）
```

### 原始协议帧测试

```bash
free:1> raw 0104000000036b6579    # 发送 GET key 原始帧
  [+] 已发送 9 字节: 0104000000036b6579
  [+] 响应 10 字节: ...

# 协议帧格式:
# Command(1B) + KeyLen(4B, big-endian) + ValueLen(4B, big-endian) + Key + Value
# 01=GET, 00000003=keylen=3, 00000000=vallen=0, 6b6579="key"
```

### 查看使用示例

```bash
free:1> usage                     # 显示7个完整使用示例
free:1> help                      # 显示所有命令列表
free:1> back                      # 返回主菜单
```

---

## 模式3：客户端设置

```
1. 自动保存测试结果:  false        # 开关切换
2. 输出目录:          test_results # Markdown报告输出路径
3. 连接超时:          5s           # TCP连接超时
4. 详细日志:          true         # 显示测试LOG输出
5. 查看当前完整配置
6. 清空测试报告记录
0. 返回主菜单
```

开启自动保存后，每次执行测试会自动在 `test_results/` 目录下生成 Markdown 报告。

---

## 完整命令速查表

| 命令 | 格式 | 说明 |
|------|------|------|
| `set` | `set <key> <value>` | 写入缓存 |
| `get` | `get <key>` | 读取缓存 |
| `delete` | `delete <key>` | 删除缓存 |
| `info` | `info` | 查看服务器信息 |
| `connect` | `connect [addr]` | 创建TCP连接 |
| `disconnect` | `disconnect [id]` | 断开连接 |
| `use` | `use <id>` | 切换活跃连接 |
| `list` | `list` | 列出所有连接 |
| `sync` | `sync <master> <slave>` | 配置主从关系 |
| `sync-set` | `sync-set <key> <value>` | 主节点写入并同步 |
| `sync-del` | `sync-del <key>` | 主节点删除并同步 |
| `full-sync` | `full-sync <master>` | 全量同步 |
| `sync-status` | `sync-status` | 查看同步状态 |
| `batch-set` | `batch-set <n> [prefix]` | 批量写入 |
| `batch-get` | `batch-get <n> [prefix]` | 批量读取 |
| `batch-del` | `batch-del <n> [prefix]` | 批量删除 |
| `route` | `route <key>` | 查看路由目标 |
| `nodes` | `nodes` | 查看节点列表 |
| `ring-info` | `ring-info` | 查看哈希环信息 |
| `lru-evict` | `lru-evict <capacity>` | LRU淘汰测试 |
| `stress` | `stress <clients> <ops>` | 并发压力测试 |
| `raw` | `raw <hex>` | 发送原始字节 |
| `help` | `help` | 显示命令帮助 |
| `usage` | `usage` | 显示使用示例 |
| `back` | `back` | 返回上级菜单 |

---

## 协议帧格式参考

自定义二进制协议帧结构：

### 请求帧

```
+----------+----------+----------+----------+
| Command  | KeyLen   | ValueLen | Data     |
| 1 byte   | 4 bytes  | 4 bytes  | variable |
+----------+----------+----------+----------+
```

- **Command**: `0x01=GET`, `0x02=SET`, `0x03=DELETE`, `0x04=INFO`
- **KeyLen**: key 的字节长度（big-endian uint32）
- **ValueLen**: value 的字节长度（big-endian uint32）
- **Data**: key + value 的原始字节

### 响应帧

```
+----------+----------+----------+----------+
| Command  | Status   | ValueLen | Body     |
| 1 byte   | (in body)| 4 bytes  | variable |
+----------+----------+----------+----------+
```

- **Status**: `0x00=SUCCESS`, `0x01=ERROR_UNKNOWN_COMMAND`, `0x02=ERROR_INVALID_KEY`, etc.
- **Body**: Status(1 byte) + Value

---

## 文件结构

```
cmd/test-client/
├── main.go              # 入口程序，菜单导航
├── test_client.go       # 核心框架（TestContext、CLIClient、断言、报告）
├── auto_tests.go        # 46个测试用例实现
├── test_descriptions.go # 测试用例描述（前置场景、过程、验证）
├── free_mode.go         # 自由测试命令 + 客户端设置
└── USAGE.md             # 本文档
```
