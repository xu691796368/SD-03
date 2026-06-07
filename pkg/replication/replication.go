// Package replication 实现简化版主从复制机制
// 提供：主节点写操作同步到从节点（SyncToSlave）
//
//	从节点断开重连后全量数据同步（InitSync、RequestFullSync、ApplyFullSync）
//
// 复用 protocol.ProtocolFrame 作为数据传输载体，不新增协议结构体
package replication

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/yourusername/sd-03-cache/pkg/node"
	"github.com/yourusername/sd-03-cache/pkg/protocol"
)

// ============ 错误定义 ============

var (
	// ErrNoNodes 节点列表为空
	ErrNoNodes = errors.New("replication: at least one node is required")

	// ErrNoValidNodes 无有效节点
	ErrNoValidNodes = errors.New("replication: no valid nodes provided")

	// ErrMasterNotFound 主节点不存在
	ErrMasterNotFound = errors.New("replication: master node not found")

	// ErrSlaveNotFound 从节点不存在
	ErrSlaveNotFound = errors.New("replication: slave node not found")

	// ErrSameNode 主从节点不能相同
	ErrSameNode = errors.New("replication: master and slave must be different nodes")

	// ErrNotConfigured 主从关系未配置
	ErrNotConfigured = errors.New("replication: master-slave relationship not configured")

	// ErrEmptyMasterID 主节点ID为空
	ErrEmptyMasterID = errors.New("replication: masterID must not be empty")

	// ErrNilFrames 数据帧列表为空
	ErrNilFrames = errors.New("replication: frames must not be nil")

	// ErrNodeNotRunning 节点未运行
	ErrNodeNotRunning = errors.New("replication: node is not running")

	// ErrEmptyKey Key为空
	ErrEmptyKey = errors.New("replication: key must not be empty")
)

// ============ 主从复制控制器结构 ============

// ReplicationController 主从复制控制器（简化版）
// 管理主从节点关系，协调写同步和全量同步操作
// 内嵌复制状态（masterID、slaveIDs、已同步计数、最后同步时间）
type ReplicationController struct {
	nodes        map[string]*node.CacheNode // 所有缓存节点（nodeID → CacheNode）
	masterID     string                     // 当前主节点ID
	slaveIDs     []string                   // 当前从节点ID列表
	syncedCount  int                        // 已同步数据条数
	lastSyncTime time.Time                  // 最后同步时间
	mu           sync.Mutex                 // 互斥锁保护所有字段
}

// ============ 构造函数 ============

// NewReplicationController 创建主从复制控制器
// nodes: 缓存节点列表，至少包含一个有效节点
// 返回error: 节点列表为空或无有效节点
func NewReplicationController(nodes []*node.CacheNode) (*ReplicationController, error) {
	if len(nodes) == 0 {
		return nil, ErrNoNodes
	}

	// 构建节点映射（nodeID → *CacheNode），过滤 nil 节点
	nodeMap := make(map[string]*node.CacheNode, len(nodes))
	for _, n := range nodes {
		if n != nil {
			nodeMap[n.GetNodeID()] = n
		}
	}

	if len(nodeMap) == 0 {
		return nil, ErrNoValidNodes
	}

	return &ReplicationController{
		nodes:    nodeMap,
		slaveIDs: make([]string, 0),
	}, nil
}

// ============ 主从关系配置 ============

// SetMasterSlave 设置主从关系
// masterID: 主节点ID，slaveID: 从节点ID
// 设置后会将主节点状态设为 Master，从节点状态设为 Slave
// 返回error: 主/从节点不存在、主从相同
func (rc *ReplicationController) SetMasterSlave(masterID, slaveID string) error {
	if masterID == "" || slaveID == "" {
		return ErrEmptyMasterID
	}

	if masterID == slaveID {
		return ErrSameNode
	}

	rc.mu.Lock()
	defer rc.mu.Unlock()

	master, ok := rc.nodes[masterID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrMasterNotFound, masterID)
	}

	slave, ok := rc.nodes[slaveID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrSlaveNotFound, slaveID)
	}

	// 设置主节点状态为 Master
	if err := master.SetStatus(node.StatusMaster); err != nil {
		return fmt.Errorf("replication: failed to set master status: %w", err)
	}

	// 设置从节点状态为 Slave，并关联主节点ID
	if err := slave.SetStatus(node.StatusSlave); err != nil {
		return fmt.Errorf("replication: failed to set slave status: %w", err)
	}
	slave.SetMasterID(masterID)

	// 更新控制器状态
	rc.masterID = masterID
	rc.slaveIDs = []string{slaveID}
	rc.syncedCount = 0
	rc.lastSyncTime = time.Time{}

	return nil
}

// ============ 写同步 ============

// SyncToSlave 主节点执行SET操作后同步到从节点（同步复制）
// 将 key-value 写入所有已配置的从节点
// 返回error: 主从关系未配置、从节点不存在、同步失败
func (rc *ReplicationController) SyncToSlave(key string, value []byte) error {
	if key == "" {
		return ErrEmptyKey
	}

	rc.mu.Lock()
	if rc.masterID == "" || len(rc.slaveIDs) == 0 {
		rc.mu.Unlock()
		return ErrNotConfigured
	}
	// 拷贝 slaveIDs 避免长时间持锁
	slaveIDs := make([]string, len(rc.slaveIDs))
	copy(slaveIDs, rc.slaveIDs)
	rc.mu.Unlock()

	// 向所有从节点写入数据
	for _, slaveID := range slaveIDs {
		slave, ok := rc.nodes[slaveID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrSlaveNotFound, slaveID)
		}

		if err := slave.Set(key, value); err != nil {
			return fmt.Errorf("replication: failed to sync to slave %s: %w", slaveID, err)
		}
	}

	// 更新同步状态
	rc.mu.Lock()
	rc.syncedCount++
	rc.lastSyncTime = time.Now()
	rc.mu.Unlock()

	return nil
}

// SyncDeleteToSlave 主节点执行DELETE操作后同步到从节点
// 从所有已配置的从节点删除指定 key
// 返回error: 主从关系未配置、从节点不存在、同步失败
func (rc *ReplicationController) SyncDeleteToSlave(key string) error {
	if key == "" {
		return ErrEmptyKey
	}

	rc.mu.Lock()
	if rc.masterID == "" || len(rc.slaveIDs) == 0 {
		rc.mu.Unlock()
		return ErrNotConfigured
	}
	slaveIDs := make([]string, len(rc.slaveIDs))
	copy(slaveIDs, rc.slaveIDs)
	rc.mu.Unlock()

	for _, slaveID := range slaveIDs {
		slave, ok := rc.nodes[slaveID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrSlaveNotFound, slaveID)
		}

		if err := slave.Delete(key); err != nil {
			return fmt.Errorf("replication: failed to sync delete to slave %s: %w", slaveID, err)
		}
	}

	rc.mu.Lock()
	rc.syncedCount++
	rc.lastSyncTime = time.Now()
	rc.mu.Unlock()

	return nil
}

// ============ 从节点全量重连同步 ============

// InitSync 从节点初始化同步（连接主节点后调用）
// 设置复制控制器的主节点ID，准备进行数据同步
// 返回error: masterID为空、主节点不存在
func (rc *ReplicationController) InitSync(masterID string) error {
	if masterID == "" {
		return ErrEmptyMasterID
	}

	rc.mu.Lock()
	defer rc.mu.Unlock()

	if _, ok := rc.nodes[masterID]; !ok {
		return fmt.Errorf("%w: %s", ErrMasterNotFound, masterID)
	}

	rc.masterID = masterID
	return nil
}

// RequestFullSync 从节点请求全量数据同步
// 从主节点导出所有缓存数据，封装为 ProtocolFrame 列表（复用协议帧结构体）
// 每个 ProtocolFrame 的 Command 字段为 CMD_SET（0x02），Key 和 Value 分别对应缓存条目
// 返回error: masterID为空、主节点不存在、导出失败
func (rc *ReplicationController) RequestFullSync(masterID string) ([]*protocol.ProtocolFrame, error) {
	if masterID == "" {
		return nil, ErrEmptyMasterID
	}

	rc.mu.Lock()
	master, ok := rc.nodes[masterID]
	rc.mu.Unlock()

	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrMasterNotFound, masterID)
	}

	// 从主节点导出所有数据
	keys, values, err := master.ExportAll()
	if err != nil {
		return nil, fmt.Errorf("replication: failed to export from master %s: %w", masterID, err)
	}

	// 将数据封装为 ProtocolFrame 列表（复用协议帧结构体，不新增数据结构）
	frames := make([]*protocol.ProtocolFrame, 0, len(keys))
	for i := range keys {
		frames = append(frames, protocol.NewFrame(
			uint8(protocol.CMD_SET),
			[]byte(keys[i]),
			values[i],
		))
	}

	return frames, nil
}

// ApplyFullSync 从节点接收全量数据并应用
// 将 ProtocolFrame 列表中的 SET 命令应用到所有已配置的从节点
// 仅处理 Command 为 CMD_SET 的帧，忽略其他命令类型
// 返回error: frames为nil、从节点未配置、应用失败
func (rc *ReplicationController) ApplyFullSync(frames []*protocol.ProtocolFrame) error {
	if frames == nil {
		return ErrNilFrames
	}

	if len(frames) == 0 {
		return nil // 空数据无需应用，不视为错误
	}

	rc.mu.Lock()
	if len(rc.slaveIDs) == 0 {
		rc.mu.Unlock()
		return ErrNotConfigured
	}
	slaveIDs := make([]string, len(rc.slaveIDs))
	copy(slaveIDs, rc.slaveIDs)
	rc.mu.Unlock()

	// 向所有从节点应用数据
	appliedCount := 0
	for _, slaveID := range slaveIDs {
		slave, ok := rc.nodes[slaveID]
		if !ok {
			return fmt.Errorf("%w: %s", ErrSlaveNotFound, slaveID)
		}

		for _, frame := range frames {
			// 仅处理 SET 命令
			if frame.Command == uint8(protocol.CMD_SET) {
				if err := slave.Set(string(frame.Key), frame.Value); err != nil {
					return fmt.Errorf("replication: failed to apply to slave %s: %w", slaveID, err)
				}
				appliedCount++
			}
		}
	}

	// 更新同步状态
	rc.mu.Lock()
	rc.syncedCount += appliedCount
	rc.lastSyncTime = time.Now()
	rc.mu.Unlock()

	return nil
}

// ============ 状态查询方法 ============

// GetMasterID 获取当前主节点ID
func (rc *ReplicationController) GetMasterID() string {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.masterID
}

// GetSlaveIDs 获取当前从节点ID列表（返回拷贝）
func (rc *ReplicationController) GetSlaveIDs() []string {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	result := make([]string, len(rc.slaveIDs))
	copy(result, rc.slaveIDs)
	return result
}

// GetSyncedCount 获取已同步数据条数
func (rc *ReplicationController) GetSyncedCount() int {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.syncedCount
}

// GetLastSyncTime 获取最后同步时间
func (rc *ReplicationController) GetLastSyncTime() time.Time {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.lastSyncTime
}

// GetNodeCount 获取管理的节点总数
func (rc *ReplicationController) GetNodeCount() int {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return len(rc.nodes)
}

// GetState 获取复制状态信息（返回 map，避免新增结构体）
func (rc *ReplicationController) GetState() map[string]interface{} {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	state := map[string]interface{}{
		"masterID":     rc.masterID,
		"slaveIDs":     rc.slaveIDs,
		"syncedCount":  rc.syncedCount,
		"lastSyncTime": rc.lastSyncTime,
		"nodeCount":    len(rc.nodes),
	}
	return state
}
