// Package node 实现缓存节点模块，集成 LRU 缓存和一致性哈希环
// 提供统一的缓存访问接口，支持节点启动/停止和状态管理
package node

import (
	"errors"
	"fmt"
	"sync"

	"github.com/yourusername/sd-03-cache/pkg/cache"
	"github.com/yourusername/sd-03-cache/pkg/shard"
)

// ============ 错误定义 ============

var (
	// ErrEmptyID 节点ID为空
	ErrEmptyID = errors.New("node: id must not be empty")

	// ErrInvalidCapacity 容量参数无效
	ErrInvalidCapacity = errors.New("node: capacity must be positive")

	// ErrNotInitialized 节点未初始化（LRU 或哈希环为空）
	ErrNotInitialized = errors.New("node: not initialized")

	// ErrNodeStopped 节点已停止
	ErrNodeStopped = errors.New("node: node is stopped")

	// ErrInvalidStatus 无效的节点状态
	ErrInvalidStatus = errors.New("node: invalid status")

	// ErrNilRing 哈希环参数为空
	ErrNilRing = errors.New("node: hash ring must not be nil")
)

// ============ 有效状态常量 ============

const (
	StatusStopped = "Stopped"
	StatusRunning = "Running"
	StatusMaster  = "Master"
	StatusSlave   = "Slave"
)

// validStatuses 记录所有有效的节点状态
var validStatuses = map[string]bool{
	StatusStopped: true,
	StatusRunning: true,
	StatusMaster:  true,
	StatusSlave:   true,
}

// ============ 缓存节点结构 ============

// CacheNode 缓存节点
// 管理 LRU 缓存实例，与哈希环集成，提供统一的缓存访问接口
type CacheNode struct {
	nodeID   string          // 节点ID
	capacity int             // 缓存容量
	lru      *cache.LRUCache // LRU 缓存实例
	ring     *shard.HashRing // 所属的哈希环
	status   string          // 节点状态（Stopped/Running/Master/Slave）
	masterID string          // 主节点ID（仅 Slave 有值）
	mu       sync.RWMutex    // 读写锁
}

// ============ 构造函数 ============

// NewCacheNode 创建缓存节点
// id 为节点标识符，capacity 为 LRU 缓存容量
func NewCacheNode(id string, capacity int) (*CacheNode, error) {
	if id == "" {
		return nil, ErrEmptyID
	}
	if capacity <= 0 {
		return nil, ErrInvalidCapacity
	}

	lru, err := cache.NewLRUCache(capacity)
	if err != nil {
		return nil, fmt.Errorf("node: failed to create LRU cache: %w", err)
	}

	return &CacheNode{
		nodeID:   id,
		capacity: capacity,
		lru:      lru,
		status:   StatusStopped,
	}, nil
}

// ============ 初始化 ============

// Init 初始化节点（绑定哈希环）
// ring 为一致性哈希环实例，不可为 nil
func (n *CacheNode) Init(ring *shard.HashRing) error {
	if ring == nil {
		return ErrNilRing
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	n.ring = ring
	return nil
}

// ============ 缓存操作 ============

// Get 获取缓存值
// 若节点未初始化或已停止，返回错误
func (n *CacheNode) Get(key string) ([]byte, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.lru == nil {
		return nil, ErrNotInitialized
	}
	if n.status == StatusStopped {
		return nil, ErrNodeStopped
	}

	val, ok := n.lru.Get(key)
	if !ok {
		return nil, nil
	}
	return val, nil
}

// Set 设置缓存值
// 若节点未初始化或已停止，返回错误
func (n *CacheNode) Set(key string, value []byte) error {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.lru == nil {
		return ErrNotInitialized
	}
	if n.status == StatusStopped {
		return ErrNodeStopped
	}

	return n.lru.Set(key, value)
}

// Delete 删除缓存值
// 若节点未初始化或已停止，返回错误
func (n *CacheNode) Delete(key string) error {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.lru == nil {
		return ErrNotInitialized
	}
	if n.status == StatusStopped {
		return ErrNodeStopped
	}

	n.lru.Delete(key)
	return nil
}

// ============ 状态管理 ============

// GetInfo 获取节点详细信息
// 返回包含节点ID、状态、缓存大小、容量、哈希环状态等信息的 map
func (n *CacheNode) GetInfo() map[string]interface{} {
	n.mu.RLock()
	defer n.mu.RUnlock()

	info := map[string]interface{}{
		"id":       n.nodeID,
		"status":   n.status,
		"capacity": n.capacity,
	}

	if n.lru != nil {
		info["size"] = n.lru.Size()
		info["isFull"] = n.lru.IsFull()
	} else {
		info["size"] = 0
		info["isFull"] = false
	}

	if n.ring != nil {
		info["ringNodes"] = n.ring.NodeCount()
	} else {
		info["ringNodes"] = 0
	}

	if n.masterID != "" {
		info["masterID"] = n.masterID
	}

	return info
}

// GetNodeID 获取节点ID
func (n *CacheNode) GetNodeID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.nodeID
}

// Size 获取当前缓存大小
func (n *CacheNode) Size() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.lru == nil {
		return 0
	}
	return n.lru.Size()
}

// GetCapacity 获取缓存容量
func (n *CacheNode) GetCapacity() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.capacity
}

// GetStatus 获取节点当前状态
func (n *CacheNode) GetStatus() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.status
}

// GetRing 获取关联的哈希环
func (n *CacheNode) GetRing() *shard.HashRing {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.ring
}

// GetMasterID 获取主节点ID（仅 Slave 有值）
func (n *CacheNode) GetMasterID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.masterID
}

// SetStatus 设置节点状态
// status 必须为有效状态值：Stopped/Running/Master/Slave
func (n *CacheNode) SetStatus(status string) error {
	if !validStatuses[status] {
		return ErrInvalidStatus
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	n.status = status
	return nil
}

// SetMasterID 设置主节点ID（用于 Slave 角色）
func (n *CacheNode) SetMasterID(masterID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.masterID = masterID
}

// ExportAll 导出节点中所有缓存数据
// 返回所有键值对，用于主从复制全量同步
// 若节点未初始化或已停止，返回错误
func (n *CacheNode) ExportAll() (keys []string, values [][]byte, err error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.lru == nil {
		return nil, nil, ErrNotInitialized
	}
	if n.status == StatusStopped {
		return nil, nil, ErrNodeStopped
	}

	keys, values = n.lru.ExportAll()
	return keys, values, nil
}

// ============ 生命周期管理 ============

// Start 启动节点
// 将节点状态设置为 Running
func (n *CacheNode) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.status == StatusRunning {
		return nil // 已启动，幂等
	}

	n.status = StatusRunning
	return nil
}

// Stop 停止节点
// 将节点状态设置为 Stopped
func (n *CacheNode) Stop() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.status == StatusStopped {
		return nil // 已停止，幂等
	}

	n.status = StatusStopped
	return nil
}
