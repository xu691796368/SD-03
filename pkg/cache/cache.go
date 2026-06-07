// Package cache 实现LRU（Least Recently Used）缓存淘汰算法
// 使用 container/list 标准库实现双向链表 + 哈希表，实现O(1)时间复杂度的缓存操作
package cache

import (
	"container/list"
	"errors"
	"sync"
)

// ============ 错误定义 ============

var (
	// ErrInvalidCapacity 容量参数无效（capacity <= 0）
	ErrInvalidCapacity = errors.New("cache: capacity must be positive")

	// ErrEmptyKey 不允许空键
	ErrEmptyKey = errors.New("cache: key must not be empty")
)

// ============ 缓存条目 ============

// cacheEntry 链表中存储的缓存条目
// 用于在淘汰时从哈希表中反向查找并删除对应的key
type cacheEntry struct {
	key   string
	value []byte
}

// ============ LRU缓存结构 ============

// LRUCache LRU缓存结构
// 使用双向链表维护访问顺序（头部=最近使用，尾部=最久未使用）
// 使用哈希表实现O(1)的Key查找
type LRUCache struct {
	capacity int                      // 最大缓存条目数
	list     *list.List               // container/list 双向链表
	cache    map[string]*list.Element // 哈希表：Key → 链表节点指针
	mu       sync.RWMutex             // 读写锁保护并发访问
}

// ============ 构造函数 ============

// NewLRUCache 创建指定容量的LRU缓存
// capacity 必须 > 0，否则返回 ErrInvalidCapacity 错误
func NewLRUCache(capacity int) (*LRUCache, error) {
	if capacity <= 0 {
		return nil, ErrInvalidCapacity
	}
	return &LRUCache{
		capacity: capacity,
		list:     list.New(),
		cache:    make(map[string]*list.Element),
	}, nil
}

// ============ 核心操作方法 ============

// Get 获取指定Key的值
// 若Key存在，将对应节点移到链表头部（标记为最近使用），返回 value, true
// 若Key不存在，返回 nil, false
// 注意：Get会修改链表顺序，因此使用写锁而非读锁
func (c *LRUCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.cache[key]; ok {
		c.list.MoveToFront(elem)
		return elem.Value.(*cacheEntry).value, true
	}
	return nil, false
}

// Set 设置Key-Value键值对
// - 空键返回 ErrEmptyKey 错误
// - 若Key已存在，更新Value并移到链表头部
// - 若Key不存在且缓存已满，先淘汰链表尾部（最久未使用）的条目
// - 新条目插入到链表头部（标记为最近使用）
func (c *LRUCache) Set(key string, value []byte) error {
	if key == "" {
		return ErrEmptyKey
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Key已存在：更新值并移到链表头部
	if elem, ok := c.cache[key]; ok {
		c.list.MoveToFront(elem)
		elem.Value.(*cacheEntry).value = value
		return nil
	}

	// 缓存已满：淘汰链表尾部（最久未使用）的条目
	if c.list.Len() >= c.capacity {
		oldest := c.list.Back()
		if oldest != nil {
			entry := c.list.Remove(oldest).(*cacheEntry)
			delete(c.cache, entry.key)
		}
	}

	// 插入新条目到链表头部
	entry := &cacheEntry{key: key, value: value}
	elem := c.list.PushFront(entry)
	c.cache[key] = elem

	return nil
}

// Delete 删除指定Key
// 若Key存在则从哈希表和链表中移除，返回 true
// 若Key不存在，返回 false
func (c *LRUCache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.cache[key]; ok {
		c.list.Remove(elem)
		delete(c.cache, key)
		return true
	}
	return false
}

// ============ 辅助方法 ============

// Size 获取当前缓存中的条目数
// 使用读锁，支持并发读取
func (c *LRUCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.list.Len()
}

// IsFull 判断缓存是否已满（当前条目数 >= 容量）
// 使用读锁，支持并发读取
func (c *LRUCache) IsFull() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.list.Len() >= c.capacity
}

// Clear 清空缓存中的所有数据
// 重置链表和哈希表
func (c *LRUCache) Clear() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.list.Init()
	c.cache = make(map[string]*list.Element)
	return nil
}

// ExportAll 导出缓存中所有键值对
// 返回 keys 和 values 两个切片，顺序与链表顺序一致（从最近访问到最久访问）
// 对 value 进行深拷贝，避免外部修改影响缓存内部数据
// 使用读锁，支持并发读取
func (c *LRUCache) ExportAll() (keys []string, values [][]byte) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	keys = make([]string, 0, c.list.Len())
	values = make([][]byte, 0, c.list.Len())

	for e := c.list.Front(); e != nil; e = e.Next() {
		entry := e.Value.(*cacheEntry)
		keys = append(keys, entry.key)
		valCopy := make([]byte, len(entry.value))
		copy(valCopy, entry.value)
		values = append(values, valCopy)
	}
	return
}
