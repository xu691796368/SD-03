// Package server 实现 TCP 缓存服务器
// 支持多客户端并发连接，基于自定义二进制协议进行命令分发
// 集成 cache（LRU缓存）、shard（一致性哈希）、node（缓存节点）、protocol（协议编解码）模块
package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/yourusername/sd-03-cache/pkg/node"
	"github.com/yourusername/sd-03-cache/pkg/protocol"
	"github.com/yourusername/sd-03-cache/pkg/shard"
)

// ============ 错误定义 ============

var (
	// ErrInvalidAddress 监听地址无效
	ErrInvalidAddress = fmt.Errorf("server: address must not be empty")

	// ErrNoNodes 缓存节点列表为空
	ErrNoNodes = fmt.Errorf("server: at least one cache node is required")

	// ErrNilRing 哈希环参数为空
	ErrNilRing = fmt.Errorf("server: hash ring must not be nil")

	// ErrServerStopped 服务器已停止
	ErrServerStopped = fmt.Errorf("server: server is stopped")

	// ErrServerRunning 服务器已在运行
	ErrServerRunning = fmt.Errorf("server: server is already running")
)

// ============ TCP服务器结构 ============

// TCPServer TCP缓存服务器
// 监听指定端口，接受客户端连接，解析协议帧并分发到对应命令处理器
type TCPServer struct {
	address  string                     // 监听地址（默认":7000"）
	listener net.Listener               // TCP监听器
	nodes    map[string]*node.CacheNode // 缓存节点映射（nodeID → CacheNode）
	ring     *shard.HashRing            // 一致性哈希环，用于请求路由
	mu       sync.RWMutex               // 读写锁保护服务器状态
	stopChan chan struct{}              // 停止信号通道
	wg       sync.WaitGroup             // 等待所有连接goroutine结束
	running  bool                       // 服务器运行状态
}

// ============ 构造函数 ============

// NewTCPServer 创建TCP服务器
// address: 监听地址（如":7000"）
// nodes: 缓存节点列表，至少包含一个节点
// ring: 一致性哈希环实例，用于请求路由
func NewTCPServer(address string, nodes []*node.CacheNode, ring *shard.HashRing) (*TCPServer, error) {
	if address == "" {
		return nil, ErrInvalidAddress
	}
	if len(nodes) == 0 {
		return nil, ErrNoNodes
	}
	if ring == nil {
		return nil, ErrNilRing
	}

	// 构建节点映射（nodeID → *CacheNode）
	nodeMap := make(map[string]*node.CacheNode, len(nodes))
	for _, n := range nodes {
		if n != nil {
			nodeMap[n.GetNodeID()] = n
		}
	}

	if len(nodeMap) == 0 {
		return nil, ErrNoNodes
	}

	return &TCPServer{
		address:  address,
		nodes:    nodeMap,
		ring:     ring,
		stopChan: make(chan struct{}),
	}, nil
}

// ============ 生命周期管理 ============

// Start 启动TCP服务器
// 开始监听端口并接受客户端连接
func (s *TCPServer) Start() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return ErrServerRunning
	}
	s.mu.Unlock()

	// 开始监听
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("server: failed to listen on %s: %w", s.address, err)
	}

	s.mu.Lock()
	s.listener = listener
	s.running = true
	s.mu.Unlock()

	log.Printf("[Server] Listening on %s", s.address)

	// 在独立goroutine中接受连接
	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Stop 停止TCP服务器
// 关闭监听器，等待所有连接处理完成
func (s *TCPServer) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil // 已停止，幂等
	}

	// 发送停止信号
	close(s.stopChan)
	s.running = false

	// 关闭监听器
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			return fmt.Errorf("server: failed to close listener: %w", err)
		}
	}

	// 等待所有连接goroutine结束（在锁外等待避免死锁）
	s.mu.Unlock()
	s.wg.Wait()
	s.mu.Lock()

	log.Printf("[Server] Stopped")
	return nil
}

// IsRunning 返回服务器是否在运行
func (s *TCPServer) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}

// Address 返回服务器实际监听地址
func (s *TCPServer) Address() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.address
}

// ============ 连接管理 ============

// acceptLoop 接受客户端连接的主循环
// 每个新连接在独立的goroutine中处理
func (s *TCPServer) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// 检查是否为正常关闭
			select {
			case <-s.stopChan:
				return // 服务器正在关闭
			default:
				log.Printf("[Server] Accept error: %v", err)
				return
			}
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection 处理单个客户端连接
// 持续读取请求帧并处理，直到连接关闭或发生错误
func (s *TCPServer) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer func() {
		conn.Close()
		log.Printf("[Server] Connection closed: %s", conn.RemoteAddr())
	}()

	log.Printf("[Server] New connection from %s", conn.RemoteAddr())

	for {
		// 检查服务器是否正在关闭
		select {
		case <-s.stopChan:
			return
		default:
		}

		// 读取协议帧头（9字节）
		header := make([]byte, protocol.FrameHeaderSize)
		if _, err := io.ReadFull(conn, header); err != nil {
			if err != io.EOF {
				log.Printf("[Server] Read header error from %s: %v", conn.RemoteAddr(), err)
			}
			return // 连接关闭或读取错误
		}

		// 解析帧头
		cmd := header[0]
		keyLen := uint32(header[1])<<24 | uint32(header[2])<<16 | uint32(header[3])<<8 | uint32(header[4])
		valueLen := uint32(header[5])<<24 | uint32(header[6])<<16 | uint32(header[7])<<8 | uint32(header[8])

		// 读取Key和Value数据
		dataSize := int(keyLen) + int(valueLen)
		if dataSize > 0 {
			data := make([]byte, dataSize)
			if _, err := io.ReadFull(conn, data); err != nil {
				log.Printf("[Server] Read data error from %s: %v", conn.RemoteAddr(), err)
				return
			}

			// 构建完整帧用于验证
			frame := &protocol.ProtocolFrame{
				Command:  cmd,
				KeyLen:   keyLen,
				ValueLen: valueLen,
				Key:      data[:keyLen],
				Value:    data[keyLen:],
			}

			// 处理请求
			if err := s.handleRequest(conn, frame); err != nil {
				log.Printf("[Server] Handle request error from %s: %v", conn.RemoteAddr(), err)
				return
			}
		} else {
			// 无数据（如INFO命令）
			frame := &protocol.ProtocolFrame{
				Command:  cmd,
				KeyLen:   0,
				ValueLen: 0,
				Key:      []byte{},
				Value:    []byte{},
			}

			if err := s.handleRequest(conn, frame); err != nil {
				log.Printf("[Server] Handle request error from %s: %v", conn.RemoteAddr(), err)
				return
			}
		}
	}
}

// ============ 请求处理 ============

// handleRequest 处理单个客户端请求
// 验证协议帧，分发到对应命令处理器，返回响应
func (s *TCPServer) handleRequest(conn net.Conn, frame *protocol.ProtocolFrame) error {
	// 验证协议帧
	if err := protocol.ValidateFrame(frame); err != nil {
		// 验证失败，返回错误响应
		errorCode := protocol.GetErrorCode(err)
		resp, encodeErr := protocol.EncodeResponse(frame.Command, uint8(errorCode), []byte(err.Error()))
		if encodeErr != nil {
			return fmt.Errorf("failed to encode error response: %w", encodeErr)
		}
		if _, writeErr := conn.Write(resp); writeErr != nil {
			return fmt.Errorf("failed to write error response: %w", writeErr)
		}
		return nil // 不关闭连接，继续处理下一个请求
	}

	// 分发命令
	resp, err := s.dispatchCommand(frame.Command, frame.Key, frame.Value)
	if err != nil {
		// 命令处理失败，返回错误响应
		resp, _ = protocol.EncodeResponse(frame.Command, uint8(protocol.ERROR_UNKNOWN_COMMAND), []byte(err.Error()))
	}

	if resp == nil {
		return nil
	}

	// 发送响应
	if _, writeErr := conn.Write(resp); writeErr != nil {
		return fmt.Errorf("failed to write response: %w", writeErr)
	}

	return nil
}

// dispatchCommand 根据命令码分发请求到对应处理器
func (s *TCPServer) dispatchCommand(cmd uint8, key, value []byte) ([]byte, error) {
	switch protocol.Command(cmd) {
	case protocol.CMD_GET:
		return s.handleGet(string(key))
	case protocol.CMD_SET:
		return s.handleSet(string(key), value)
	case protocol.CMD_DELETE:
		return s.handleDelete(string(key))
	case protocol.CMD_INFO:
		return s.handleInfo()
	default:
		return protocol.EncodeResponse(cmd, uint8(protocol.ERROR_UNKNOWN_COMMAND),
			[]byte(fmt.Sprintf("unknown command: 0x%02X", cmd)))
	}
}

// ============ 命令处理器 ============

// handleGet 处理GET命令
// 通过哈希环路由到对应节点，查询缓存值
func (s *TCPServer) handleGet(key string) ([]byte, error) {
	// 通过哈希环定位节点
	nodeID := s.ring.GetNode(key)
	if nodeID == "" {
		return protocol.EncodeResponse(uint8(protocol.CMD_GET), uint8(protocol.ERROR_INVALID_KEY),
			[]byte("no available node for key"))
	}

	// 查找节点
	n, ok := s.nodes[nodeID]
	if !ok {
		return protocol.EncodeResponse(uint8(protocol.CMD_GET), uint8(protocol.ERROR_INVALID_KEY),
			[]byte("node not found: "+nodeID))
	}

	// 执行GET操作
	val, err := n.Get(key)
	if err != nil {
		return protocol.EncodeResponse(uint8(protocol.CMD_GET), uint8(protocol.ERROR_UNKNOWN_COMMAND),
			[]byte(err.Error()))
	}

	if val == nil {
		// Key不存在
		return protocol.EncodeResponse(uint8(protocol.CMD_GET), uint8(protocol.SUCCESS), []byte{})
	}

	return protocol.EncodeResponse(uint8(protocol.CMD_GET), uint8(protocol.SUCCESS), val)
}

// handleSet 处理SET命令
// 通过哈希环路由到对应节点，设置缓存值
func (s *TCPServer) handleSet(key string, value []byte) ([]byte, error) {
	// 通过哈希环定位节点
	nodeID := s.ring.GetNode(key)
	if nodeID == "" {
		return protocol.EncodeResponse(uint8(protocol.CMD_SET), uint8(protocol.ERROR_INVALID_KEY),
			[]byte("no available node for key"))
	}

	// 查找节点
	n, ok := s.nodes[nodeID]
	if !ok {
		return protocol.EncodeResponse(uint8(protocol.CMD_SET), uint8(protocol.ERROR_INVALID_KEY),
			[]byte("node not found: "+nodeID))
	}

	// 执行SET操作
	if err := n.Set(key, value); err != nil {
		return protocol.EncodeResponse(uint8(protocol.CMD_SET), uint8(protocol.ERROR_INVALID_VALUE),
			[]byte(err.Error()))
	}

	return protocol.EncodeResponse(uint8(protocol.CMD_SET), uint8(protocol.SUCCESS), []byte{})
}

// handleDelete 处理DELETE命令
// 通过哈希环路由到对应节点，删除缓存值
func (s *TCPServer) handleDelete(key string) ([]byte, error) {
	// 通过哈希环定位节点
	nodeID := s.ring.GetNode(key)
	if nodeID == "" {
		return protocol.EncodeResponse(uint8(protocol.CMD_DELETE), uint8(protocol.ERROR_INVALID_KEY),
			[]byte("no available node for key"))
	}

	// 查找节点
	n, ok := s.nodes[nodeID]
	if !ok {
		return protocol.EncodeResponse(uint8(protocol.CMD_DELETE), uint8(protocol.ERROR_INVALID_KEY),
			[]byte("node not found: "+nodeID))
	}

	// 执行DELETE操作
	if err := n.Delete(key); err != nil {
		return protocol.EncodeResponse(uint8(protocol.CMD_DELETE), uint8(protocol.ERROR_UNKNOWN_COMMAND),
			[]byte(err.Error()))
	}

	return protocol.EncodeResponse(uint8(protocol.CMD_DELETE), uint8(protocol.SUCCESS), []byte{})
}

// handleInfo 处理INFO命令
// 收集所有节点的信息并返回
func (s *TCPServer) handleInfo() ([]byte, error) {
	infos := make(map[string]interface{})
	for id, n := range s.nodes {
		infos[id] = n.GetInfo()
	}

	data, err := json.Marshal(infos)
	if err != nil {
		return protocol.EncodeResponse(uint8(protocol.CMD_INFO), uint8(protocol.ERROR_UNKNOWN_COMMAND),
			[]byte("failed to marshal info: "+err.Error()))
	}

	return protocol.EncodeResponse(uint8(protocol.CMD_INFO), uint8(protocol.SUCCESS), data)
}

// ============ 辅助方法 ============

// GetNodeCount 返回服务器管理的缓存节点数量
func (s *TCPServer) GetNodeCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.nodes)
}
