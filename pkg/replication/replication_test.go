package replication

import (
	"fmt"
	"testing"
	"time"

	"github.com/yourusername/sd-03-cache/pkg/node"
	"github.com/yourusername/sd-03-cache/pkg/protocol"
)

// ============ 测试辅助函数 ============

// newStartedNode 创建并启动一个缓存节点
func newStartedNode(id string, capacity int) *node.CacheNode {
	n, err := node.NewCacheNode(id, capacity)
	if err != nil {
		panic(err)
	}
	if err := n.Start(); err != nil {
		panic(err)
	}
	return n
}

// newTestController 创建测试用复制控制器
// 默认创建 master("master-1", 100) 和 slave("slave-1", 100)
func newTestController() (*ReplicationController, *node.CacheNode, *node.CacheNode) {
	master := newStartedNode("master-1", 100)
	slave := newStartedNode("slave-1", 100)

	rc, err := NewReplicationController([]*node.CacheNode{master, slave})
	if err != nil {
		panic(err)
	}

	if err := rc.SetMasterSlave("master-1", "slave-1"); err != nil {
		panic(err)
	}

	return rc, master, slave
}

// ============ NewReplicationController 构造函数测试 ============

func TestNewReplicationController_Valid(t *testing.T) {
	n1 := newStartedNode("node-1", 10)
	n2 := newStartedNode("node-2", 10)

	rc, err := NewReplicationController([]*node.CacheNode{n1, n2})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if rc == nil {
		t.Fatal("expected non-nil controller")
	}
	if rc.GetNodeCount() != 2 {
		t.Errorf("expected node count 2, got %d", rc.GetNodeCount())
	}
}

func TestNewReplicationController_EmptyNodes(t *testing.T) {
	rc, err := NewReplicationController([]*node.CacheNode{})
	if err != ErrNoNodes {
		t.Errorf("expected ErrNoNodes, got %v", err)
	}
	if rc != nil {
		t.Error("expected nil controller")
	}
}

func TestNewReplicationController_NilNodes(t *testing.T) {
	rc, err := NewReplicationController([]*node.CacheNode{nil, nil})
	if err != ErrNoValidNodes {
		t.Errorf("expected ErrNoValidNodes, got %v", err)
	}
	if rc != nil {
		t.Error("expected nil controller")
	}
}

func TestNewReplicationController_SingleNilNode(t *testing.T) {
	n1 := newStartedNode("node-1", 10)
	rc, err := NewReplicationController([]*node.CacheNode{n1, nil})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if rc.GetNodeCount() != 1 {
		t.Errorf("expected node count 1, got %d", rc.GetNodeCount())
	}
}

// ============ SetMasterSlave 主从关系配置测试 ============

func TestSetMasterSlave_Valid(t *testing.T) {
	master := newStartedNode("m-1", 10)
	slave := newStartedNode("s-1", 10)

	rc, _ := NewReplicationController([]*node.CacheNode{master, slave})
	err := rc.SetMasterSlave("m-1", "s-1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// 验证主节点状态
	if master.GetStatus() != node.StatusMaster {
		t.Errorf("expected master status %s, got %s", node.StatusMaster, master.GetStatus())
	}

	// 验证从节点状态
	if slave.GetStatus() != node.StatusSlave {
		t.Errorf("expected slave status %s, got %s", node.StatusSlave, slave.GetStatus())
	}

	// 验证从节点关联主节点
	if slave.GetMasterID() != "m-1" {
		t.Errorf("expected slave masterID 'm-1', got '%s'", slave.GetMasterID())
	}

	// 验证控制器状态
	if rc.GetMasterID() != "m-1" {
		t.Errorf("expected masterID 'm-1', got '%s'", rc.GetMasterID())
	}
	slaveIDs := rc.GetSlaveIDs()
	if len(slaveIDs) != 1 || slaveIDs[0] != "s-1" {
		t.Errorf("expected slaveIDs ['s-1'], got %v", slaveIDs)
	}
}

func TestSetMasterSlave_MasterNotFound(t *testing.T) {
	n1 := newStartedNode("n-1", 10)
	rc, _ := NewReplicationController([]*node.CacheNode{n1})

	err := rc.SetMasterSlave("nonexistent", "n-1")
	if err == nil {
		t.Fatal("expected error for nonexistent master")
	}
}

func TestSetMasterSlave_SlaveNotFound(t *testing.T) {
	n1 := newStartedNode("n-1", 10)
	rc, _ := NewReplicationController([]*node.CacheNode{n1})

	err := rc.SetMasterSlave("n-1", "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent slave")
	}
}

func TestSetMasterSlave_SameNode(t *testing.T) {
	n1 := newStartedNode("n-1", 10)
	rc, _ := NewReplicationController([]*node.CacheNode{n1})

	err := rc.SetMasterSlave("n-1", "n-1")
	if err != ErrSameNode {
		t.Errorf("expected ErrSameNode, got %v", err)
	}
}

func TestSetMasterSlave_EmptyIDs(t *testing.T) {
	n1 := newStartedNode("n-1", 10)
	rc, _ := NewReplicationController([]*node.CacheNode{n1})

	err := rc.SetMasterSlave("", "n-1")
	if err != ErrEmptyMasterID {
		t.Errorf("expected ErrEmptyMasterID, got %v", err)
	}

	err = rc.SetMasterSlave("n-1", "")
	if err != ErrEmptyMasterID {
		t.Errorf("expected ErrEmptyMasterID, got %v", err)
	}
}

// ============ SyncToSlave 写同步测试 ============

func TestSyncToSlave_Basic(t *testing.T) {
	rc, _, slave := newTestController()

	err := rc.SyncToSlave("key1", []byte("value1"))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// 验证从节点数据
	val, err := slave.Get("key1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("expected 'value1', got '%s'", string(val))
	}

	// 验证同步计数
	if rc.GetSyncedCount() != 1 {
		t.Errorf("expected synced count 1, got %d", rc.GetSyncedCount())
	}
}

func TestSyncToSlave_MultipleKeys(t *testing.T) {
	rc, _, slave := newTestController()

	// 同步多个键值对
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		value := []byte(fmt.Sprintf("value%d", i))
		if err := rc.SyncToSlave(key, value); err != nil {
			t.Fatalf("sync %s failed: %v", key, err)
		}
	}

	// 验证从节点数据
	if slave.Size() != 5 {
		t.Errorf("expected slave size 5, got %d", slave.Size())
	}

	// 验证同步计数
	if rc.GetSyncedCount() != 5 {
		t.Errorf("expected synced count 5, got %d", rc.GetSyncedCount())
	}

	// 验证数据一致性
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		expected := fmt.Sprintf("value%d", i)
		val, _ := slave.Get(key)
		if string(val) != expected {
			t.Errorf("key %s: expected '%s', got '%s'", key, expected, string(val))
		}
	}
}

func TestSyncToSlave_Overwrite(t *testing.T) {
	rc, _, slave := newTestController()

	// 先同步
	rc.SyncToSlave("key1", []byte("old_value"))
	// 覆盖同步
	rc.SyncToSlave("key1", []byte("new_value"))

	val, _ := slave.Get("key1")
	if string(val) != "new_value" {
		t.Errorf("expected 'new_value', got '%s'", string(val))
	}
}

func TestSyncToSlave_NotConfigured(t *testing.T) {
	master := newStartedNode("m-1", 10)
	slave := newStartedNode("s-1", 10)
	rc, _ := NewReplicationController([]*node.CacheNode{master, slave})

	// 未配置主从关系就调用 SyncToSlave
	err := rc.SyncToSlave("key1", []byte("value1"))
	if err != ErrNotConfigured {
		t.Errorf("expected ErrNotConfigured, got %v", err)
	}
}

func TestSyncToSlave_EmptyKey(t *testing.T) {
	rc, _, _ := newTestController()

	err := rc.SyncToSlave("", []byte("value"))
	if err != ErrEmptyKey {
		t.Errorf("expected ErrEmptyKey, got %v", err)
	}
}

func TestSyncToSlave_SyncLatency(t *testing.T) {
	rc, _, slave := newTestController()

	// 验证同步延迟 < 10ms（spec 要求）
	start := time.Now()
	err := rc.SyncToSlave("key1", []byte("value1"))
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if elapsed > 10*time.Millisecond {
		t.Errorf("sync latency %v exceeds 10ms requirement", elapsed)
	}

	// 验证最后同步时间已更新
	lastSync := rc.GetLastSyncTime()
	if lastSync.IsZero() {
		t.Error("expected lastSyncTime to be set")
	}
	if lastSync.Sub(start) > 10*time.Millisecond {
		t.Errorf("lastSyncTime not within expected range")
	}

	_ = slave // slave used implicitly via rc
}

// ============ SyncDeleteToSlave 删除同步测试 ============

func TestSyncDeleteToSlave_Basic(t *testing.T) {
	rc, _, slave := newTestController()

	// 先写入数据
	rc.SyncToSlave("key1", []byte("value1"))
	rc.SyncToSlave("key2", []byte("value2"))

	// 同步删除
	err := rc.SyncDeleteToSlave("key1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// 验证 key1 已删除
	val, _ := slave.Get("key1")
	if val != nil {
		t.Errorf("expected nil for deleted key, got '%s'", string(val))
	}

	// 验证 key2 仍在
	val, _ = slave.Get("key2")
	if string(val) != "value2" {
		t.Errorf("expected 'value2', got '%s'", string(val))
	}
}

func TestSyncDeleteToSlave_NotConfigured(t *testing.T) {
	master := newStartedNode("m-1", 10)
	slave := newStartedNode("s-1", 10)
	rc, _ := NewReplicationController([]*node.CacheNode{master, slave})

	err := rc.SyncDeleteToSlave("key1")
	if err != ErrNotConfigured {
		t.Errorf("expected ErrNotConfigured, got %v", err)
	}
}

// ============ InitSync 初始化同步测试 ============

func TestInitSync_Valid(t *testing.T) {
	master := newStartedNode("m-1", 10)
	slave := newStartedNode("s-1", 10)
	rc, _ := NewReplicationController([]*node.CacheNode{master, slave})

	err := rc.InitSync("m-1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if rc.GetMasterID() != "m-1" {
		t.Errorf("expected masterID 'm-1', got '%s'", rc.GetMasterID())
	}
}

func TestInitSync_EmptyMasterID(t *testing.T) {
	n1 := newStartedNode("n-1", 10)
	rc, _ := NewReplicationController([]*node.CacheNode{n1})

	err := rc.InitSync("")
	if err != ErrEmptyMasterID {
		t.Errorf("expected ErrEmptyMasterID, got %v", err)
	}
}

func TestInitSync_MasterNotFound(t *testing.T) {
	n1 := newStartedNode("n-1", 10)
	rc, _ := NewReplicationController([]*node.CacheNode{n1})

	err := rc.InitSync("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent master")
	}
}

// ============ RequestFullSync 全量数据请求测试 ============

func TestRequestFullSync_WithData(t *testing.T) {
	rc, master, _ := newTestController()

	// 向主节点写入数据
	master.Set("key1", []byte("value1"))
	master.Set("key2", []byte("value2"))
	master.Set("key3", []byte("value3"))

	frames, err := rc.RequestFullSync("master-1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(frames) != 3 {
		t.Fatalf("expected 3 frames, got %d", len(frames))
	}

	// 验证帧数据
	keySet := make(map[string]string)
	for _, f := range frames {
		if f.Command != uint8(protocol.CMD_SET) {
			t.Errorf("expected CMD_SET (0x02), got 0x%02X", f.Command)
		}
		keySet[string(f.Key)] = string(f.Value)
	}

	expected := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}
	for k, v := range expected {
		if keySet[k] != v {
			t.Errorf("key %s: expected '%s', got '%s'", k, v, keySet[k])
		}
	}
}

func TestRequestFullSync_EmptyMaster(t *testing.T) {
	rc, _, _ := newTestController()

	// 主节点无数据
	frames, err := rc.RequestFullSync("master-1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(frames) != 0 {
		t.Errorf("expected 0 frames for empty master, got %d", len(frames))
	}
}

func TestRequestFullSync_EmptyMasterID(t *testing.T) {
	rc, _, _ := newTestController()

	frames, err := rc.RequestFullSync("")
	if err != ErrEmptyMasterID {
		t.Errorf("expected ErrEmptyMasterID, got %v", err)
	}
	if frames != nil {
		t.Error("expected nil frames")
	}
}

func TestRequestFullSync_MasterNotFound(t *testing.T) {
	rc, _, _ := newTestController()

	frames, err := rc.RequestFullSync("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent master")
	}
	if frames != nil {
		t.Error("expected nil frames for error")
	}
}

func TestRequestFullSync_LargeDataSet(t *testing.T) {
	// 创建容量为 1000 的节点以支持大量数据
	master := newStartedNode("master-1", 1000)
	slave := newStartedNode("slave-1", 1000)
	rc, _ := NewReplicationController([]*node.CacheNode{master, slave})
	rc.SetMasterSlave("master-1", "slave-1")

	// 写入大量数据
	count := 1000
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%04d", i)
		value := []byte(fmt.Sprintf("value-%04d", i))
		master.Set(key, value)
	}

	frames, err := rc.RequestFullSync("master-1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(frames) != count {
		t.Errorf("expected %d frames, got %d", count, len(frames))
	}
}

// ============ ApplyFullSync 全量数据应用测试 ============

func TestApplyFullSync_Basic(t *testing.T) {
	rc, _, slave := newTestController()

	// 构造全量数据帧
	frames := []*protocol.ProtocolFrame{
		protocol.NewFrame(uint8(protocol.CMD_SET), []byte("k1"), []byte("v1")),
		protocol.NewFrame(uint8(protocol.CMD_SET), []byte("k2"), []byte("v2")),
		protocol.NewFrame(uint8(protocol.CMD_SET), []byte("k3"), []byte("v3")),
	}

	err := rc.ApplyFullSync(frames)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// 验证从节点数据
	if slave.Size() != 3 {
		t.Errorf("expected slave size 3, got %d", slave.Size())
	}

	expected := map[string]string{"k1": "v1", "k2": "v2", "k3": "v3"}
	for k, v := range expected {
		val, _ := slave.Get(k)
		if string(val) != v {
			t.Errorf("key %s: expected '%s', got '%s'", k, v, string(val))
		}
	}
}

func TestApplyFullSync_NilFrames(t *testing.T) {
	rc, _, _ := newTestController()

	err := rc.ApplyFullSync(nil)
	if err != ErrNilFrames {
		t.Errorf("expected ErrNilFrames, got %v", err)
	}
}

func TestApplyFullSync_EmptyFrames(t *testing.T) {
	rc, _, _ := newTestController()

	err := rc.ApplyFullSync([]*protocol.ProtocolFrame{})
	if err != nil {
		t.Fatalf("expected no error for empty frames, got %v", err)
	}
}

func TestApplyFullSync_NotConfigured(t *testing.T) {
	n1 := newStartedNode("n-1", 10)
	n2 := newStartedNode("n-2", 10)
	rc, _ := NewReplicationController([]*node.CacheNode{n1, n2})

	// 未配置主从关系
	frames := []*protocol.ProtocolFrame{
		protocol.NewFrame(uint8(protocol.CMD_SET), []byte("k1"), []byte("v1")),
	}

	err := rc.ApplyFullSync(frames)
	if err != ErrNotConfigured {
		t.Errorf("expected ErrNotConfigured, got %v", err)
	}
}

func TestApplyFullSync_IgnoresNonSetCommands(t *testing.T) {
	rc, _, slave := newTestController()

	// 混合命令帧，只有 SET 应该被应用
	frames := []*protocol.ProtocolFrame{
		protocol.NewFrame(uint8(protocol.CMD_SET), []byte("k1"), []byte("v1")),
		protocol.NewFrame(uint8(protocol.CMD_GET), []byte("k1"), []byte{}),
		protocol.NewFrame(uint8(protocol.CMD_DELETE), []byte("k1"), []byte{}),
		protocol.NewFrame(uint8(protocol.CMD_SET), []byte("k2"), []byte("v2")),
	}

	err := rc.ApplyFullSync(frames)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// 只有 k1 和 k2 被写入（2个 SET 命令）
	if slave.Size() != 2 {
		t.Errorf("expected slave size 2, got %d", slave.Size())
	}
}

// ============ 全量重连同步集成测试 ============

func TestFullReconnectSync_Complete(t *testing.T) {
	// 场景：从节点断开重连后恢复同步
	// 1. 配置主从
	// 2. 向主节点写入数据
	// 3. 从节点断开（模拟）
	// 4. 从节点重连：InitSync → RequestFullSync → ApplyFullSync
	// 5. 验证数据一致

	master := newStartedNode("master-1", 1000)
	slave := newStartedNode("slave-1", 1000)

	rc, _ := NewReplicationController([]*node.CacheNode{master, slave})
	rc.SetMasterSlave("master-1", "slave-1")

	// 向主节点写入 100 条数据
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%03d", i)
		value := []byte(fmt.Sprintf("value-%03d", i))
		master.Set(key, value)
	}

	// 模拟从节点断开重连：清空从节点数据
	slave.Stop()
	slave.Start()

	// 步骤1: InitSync
	err := rc.InitSync("master-1")
	if err != nil {
		t.Fatalf("InitSync failed: %v", err)
	}

	// 步骤2: RequestFullSync
	frames, err := rc.RequestFullSync("master-1")
	if err != nil {
		t.Fatalf("RequestFullSync failed: %v", err)
	}
	if len(frames) != 100 {
		t.Errorf("expected 100 frames, got %d", len(frames))
	}

	// 步骤3: ApplyFullSync
	err = rc.ApplyFullSync(frames)
	if err != nil {
		t.Fatalf("ApplyFullSync failed: %v", err)
	}

	// 验证从节点数据完整性
	if slave.Size() != 100 {
		t.Errorf("expected slave size 100, got %d", slave.Size())
	}

	// 抽样验证数据一致性
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%03d", i*10)
		expected := fmt.Sprintf("value-%03d", i*10)

		masterVal, _ := master.Get(key)
		slaveVal, _ := slave.Get(key)

		if string(masterVal) != expected {
			t.Errorf("master key %s: expected '%s', got '%s'", key, expected, string(masterVal))
		}
		if string(slaveVal) != expected {
			t.Errorf("slave key %s: expected '%s', got '%s'", key, expected, string(slaveVal))
		}
	}
}

func TestFullReconnectSync_WithExistingSlaveData(t *testing.T) {
	// 场景：从节点有旧数据，全量同步后应覆盖
	master := newStartedNode("m-1", 100)
	slave := newStartedNode("s-1", 100)

	rc, _ := NewReplicationController([]*node.CacheNode{master, slave})
	rc.SetMasterSlave("m-1", "s-1")

	// 主节点写入新数据
	master.Set("key1", []byte("new_value1"))
	master.Set("key2", []byte("new_value2"))

	// 从节点有旧数据（模拟残留）
	slave.Set("key1", []byte("old_value1"))
	slave.Set("key3", []byte("old_value3"))

	// 全量同步
	frames, _ := rc.RequestFullSync("m-1")
	rc.ApplyFullSync(frames)

	// 验证从节点数据
	val, _ := slave.Get("key1")
	if string(val) != "new_value1" {
		t.Errorf("expected 'new_value1', got '%s'", string(val))
	}

	val, _ = slave.Get("key2")
	if string(val) != "new_value2" {
		t.Errorf("expected 'new_value2', got '%s'", string(val))
	}

	// key3 仍在（全量同步不删除从节点独有数据）
	val, _ = slave.Get("key3")
	if string(val) != "old_value3" {
		t.Errorf("expected 'old_value3' to remain, got '%s'", string(val))
	}
}

// ============ 写同步 + 全量同步组合测试 ============

func TestWriteSyncThenFullSync(t *testing.T) {
	// 场景：主节点写入数据并通过 SyncToSlave 同步到从节点，
	// 然后从节点断开重连，通过全量同步恢复
	master := newStartedNode("m-1", 100)
	slave := newStartedNode("s-1", 100)

	rc, _ := NewReplicationController([]*node.CacheNode{master, slave})
	rc.SetMasterSlave("m-1", "s-1")

	// 主节点写入 5 条数据
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		value := []byte(fmt.Sprintf("value%d", i))
		master.Set(key, value)
		// 同步写入到从节点
		rc.SyncToSlave(key, value)
	}

	// 主节点额外写入数据（未通过 SyncToSlave）
	master.Set("extra-key", []byte("extra-val"))

	// 验证当前状态：主节点 6 条，从节点 5 条
	if master.Size() != 6 {
		t.Errorf("expected master size 6, got %d", master.Size())
	}
	if slave.Size() != 5 {
		t.Errorf("expected slave size 5, got %d", slave.Size())
	}

	// 模拟从节点重启（丢失内存数据）
	slave.Stop()
	slave.Start()

	// 全量同步恢复数据
	rc.InitSync("m-1")
	frames, err := rc.RequestFullSync("m-1")
	if err != nil {
		t.Fatalf("RequestFullSync failed: %v", err)
	}

	// 主节点应有 6 条数据
	if len(frames) != 6 {
		t.Errorf("expected 6 frames, got %d", len(frames))
	}

	err = rc.ApplyFullSync(frames)
	if err != nil {
		t.Fatalf("ApplyFullSync failed: %v", err)
	}

	// 验证从节点已恢复全部 6 条数据
	if slave.Size() != 6 {
		t.Errorf("expected slave size 6, got %d", slave.Size())
	}
}

// ============ GetState 状态查询测试 ============

func TestGetState_Initial(t *testing.T) {
	n1 := newStartedNode("n-1", 10)
	rc, _ := NewReplicationController([]*node.CacheNode{n1})

	state := rc.GetState()
	if state["masterID"] != "" {
		t.Errorf("expected empty masterID, got '%s'", state["masterID"])
	}
	if state["syncedCount"] != 0 {
		t.Errorf("expected syncedCount 0, got %d", state["syncedCount"])
	}
}

func TestGetState_AfterSetup(t *testing.T) {
	rc, _, _ := newTestController()
	rc.SyncToSlave("k1", []byte("v1"))

	state := rc.GetState()
	if state["masterID"] != "master-1" {
		t.Errorf("expected 'master-1', got '%s'", state["masterID"])
	}
	slaveIDs := state["slaveIDs"].([]string)
	if len(slaveIDs) != 1 || slaveIDs[0] != "slave-1" {
		t.Errorf("expected ['slave-1'], got %v", slaveIDs)
	}
	if state["syncedCount"] != 1 {
		t.Errorf("expected syncedCount 1, got %d", state["syncedCount"])
	}
}

// ============ 并发安全性测试 ============

func TestSyncToSlave_Concurrent(t *testing.T) {
	rc, master, slave := newTestController()

	// 并发执行 10 次 SyncToSlave
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(idx int) {
			key := fmt.Sprintf("concurrent-key%d", idx)
			value := []byte(fmt.Sprintf("concurrent-val%d", idx))
			err := rc.SyncToSlave(key, value)
			if err != nil {
				t.Errorf("concurrent sync %d failed: %v", idx, err)
			}
			done <- true
		}(i)
	}

	// 等待所有 goroutine 完成
	for i := 0; i < 10; i++ {
		<-done
	}

	// 验证同步计数
	if rc.GetSyncedCount() != 10 {
		t.Errorf("expected synced count 10, got %d", rc.GetSyncedCount())
	}

	// 验证从节点数据
	if slave.Size() != 10 {
		t.Errorf("expected slave size 10, got %d", slave.Size())
	}

	_ = master // master used implicitly via rc
}
