package store

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/hashicorp/raft"
)

// Command 代表应用到状态机上的指令
type Command struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

// FSM 实现了 raft.FSM 接口，是我们的 KV 存储状态机
type FSM struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewFSM 创建一个新的 FSM
func NewFSM() *FSM {
	return &FSM{
		data: make(map[string]string),
	}
}

// Apply 将 Raft 提交的日志应用到状态机上
func (f *FSM) Apply(logEntry *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(logEntry.Data, &cmd); err != nil {
		log.Printf("Failed to unmarshal command: %s", err)
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Op {
	case "SET":
		f.data[cmd.Key] = cmd.Value
		log.Printf("Applied SET command for key '%s'", cmd.Key)
	case "DELETE":
		delete(f.data, cmd.Key)
		log.Printf("Applied DELETE command for key '%s'", cmd.Key)
	default:
		log.Printf("Unrecognized command op: %s", cmd.Op)
		return fmt.Errorf("unrecognized command op: %s", cmd.Op)
	}

	return nil
}

// Get 从状态机中查询一个 key
func (f *FSM) Get(key string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	value, ok := f.data[key]
	return value, ok
}

// Snapshot 用于创建状态机的快照，以便进行日志压缩
// 在这个简单实现中，我们直接将整个 map 序列化
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// 复制 map 以避免并发问题
	snapshotData := make(map[string]string)
	for k, v := range f.data {
		snapshotData[k] = v
	}

	return &fsmSnapshot{data: snapshotData}, nil
}

// Restore 用于从快照中恢复状态机
func (f *FSM) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()

	var snapshotData map[string]string
	if err := json.NewDecoder(snapshot).Decode(&snapshotData); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = snapshotData
	return nil
}

// fsmSnapshot 实现了 raft.FSMSnapshot 接口
type fsmSnapshot struct {
	data map[string]string
}

// Persist 将快照数据写入 sink
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// 将 map 编码为 JSON
		bytes, err := json.Marshal(s.data)
		if err != nil {
			return err
		}

		// 写入 sink
		if _, err := sink.Write(bytes); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

// Release 是一个空操作
func (s *fsmSnapshot) Release() {}
