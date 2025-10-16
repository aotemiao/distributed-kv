package node

import (
	"distributed-kv/internal/store"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

// NodeConfig
type NodeConfig struct {
	NodeID          string
	RaftAddr        string
	RaftDir         string
	ShouldBootstrap bool // 是否应该执行集群引导
}

// RaftPeer 结构用于集群引导
type RaftPeer struct {
	ID      raft.ServerID
	Address raft.ServerAddress
}

type Node struct {
	Raft *raft.Raft
	FSM  *store.FSM
}

func NewNode(cfg NodeConfig, peers []RaftPeer) (*Node, error) {
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)

	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftAddr)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(cfg.RaftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	fsm := store.NewFSM()

	if err := os.MkdirAll(cfg.RaftDir, 0700); err != nil {
		return nil, err
	}

	// 先创建存储实例，以便我们能检查其中是否存在状态
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.RaftDir, "raft-log.bolt"))
	if err != nil {
		return nil, err
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.RaftDir, "raft-stable.bolt"))
	if err != nil {
		return nil, err
	}

	snapshotStore, err := raft.NewFileSnapshotStore(cfg.RaftDir, 2, os.Stderr)
	if err != nil {
		return nil, err
	}

	// 使用 Raft 官方的函数来检查是否存在既有状态
	hasState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
	if err != nil {
		return nil, err
	}
	isNewCluster := !hasState

	raftNode, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, err
	}

	// 只有在 ShouldBootstrap 为 true 且是新集群时才执行引导操作
	if isNewCluster && cfg.ShouldBootstrap {
		fmt.Printf("检测到是全新节点，正在根据配置进行引导，伙伴节点列表: %+v\n", peers)
		var raftServers []raft.Server
		for _, peer := range peers {
			raftServers = append(raftServers, raft.Server{
				ID:      peer.ID,
				Address: peer.Address,
			})
		}
		bootstrapConfig := raft.Configuration{Servers: raftServers}
		f := raftNode.BootstrapCluster(bootstrapConfig)
		if err := f.Error(); err != nil {
			return nil, fmt.Errorf("引导节点失败: %w", err)
		}
	} else if isNewCluster && !cfg.ShouldBootstrap {
		fmt.Println("新节点将等待被现有集群添加...")
	}

	return &Node{Raft: raftNode, FSM: fsm}, nil
}
