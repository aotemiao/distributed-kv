package main

import (
	"distributed-kv/internal/config" // 导入 config 包
	"distributed-kv/internal/node"
	"distributed-kv/internal/transport"
	"flag"
	"log"
	"path/filepath"

	"github.com/hashicorp/raft"
)

func main() {
	// 命令行参数大大简化
	configFile := flag.String("config", "config.json", "Path to the configuration file")
	nodeID := flag.String("id", "", "Node ID (must match one in the config file)")
	dataDir := flag.String("dir", "data", "Raft data directory")
	flag.Parse()

	if *nodeID == "" {
		log.Fatal("Node ID is required")
	}

	// 1. 加载配置文件
	cfg, err := config.Load(*configFile)
	if err != nil {
		log.Fatalf("Failed to load config: %s", err)
	}

	// 2. 从配置中找到当前节点和所有伙伴的信息
	var currentNodeConfig *config.PeerConfig
	var peers []node.RaftPeer
	for _, peer := range cfg.Peers {
		if peer.NodeID == *nodeID {
			// 找到当前节点的配置
			p := peer
			currentNodeConfig = &p
		}
		peers = append(peers, node.RaftPeer{
			ID:      raft.ServerID(peer.NodeID),
			Address: raft.ServerAddress(peer.RaftAddr),
		})
	}

	if currentNodeConfig == nil {
		log.Fatalf("Node ID %s not found in config file", *nodeID)
	}

	// 3. 创建 Raft 节点
	raftNode, err := node.NewNode(node.NodeConfig{
		NodeID:   currentNodeConfig.NodeID,
		RaftAddr: currentNodeConfig.RaftAddr,
		RaftDir:  filepath.Join(*dataDir, currentNodeConfig.NodeID),
	}, peers)
	if err != nil {
		log.Fatalf("Failed to create raft node: %s", err)
	}

	// 4. 创建并启动 HTTP 服务器
	// 注意：服务发现的逻辑不再需要了，因为所有节点信息都在配置里
	// 代理逻辑需要微调，以从配置中读取 HTTP 地址
	httpServer := transport.NewHTTPServer(cfg, raftNode.Raft, raftNode.FSM) // 将整个配置注入
	if err := httpServer.Start(currentNodeConfig.HTTPAddr); err != nil {
		log.Fatalf("Failed to start HTTP server: %s", err)
	}
}
