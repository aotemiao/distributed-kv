package main

import (
	"bytes"
	"distributed-kv/internal/discovery"
	"distributed-kv/internal/node"
	"distributed-kv/internal/transport"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/raft"
)

func main() {
	httpAddr := flag.String("http", "", "HTTP server address")
	nodeID := flag.String("id", "", "Node ID")
	raftAddr := flag.String("raft", "", "Raft server address")
	dataDir := flag.String("dir", "data", "Raft data directory")
	consulAddr := flag.String("consul", "127.0.0.1:8500", "Consul address")
	flag.Parse()

	// 若未提供，生成随机 NodeID
	if *nodeID == "" {
		rand.Seed(time.Now().UnixNano())
		*nodeID = fmt.Sprintf("node-%08x", rand.Uint32())
	}

	// 若未提供，选择本机回环地址+随机可用端口作为 HTTP 地址
	if *httpAddr == "" {
		h, err := pickFreePortAddr("127.0.0.1")
		if err != nil {
			log.Fatalf("Failed to pick http port: %v", err)
		}
		*httpAddr = h
	}

	// 若未提供，选择本机回环地址+随机可用端口作为 Raft 地址
	if *raftAddr == "" {
		r, err := pickFreePortAddr("127.0.0.1")
		if err != nil {
			log.Fatalf("Failed to pick raft port: %v", err)
		}
		*raftAddr = r
	}

	// 1. 创建 Consul 客户端
	consulClient, err := discovery.NewConsulClient(*consulAddr)
	if err != nil {
		log.Fatalf("Failed to create consul client: %s", err)
	}

	// 2. 创建 HTTP 服务器 (此时 Raft 实例为 nil)
	httpServer := transport.NewHTTPServer(nil, consulClient)

	// 3. 在后台 goroutine 中立即启动 HTTP 服务
	go func() {
		if err := httpServer.Start(*httpAddr); err != nil {
			log.Fatalf("Failed to start HTTP server: %s", err)
		}
	}()
	log.Printf("HTTP server started at %s", *httpAddr)

	// 4. 注册服务到 Consul (现在 /health 接口已经可用)
	err = consulClient.RegisterService(*nodeID, *httpAddr, *raftAddr)
	if err != nil {
		log.Fatalf("Failed to register service with consul: %s", err)
	}

	// 5. 检查集群状态，决定是初始引导还是加入现有集群
	var peers []node.RaftPeer
	var shouldBootstrap bool

	for {
		log.Println("Waiting for peers to form a cluster...")
		peerMap, err := consulClient.GetHealthyPeers()
		if err != nil {
			log.Printf("Failed to get peers from consul: %s", err)
			time.Sleep(2 * time.Second)
			continue
		}

		// 首先检查 Consul KV 中是否已经有集群引导标记
		kv := consulClient.Client().KV()
		pair, _, err := kv.Get("distributed-kv/bootstrapped", nil)
		if err != nil {
			log.Printf("Failed to check bootstrap status: %s", err)
			time.Sleep(2 * time.Second)
			continue
		}

		if pair != nil {
			// 集群已经被引导过
			log.Println("Cluster already bootstrapped. Joining as new member")
			shouldBootstrap = false
			peers = nil
			break
		}

		// 集群还未引导，检查是否有足够的节点
		if len(peerMap) >= discovery.MinClusterSize {
			// 尝试获取分布式锁来执行引导
			log.Printf("Found %d peers. Attempting to acquire bootstrap lock", len(peerMap))

			lockOpts := &api.LockOptions{
				Key:         "distributed-kv/bootstrap-lock",
				SessionTTL:  "10s",
				SessionName: *nodeID,
			}

			lock, err := consulClient.Client().LockOpts(lockOpts)
			if err != nil {
				log.Printf("Failed to create lock: %s", err)
				time.Sleep(2 * time.Second)
				continue
			}

			// 尝试获取锁（非阻塞，带超时）
			lockCh, err := lock.Lock(make(chan struct{}))
			if err != nil {
				log.Printf("Failed to acquire lock: %s", err)
				time.Sleep(2 * time.Second)
				continue
			}

			if lockCh == nil {
				// 没有获取到锁，说明其他节点正在引导
				log.Println("Another node is bootstrapping. Waiting...")
				time.Sleep(2 * time.Second)
				continue
			}

			// 获取到锁，再次检查是否已被引导
			pair, _, err = kv.Get("distributed-kv/bootstrapped", nil)
			if err == nil && pair != nil {
				log.Println("Cluster was already bootstrapped by another node")
				lock.Unlock()
				shouldBootstrap = false
				peers = nil
				break
			}

			// 执行引导
			log.Println("Acquired bootstrap lock. Will bootstrap cluster")
			shouldBootstrap = true
			for id, rAddr := range peerMap {
				peers = append(peers, node.RaftPeer{
					ID:      raft.ServerID(id),
					Address: raft.ServerAddress(rAddr),
				})
			}

			// 在 Consul KV 中标记集群已引导
			_, err = kv.Put(&api.KVPair{
				Key:   "distributed-kv/bootstrapped",
				Value: []byte("true"),
			}, nil)
			if err != nil {
				log.Printf("Failed to set bootstrap flag: %s", err)
			}

			lock.Unlock()
			break
		}
		time.Sleep(2 * time.Second)
	}

	// 7. 初始化 Raft 节点
	raftNode, err := node.NewNode(node.NodeConfig{
		NodeID:          *nodeID,
		RaftAddr:        *raftAddr,
		RaftDir:         filepath.Join(*dataDir, *nodeID),
		ShouldBootstrap: shouldBootstrap,
	}, peers)
	if err != nil {
		log.Fatalf("Failed to create raft node: %s", err)
	}

	// 8. 将初始化好的 Raft 实例和 FSM 注入到已在运行的 HTTP 服务器中
	httpServer.SetRaft(raftNode.Raft, raftNode.FSM)
	log.Println("Raft node injected into HTTP server. System is fully operational.")

	// 8.5. 如果是新节点加入现有集群，需要主动请求加入
	if !shouldBootstrap {
		go func() {
			for {
				time.Sleep(2 * time.Second)
				// 尝试从 Consul 找到 Leader 并请求加入
				peerMap, err := consulClient.GetHealthyPeers()
				if err != nil {
					log.Printf("Failed to get peers: %s", err)
					continue
				}

				// 尝试每个节点，直到找到 Leader
				for peerID := range peerMap {
					if peerID == *nodeID {
						continue
					}

					// 通过 Consul 查询该节点的 HTTP 地址
					services, _, err := consulClient.Client().Health().Service(discovery.ServiceName, "", true, nil)
					if err != nil {
						continue
					}

					var peerHTTPAddr string
					for _, service := range services {
						if service.Service.ID == peerID {
							peerHTTPAddr = fmt.Sprintf("%s:%d", service.Service.Address, service.Service.Port)
							break
						}
					}

					if peerHTTPAddr == "" {
						continue
					}

					// 发送加入请求
					joinURL := fmt.Sprintf("http://%s/join", peerHTTPAddr)
					reqBody := fmt.Sprintf(`{"node_id":"%s","raft_addr":"%s"}`, *nodeID, *raftAddr)

					resp, err := http.Post(joinURL, "application/json", bytes.NewBufferString(reqBody))
					if err != nil {
						log.Printf("Failed to send join request to %s: %s", peerHTTPAddr, err)
						continue
					}

					joined := false
					func() {
						defer resp.Body.Close()
						io.Copy(io.Discard, resp.Body)

						if resp.StatusCode == http.StatusOK {
							log.Printf("Successfully joined cluster via %s", peerHTTPAddr)
							joined = true
							return
						}
						log.Printf("Join request to %s returned status %d", peerHTTPAddr, resp.StatusCode)
					}()

					if joined {
						return // 成功加入，退出 goroutine
					}
				}
			}
		}()
	}

	// 9. 监听系统信号，实现优雅关闭
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down node...")

	if err := consulClient.DeregisterService(*nodeID); err != nil {
		log.Printf("Failed to deregister service: %s", err)
	}
	if err := raftNode.Raft.Shutdown().Error(); err != nil {
		log.Printf("Failed to shut down raft: %s", err)
	}
	log.Println("Node shut down successfully.")
}

func pickFreePortAddr(host string) (string, error) {
	l, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		return "", err
	}
	defer l.Close()
	return l.Addr().String(), nil
}
