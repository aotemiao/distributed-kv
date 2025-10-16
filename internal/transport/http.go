package transport

import (
	"distributed-kv/internal/discovery"
	"distributed-kv/internal/store"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// Server 结构体增加一个读写锁来保护对 raftNode 的并发访问
type Server struct {
	mu       sync.RWMutex
	raftNode *raft.Raft
	fsm      *store.FSM
	consul   *discovery.ConsulClient
}

// NewHTTPServer 创建时 Raft 实例可以为 nil
func NewHTTPServer(f *store.FSM, c *discovery.ConsulClient) *Server {
	return &Server{
		fsm:    f,
		consul: c,
	}
}

// SetRaft 允许我们在 Raft 初始化后，将其安全地注入到 Server 中
func (s *Server) SetRaft(r *raft.Raft, f *store.FSM) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.raftNode = r
	s.fsm = f // FSM 也在这里注入
}

// Start 启动 HTTP 服务器，并注册所有 API 端点
func (s *Server) Start(httpAddr string) error {
	log.Printf("HTTP server listening on %s", httpAddr)

	mux := http.NewServeMux()
	mux.HandleFunc("/kv", s.kvHandler)
	mux.HandleFunc("/health", s.healthHandler)
	mux.HandleFunc("/join", s.joinHandler)

	return http.ListenAndServe(httpAddr, mux)
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// joinHandler 处理新节点加入集群的请求
func (s *Server) joinHandler(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.raftNode == nil {
		http.Error(w, "raft node not ready yet", http.StatusServiceUnavailable)
		return
	}

	// 只有 Leader 可以添加节点
	if s.raftNode.State() != raft.Leader {
		s.proxyToLeader(w, r)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var joinReq struct {
		NodeID   string `json:"node_id"`
		RaftAddr string `json:"raft_addr"`
	}

	if err := json.NewDecoder(r.Body).Decode(&joinReq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("Received join request from node %s at %s", joinReq.NodeID, joinReq.RaftAddr)

	// 添加节点到集群
	future := s.raftNode.AddVoter(
		raft.ServerID(joinReq.NodeID),
		raft.ServerAddress(joinReq.RaftAddr),
		0, 0,
	)

	if err := future.Error(); err != nil {
		http.Error(w, fmt.Sprintf("failed to add voter: %s", err), http.StatusInternalServerError)
		return
	}

	log.Printf("Successfully added node %s to cluster", joinReq.NodeID)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "joined"})
}

// kvHandler 检查 Leader 身份，并进行路由
func (s *Server) kvHandler(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 在 Raft 节点被注入之前，服务是不可用的
	if s.raftNode == nil {
		http.Error(w, "raft node not ready yet", http.StatusServiceUnavailable)
		return
	}

	if s.raftNode.State() != raft.Leader {
		s.proxyToLeader(w, r)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, r)
	case http.MethodPost:
		s.handleSet(w, r)
	case http.MethodDelete:
		s.handleDelete(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// proxyToLeader 通过 Consul 发现 Leader 并代理请求
func (s *Server) proxyToLeader(w http.ResponseWriter, r *http.Request) {
	// RLock 在上层 kvHandler 中已被获取
	leaderRaftAddr := string(s.raftNode.Leader())
	if leaderRaftAddr == "" {
		http.Error(w, "no leader elected", http.StatusServiceUnavailable)
		return
	}

	services, _, err := s.consul.Client().Health().Service(discovery.ServiceName, "", true, nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to query consul for services: %s", err), http.StatusInternalServerError)
		return
	}

	var leaderHTTPAddr string
	for _, service := range services {
		if service.Service.Meta["raft_addr"] == leaderRaftAddr {
			leaderHTTPAddr = fmt.Sprintf("%s:%d", service.Service.Address, service.Service.Port)
			break
		}
	}

	if leaderHTTPAddr == "" {
		http.Error(w, "could not resolve leader's HTTP address from consul", http.StatusInternalServerError)
		return
	}

	targetURL := fmt.Sprintf("http://%s%s", leaderHTTPAddr, r.RequestURI)
	log.Printf("Proxying request to leader at %s", targetURL)

	proxyReq, err := http.NewRequest(r.Method, targetURL, r.Body)
	if err != nil {
		http.Error(w, "failed to create proxy request", http.StatusInternalServerError)
		return
	}
	proxyReq.Header = r.Header

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(proxyReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to proxy request: %s", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}
	value, ok := s.fsm.Get(key)
	if !ok {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"key": key, "value": value})
}

func (s *Server) handleSet(w http.ResponseWriter, r *http.Request) {
	var req store.Command
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req.Op = "SET"
	cmdBytes, _ := json.Marshal(req)

	applyFuture := s.raftNode.Apply(cmdBytes, 500*time.Millisecond)
	if err := applyFuture.Error(); err != nil {
		http.Error(w, fmt.Sprintf("failed to apply command: %s", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}
	cmd := store.Command{Op: "DELETE", Key: key}
	cmdBytes, _ := json.Marshal(cmd)

	applyFuture := s.raftNode.Apply(cmdBytes, 500*time.Millisecond)
	if err := applyFuture.Error(); err != nil {
		http.Error(w, fmt.Sprintf("failed to apply command: %s", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
