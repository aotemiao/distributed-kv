package transport

import (
	"distributed-kv/internal/config"
	"distributed-kv/internal/store"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/hashicorp/raft"
)

// Server 是我们的 HTTP 服务器
type Server struct {
	raftNode *raft.Raft
	fsm      *store.FSM
	config   *config.Config // 注入整个配置
}

// NewHTTPServer 创建一个新的 HTTP 服务器实例
func NewHTTPServer(cfg *config.Config, raftNode *raft.Raft, fsm *store.FSM) *Server {
	return &Server{
		config:   cfg,
		raftNode: raftNode,
		fsm:      fsm,
	}
}

// Start 启动 HTTP 服务器
func (s *Server) Start(httpAddr string) error {
	log.Printf("HTTP server starting on %s", httpAddr)
	http.HandleFunc("/kv", s.kvHandler)
	return http.ListenAndServe(httpAddr, nil)
}

// kvHandler 处理键值操作
func (s *Server) kvHandler(w http.ResponseWriter, r *http.Request) {
	// 1. 检查当前节点是否为 Leader
	if s.raftNode.State() != raft.Leader {
		// 如果不是 Leader，代理请求给 Leader
		s.proxyToLeader(w, r)
		return
	}

	// 2. 如果是 Leader，则直接处理
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

	cmdBytes, err := json.Marshal(req)
	if err != nil {
		http.Error(w, "failed to marshal command", http.StatusInternalServerError)
		return
	}

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

	cmd := store.Command{
		Op:  "DELETE",
		Key: key,
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		http.Error(w, "failed to marshal command", http.StatusInternalServerError)
		return
	}

	applyFuture := s.raftNode.Apply(cmdBytes, 500*time.Millisecond)
	if err := applyFuture.Error(); err != nil {
		http.Error(w, fmt.Sprintf("failed to apply command: %s", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) proxyToLeader(w http.ResponseWriter, r *http.Request) {
	leaderRaftAddr, _ := s.raftNode.LeaderWithID()
	if leaderRaftAddr == "" {
		http.Error(w, "no leader elected", http.StatusServiceUnavailable)
		return
	}

	// 从配置中查找 Leader 的 HTTP 地址
	var leaderHTTPAddr string
	for _, peer := range s.config.Peers {
		if peer.RaftAddr == string(leaderRaftAddr) {
			leaderHTTPAddr = peer.HTTPAddr
			break
		}
	}

	targetURL := fmt.Sprintf("http://%s%s", leaderHTTPAddr, r.RequestURI)

	// 后续代理逻辑不变
	proxyReq, err := http.NewRequest(r.Method, targetURL, r.Body)
	if err != nil {
		http.Error(w, "failed to create proxy request", http.StatusInternalServerError)
		return
	}
	proxyReq.Header = r.Header
	client := &http.Client{}
	resp, err := client.Do(proxyReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to proxy request: %s", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}
