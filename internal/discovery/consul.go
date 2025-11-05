package discovery

import (
	"fmt"
	"log"
	"net"
	"strconv"

	"github.com/hashicorp/consul/api"
)

const (
	ServiceName    = "distributed-kv"
	MinClusterSize = 3
)

type ConsulClient struct {
	client *api.Client
}

func NewConsulClient(addr string) (*ConsulClient, error) {
	config := api.DefaultConfig()
	config.Address = addr
	client, err := api.NewClient(config)
	if err != nil {
		return nil, err
	}
	return &ConsulClient{client: client}, nil
}

// Client 返回底层的 Consul API 客户端
func (c *ConsulClient) Client() *api.Client {
	return c.client
}

// RegisterService 向 Consul 注册服务
func (c *ConsulClient) RegisterService(id, httpAddr, raftAddr string) error {
	host, portStr, err := net.SplitHostPort(httpAddr)
	if err != nil {
		return fmt.Errorf("failed to parse httpAddr %q: %w", httpAddr, err)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("failed to parse port from httpAddr %q: %w", httpAddr, err)
	}

	reg := &api.AgentServiceRegistration{
		ID:      id,
		Name:    ServiceName,
		Address: host,
		Port:    port,
		Meta: map[string]string{
			"raft_addr": raftAddr,
		},
		Check: &api.AgentServiceCheck{
			HTTP:                           fmt.Sprintf("http://%s/health", httpAddr),
			Interval:                       "10s",
			Timeout:                        "1s",
			DeregisterCriticalServiceAfter: "30s", // 在服务持续不健康30秒后自动注销
		},
	}
	log.Printf("Registering service '%s' with Consul at %s", id, httpAddr)
	return c.client.Agent().ServiceRegister(reg)
}

// DeregisterService 从 Consul 注销服务
func (c *ConsulClient) DeregisterService(id string) error {
	log.Printf("Deregistering service '%s' from Consul", id)
	return c.client.Agent().ServiceDeregister(id)
}

// GetHealthyPeers 获取所有健康的伙伴节点
func (c *ConsulClient) GetHealthyPeers() (map[string]string, error) {
	services, _, err := c.client.Health().Service(ServiceName, "", true, nil)
	if err != nil {
		return nil, err
	}

	peers := make(map[string]string) // map[NodeID]RaftAddr
	for _, service := range services {
		nodeID := service.Service.ID
		raftAddr := service.Service.Meta["raft_addr"]
		peers[nodeID] = raftAddr
	}
	return peers, nil
}
