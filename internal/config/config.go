package config

import (
	"encoding/json"
	"os"
)

type PeerConfig struct {
	NodeID   string `json:"node_id"`
	RaftAddr string `json:"raft_addr"`
	HTTPAddr string `json:"http_addr"`
}

type Config struct {
	Peers []PeerConfig `json:"peers"`
}

func Load(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var cfg Config
	if err := json.NewDecoder(file).Decode(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
