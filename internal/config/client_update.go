package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// UpdateClients atomically edits only server.clients, preserving other YAML
// fields, comments and node order. Exclusive local mutations fail rather than
// overwriting another client command. A leftover lock is an explicit error.
func UpdateClients(path string, update func([]ClientConfig) ([]ClientConfig, error)) ([]ClientConfig, error) {
	if path == "" {
		path = DefaultPath()
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(path+".clients.lock", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("client configuration is locked or unavailable: %w", err)
	}
	defer func() { _ = lock.Close(); _ = os.Remove(path + ".clients.lock") }()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading client configuration: %w", err)
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("invalid client configuration YAML")
	}
	var cfg Config
	if err := doc.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("invalid client configuration")
	}
	clients, err := update(cfg.Server.Clients)
	if err != nil {
		return nil, err
	}
	cfg.Server.Clients = clients
	if err := ValidateClients(&cfg); err != nil {
		return nil, err
	}
	if len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return nil, fmt.Errorf("configuration must be a YAML mapping")
	}
	root := doc.Content[0]
	server := mappingValue(root, "server")
	if server == nil {
		server = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		root.Content = append(root.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "server"}, server)
	}
	if server.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("server must be a YAML mapping")
	}
	var node yaml.Node
	if err := node.Encode(clients); err != nil {
		return nil, err
	}
	setMappingValue(server, "clients", &node)
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(&doc); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	// A concurrent editor is detected before publish. All client commands share
	// the exclusive lock; full-config editor concurrency remains tracked by #34.
	latest, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(latest, data) {
		return nil, fmt.Errorf("configuration changed during client update; retry")
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".clients-*")
	if err != nil {
		return nil, err
	}
	defer func() { _ = tmp.Close(); _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(buf.Bytes()); err != nil {
		return nil, err
	}
	if err := tmp.Sync(); err != nil {
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return nil, err
	}
	return clients, nil
}

func mappingValue(node *yaml.Node, key string) *yaml.Node {
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1]
		}
	}
	return nil
}
func setMappingValue(node *yaml.Node, key string, value *yaml.Node) {
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			node.Content[i+1] = value
			return
		}
	}
	node.Content = append(node.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}, value)
}
