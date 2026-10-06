package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func clientFixture() ClientConfig {
	return ClientConfig{ID: "client_00000000000000000000000000000001", Name: "peer", TokenHash: ClientTokenHash("fake-peer-token"), Protocols: []string{"mcp"}, Tools: []string{"agent_run", "ping"}, Agents: []string{"expert"}}
}

func TestClientConfigurationValidation(t *testing.T) {
	cases := map[string]func(*Config){
		"malformed ID":     func(c *Config) { c.Server.Clients[0].ID = "../peer" },
		"malformed name":   func(c *Config) { c.Server.Clients[0].Name = "../peer" },
		"malformed hash":   func(c *Config) { c.Server.Clients[0].TokenHash = "fake-raw-token" },
		"unknown protocol": func(c *Config) { c.Server.Clients[0].Protocols = []string{"a2a"} },
		"empty protocol":   func(c *Config) { c.Server.Clients[0].Protocols = nil },
		"empty tools":      func(c *Config) { c.Server.Clients[0].Tools = nil },
		"wildcard":         func(c *Config) { c.Server.Clients[0].Tools = []string{"*"} },
		"tool group":       func(c *Config) { c.Server.Clients[0].Tools = []string{"agent"} },
		"mutation tool":    func(c *Config) { c.Server.Clients[0].Tools = []string{"agent_update"} },
		"unknown agent":    func(c *Config) { c.Server.Clients[0].Agents = []string{"other"} },
		"empty agents":     func(c *Config) { c.Server.Clients[0].Agents = nil },
		"duplicate id": func(c *Config) {
			b := clientFixture()
			b.Name = "second"
			b.TokenHash = ClientTokenHash("fake-second")
			c.Server.Clients = append(c.Server.Clients, b)
		},
		"duplicate name": func(c *Config) {
			b := clientFixture()
			b.ID = "client_00000000000000000000000000000002"
			b.TokenHash = ClientTokenHash("fake-second")
			c.Server.Clients = append(c.Server.Clients, b)
		},
		"duplicate hash": func(c *Config) {
			b := clientFixture()
			b.ID = "client_00000000000000000000000000000002"
			b.Name = "second"
			c.Server.Clients = append(c.Server.Clients, b)
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := &Config{Agents: []AgentConfig{{Name: "expert"}}, Server: ServerConfig{Clients: []ClientConfig{clientFixture()}}}
			require.NoError(t, ValidateClients(cfg))
			mutate(cfg)
			require.Error(t, ValidateClients(cfg))
			require.Error(t, Save(filepath.Join(t.TempDir(), "config.yaml"), cfg))
		})
	}
	cfg := &Config{Server: ServerConfig{Clients: []ClientConfig{clientFixture()}}}
	cfg.Server.Clients[0].Tools = []string{"ping"}
	cfg.Server.Clients[0].Agents = nil
	require.NoError(t, ValidateClients(cfg))
}

func TestUpdateClientsPreservesYAMLAndRejectsFailedMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "aviary.yaml")
	original := []byte("# operator comment\nserver:\n  port: 17777 # custom port\nagents:\n  - name: expert\n    rules: |\n      Keep this instruction.\nfuture_setting: preserved\n")
	require.NoError(t, os.WriteFile(path, original, 0o600))
	clients, err := UpdateClients(path, func(_ []ClientConfig) ([]ClientConfig, error) { return []ClientConfig{clientFixture()}, nil })
	require.NoError(t, err)
	require.Len(t, clients, 1)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	for _, expected := range []string{"# operator comment", "17777 # custom port", "Keep this instruction.", "future_setting: preserved"} {
		require.Contains(t, string(data), expected)
	}
	require.NotContains(t, string(data), "fake-peer-token")
	before := string(data)
	_, err = UpdateClients(path, func(c []ClientConfig) ([]ClientConfig, error) { return append(c, c[0]), nil })
	require.Error(t, err)
	data, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, string(data))
	require.NoError(t, os.WriteFile(path+".clients.lock", nil, 0o600))
	_, err = UpdateClients(path, func(_ []ClientConfig) ([]ClientConfig, error) { return nil, nil })
	require.Error(t, err)
	data, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, string(data))
}

func TestNewClientCredentialHas256BitsAndDistinctIdentity(t *testing.T) {
	id, token, err := NewClientCredential()
	require.NoError(t, err)
	require.True(t, clientIDPattern.MatchString(id))
	require.Len(t, strings.TrimPrefix(token, "aviary_client_"), 64)
	id2, token2, err := NewClientCredential()
	require.NoError(t, err)
	require.NotEqual(t, id, id2)
	require.NotEqual(t, token, token2)
}
