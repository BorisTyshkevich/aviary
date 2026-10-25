package config

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
)

// ClientConfig is an inbound caller's identity and exact surface grants.
type ClientConfig struct {
	ID        string   `yaml:"id" json:"id"`
	Name      string   `yaml:"name" json:"name"`
	TokenHash string   `yaml:"token_hash" json:"token_hash"`
	Protocols []string `yaml:"protocols" json:"protocols"`
	Tools     []string `yaml:"tools,omitempty" json:"tools,omitempty"`
	Agents    []string `yaml:"agents,omitempty" json:"agents,omitempty"`
}

var clientIDPattern = regexp.MustCompile(`^client_[0-9a-f]{32}$`)
var clientNamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$`)
var clientHashPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// ClientTokenHash returns the versioned digest of a high-entropy credential.
func ClientTokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// NewClientCredential generates an immutable ID and a 256-bit bearer token.
func NewClientCredential() (id, token string, err error) {
	b := make([]byte, 48)
	if _, err = rand.Read(b); err != nil {
		return "", "", fmt.Errorf("generating client credential: %w", err)
	}
	return "client_" + hex.EncodeToString(b[:16]), "aviary_client_" + hex.EncodeToString(b[16:]), nil
}

// ValidateClients rejects unsupported or ambiguous inbound authorization.
func ValidateClients(cfg *Config) error {
	ids, names, hashes := map[string]bool{}, map[string]bool{}, map[string]bool{}
	agents := map[string]bool{}
	for _, a := range cfg.Agents {
		agents[a.Name] = true
	}
	for i, c := range cfg.Server.Clients {
		if !clientIDPattern.MatchString(c.ID) || !clientNamePattern.MatchString(c.Name) || !clientHashPattern.MatchString(c.TokenHash) {
			return fmt.Errorf("server.clients[%d]: invalid id, name or token_hash", i)
		}
		if ids[c.ID] || names[c.Name] || hashes[c.TokenHash] {
			return fmt.Errorf("server.clients[%d]: duplicate id, name or token_hash", i)
		}
		ids[c.ID], names[c.Name], hashes[c.TokenHash] = true, true, true
		if len(c.Protocols) != 1 || c.Protocols[0] != "mcp" {
			return fmt.Errorf("server.clients[%d]: protocols must contain exactly mcp", i)
		}
		if len(c.Tools) == 0 {
			return fmt.Errorf("server.clients[%d]: MCP tools must not be empty", i)
		}
		seen := map[string]bool{}
		for _, t := range c.Tools {
			if (t != "ping" && t != "agent_run") || seen[t] {
				return fmt.Errorf("server.clients[%d]: unsupported or duplicate tool grant", i)
			}
			seen[t] = true
		}
		if slices.Contains(c.Tools, "agent_run") && len(c.Agents) == 0 {
			return fmt.Errorf("server.clients[%d]: agent_run requires agents", i)
		}
		seen = map[string]bool{}
		for _, a := range c.Agents {
			if !agents[a] || seen[a] {
				return fmt.Errorf("server.clients[%d]: unknown or duplicate agent grant", i)
			}
			seen[a] = true
		}
	}
	return nil
}

// ClientPolicyRevision is the acknowledgment of the exact installed client policy.
func ClientPolicyRevision(clients []ClientConfig) string {
	if len(clients) == 0 {
		clients = nil
	}
	data, _ := json.Marshal(clients)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
