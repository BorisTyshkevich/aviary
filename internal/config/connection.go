package config

import "github.com/lsegal/aviary/internal/endpointpolicy"

// ConnectionPolicyConfig configures generic outbound endpoint authorization.
// It is intentionally independent of MCP so direct database clients can use it.
type ConnectionPolicyConfig struct {
	Network endpointpolicy.Policy `yaml:"network" json:"network"`
}
