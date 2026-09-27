package config

import (
	"fmt"
	"strings"
	"time"
)

// HooksConfig configures lifecycle executables for an agent.
type HooksConfig struct {
	BeforeTurn *BeforeTurnHookConfig `yaml:"before_turn,omitempty" json:"before_turn,omitempty"`
}

// BeforeTurnHookConfig invokes a trusted executable before the first model request.
type BeforeTurnHookConfig struct {
	Argv            []string `yaml:"argv" json:"argv"`
	Timeout         string   `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	OnError         string   `yaml:"on_error,omitempty" json:"on_error,omitempty"`
	AllowCredential bool     `yaml:"allow_credential,omitempty" json:"allow_credential,omitempty"`
}

func (v *validator) checkPreparationHook(field string, hooks *HooksConfig) {
	if hooks == nil || hooks.BeforeTurn == nil {
		return
	}
	h := hooks.BeforeTurn
	base := field + ".hooks.before_turn"
	if len(h.Argv) == 0 || len(h.Argv) > 32 || strings.TrimSpace(h.Argv[0]) == "" {
		v.errorf(base+".argv", "before_turn requires 1-32 executable arguments")
	}
	for i, arg := range h.Argv {
		if len(arg) > 4096 {
			v.errorf(fmt.Sprintf("%s.argv[%d]", base, i), "hook argument exceeds 4096 bytes")
		}
	}
	if h.Timeout != "" {
		d, err := time.ParseDuration(h.Timeout)
		if err != nil || d <= 0 || d > 2*time.Minute {
			v.errorf(base+".timeout", "hook timeout must be positive and at most 2m")
		}
	}
	if h.OnError != "" && h.OnError != "continue" && h.OnError != "stop" {
		v.errorf(base+".on_error", "on_error must be continue or stop")
	}
}
