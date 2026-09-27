package config

import (
	"fmt"
	"strings"
	"time"
)

// HooksConfig configures lifecycle executables for an agent.
type HooksConfig struct {
	BeforeTurn  *BeforeTurnHookConfig `yaml:"before_turn,omitempty" json:"before_turn,omitempty"`
	PostConnect *BeforeTurnHookConfig `yaml:"post_connect,omitempty" json:"post_connect,omitempty"`
}

// BeforeTurnHookConfig configures a trusted lifecycle executable.
type BeforeTurnHookConfig struct {
	Argv            []string `yaml:"argv" json:"argv"`
	Timeout         string   `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	OnError         string   `yaml:"on_error,omitempty" json:"on_error,omitempty"`
	AllowCredential bool     `yaml:"allow_credential,omitempty" json:"allow_credential,omitempty"`
}

func (v *validator) checkPreparationHook(field string, hooks *HooksConfig) {
	if hooks == nil {
		return
	}
	if hooks.BeforeTurn != nil && hooks.PostConnect != nil {
		v.errorf(field+".hooks", "before_turn and post_connect cannot both be configured")
	}
	if hooks.BeforeTurn != nil {
		v.checkHookConfig(field+".hooks.before_turn", hooks.BeforeTurn)
	}
	if hooks.PostConnect != nil {
		v.checkHookConfig(field+".hooks.post_connect", hooks.PostConnect)
		if !hooks.PostConnect.AllowCredential {
			v.errorf(field+".hooks.post_connect.allow_credential", "post_connect requires credential forwarding")
		}
		if hooks.PostConnect.OnError == "stop" {
			v.errorf(field+".hooks.post_connect.on_error", "post_connect cannot undo a saved login")
		}
	}
}

func (v *validator) checkHookConfig(base string, h *BeforeTurnHookConfig) {
	if len(h.Argv) == 0 || len(h.Argv) > 32 || strings.TrimSpace(h.Argv[0]) == "" {
		v.errorf(base+".argv", "hook requires 1-32 executable arguments")
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
