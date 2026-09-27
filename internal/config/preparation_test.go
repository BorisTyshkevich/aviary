package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBeforeTurnConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		hook  BeforeTurnHookConfig
		field string
	}{
		{"missing argv", BeforeTurnHookConfig{}, "argv"},
		{"bad timeout", BeforeTurnHookConfig{Argv: []string{"fake-executable"}, Timeout: "3m"}, "timeout"},
		{"bad policy", BeforeTurnHookConfig{Argv: []string{"fake-executable"}, OnError: "ignore"}, "on_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{Agents: []AgentConfig{{Name: "bot", Hooks: &HooksConfig{BeforeTurn: &tc.hook}}}}
			issues := Validate(cfg, nil)
			found := false
			for _, issue := range issues {
				if issue.Level == LevelError && issue.Field == "agents[0].hooks.before_turn."+tc.field {
					found = true
				}
			}
			require.True(t, found)
		})
	}
	cfg := &Config{Agents: []AgentConfig{{Name: "bot", Hooks: &HooksConfig{BeforeTurn: &BeforeTurnHookConfig{Argv: []string{"fake-executable", "prepare"}, Timeout: "15s", OnError: "continue"}}}}}
	for _, issue := range Validate(cfg, nil) {
		require.False(t, issue.Level == LevelError && strings.HasPrefix(issue.Field, "agents[0].hooks"))
	}
}

func TestPostConnectHookRequiresCredentialAndExcludesBeforeTurn(t *testing.T) {
	for _, tc := range []struct {
		name  string
		hooks HooksConfig
		field string
	}{
		{name: "missing credential forwarding", hooks: HooksConfig{PostConnect: &BeforeTurnHookConfig{Argv: []string{"collector"}}}, field: "agents[0].hooks.post_connect.allow_credential"},
		{name: "cannot stop saved login", hooks: HooksConfig{PostConnect: &BeforeTurnHookConfig{Argv: []string{"collector"}, AllowCredential: true, OnError: "stop"}}, field: "agents[0].hooks.post_connect.on_error"},
		{name: "one collection lifecycle", hooks: HooksConfig{BeforeTurn: &BeforeTurnHookConfig{Argv: []string{"collector"}}, PostConnect: &BeforeTurnHookConfig{Argv: []string{"collector"}, AllowCredential: true}}, field: "agents[0].hooks"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{Agents: []AgentConfig{{Name: "bot", Hooks: &tc.hooks}}}
			issues := Validate(cfg, nil)
			found := false
			for _, issue := range issues {
				found = found || issue.Level == LevelError && issue.Field == tc.field
			}
			require.True(t, found)
		})
	}
}
