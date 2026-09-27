package mcp

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/lsegal/aviary/internal/agent"
	"github.com/lsegal/aviary/internal/config"
)

func agentToolPermitted(ctx context.Context, name string) error {
	if agent.PrivateDataContext(ctx) {
		if strings.HasPrefix(name, "browser_") || strings.HasPrefix(name, "chlab_") || name == "web_search" {
			return fmt.Errorf("shared browser, lab and search state are unavailable during a private evidence turn")
		}
		switch name {
		case "agent_file_write", "file_write", "file_append", "file_copy", "file_move", "agent_rules_set", "agent_run", "session_send", "channel_send_file", "task_schedule",
			"agent_add", "agent_update", "agent_template_sync", "session_create", "session_set_target",
			"config_save", "config_task_rename", "config_task_move_to_file", "config_task_convert_to_script",
			"auth_set", "auth_login_anthropic_complete", "auth_login_gemini_complete", "auth_login_openai_complete", "auth_login_github_copilot_complete":
			return fmt.Errorf("shared persistence and nested runs are unavailable during a private evidence turn")
		}
	}
	allowed, scoped := agent.ToolPolicyAllows(ctx, name)
	if !allowed {
		return fmt.Errorf("tool %q is not enabled for this turn", name)
	}
	runner := runnerForAgentContext(ctx)
	if runner == nil {
		return nil
	}
	cfg := runner.Config()
	preset := config.EffectivePermissionsPreset(nil)
	if cfg != nil {
		preset = config.EffectivePermissionsPreset(cfg.Permissions)
	}
	if !config.IsToolAllowedByPreset(preset, name) {
		return fmt.Errorf("tool %q is not enabled for this agent", name)
	}
	if name == "exec" && !agentHasExecConfig(ctx) {
		return fmt.Errorf("tool %q is not enabled for this agent", name)
	}
	if cfg != nil && cfg.Permissions != nil {
		if !scoped && len(cfg.Permissions.Tools) > 0 {
			allowed := config.ClampToolNamesForPreset(preset, cfg.Permissions.Tools)
			if !slices.Contains(allowed, name) {
				return fmt.Errorf("tool %q is not enabled for this agent", name)
			}
		}
		disabled := config.ClampToolNamesForPreset(preset, cfg.Permissions.DisabledTools)
		if slices.Contains(disabled, name) {
			return fmt.Errorf("tool %q is disabled for this agent", name)
		}
	}
	return nil
}
