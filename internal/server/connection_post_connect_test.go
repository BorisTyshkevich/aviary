package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/agent"
	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/connections"
	"github.com/lsegal/aviary/internal/preparation"
)

func TestPostConnectCollectorHelper(_ *testing.T) {
	if len(os.Args) < 2 || (os.Args[len(os.Args)-1] != "--post-connect-helper" && os.Args[len(os.Args)-1] != "--wrong-artifact") {
		return
	}
	var req struct {
		ArtifactDir string `json:"artifact_dir"`
		Credential  struct {
			Password string `json:"password"`
		} `json:"credential"`
	}
	if json.NewDecoder(os.Stdin).Decode(&req) != nil || req.Credential.Password != "fake-password" {
		os.Exit(2)
	}
	name := "evidence.json"
	if os.Args[len(os.Args)-1] == "--wrong-artifact" {
		name = "other.json"
	}
	if os.WriteFile(filepath.Join(req.ArtifactDir, name), []byte(`{"version":"25.8"}`), 0o600) != nil {
		os.Exit(2)
	}
	_, _ = fmt.Fprintf(os.Stdout, `{"status":"complete","summary":"private evidence","public_summary":"Version 25.8; uptime 1d 2h.","artifacts":[%q],"observed_at":"2026-09-27T00:00:00Z","producer_revision":"fake-v1"}`, name)
	os.Exit(0)
}

func TestCollectConnectionEvidencePersistsBeyondGenericArtifactRetention(t *testing.T) {
	serviceDir := filepath.Join(t.TempDir(), "connections")
	service, err := connections.Open(serviceDir)
	require.NoError(t, err)
	engineDir := filepath.Join(t.TempDir(), "preparation")
	engine, err := preparation.Open(engineDir)
	require.NoError(t, err)
	scope := connections.Scope{AgentID: "agent", InstallationID: "install", WorkspaceID: "workspace", ChannelID: "channel", RootThreadID: "thread"}
	target, _, err := service.Select(scope, "clickhouse", "https://fake.example")
	require.NoError(t, err)
	principal := connections.Principal{InstallationID: "install", WorkspaceID: "workspace", UserID: "alice"}
	require.NoError(t, service.PutPrompt(connections.Prompt{Principal: principal, DMChannelID: "dm", DMRootID: "prompt", Target: target, Stage: "password", Username: "alice", ExpiresAt: time.Now().Add(time.Minute)}))
	require.NoError(t, service.CompletePassword(context.Background(), principal, "dm", "prompt", "100.000001", "fake-password", func(context.Context, connections.Target, connections.Credential) error { return nil }))
	hook := config.BeforeTurnHookConfig{Argv: []string{os.Args[0], "-test.run=TestPostConnectCollectorHelper", "--", "--post-connect-helper"}, Timeout: "3s", AllowCredential: true}
	summary, err := collectConnectionEvidence(context.Background(), service, engine, hook, target, principal)
	require.NoError(t, err)
	require.Equal(t, "Version 25.8; uptime 1d 2h.", summary)
	execution := connections.Execution{Kind: connections.Interactive, Scope: scope, Principal: principal}
	credential, ok := service.CredentialFor(execution, target)
	require.True(t, ok)
	service.FinishEvidence(principal, target, credential.Version)
	snapshot, _, ok := service.EvidenceFor(execution, target)
	require.True(t, ok)
	require.JSONEq(t, `{"version":"25.8"}`, string(snapshot.Content))
	wrongHook := hook
	wrongHook.Argv = append(append([]string(nil), hook.Argv[:len(hook.Argv)-1]...), "--wrong-artifact")
	_, err = collectConnectionEvidence(context.Background(), service, engine, wrongHook, target, principal)
	require.Error(t, err)
	still, _, ok := service.EvidenceFor(execution, target)
	require.True(t, ok)
	require.Equal(t, snapshot.RunID, still.RunID)
	_, err = engine.Read(context.Background(), preparation.Input{Execution: execution, Target: target, CredentialVersion: credential.Version}, snapshot.RunID, "evidence.json", 1<<20)
	require.ErrorIs(t, err, preparation.ErrForbidden)
	require.NoError(t, os.RemoveAll(engineDir))
	service, err = connections.Open(serviceDir)
	require.NoError(t, err)
	snapshot, _, ok = service.EvidenceFor(execution, target)
	require.True(t, ok)
	require.JSONEq(t, `{"version":"25.8"}`, string(snapshot.Content))
}

func TestPostConnectSkipsCredentialHookWhenQueryToolIsDenied(t *testing.T) {
	service, err := connections.Open(t.TempDir())
	require.NoError(t, err)
	engine, err := preparation.Open(t.TempDir())
	require.NoError(t, err)
	scope := connections.Scope{AgentID: "agent", InstallationID: "install", WorkspaceID: "workspace", ChannelID: "channel", RootThreadID: "thread"}
	target, _, err := service.Select(scope, "clickhouse", "https://fake.example")
	require.NoError(t, err)
	principal := connections.Principal{InstallationID: "install", WorkspaceID: "workspace", UserID: "alice"}
	require.NoError(t, service.PutPrompt(connections.Prompt{Principal: principal, DMChannelID: "dm", DMRootID: "prompt", Target: target, Stage: "password", Username: "alice", ExpiresAt: time.Now().Add(time.Minute)}))
	require.NoError(t, service.CompletePassword(context.Background(), principal, "dm", "prompt", "100.000001", "fake-password", func(context.Context, connections.Target, connections.Credential) error { return nil }))
	hook := &config.BeforeTurnHookConfig{Argv: []string{os.Args[0], "-test.run=TestPostConnectCollectorHelper", "--", "--post-connect-helper"}, Timeout: "3s", AllowCredential: true}
	for _, tc := range []struct {
		name         string
		routeAllowed bool
		tools        []string
	}{
		{name: "route denies query", routeAllowed: false, tools: []string{"clickhouse_query", "artifact_read"}},
		{name: "agent denies query", routeAllowed: true, tools: []string{"artifact_read"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := agent.NewManager(nil)
			manager.Reconcile(&config.Config{Agents: []config.AgentConfig{{Name: "agent", Model: "test/model", Hooks: &config.HooksConfig{PostConnect: hook}, Permissions: &config.PermissionsConfig{Preset: config.PermissionsPresetFull, Tools: tc.tools}}}})
			srv := &Server{agents: manager, connections: service, preparation: engine}
			_, err := srv.collectAfterConnect(context.Background(), target, principal, tc.routeAllowed)
			require.ErrorIs(t, err, preparation.ErrForbidden)
			execution := connections.Execution{Kind: connections.Interactive, Scope: scope, Principal: principal}
			_, _, ok := service.EvidenceFor(execution, target)
			require.False(t, ok)
		})
	}
}
