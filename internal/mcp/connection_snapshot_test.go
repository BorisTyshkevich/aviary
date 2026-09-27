package mcp

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/agent"
	"github.com/lsegal/aviary/internal/connections"
	"github.com/lsegal/aviary/internal/preparation"
)

func TestArtifactReadUsesCurrentPersonalConnectionSnapshot(t *testing.T) {
	service, err := connections.Open(filepath.Join(t.TempDir(), "connections"))
	require.NoError(t, err)
	scope := connections.Scope{AgentID: "bot", InstallationID: "install", WorkspaceID: "workspace", ChannelID: "channel", RootThreadID: "thread"}
	principal := connections.Principal{InstallationID: "install", WorkspaceID: "workspace", UserID: "alice"}
	target, _, err := service.Select(scope, "clickhouse", "https://fake.example")
	require.NoError(t, err)
	require.NoError(t, service.PutPrompt(connections.Prompt{Principal: principal, DMChannelID: "dm", DMRootID: "prompt", Target: target, Stage: "password", Username: "alice", ExpiresAt: time.Now().Add(time.Minute)}))
	require.NoError(t, service.CompletePassword(context.Background(), principal, "dm", "prompt", "100.000001", "fake-password", func(context.Context, connections.Target, connections.Credential) error { return nil }))
	execution := connections.Execution{Kind: connections.Interactive, Scope: scope, Principal: principal}
	credential, ok := service.CredentialFor(execution, target)
	require.True(t, ok)
	lease, err := service.Begin(scope)
	require.NoError(t, err)
	defer lease.End()
	runID := "0123456789abcdef0123456789abcdef"
	state := agent.PreparationState{
		Input:    preparation.Input{Execution: execution, Target: target, CredentialVersion: credential.Version},
		Result:   preparation.Result{Status: "complete", Artifacts: []string{"evidence.json"}, RunID: runID},
		Snapshot: []byte(`{"version":"25.8"}`),
	}
	ctx := agent.WithPreparationState(agent.WithSessionAgentID(connections.WithLease(connections.WithExecution(context.Background(), execution), lease), "bot"), state)
	ctx = agent.WithToolPolicy(ctx, func(name string) bool { return name == "artifact_read" })
	oldDeps, oldDepsSet := GetDeps(), depsSet
	SetDeps(&Deps{Connections: service})
	t.Cleanup(func() { globalDeps, depsSet = oldDeps, oldDepsSet })
	client, err := NewInProcessClient(ctx, NewServer())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	tools, err := client.ListTools(ctx)
	require.NoError(t, err)
	found := false
	for _, tool := range tools {
		if tool.Name == "artifact_read" {
			found = true
		}
	}
	require.True(t, found, "snapshot turn did not list artifact_read")
	read := func(c context.Context, args map[string]any) (string, bool) {
		result, err := client.CallTool(c, "artifact_read", args)
		require.NoError(t, err)
		return extractText(result), result.IsError
	}
	content, failed := read(ctx, map[string]any{"run_id": runID, "path": "evidence.json", "max_bytes": 64})
	require.False(t, failed, content)
	require.True(t, strings.Contains(content, `\"version\":\"25.8\"`), content)
	for _, args := range []map[string]any{
		{"run_id": "bad", "path": "evidence.json", "max_bytes": 64},
		{"run_id": runID, "path": "other.json", "max_bytes": 64},
		{"run_id": runID, "path": "evidence.json", "max_bytes": 3},
	} {
		_, failed := read(ctx, args)
		require.True(t, failed)
	}
	stale := state
	stale.Input.CredentialVersion = "different-version"
	staleCtx := agent.WithPreparationState(ctx, stale)
	staleClient, err := NewInProcessClient(staleCtx, NewServer())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, staleClient.Close()) })
	staleResult, err := staleClient.CallTool(staleCtx, "artifact_read", map[string]any{"run_id": runID, "path": "evidence.json", "max_bytes": 64})
	require.NoError(t, err)
	require.True(t, staleResult.IsError, "stale credential read snapshot")
}
