package mcp

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/agent"
	"github.com/lsegal/aviary/internal/store"
)

func TestTurnPolicySurvivesIndirectScriptClient(t *testing.T) {
	ctx := agent.WithToolPolicy(context.Background(), func(name string) bool { return name == "ping" })
	client, err := newScriptToolClient(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	tools, err := client.ListTools(ctx)
	require.NoError(t, err)
	for _, tool := range tools {
		require.Equal(t, "ping", tool.Name)
	}
	_, err = client.CallToolText(ctx, "agent_run", map[string]any{"name": "other", "message": "hello"})
	require.ErrorContains(t, err, "not enabled for this turn")
}

func TestPrivateStoresCannotBePublishedAsChannelFiles(t *testing.T) {
	oldDir := store.DataDir()
	store.SetDataDir(t.TempDir())
	t.Cleanup(func() { store.SetDataDir(oldDir) })
	client, err := NewInProcessClient(context.Background(), NewServer())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	for _, dir := range []string{store.DirAuth, "connections", "preparation"} {
		path := filepath.Join(store.SubDir(dir), "private.json")
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
		require.NoError(t, os.WriteFile(path, []byte("fake-private-data"), 0o600))
		result, err := client.CallTool(context.Background(), "channel_send_file", map[string]any{"file_path": path})
		require.NoError(t, err)
		require.True(t, result.IsError)
		require.Contains(t, extractText(result), "private runtime storage")
		require.NotContains(t, extractText(result), "fake-private-data")
	}
}
