package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/server"
)

func TestClientCLIAddRotateRemoveAndOfflineStatus(t *testing.T) {
	oldCfg, oldURL, oldToken := cfgFile, serverURL, token
	t.Cleanup(func() { cfgFile, serverURL, token = oldCfg, oldURL, oldToken })
	cfgFile = filepath.Join(t.TempDir(), "custom.yaml")
	original := []byte("# preserve me\nserver:\n  port: 17777\nagents:\n  - name: expert\n    rules: Keep this rule.\n")
	require.NoError(t, os.WriteFile(cfgFile, original, 0o600))
	t.Setenv("AVIARY_PID_FILE", filepath.Join(t.TempDir(), "offline.pid"))
	run := func(args ...string) (string, string, error) {
		command := newClientCommand()
		var stdout, stderr bytes.Buffer
		command.SetOut(&stdout)
		command.SetErr(&stderr)
		command.SetArgs(args)
		err := command.Execute()
		return stdout.String(), stderr.String(), err
	}
	raw, status, err := run("add", "peer", "--protocols", "mcp", "--tools", "agent_run,ping", "--agents", "expert")
	require.NoError(t, err)
	require.Contains(t, status, "server offline")
	require.Equal(t, 1, strings.Count(raw, "\n"))
	cfg, err := config.Load(cfgFile)
	require.NoError(t, err)
	require.Len(t, cfg.Server.Clients, 1)
	id := cfg.Server.Clients[0].ID
	require.Equal(t, config.ClientTokenHash(strings.TrimSpace(raw)), cfg.Server.Clients[0].TokenHash)
	data, err := os.ReadFile(cfgFile)
	require.NoError(t, err)
	require.NotContains(t, string(data), strings.TrimSpace(raw))
	require.Contains(t, string(data), "# preserve me")
	require.Contains(t, string(data), "Keep this rule.")
	rotated, status, err := run("rotate", "peer")
	require.NoError(t, err)
	require.Contains(t, status, "server offline")
	require.NotEqual(t, raw, rotated)
	cfg, err = config.Load(cfgFile)
	require.NoError(t, err)
	require.Equal(t, id, cfg.Server.Clients[0].ID)
	require.Equal(t, config.ClientTokenHash(strings.TrimSpace(rotated)), cfg.Server.Clients[0].TokenHash)
	stdout, _, err := run("remove", "peer")
	require.NoError(t, err)
	require.Empty(t, stdout)
	cfg, err = config.Load(cfgFile)
	require.NoError(t, err)
	require.Empty(t, cfg.Server.Clients)
	_, _, err = run("add", "peer", "--protocols", "mcp", "--tools", "ping")
	require.NoError(t, err)
	cfg, err = config.Load(cfgFile)
	require.NoError(t, err)
	require.NotEqual(t, id, cfg.Server.Clients[0].ID)
	before, err := os.ReadFile(cfgFile)
	require.NoError(t, err)
	stdout, _, err = run("add", "invalid", "--protocols", "mcp", "--tools", "config_save")
	require.Error(t, err)
	require.Empty(t, stdout)
	after, err := os.ReadFile(cfgFile)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NoError(t, os.WriteFile(cfgFile+".clients.lock", nil, 0o600))
	stdout, _, err = run("rotate", "peer")
	require.Error(t, err)
	require.Empty(t, stdout)
}

func TestClientCLIRuntimeAcknowledgmentAndFailure(t *testing.T) {
	oldCfg, oldURL, oldToken := cfgFile, serverURL, token
	t.Cleanup(func() { cfgFile, serverURL, token = oldCfg, oldURL, oldToken })
	cfgFile = filepath.Join(t.TempDir(), "custom.yaml")
	require.NoError(t, os.WriteFile(cfgFile, []byte("server: {}\n"), 0o600))
	t.Setenv("AVIARY_PID_FILE", filepath.Join(t.TempDir(), "running.pid"))
	require.NoError(t, server.WritePID())
	reject := false
	acknowledgments := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer fake-admin", r.Header.Get("Authorization"))
		require.Equal(t, "/api/clients/reload", r.URL.Path)
		var payload map[string]string
		require.NoError(t, json.NewDecoder(r.Body).Decode(&payload))
		require.Equal(t, cfgFile, payload["path"])
		cfg, err := config.Load(cfgFile)
		require.NoError(t, err)
		require.Equal(t, config.ClientPolicyRevision(cfg.Server.Clients), payload["revision"])
		acknowledgments++
		if reject {
			w.WriteHeader(http.StatusConflict)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"revision": payload["revision"]})
	}))
	defer backend.Close()
	serverURL, token = backend.URL, "fake-admin"
	run := func(args ...string) (string, string, error) {
		command := newClientCommand()
		command.PersistentFlags().String("server", backend.URL, "test endpoint")
		var stdout, stderr bytes.Buffer
		command.SetOut(&stdout)
		command.SetErr(&stderr)
		command.SetArgs(append(args, "--server", backend.URL))
		err := command.Execute()
		return stdout.String(), stderr.String(), err
	}
	stdout, status, err := run("add", "peer", "--protocols", "mcp", "--tools", "ping")
	require.NoError(t, err)
	require.NotEmpty(t, stdout)
	require.Contains(t, status, "installed and acknowledged")
	require.Equal(t, 1, acknowledgments)
	reject = true
	stdout, status, err = run("rotate", "peer")
	require.ErrorContains(t, err, "revocation is not confirmed")
	require.NotEmpty(t, stdout)
	require.NotContains(t, status, "installed and acknowledged")
	require.Equal(t, 2, acknowledgments)
}

func TestClientCLIRejectsMismatchedAcknowledgment(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"revision": "wrong"})
	}))
	defer backend.Close()
	require.ErrorContains(t, acknowledgeClientPolicy(context.Background(), backend.URL, "fake-admin", "expected", "unused.yaml"), "did not match")
}
