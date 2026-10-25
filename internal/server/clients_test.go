package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/clientauth"
	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/mcp"
)

func clientAPIFixture(t *testing.T) (*Server, *httptest.Server, *config.Config) {
	t.Helper()
	cfg := &config.Config{Server: config.ServerConfig{Clients: []config.ClientConfig{{ID: "client_00000000000000000000000000000001", Name: "peer", TokenHash: config.ClientTokenHash("fake-client"), Protocols: []string{"mcp"}, Tools: []string{"ping"}}}}}
	path := filepath.Join(t.TempDir(), "custom.yaml")
	require.NoError(t, config.Save(path, cfg))
	registry := clientauth.New()
	require.NoError(t, registry.Install(cfg))
	s := &Server{token: "fake-admin", clients: registry, configPath: path, mux: http.NewServeMux()}
	s.registerRoutes()
	httpServer := httptest.NewServer(s.mux)
	t.Cleanup(httpServer.Close)
	return s, httpServer, cfg
}

func TestClientCredentialsCannotUseAdministratorAPIs(t *testing.T) {
	_, server, _ := clientAPIFixture(t)
	for _, path := range []string{"/api/logs", "/api/logs/history", "/api/daemons", "/api/version", "/api/clients/reload"} {
		request, err := http.NewRequest(http.MethodGet, server.URL+path, nil)
		require.NoError(t, err)
		request.Header.Set("Authorization", "Bearer fake-client")
		response, err := http.DefaultClient.Do(request)
		require.NoError(t, err)
		_ = response.Body.Close()
		require.Equal(t, http.StatusUnauthorized, response.StatusCode, path)
	}
	for _, source := range []string{"cookie", "query"} {
		request, err := http.NewRequest(http.MethodPost, server.URL+"/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`))
		require.NoError(t, err)
		if source == "cookie" {
			request.AddCookie(&http.Cookie{Name: "aviary_session", Value: "fake-client"})
		} else {
			request.URL.RawQuery = "token=fake-client"
		}
		response, err := http.DefaultClient.Do(request)
		require.NoError(t, err)
		_ = response.Body.Close()
		require.Equal(t, http.StatusUnauthorized, response.StatusCode)
	}
	for _, submitted := range []string{"fake-client", "fake-admin"} {
		body, _ := json.Marshal(map[string]string{"token": submitted})
		response, err := http.Post(server.URL+"/api/login", "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		_ = response.Body.Close()
		expected := http.StatusUnauthorized
		if submitted == "fake-admin" {
			expected = http.StatusOK
		}
		require.Equal(t, expected, response.StatusCode)
	}
	client, err := mcp.NewRemoteClient(context.Background(), server.URL, "fake-client")
	require.NoError(t, err)
	defer client.Close() //nolint:errcheck
	result, err := client.CallToolText(context.Background(), "ping", nil)
	require.NoError(t, err)
	require.Equal(t, "pong", result)
}

func TestClientReloadAcknowledgesExactPolicyAndConfigurationPath(t *testing.T) {
	s, server, cfg := clientAPIFixture(t)
	oldRevision := s.clients.Revision()
	cfg.Server.Clients[0].TokenHash = config.ClientTokenHash("fake-rotated")
	require.NoError(t, config.Save(s.configPath, cfg))
	post := func(revision, path string) int {
		body, _ := json.Marshal(map[string]string{"revision": revision, "path": path})
		request, err := http.NewRequest(http.MethodPost, server.URL+"/api/clients/reload", bytes.NewReader(body))
		require.NoError(t, err)
		request.Header.Set("Authorization", "Bearer fake-admin")
		response, err := http.DefaultClient.Do(request)
		require.NoError(t, err)
		defer response.Body.Close() //nolint:errcheck
		if response.StatusCode == http.StatusOK {
			var result map[string]string
			require.NoError(t, json.NewDecoder(response.Body).Decode(&result))
			require.Equal(t, revision, result["revision"])
		}
		return response.StatusCode
	}
	require.Equal(t, http.StatusConflict, post(oldRevision, s.configPath))
	require.Equal(t, oldRevision, s.clients.Revision())
	revision := config.ClientPolicyRevision(cfg.Server.Clients)
	require.Equal(t, http.StatusConflict, post(revision, filepath.Join(t.TempDir(), "other.yaml")))
	require.Equal(t, http.StatusOK, post(revision, s.configPath))
	require.Equal(t, revision, s.clients.Revision())
	_, ok := s.clients.Authenticate("fake-client")
	require.False(t, ok)
	_, ok = s.clients.Authenticate("fake-rotated")
	require.True(t, ok)
	cfg.Server.Clients = nil
	require.NoError(t, config.Save(s.configPath, cfg))
	require.Equal(t, http.StatusOK, post(config.ClientPolicyRevision(nil), s.configPath))
	_, ok = s.clients.Authenticate("fake-rotated")
	require.False(t, ok)
}

func TestClientWatcherCannotReinstallSnapshotReadBeforeRevocation(t *testing.T) {
	setupServerDataDir(t)
	resetSlogForTest()
	t.Cleanup(resetSlogForTest)
	cfg := &config.Config{Server: config.ServerConfig{Clients: []config.ClientConfig{{ID: "client_00000000000000000000000000000001", Name: "peer", TokenHash: config.ClientTokenHash("fake-old"), Protocols: []string{"mcp"}, Tools: []string{"ping"}}}}}
	path := filepath.Join(t.TempDir(), "custom.yaml")
	require.NoError(t, config.Save(path, cfg))
	s := New(cfg, "fake-admin", path)
	t.Cleanup(s.agents.Stop)
	stale, err := config.Load(path)
	require.NoError(t, err)
	current, err := config.Load(path)
	require.NoError(t, err)
	current.Server.Clients[0].TokenHash = config.ClientTokenHash("fake-new")
	require.NoError(t, config.Save(path, current))
	require.NoError(t, s.clients.Install(current))
	s.clientMCP.Reconcile()
	s.reloadConfigFromDisk(stale)
	_, ok := s.clients.Authenticate("fake-old")
	require.False(t, ok)
	_, ok = s.clients.Authenticate("fake-new")
	require.True(t, ok)
}
