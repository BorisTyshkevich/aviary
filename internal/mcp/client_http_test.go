package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/agent"
	"github.com/lsegal/aviary/internal/clientauth"
	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/llm"
	"github.com/lsegal/aviary/internal/store"
)

func inboundFixture() *config.Config {
	return &config.Config{Agents: []config.AgentConfig{{Name: "expert"}, {Name: "other"}}, Server: config.ServerConfig{Clients: []config.ClientConfig{
		{ID: "client_00000000000000000000000000000001", Name: "alice", TokenHash: config.ClientTokenHash("fake-alice"), Protocols: []string{"mcp"}, Tools: []string{"agent_run", "ping"}, Agents: []string{"expert"}},
		{ID: "client_00000000000000000000000000000002", Name: "bob", TokenHash: config.ClientTokenHash("fake-bob"), Protocols: []string{"mcp"}, Tools: []string{"ping"}},
	}}}
}

func inboundHTTP(t *testing.T, cfg *config.Config) (*httptest.Server, *clientauth.Registry, *ClientHTTPHandler) {
	t.Helper()
	registry := clientauth.New()
	require.NoError(t, registry.Install(cfg))
	handler := NewClientHTTPHandler(NewServer(), registry)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if clientauth.EqualToken(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), "fake-admin") {
			AdminPrincipalHandler("fake-admin", handler).ServeHTTP(w, r)
			return
		}
		PrincipalMiddleware(registry, handler).ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	return server, registry, handler
}

func inboundRemote(t *testing.T, url, token string) *RemoteClient {
	t.Helper()
	c, err := NewRemoteClient(context.Background(), url, token)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestClientHTTPToolCatalogAndTransportOwnership(t *testing.T) {
	store.SetDataDir(t.TempDir())
	t.Cleanup(func() { store.SetDataDir("") })
	server, _, _ := inboundHTTP(t, inboundFixture())
	alice, bob, admin := inboundRemote(t, server.URL, "fake-alice"), inboundRemote(t, server.URL, "fake-bob"), inboundRemote(t, server.URL, "fake-admin")
	tools, err := alice.ListTools(context.Background())
	require.NoError(t, err)
	require.Len(t, tools, 2)
	tools, err = bob.ListTools(context.Background())
	require.NoError(t, err)
	require.Len(t, tools, 1)
	require.Equal(t, "ping", tools[0].Name)
	tools, err = admin.ListTools(context.Background())
	require.NoError(t, err)
	require.Greater(t, len(tools), 2)
	result, err := alice.CallTool(context.Background(), "ping", map[string]any{})
	require.NoError(t, err)
	require.Equal(t, "pong", extractText(result))
	for _, name := range []string{"config_save", "agent_update", "agent_rules_set", "session_send", "agent_stop", "exec", "task_run", "server_status", "unknown"} {
		_, err = alice.CallTool(context.Background(), name, map[string]any{})
		require.Error(t, err, "%s", name)
	}
	_, err = bob.CallTool(context.Background(), "agent_run", map[string]any{"name": "expert", "message": "forbidden"})
	require.Error(t, err)
	_, err = os.Stat(store.AgentDir("expert"))
	require.True(t, os.IsNotExist(err))
	for _, test := range []struct{ token, session string }{{"fake-bob", alice.session.ID()}, {"fake-alice", admin.session.ID()}, {"fake-admin", alice.session.ID()}} {
		for _, verb := range []string{http.MethodPost, http.MethodGet, http.MethodDelete} {
			var body io.Reader
			if verb == http.MethodPost {
				body = strings.NewReader(`{"jsonrpc":"2.0","id":9,"method":"tools/list","params":{}}`)
			}
			req, err := http.NewRequest(verb, server.URL+"/mcp", body)
			require.NoError(t, err)
			req.Header.Set("Authorization", "Bearer "+test.token)
			req.Header.Set("Mcp-Session-Id", test.session)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Accept", "application/json, text/event-stream")
			response, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			_ = response.Body.Close()
			require.Equal(t, http.StatusForbidden, response.StatusCode, "%s", verb)
		}
	}
	for _, source := range []string{"cookie", "query"} {
		req, err := http.NewRequest(http.MethodPost, server.URL+"/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`))
		require.NoError(t, err)
		if source == "cookie" {
			req.AddCookie(&http.Cookie{Name: "aviary_session", Value: "fake-alice"})
		} else {
			req.URL.RawQuery = "token=fake-alice"
		}
		response, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		_ = response.Body.Close()
		require.Equal(t, http.StatusUnauthorized, response.StatusCode)
	}
}

// Verify the HTTP boundary itself as well as agent execution below: removing
// the header guard must fail even if a runner happens to overwrite its context.
func TestClientHTTPForgedAgentHeaderIsStripped(t *testing.T) {
	registry := clientauth.New()
	require.NoError(t, registry.Install(inboundFixture()))
	observed := false
	handler := withHTTPRequestContext(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed = true
		require.Empty(t, r.Header.Get("X-Aviary-Agent-ID"))
		_, ok := agent.SessionAgentIDFromContext(r.Context())
		require.False(t, ok)
		p, _, ok := clientauth.FromContext(r.Context())
		require.True(t, ok)
		require.Equal(t, inboundFixture().Server.Clients[0].ID, p.ID)
		w.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	request.Header.Set("Authorization", "Bearer fake-alice")
	request.Header.Set("X-Aviary-Agent-ID", "other")
	response := httptest.NewRecorder()
	PrincipalMiddleware(registry, handler).ServeHTTP(response, request)
	require.Equal(t, http.StatusNoContent, response.Code)
	require.True(t, observed)
}

func TestClientHTTPAgentExecutionUsesAgentToolsAndOwnedSessions(t *testing.T) {
	store.SetDataDir(t.TempDir())
	t.Cleanup(func() { store.SetDataDir("") })
	require.NoError(t, store.EnsureDirs())
	var mu sync.Mutex
	rounds := 0
	catalogSeen := false
	resultSeen := false
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Messages []struct{ Role, Content string }
			Tools    []struct{ Function struct{ Name string } }
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		mu.Lock()
		defer mu.Unlock()
		rounds++
		for _, tool := range request.Tools {
			if tool.Function.Name == "agent_list" {
				catalogSeen = true
			}
		}
		if rounds == 1 {
			writeOpenAIToolCall(t, w, "agent_list", map[string]any{})
			return
		}
		for _, message := range request.Messages {
			if message.Role == "tool" && strings.Contains(message.Content, "expert") {
				resultSeen = true
			}
		}
		writeOpenAIText(w, "owned conversation answered")
	}))
	t.Cleanup(model.Close)
	factory := llm.NewFactory(func(string) (string, error) { return "", nil }).WithProviderOptionsResolver(func(string) (llm.ProviderOptions, bool) { return llm.ProviderOptions{BaseURI: model.URL}, true })
	cfg := inboundFixture()
	cfg.Agents[0].Model = "vllm/test-model"
	cfg.Agents[0].Permissions = &config.PermissionsConfig{Tools: []string{"agent_list"}}
	manager := agent.NewManager(factory)
	manager.Reconcile(cfg)
	t.Cleanup(manager.Stop)
	oldDeps, oldSet := globalDeps, depsSet
	SetDeps(&Deps{Agents: manager})
	t.Cleanup(func() { globalDeps, depsSet = oldDeps, oldSet })
	agent.SetToolClientFactory(NewAgentToolClient)
	t.Cleanup(func() { agent.SetToolClientFactory(nil) })
	server, _, _ := inboundHTTP(t, cfg)
	alice := inboundRemote(t, server.URL, "fake-alice")
	// Reject incorrect agents and foreign/shared conversation references before persistence.
	for _, args := range []map[string]any{{"name": "other", "message": "denied"}, {"name": "expert", "session_id": "main", "message": "stop"}, {"name": "expert", "session_id": "../main", "message": "denied"}} {
		res, err := alice.CallTool(context.Background(), "agent_run", args)
		require.True(t, err != nil || res.IsError)
	}
	entries, err := os.ReadDir(filepath.Join(store.AgentDir("expert"), "sessions"))
	require.True(t, os.IsNotExist(err) || len(entries) == 0)
	// Send the impersonation header through the real MCP HTTP transport.
	request, err := http.NewRequest(http.MethodPost, server.URL+"/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":123,"method":"tools/call","params":{"name":"agent_run","arguments":{"name":"expert","message":"answer"}}}`))
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer fake-alice")
	request.Header.Set("Mcp-Session-Id", alice.session.ID())
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	request.Header.Set("X-Aviary-Agent-ID", "other")
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Contains(t, string(body), "owned conversation answered")
	mu.Lock()
	require.True(t, catalogSeen)
	require.True(t, resultSeen)
	require.Equal(t, 2, rounds)
	mu.Unlock()
	sess, err := agent.NewSessionManager().ClientSession(cfg.Server.Clients[0].ID, "expert", "", "", false)
	require.NoError(t, err)
	_, err = os.Stat(store.SessionPath("expert", "main"))
	require.True(t, os.IsNotExist(err))
	checkpoints, err := os.ReadDir(store.CheckpointDir("expert"))
	require.True(t, os.IsNotExist(err) || len(checkpoints) == 0)
	stop, err := alice.CallTool(context.Background(), "agent_run", map[string]any{"name": "expert", "session_id": sess.ID, "message": "stop"})
	require.NoError(t, err)
	require.Contains(t, extractText(stop), sess.ID)
}

func TestClientHTTPRotationClosesTransportWhileExecutionContinues(t *testing.T) {
	store.SetDataDir(t.TempDir())
	t.Cleanup(func() { store.SetDataDir("") })
	started, finish, canceled := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(started) })
		select {
		case <-finish:
			writeOpenAIText(w, "finished after rotation")
		case <-r.Context().Done():
			close(canceled)
		}
	}))
	t.Cleanup(model.Close)
	cfg := inboundFixture()
	cfg.Agents[0].Model = "vllm/test-model"
	factory := llm.NewFactory(func(string) (string, error) { return "", nil }).WithProviderOptionsResolver(func(string) (llm.ProviderOptions, bool) { return llm.ProviderOptions{BaseURI: model.URL}, true })
	manager := agent.NewManager(factory)
	manager.Reconcile(cfg)
	t.Cleanup(manager.Stop)
	oldDeps, oldSet := globalDeps, depsSet
	SetDeps(&Deps{Agents: manager})
	t.Cleanup(func() { globalDeps, depsSet = oldDeps, oldSet })
	server, registry, handler := inboundHTTP(t, cfg)
	alice := inboundRemote(t, server.URL, "fake-alice")
	responseDone := make(chan struct{})
	go func() {
		_, _ = alice.CallTool(context.Background(), "agent_run", map[string]any{"name": "expert", "message": "wait", "bare": true})
		close(responseDone)
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("model did not start")
	}
	cfg.Server.Clients[0].TokenHash = config.ClientTokenHash("fake-rotated")
	require.NoError(t, registry.Install(cfg))
	handler.Reconcile()
	select {
	case <-responseDone:
	case <-time.After(5 * time.Second):
		t.Fatal("old response stream did not close")
	}
	select {
	case <-canceled:
		t.Fatal("rotation canceled accepted execution")
	default:
	}
	_, ok := registry.Authenticate("fake-alice")
	require.False(t, ok)
	rotated := inboundRemote(t, server.URL, "fake-rotated")
	result, err := rotated.CallTool(context.Background(), "ping", map[string]any{})
	require.NoError(t, err)
	require.Equal(t, "pong", extractText(result))
	close(finish)
	sess, err := agent.NewSessionManager().ClientSession(cfg.Server.Clients[0].ID, "expert", "", "", false)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		data, _ := os.ReadFile(store.SessionPath("expert", sess.ID))
		return strings.Contains(string(data), "finished after rotation")
	}, 5*time.Second, 10*time.Millisecond)
	// Old session identifiers cannot be resumed with the new credential.
	req, err := http.NewRequest(http.MethodDelete, server.URL+"/mcp", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer fake-rotated")
	req.Header.Set("Mcp-Session-Id", alice.session.ID())
	response, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = response.Body.Close()
	require.Equal(t, http.StatusNotFound, response.StatusCode)
}

func TestClientHTTPLogsOmitCredentialsAndArguments(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	server, _, _ := inboundHTTP(t, inboundFixture())
	alice := inboundRemote(t, server.URL, "fake-alice")
	_, err := alice.CallTool(context.Background(), "ping", map[string]any{"token": "fake-do-not-log"})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "client_name=alice")
	require.Contains(t, buf.String(), "client_00000000000000000000000000000001")
	require.NotContains(t, buf.String(), "fake-alice")
	require.NotContains(t, buf.String(), "fake-do-not-log")
}

func TestClientHTTPRemovalAndAgentScopeRemovalCancelAcceptedWork(t *testing.T) {
	for _, change := range []string{"remove", "agent", "tool"} {
		t.Run(change, func(t *testing.T) {
			store.SetDataDir(t.TempDir())
			t.Cleanup(func() { store.SetDataDir("") })
			started := make(chan struct{})
			finish := make(chan struct{})
			var release sync.Once
			defer release.Do(func() { close(finish) })
			model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				close(started)
				select {
				case <-finish:
					writeOpenAIText(w, "fake answer")
				case <-r.Context().Done():
				}
			}))
			t.Cleanup(model.Close)
			cfg := inboundFixture()
			cfg.Agents[0].Model = "vllm/test-model"
			factory := llm.NewFactory(func(string) (string, error) { return "", nil }).WithProviderOptionsResolver(func(string) (llm.ProviderOptions, bool) { return llm.ProviderOptions{BaseURI: model.URL}, true })
			manager := agent.NewManager(factory)
			manager.Reconcile(cfg)
			t.Cleanup(manager.Stop)
			oldDeps, oldSet := globalDeps, depsSet
			SetDeps(&Deps{Agents: manager})
			t.Cleanup(func() { globalDeps, depsSet = oldDeps, oldSet })
			server, registry, handler := inboundHTTP(t, cfg)
			alice := inboundRemote(t, server.URL, "fake-alice")
			done := make(chan struct{})
			go func() {
				_, _ = alice.CallTool(context.Background(), "agent_run", map[string]any{"name": "expert", "message": "wait", "bare": true})
				close(done)
			}()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("model did not start")
			}
			switch change {
			case "remove":
				cfg.Server.Clients = cfg.Server.Clients[1:]
			case "agent":
				cfg.Server.Clients[0].Agents = []string{"other"}
			case "tool":
				cfg.Server.Clients[0].Tools = []string{"ping"}
			}
			require.NoError(t, registry.Install(cfg))
			handler.Reconcile()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("revoked transport did not stop")
			}
			drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, manager.Drain(drainCtx))
			rows, err := os.ReadDir(store.CheckpointDir("expert"))
			require.True(t, os.IsNotExist(err) || len(rows) == 0)
		})
	}
}

func TestPublicClientToolProgressDoesNotExposeDetail(t *testing.T) {
	result := publicClientToolProgress(&agent.PublicToolEvent{Name: "fake_tool", InvocationID: "fake-call", State: agent.ToolStateStarted, Detail: "fake-private-evidence"})
	require.Contains(t, result, `"name":"fake_tool"`)
	require.Contains(t, result, `"state":"started"`)
	require.NotContains(t, result, "fake-private-evidence")
}
