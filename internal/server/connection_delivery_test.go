package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/agent"
	"github.com/lsegal/aviary/internal/channels"
	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/connections"
	"github.com/lsegal/aviary/internal/domain"
	"github.com/lsegal/aviary/internal/store"
)

type deliveryTestChannel struct {
	stubChannel
	mu     sync.Mutex
	fail   bool
	sent   []string
	onSend func()
	once   sync.Once
}

type sensitiveDeliveryToolClient struct {
	mu   sync.Mutex
	name string
	args map[string]any
}

func (c *sensitiveDeliveryToolClient) ListTools(context.Context) ([]agent.ToolInfo, error) {
	return []agent.ToolInfo{{Name: "sensitive_tool", Description: "test-only sensitive tool", InputSchema: map[string]any{"type": "object"}}}, nil
}

func (c *sensitiveDeliveryToolClient) CallToolText(_ context.Context, name string, args map[string]any) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.name, c.args = name, args
	return "fake-sensitive-result", nil
}

func (c *sensitiveDeliveryToolClient) Close() error { return nil }

func (c *deliveryTestChannel) SendThreadMessageAndGetID(_, _, text string) (string, error) {
	if c.onSend != nil {
		c.once.Do(c.onSend)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail {
		return "", errors.New("fake delivery failure")
	}
	c.sent = append(c.sent, text)
	return "posted", nil
}

func TestPrivateSlackDeliveryDoesNotFanOutToSessionRegistry(t *testing.T) {
	setupServerDataDir(t)
	resetSlogForTest()
	cfg := &config.Config{Agents: []config.AgentConfig{{Name: "bot", Model: "stub"}}}
	srv := New(cfg, "fake-token")
	scope := privateSlackDeliveryScope()
	selectPrivateSlackDeliveryTarget(t, srv, scope)
	var registryCalls int
	var registryMu sync.Mutex
	leaseHeld := false
	ch := &deliveryTestChannel{}
	ch.onSend = func() {
		_, _, err := srv.connections.Select(scope, "clickhouse", "https://replacement.example:8443")
		leaseHeld = errors.Is(err, connections.ErrBusy)
		session, err := agent.NewSessionManager().GetOrCreateNamed("bot", "slack:C123")
		if err != nil || session == nil {
			return
		}
		agent.RegisterSessionDelivery("bot", session.ID, "slack", "other-thread", func(string) { registryMu.Lock(); registryCalls++; registryMu.Unlock() })
	}
	srv.handleIncomingChannelMessage(context.Background(), "bot", "slack", "alerts", ch, channels.IncomingMessage{
		Type: "slack", InstallationID: "install", WorkspaceID: "workspace", Channel: "C123", ThreadTS: "1700000000.000001", From: "U123", Text: "question",
	})
	runner, ok := srv.agents.Get("bot")
	require.True(t, ok)
	runner.Wait()
	require.Len(t, ch.posted(), 1)
	require.True(t, leaseHeld, "selected private attachment must remain leased while its answer is posted")
	registryMu.Lock()
	calls := registryCalls
	registryMu.Unlock()
	require.Zero(t, calls, "private answer must only be delivered through its originating streamer")
}

func (c *deliveryTestChannel) posted() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.sent...)
}

func TestPrivateSlackAnswerEntersHistoryOnlyAfterDelivery(t *testing.T) {
	for _, tc := range []struct {
		name        string
		fail        bool
		wantAnswers int
	}{{"failed delivery", true, 0}, {"successful delivery", false, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			setupServerDataDir(t)
			resetSlogForTest()
			cfg := &config.Config{Agents: []config.AgentConfig{{Name: "bot", Model: "stub"}}}
			srv := New(cfg, "fake-token")
			selectPrivateSlackDeliveryTarget(t, srv, privateSlackDeliveryScope())
			ch := &deliveryTestChannel{fail: tc.fail}
			srv.handleIncomingChannelMessage(context.Background(), "bot", "slack", "alerts", ch, channels.IncomingMessage{
				Type: "slack", InstallationID: "install", WorkspaceID: "workspace", Channel: "C123", ThreadTS: "1700000000.000001", From: "U123", Text: "question",
			})
			runner, ok := srv.agents.Get("bot")
			require.True(t, ok)
			runner.Wait()
			sessions, err := agent.NewSessionManager().List("bot")
			require.NoError(t, err)
			var sessionID string
			for _, session := range sessions {
				if session != nil && session.Name == "slack:C123" {
					sessionID = session.ID
					break
				}
			}
			require.NotEmpty(t, sessionID)
			messages, err := store.ReadJSONL[domain.Message](store.SessionPath("bot", sessionID))
			require.NoError(t, err)
			answers := 0
			for _, message := range messages {
				if message.Role == domain.MessageRoleAssistant {
					answers++
				}
			}
			require.Equal(t, tc.wantAnswers, answers)
			if tc.fail {
				require.Empty(t, ch.posted())
			} else {
				require.Len(t, ch.posted(), 1)
			}
		})
	}
}

func TestPrivateVerboseSlackToolDataIsNotPosted(t *testing.T) {
	setupServerDataDir(t)
	resetSlogForTest()
	var (
		mu     sync.Mutex
		rounds int
		valid  bool
	)
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			http.NotFound(w, r)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		rounds++
		switch rounds {
		case 1:
			writeDeliveryToolCall(w, "sensitive_tool", map[string]any{"token": "fake-sensitive-argument"})
		case 2:
			valid = true
			writeDeliveryText(w, "safe final answer")
		default:
			http.Error(w, "unexpected model round", http.StatusBadRequest)
		}
	}))
	t.Cleanup(model.Close)

	verbose := true
	cfg := &config.Config{
		Models: config.ModelsConfig{Providers: map[string]config.ProviderConfig{"vllm": {BaseURI: model.URL}}},
		Agents: []config.AgentConfig{{Name: "bot", Model: "vllm/test", Verbose: &verbose}},
	}
	srv := New(cfg, "fake-token")
	selectPrivateSlackDeliveryTarget(t, srv, privateSlackDeliveryScope())
	toolClient := &sensitiveDeliveryToolClient{}
	agent.SetToolClientFactory(func(context.Context) (agent.ToolClient, error) { return toolClient, nil })
	t.Cleanup(func() { agent.SetToolClientFactory(nil) })

	ch := &deliveryTestChannel{}
	srv.handleIncomingChannelMessage(context.Background(), "bot", "slack", "alerts", ch, channels.IncomingMessage{
		Type: "slack", InstallationID: "install", WorkspaceID: "workspace", Channel: "C123", ThreadTS: "1700000000.000001", From: "U123", Text: "question",
	})
	runner, ok := srv.agents.Get("bot")
	require.True(t, ok)
	runner.Wait()

	mu.Lock()
	gotRounds, modelValid := rounds, valid
	mu.Unlock()
	require.Equal(t, 2, gotRounds)
	require.True(t, modelValid)
	toolClient.mu.Lock()
	toolName, token := toolClient.name, fmt.Sprint(toolClient.args["token"])
	toolClient.mu.Unlock()
	require.Equal(t, "sensitive_tool", toolName)
	require.Equal(t, "fake-sensitive-argument", token)
	posted := ch.posted()
	require.Equal(t, []string{"safe final answer"}, posted)
	for _, text := range posted {
		require.NotContains(t, text, "fake-sensitive-argument")
		require.NotContains(t, text, "fake-sensitive-result")
	}
}

func writeDeliveryToolCall(w http.ResponseWriter, name string, arguments map[string]any) {
	encoded, _ := json.Marshal(arguments)
	w.Header().Set("Content-Type", "text/event-stream")
	_, _ = fmt.Fprintf(w, "data: {\"id\":\"test\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-private\",\"type\":\"function\",\"function\":{\"name\":%q,\"arguments\":%q}}]},\"finish_reason\":null}]}\n\n", name, string(encoded))
	_, _ = io.WriteString(w, "data: {\"id\":\"test\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n")
}

func writeDeliveryText(w http.ResponseWriter, text string) {
	w.Header().Set("Content-Type", "text/event-stream")
	_, _ = fmt.Fprintf(w, "data: {\"id\":\"test\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":%q},\"finish_reason\":null}]}\n\n", text)
	_, _ = io.WriteString(w, "data: {\"id\":\"test\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
}

func privateSlackDeliveryScope() connections.Scope {
	return connections.Scope{AgentID: "bot", InstallationID: "install", WorkspaceID: "workspace", ChannelID: "C123", RootThreadID: "1700000000.000001"}
}

func selectPrivateSlackDeliveryTarget(t *testing.T, srv *Server, scope connections.Scope) {
	t.Helper()
	require.NotNil(t, srv.connections)
	target, changed, err := srv.connections.Select(scope, "clickhouse", "https://attached.example:8443")
	require.NoError(t, err)
	require.True(t, changed)
	require.NotEmpty(t, target.Generation)
}
