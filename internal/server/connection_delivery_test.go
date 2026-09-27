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
	"time"

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
	mu      sync.Mutex
	fail    bool
	sent    []string
	deleted []string
	onSend  func()
	once    sync.Once
}

type sensitiveDeliveryToolClient struct {
	mu    sync.Mutex
	name  string
	args  map[string]any
	calls int
}

func (c *sensitiveDeliveryToolClient) ListTools(context.Context) ([]agent.ToolInfo, error) {
	return []agent.ToolInfo{{Name: "sensitive_tool", Description: "test-only sensitive tool", InputSchema: map[string]any{"type": "object"}}}, nil
}

func (c *sensitiveDeliveryToolClient) CallToolText(_ context.Context, name string, args map[string]any) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.name, c.args = name, args
	c.calls++
	if c.calls == 1 {
		return "", errors.New("fake-sensitive-tool-error")
	}
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

func (c *deliveryTestChannel) PostThreadTextContext(_ context.Context, channel, thread, text string) (string, error) {
	return c.SendThreadMessageAndGetID(channel, thread, text)
}

func (c *deliveryTestChannel) EditThreadTextContext(_ context.Context, _, _, text string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail {
		return errors.New("fake delivery failure")
	}
	if len(c.sent) != 0 {
		c.sent[len(c.sent)-1] = text
	}
	return nil
}

func (c *deliveryTestChannel) DeleteThreadMessageContext(_ context.Context, _, ts string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleted = append(c.deleted, ts)
	return nil
}

func (c *deliveryTestChannel) deletedMessages() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.deleted...)
}

func (c *deliveryTestChannel) ShareThreadMarkdownFileContext(_ context.Context, _, _, _, answer string) error {
	_, err := c.SendThreadMessageAndGetID("C123", "1700000000.000001", answer)
	return err
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

func TestSlackToolDataIsNotPosted(t *testing.T) {
	for _, private := range []bool{false, true} {
		name := "public"
		if private {
			name = "private"
		}
		t.Run(name, func(t *testing.T) {
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
				case 1, 2:
					writeDeliveryToolCall(w, "sensitive_tool", map[string]any{"token": "fake-sensitive-argument", "path": "/fake-sensitive-path"})
				case 3:
					valid = true
					writeDeliveryText(w, "safe final answer")
				default:
					http.Error(w, "unexpected model round", http.StatusBadRequest)
				}
			}))
			t.Cleanup(model.Close)

			cfg := &config.Config{
				Models: config.ModelsConfig{Providers: map[string]config.ProviderConfig{"vllm": {BaseURI: model.URL}}},
				Agents: []config.AgentConfig{{Name: "bot", Model: "vllm/test"}},
			}
			srv := New(cfg, "fake-token")
			if private {
				selectPrivateSlackDeliveryTarget(t, srv, privateSlackDeliveryScope())
			}
			toolClient := &sensitiveDeliveryToolClient{}
			agent.SetToolClientFactory(func(context.Context) (agent.ToolClient, error) { return toolClient, nil })
			t.Cleanup(func() { agent.SetToolClientFactory(nil) })

			ch := &statusDeliveryChannel{}
			srv.handleIncomingChannelMessage(context.Background(), "bot", "slack", "alerts", ch, channels.IncomingMessage{
				Type: "slack", InstallationID: "install", WorkspaceID: "workspace", Channel: "C123", ThreadTS: "1700000000.000001", From: "U123", Text: "question",
			})
			runner, ok := srv.agents.Get("bot")
			require.True(t, ok)
			runner.Wait()

			mu.Lock()
			gotRounds, modelValid := rounds, valid
			mu.Unlock()
			require.Equal(t, 3, gotRounds)
			require.True(t, modelValid)
			toolClient.mu.Lock()
			toolName, token := toolClient.name, fmt.Sprint(toolClient.args["token"])
			toolClient.mu.Unlock()
			require.Equal(t, "sensitive_tool", toolName)
			require.Equal(t, "fake-sensitive-argument", token)
			require.Equal(t, "/fake-sensitive-path", toolClient.args["path"])
			posted := ch.posted()
			require.Equal(t, []string{"safe final answer"}, posted)
			require.Equal(t, []string{"is thinking"}, ch.snapshotStatuses())
			for _, text := range posted {
				require.NotContains(t, text, "fake-sensitive-argument")
				require.NotContains(t, text, "fake-sensitive-result")
				require.NotContains(t, text, "fake-sensitive-tool-error")
				require.NotContains(t, text, "/fake-sensitive-path")
			}
		})
	}
}

func TestSlackTerminalErrorUsesFixedPublicText(t *testing.T) {
	for _, private := range []bool{false, true} {
		name := "public"
		if private {
			name = "private"
		}
		t.Run(name, func(t *testing.T) {
			setupServerDataDir(t)
			resetSlogForTest()
			model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "fake-sensitive-provider-error", http.StatusInternalServerError)
			}))
			t.Cleanup(model.Close)
			cfg := &config.Config{
				Models: config.ModelsConfig{Providers: map[string]config.ProviderConfig{"vllm": {BaseURI: model.URL}}},
				Agents: []config.AgentConfig{{Name: "bot", Model: "vllm/test"}},
			}
			srv := New(cfg, "fake-token")
			if private {
				selectPrivateSlackDeliveryTarget(t, srv, privateSlackDeliveryScope())
			}
			ch := &deliveryTestChannel{}
			srv.handleIncomingChannelMessage(context.Background(), "bot", "slack", "alerts", ch, channels.IncomingMessage{
				Type: "slack", InstallationID: "install", WorkspaceID: "workspace", Channel: "C123", ThreadTS: "1700000000.000001", From: "U123", Text: "question",
			})
			runner, ok := srv.agents.Get("bot")
			require.True(t, ok)
			runner.Wait()
			require.Equal(t, []string{"Unable to complete this request."}, ch.posted())
		})
	}
}

func TestSlackNoReplyClearsStatusWithoutPosting(t *testing.T) {
	setupServerDataDir(t)
	resetSlogForTest()
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeDeliveryText(w, "NO_REPLY")
	}))
	t.Cleanup(model.Close)
	cfg := &config.Config{
		Models: config.ModelsConfig{Providers: map[string]config.ProviderConfig{"vllm": {BaseURI: model.URL}}},
		Agents: []config.AgentConfig{{Name: "bot", Model: "vllm/test"}},
	}
	srv := New(cfg, "fake-token")
	ch := &statusDeliveryChannel{}
	srv.handleIncomingChannelMessage(context.Background(), "bot", "slack", "alerts", ch, channels.IncomingMessage{
		Type: "slack", Channel: "C123", ThreadTS: "1700000000.000001", From: "U123", Text: "question",
	})
	runner, ok := srv.agents.Get("bot")
	require.True(t, ok)
	runner.Wait()
	require.Empty(t, ch.posted())
	require.Equal(t, []string{"is thinking", ""}, ch.snapshotStatuses())
}

type slowDeliveryToolClient struct{ delay time.Duration }

func (c *slowDeliveryToolClient) ListTools(context.Context) ([]agent.ToolInfo, error) {
	return []agent.ToolInfo{{Name: "synthetic_tool", InputSchema: map[string]any{"type": "object"}}}, nil
}

func (c *slowDeliveryToolClient) CallToolText(ctx context.Context, _ string, _ map[string]any) (string, error) {
	select {
	case <-time.After(c.delay):
		return "synthetic result", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (c *slowDeliveryToolClient) Close() error { return nil }

func TestSlackToolProgressUsesSelectedRouteOnSharedChannel(t *testing.T) {
	setupServerDataDir(t)
	resetSlogForTest()
	var rounds int
	var roundsMu sync.Mutex
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		roundsMu.Lock()
		defer roundsMu.Unlock()
		rounds++
		if rounds%2 == 1 {
			writeDeliveryToolCall(w, "synthetic_tool", map[string]any{"path": "/fake-private-path"})
		} else {
			writeDeliveryText(w, "synthetic final")
		}
	}))
	t.Cleanup(model.Close)
	on, off := true, false
	cfg := &config.Config{
		Models: config.ModelsConfig{Providers: map[string]config.ProviderConfig{"vllm": {BaseURI: model.URL}}},
		Agents: []config.AgentConfig{{Name: "bot", Model: "vllm/test", Channels: []config.ChannelConfig{
			{Type: "slack", ID: "quiet", ToolProgress: &off},
			{Type: "slack", ID: "active", ToolProgress: &on},
		}}},
	}
	srv := New(cfg, "fake-token")
	tool := &slowDeliveryToolClient{delay: 2 * time.Second}
	agent.SetToolClientFactory(func(context.Context) (agent.ToolClient, error) { return tool, nil })
	t.Cleanup(func() { agent.SetToolClientFactory(nil) })
	shared := &deliveryTestChannel{}
	runner, ok := srv.agents.Get("bot")
	require.True(t, ok)
	srv.handleIncomingChannelMessage(context.Background(), "bot", "slack", "quiet", shared, channels.IncomingMessage{
		Type: "slack", Channel: "C1", ThreadTS: "1700000000.000001", From: "U1", Text: "quiet",
	})
	runner.Wait()
	require.Equal(t, []string{"synthetic final"}, shared.posted())
	require.Empty(t, shared.deletedMessages())
	srv.handleIncomingChannelMessage(context.Background(), "bot", "slack", "active", shared, channels.IncomingMessage{
		Type: "slack", Channel: "C2", ThreadTS: "1700000000.000002", From: "U1", Text: "active",
	})
	runner.Wait()
	posted := shared.posted()
	require.Len(t, posted, 3)
	require.Contains(t, posted[1], "Tool progress")
	require.Contains(t, posted[1], "synthetic_tool")
	require.NotContains(t, posted[1], "/fake-private-path")
	require.Equal(t, "synthetic final", posted[2])
	require.Equal(t, []string{"posted"}, shared.deletedMessages())
}

func TestSlackPrivateTurnSuppressesEnabledToolProgress(t *testing.T) {
	setupServerDataDir(t)
	resetSlogForTest()
	var rounds int
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		rounds++
		if rounds == 1 {
			writeDeliveryToolCall(w, "synthetic_tool", map[string]any{"path": "/fake-private-path"})
		} else {
			writeDeliveryText(w, "synthetic private answer")
		}
	}))
	t.Cleanup(model.Close)
	on := true
	cfg := &config.Config{
		Models: config.ModelsConfig{Providers: map[string]config.ProviderConfig{"vllm": {BaseURI: model.URL}}},
		Agents: []config.AgentConfig{{Name: "bot", Model: "vllm/test", Channels: []config.ChannelConfig{{Type: "slack", ID: "active", ToolProgress: &on}}}},
	}
	srv := New(cfg, "fake-token")
	selectPrivateSlackDeliveryTarget(t, srv, privateSlackDeliveryScope())
	tool := &slowDeliveryToolClient{delay: 2 * time.Second}
	agent.SetToolClientFactory(func(context.Context) (agent.ToolClient, error) { return tool, nil })
	t.Cleanup(func() { agent.SetToolClientFactory(nil) })
	ch := &deliveryTestChannel{}
	srv.handleIncomingChannelMessage(context.Background(), "bot", "slack", "active", ch, channels.IncomingMessage{
		Type: "slack", InstallationID: "install", WorkspaceID: "workspace", Channel: "C123", ThreadTS: "1700000000.000001", From: "U123", Text: "question",
	})
	runner, ok := srv.agents.Get("bot")
	require.True(t, ok)
	runner.Wait()
	require.Equal(t, []string{"synthetic private answer"}, ch.posted())
	require.Empty(t, ch.deletedMessages())
}

type statusDeliveryChannel struct {
	deliveryTestChannel
	statusMu sync.Mutex
	statuses []string
}

func (c *statusDeliveryChannel) SendAssistantStatusContext(_ context.Context, _, _, status string) error {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	c.statuses = append(c.statuses, status)
	return nil
}

func (c *statusDeliveryChannel) snapshotStatuses() []string {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	return append([]string(nil), c.statuses...)
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
