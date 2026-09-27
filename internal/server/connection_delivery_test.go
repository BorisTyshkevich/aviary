package server

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/agent"
	"github.com/lsegal/aviary/internal/channels"
	"github.com/lsegal/aviary/internal/config"
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
	var registryCalls int
	var registryMu sync.Mutex
	ch := &deliveryTestChannel{}
	ch.onSend = func() {
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
