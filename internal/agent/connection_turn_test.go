package agent

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/connections"
	"github.com/lsegal/aviary/internal/domain"
	"github.com/lsegal/aviary/internal/llm"
	"github.com/lsegal/aviary/internal/store"
)

func testConnectionExecution() connections.Execution {
	return connections.Execution{
		Kind:      connections.Interactive,
		Scope:     connections.Scope{AgentID: "private", InstallationID: "app", WorkspaceID: "team", ChannelID: "channel", RootThreadID: "thread"},
		Principal: connections.Principal{InstallationID: "app", WorkspaceID: "team", UserID: "alice"},
	}
}

func TestConnectionTurnReservesEmptyThreadAndDropsScheduledIdentity(t *testing.T) {
	service, err := connections.Open(t.TempDir())
	require.NoError(t, err)
	r := NewAgentRunner(&domain.Agent{ID: "private"}, &config.AgentConfig{}, nil, nil)
	r.connections = service
	e := testConnectionExecution()
	ctx := connections.WithExecution(context.Background(), e)
	reserved, release, err := r.reserveConnectionTurn(ctx)
	require.NoError(t, err)
	defer release()
	_, leased := connections.LeaseFromContext(reserved)
	require.True(t, leased)
	_, _, err = service.Select(e.Scope, "clickhouse", "https://db.example:8443")
	require.ErrorIs(t, err, connections.ErrBusy)
	release()
	_, _, err = service.Select(e.Scope, "clickhouse", "https://db.example:8443")
	require.NoError(t, err)
	ctx, end, err := r.reserveConnectionTurn(WithTaskID(ctx, "scheduled"))
	require.NoError(t, err)
	defer end()
	scheduled, _ := connections.ExecutionFromContext(ctx)
	require.Equal(t, connections.Scheduled, scheduled.Kind)
	require.False(t, scheduled.Personal())
}

func TestPrivateTurnDoesNotPersistToolEvidence(t *testing.T) {
	setTestDataDir(t)
	r := NewAgentRunner(&domain.Agent{ID: "private"}, &config.AgentConfig{}, nil, nil)
	ctx := privateTestContext(t, r)
	client := &privateEvidenceClient{}
	text, stop := r.executeToolCall(ctx, func(StreamEvent) {}, client, "shared", nil, toolEventRecord{Name: "artifact_read"}, "artifact_read", nil)
	require.False(t, stop)
	require.Equal(t, "alice-private-evidence", text)
	rows, err := store.ReadJSONL[domain.Message](store.SessionPath("private", "shared"))
	if err == nil {
		require.Empty(t, rows)
	}
}

type privateEvidenceClient struct{}

func (*privateEvidenceClient) ListTools(context.Context) ([]ToolInfo, error) { return nil, nil }
func (*privateEvidenceClient) Close() error                                  { return nil }
func (*privateEvidenceClient) CallToolText(context.Context, string, map[string]any) (string, error) {
	return "alice-private-evidence", nil
}

func TestPrivateTurnDoesNotResumeSharedProviderConversation(t *testing.T) {
	setTestDataDir(t)
	provider := &sequenceProvider{responses: [][]llm.Event{{{Type: llm.EventTypeText, Text: "answer"}}}}
	r := NewAgentRunner(&domain.Agent{ID: "private", Name: "private"}, &config.AgentConfig{}, provider, nil)
	session, err := NewSessionManager().GetOrCreateNamed("private", "channel")
	require.NoError(t, err)
	require.NoError(t, store.WriteSessionMeta(store.SessionMetaPath("private", session.ID), store.SessionMeta{Conversation: &store.ConversationMeta{ID: "alice-response", LastUsedAt: time.Now()}}))
	ctx := WithSessionID(privateTestContext(t, r), session.ID)
	done := make(chan struct{})
	r.PromptWithOverrides(ctx, "hello", RunOverrides{Bare: true}, func(event StreamEvent) {
		if event.Type == StreamEventDone || event.Type == StreamEventError {
			close(done)
		}
	})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("turn did not finish")
	}
	r.active.Wait()
	require.Empty(t, provider.requests[0].PreviousResponseID)
}

func privateTestContext(t *testing.T, r *AgentRunner) context.Context {
	t.Helper()
	service, err := connections.Open(t.TempDir())
	require.NoError(t, err)
	e := testConnectionExecution()
	_, _, err = service.Select(e.Scope, "clickhouse", "https://db.example:8443")
	require.NoError(t, err)
	r.connections = service
	lease, err := service.Begin(e.Scope)
	require.NoError(t, err)
	t.Cleanup(lease.End)
	return connections.WithLease(connections.WithExecution(context.Background(), e), lease)
}

func TestNestedToolPolicyCannotWidenParent(t *testing.T) {
	ctx := WithToolPolicy(context.Background(), func(name string) bool { return name == "ping" })
	ctx = WithToolPolicy(ctx, func(string) bool { return true })
	allowed, present := ToolPolicyAllows(ctx, "agent_run")
	require.True(t, present)
	require.False(t, allowed)
	allowed, _ = ToolPolicyAllows(ctx, "ping")
	require.True(t, allowed)
}

func TestOrdinarySlackTurnRetainsImageHistoryWhenTextDeliveryIsDeferred(t *testing.T) {
	setTestDataDir(t)
	provider := &sequenceProvider{responses: [][]llm.Event{{{Type: llm.EventTypeMedia, MediaURL: "data:image/png;base64,ZmFrZQ=="}, {Type: llm.EventTypeText, Text: "answer"}}}}
	r := NewAgentRunner(&domain.Agent{ID: "ordinary", Name: "ordinary"}, &config.AgentConfig{}, provider, nil)
	session, err := NewSessionManager().GetOrCreateNamed("ordinary", "slack:channel")
	require.NoError(t, err)
	e := testConnectionExecution()
	e.Scope.AgentID = "ordinary"
	ctx := WithSessionID(connections.WithExecution(context.Background(), e), session.ID)
	r.PromptWithOverrides(ctx, "question", RunOverrides{Bare: true, DeferAnswerPersistence: true}, func(StreamEvent) {})
	r.Wait()
	messages, err := store.ReadJSONL[domain.Message](store.SessionPath("ordinary", session.ID))
	require.NoError(t, err)
	images, answers := 0, 0
	for _, msg := range messages {
		if msg.Role == domain.MessageRoleAssistant {
			if msg.MediaURL != "" {
				images++
			}
			if msg.Content != "" {
				answers++
			}
		}
	}
	require.Equal(t, 1, images)
	require.Zero(t, answers)
}
