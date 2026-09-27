package agent

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/domain"
	"github.com/lsegal/aviary/internal/llm"
	"github.com/lsegal/aviary/internal/store"
)

func fakeSlackCheckpoint() SlackCheckpoint {
	return SlackCheckpoint{
		InstallationID: "bot-fake", WorkspaceID: "team-fake", ConfiguredID: "route-fake",
		ChannelID: "channel-fake", RootThreadTS: "123.456",
	}
}

func TestSlackCheckpointInitialWritePrecedesExecutionAndOmitsPayload(t *testing.T) {
	setTestDataDir(t)
	provider := newBlockingProvider()
	runner := NewAgentRunner(&domain.Agent{ID: "slack-private", Name: "slack-private", Model: "test/model"},
		&config.AgentConfig{Name: "slack-private"}, provider, nil)
	handle := NewSlackCheckpointHandle(fakeSlackCheckpoint())
	admission := runner.PromptMediaWithOverrides(context.Background(), "fake-secret-prompt", "fake-secret-media",
		RunOverrides{Checkpoint: handle, SuppressDelivery: true, DeferAnswerPersistence: true})
	require.Equal(t, AdmissionAccepted, admission.Status)
	select {
	case <-provider.ready:
	case <-time.After(time.Second):
		t.Fatal("provider did not start")
	}
	require.True(t, handle.Initialized(), "checkpoint must exist before provider execution")
	path := store.CheckpointPath("slack-private", admission.RunID)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	for _, forbidden := range []string{"fake-secret-prompt", "fake-secret-media", `"message"`, `"media_url"`, `"overrides"`, `"checkpoint"`} {
		require.NotContains(t, string(data), forbidden)
	}
	cp, err := store.ReadJSON[RunCheckpoint](path)
	require.NoError(t, err)
	require.Equal(t, fakeSlackCheckpoint().ChannelID, cp.Slack.ChannelID)
	require.Equal(t, SlackDispositionPending, cp.Slack.Disposition)
	require.Empty(t, cp.Message)
	require.Nil(t, cp.Overrides)
	runner.Stop()
	runner.Wait()
	require.FileExists(t, path, "pending Slack delivery must remain after runner stop")
}

func TestSlackCheckpointInitialFailureStopsBeforeProviderAndPublicProgress(t *testing.T) {
	setTestDataDir(t)
	const agentID = "slack-initial-failure"
	dir := store.CheckpointDir(agentID)
	require.NoError(t, os.MkdirAll(filepath.Dir(dir), 0o700))
	require.NoError(t, os.WriteFile(dir, []byte("block checkpoint directory"), 0o600))
	provider := &sequenceProvider{responses: [][]llm.Event{{{Type: llm.EventTypeDone}}}}
	runner := NewAgentRunner(&domain.Agent{ID: agentID, Name: agentID, Model: "test/model"},
		&config.AgentConfig{Name: agentID}, provider, nil)
	handle := NewSlackCheckpointHandle(fakeSlackCheckpoint())
	terminal := make(chan StreamEvent, 1)
	var publicEvents int
	admission := runner.PromptWithOverrides(context.Background(), "request", RunOverrides{Checkpoint: handle}, func(event StreamEvent) {
		if event.Type == StreamEventToolProgress {
			publicEvents++
		}
		if event.Type == StreamEventError {
			terminal <- event
		}
	})
	require.Equal(t, AdmissionAccepted, admission.Status)
	select {
	case event := <-terminal:
		require.Error(t, event.Err)
		require.False(t, handle.Initialized(), "server must use fixed initial-failure notice path")
	case <-time.After(time.Second):
		t.Fatal("initial storage failure was not reported")
	}
	runner.Wait()
	require.Zero(t, provider.callCount())
	require.Zero(t, publicEvents)
}

func TestSlackCheckpointLivesThroughTerminalCallbackThenRetires(t *testing.T) {
	setTestDataDir(t)
	const agentID = "slack-terminal"
	provider := &sequenceProvider{responses: [][]llm.Event{{{Type: llm.EventTypeDone}}}}
	runner := NewAgentRunner(&domain.Agent{ID: agentID, Name: agentID, Model: "test/model"},
		&config.AgentConfig{Name: agentID}, provider, nil)
	handle := NewSlackCheckpointHandle(fakeSlackCheckpoint())
	terminal := make(chan error, 1)
	admission := runner.PromptWithOverrides(context.Background(), "request", RunOverrides{Checkpoint: handle}, func(event StreamEvent) {
		if event.Type != StreamEventDone {
			return
		}
		if _, err := os.Stat(handle.path); err != nil {
			terminal <- err
			return
		}
		terminal <- handle.RecordTerminal(SlackDispositionHandled, false, "")
	})
	require.Equal(t, AdmissionAccepted, admission.Status)
	select {
	case err := <-terminal:
		require.NoError(t, err, "terminal callback must run inside checkpoint lifetime")
	case <-time.After(time.Second):
		t.Fatal("terminal callback did not complete")
	}
	runner.Wait()
	require.NoFileExists(t, store.CheckpointPath(agentID, admission.RunID))
}

func TestGenericRecoveryLeavesSlackClaimAndCheckpointForAuthenticatedOwner(t *testing.T) {
	setTestDataDir(t)
	path := store.CheckpointPath("slack-recovery", "run-1")
	require.NoError(t, store.WriteJSON(path, RunCheckpoint{
		AgentName: "slack-recovery", SessionID: "session", CreatedAt: time.Now().Add(-24 * time.Hour),
		Slack: ptrSlackCheckpoint(fakeSlackCheckpoint()),
	}))
	release, claimed := ClaimCheckpointRecovery(path, nil)
	require.True(t, claimed)
	woke := make(chan struct{}, 1)
	_, claimed = ClaimCheckpointRecovery(path, func() { woke <- struct{}{} })
	require.False(t, claimed)
	m := NewManager(nil)
	provider := &sequenceProvider{responses: [][]llm.Event{{{Type: llm.EventTypeDone}}}}
	runner := newTestRunner(m, "slack-recovery", "slack-recovery", provider)
	m.recoverCheckpoints(runner)
	release()
	select {
	case <-woke:
	case <-time.After(time.Second):
		t.Fatal("generic recovery displaced authenticated Slack recovery wake")
	}
	require.FileExists(t, path)
	require.Zero(t, provider.callCount())
}

func TestUserStopRetainsSlackTerminalAndCleanupCheckpoint(t *testing.T) {
	setTestDataDir(t)
	for _, session := range []string{"session", "other"} {
		path := store.CheckpointPath("slack-stop", session)
		require.NoError(t, store.WriteJSON(path, RunCheckpoint{
			SessionID: session, Slack: &SlackCheckpoint{
				InstallationID: "bot-fake", WorkspaceID: "team-fake", ChannelID: "channel-fake",
				RootThreadTS: "123.456", Disposition: SlackDispositionHandled, CleanupPending: true,
			},
		}))
	}
	ordinary := store.CheckpointPath("slack-stop", "ordinary")
	require.NoError(t, store.WriteJSON(ordinary, RunCheckpoint{SessionID: "session", Message: "ordinary request"}))
	require.NoError(t, RetireCheckpointsForUserStop("slack-stop", "session"))
	require.NoFileExists(t, ordinary)
	require.FileExists(t, store.CheckpointPath("slack-stop", "session"))
	require.FileExists(t, store.CheckpointPath("slack-stop", "other"))
	require.NoError(t, RetireCheckpointsForUserStop("slack-stop", ""))
	require.FileExists(t, store.CheckpointPath("slack-stop", "session"))
	require.FileExists(t, store.CheckpointPath("slack-stop", "other"))
}

func ptrSlackCheckpoint(cp SlackCheckpoint) *SlackCheckpoint { return &cp }

func TestSlackCheckpointRejectsIncompleteOriginalTarget(t *testing.T) {
	setTestDataDir(t)
	base := fakeSlackCheckpoint()
	for _, test := range []struct {
		name string
		edit func(*SlackCheckpoint, *RunCheckpoint)
	}{
		{"installation", func(m *SlackCheckpoint, _ *RunCheckpoint) { m.InstallationID = "" }},
		{"workspace", func(m *SlackCheckpoint, _ *RunCheckpoint) { m.WorkspaceID = "" }},
		{"channel", func(m *SlackCheckpoint, _ *RunCheckpoint) { m.ChannelID = "" }},
		{"thread", func(m *SlackCheckpoint, _ *RunCheckpoint) { m.RootThreadTS = "" }},
		{"agent", func(_ *SlackCheckpoint, cp *RunCheckpoint) { cp.AgentName = "" }},
		{"session", func(_ *SlackCheckpoint, cp *RunCheckpoint) { cp.SessionID = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			meta := base
			cp := RunCheckpoint{AgentName: "agent", SessionID: "session"}
			test.edit(&meta, &cp)
			handle := NewSlackCheckpointHandle(meta)
			path := store.CheckpointPath("agent", test.name)
			require.Error(t, handle.bind(path, cp))
			require.False(t, handle.Initialized())
			require.NoFileExists(t, path)
		})
	}
	// Channel IDs may be omitted in valid configuration; the empty configured
	// route key remains exact and must not be mistaken for a missing target.
	base.ConfiguredID = ""
	handle := NewSlackCheckpointHandle(base)
	require.NoError(t, handle.bind(store.CheckpointPath("agent", "default-route"),
		RunCheckpoint{AgentName: "agent", SessionID: "session"}))
}

func TestSlackCheckpointNoticeMarkerAndMonotonicTerminal(t *testing.T) {
	setTestDataDir(t)
	path := store.CheckpointPath("slack-handle", "run-1")
	handle := NewSlackCheckpointHandle(fakeSlackCheckpoint())
	require.NoError(t, handle.bind(path, RunCheckpoint{AgentName: "slack-handle", SessionID: "session", CreatedAt: time.Now()}))
	require.NoError(t, handle.RecordNoticeAttempt())
	cp, err := store.ReadJSON[RunCheckpoint](path)
	require.NoError(t, err)
	require.Equal(t, SlackDispositionUnconfirmed, cp.Slack.Disposition,
		"crash after marker must suppress a fresh standalone post")
	require.True(t, cp.Slack.NoticeAttempted)
	require.NoError(t, handle.RecordTerminal(SlackDispositionPending, false, ""))
	cp, err = store.ReadJSON[RunCheckpoint](path)
	require.NoError(t, err)
	require.Equal(t, SlackDispositionPending, cp.Slack.Disposition)
	require.False(t, cp.Slack.NoticeAttempted, "durable definite rejection permits bounded retry")
	require.NoError(t, handle.RecordTerminal(SlackDispositionHandled, true, "progress-1"))
	require.NoError(t, handle.RecordTerminal(SlackDispositionUnconfirmed, true, "progress-1"))
	cp, err = store.ReadJSON[RunCheckpoint](path)
	require.NoError(t, err)
	require.Equal(t, SlackDispositionHandled, cp.Slack.Disposition)
	require.True(t, cp.Slack.CleanupPending)
	require.NoError(t, handle.retireIfHandled())
	require.FileExists(t, path, "accepted answer with pending progress cleanup remains durable")
	require.NoError(t, handle.RecordTerminal(SlackDispositionHandled, false, "progress-1"))
	require.NoError(t, handle.retireIfHandled())
	require.NoFileExists(t, path)
}

func TestSlackCheckpointConcurrentUpdatesPreserveBothFields(t *testing.T) {
	setTestDataDir(t)
	path := store.CheckpointPath("slack-concurrent", "run-1")
	handle := NewSlackCheckpointHandle(fakeSlackCheckpoint())
	require.NoError(t, handle.bind(path, RunCheckpoint{AgentName: "slack-concurrent", SessionID: "session", CreatedAt: time.Now()}))
	var group sync.WaitGroup
	errors := make(chan error, 2)
	group.Add(2)
	go func() { defer group.Done(); errors <- handle.RecordProgressTimestamp("progress-1") }()
	go func() { defer group.Done(); errors <- handle.RecordNoticeAttempt() }()
	group.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	cp, err := store.ReadJSON[RunCheckpoint](path)
	require.NoError(t, err)
	require.Equal(t, "progress-1", cp.Slack.ProgressTS)
	require.True(t, cp.Slack.CleanupPending)
	require.True(t, cp.Slack.NoticeAttempted)
	require.Equal(t, SlackDispositionUnconfirmed, cp.Slack.Disposition)
}

func TestSlackCheckpointFailedUpdateKeepsLastDurableState(t *testing.T) {
	setTestDataDir(t)
	path := store.CheckpointPath("slack-update-failure", "run-1")
	handle := NewSlackCheckpointHandle(fakeSlackCheckpoint())
	require.NoError(t, handle.bind(path, RunCheckpoint{AgentName: "slack-update-failure", SessionID: "session", CreatedAt: time.Now()}))
	backup := path + ".saved"
	require.NoError(t, os.Rename(path, backup))
	require.NoError(t, os.Mkdir(path, 0o700)) // Atomic rename onto a directory must fail.
	require.Error(t, handle.RecordProgressTimestamp("progress-1"))
	require.Empty(t, handle.checkpoint.Slack.ProgressTS, "failed write cannot advance in-memory state")
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Rename(backup, path))
	cp, err := store.ReadJSON[RunCheckpoint](path)
	require.NoError(t, err)
	require.Empty(t, cp.Slack.ProgressTS)
	require.NoError(t, handle.RecordTerminal(SlackDispositionHandled, true, "progress-1"),
		"terminal write must recover the accepted progress timestamp after its earlier write failed")
	cp, err = store.ReadJSON[RunCheckpoint](path)
	require.NoError(t, err)
	require.Equal(t, "progress-1", cp.Slack.ProgressTS)
	require.Equal(t, SlackDispositionHandled, cp.Slack.Disposition)
	require.True(t, cp.Slack.CleanupPending)
	require.NoError(t, handle.retireIfHandled())
	require.FileExists(t, path, "known progress remains for recovery cleanup")
	require.Error(t, handle.RecordTerminal(SlackDispositionHandled, true, "other-progress"),
		"a different timestamp cannot replace the original accepted message")
}
