package agent

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/domain"
	"github.com/lsegal/aviary/internal/llm"
	"github.com/lsegal/aviary/internal/store"
)

func TestRunAdmissionRejectedBeforeOwnership(t *testing.T) {
	setTestDataDir(t)
	r := NewAgentRunner(&domain.Agent{ID: "admission", Name: "admission"}, &config.AgentConfig{Name: "admission"}, nil, nil)
	r.Stop()
	called := false
	got := r.Prompt(context.Background(), "request", func(StreamEvent) { called = true })
	require.Equal(t, AdmissionRejectedStopping, got.Status)
	require.Empty(t, got.RunID)
	require.False(t, called)
	require.Zero(t, checkpointCount("admission"))
}

func TestRunStopCauseAndCheckpointOwnership(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause StopCause
		stop  func(*testing.T, *AgentRunner)
	}{
		{"user", StopCauseUser, func(t *testing.T, _ *AgentRunner) { require.Equal(t, 1, StopSession("stopped", "session")) }},
		{"runner", StopCauseRunner, func(_ *testing.T, r *AgentRunner) { r.Stop() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setTestDataDir(t)
			provider := newBlockingProvider()
			r := NewAgentRunner(&domain.Agent{ID: "stopped", Name: "stopped"}, &config.AgentConfig{Name: "stopped"}, provider, nil)
			terminal := make(chan StreamEvent, 1)
			release := make(chan struct{})
			admission := r.Prompt(WithSessionID(context.Background(), "session"), "request", func(e StreamEvent) {
				if e.Type == StreamEventStop {
					terminal <- e
					<-release
				}
			})
			require.Equal(t, AdmissionAccepted, admission.Status)
			select {
			case <-provider.ready:
			case <-time.After(time.Second):
				t.Fatal("run did not start")
			}
			path := store.CheckpointPath("stopped", admission.RunID)
			_, err := os.Stat(path)
			require.NoError(t, err)
			tc.stop(t, r)
			select {
			case event := <-terminal:
				require.Equal(t, tc.cause, event.StopCause)
			case <-time.After(time.Second):
				t.Fatal("stop event did not arrive")
			}
			require.True(t, checkpointIsLive(path))
			_, err = os.Stat(path)
			require.NoError(t, err, "checkpoint retired before terminal callback returned")
			close(release)
			r.Wait()
			require.False(t, checkpointIsLive(path))
			_, err = os.Stat(path)
			if tc.cause == StopCauseRunner {
				require.NoError(t, err, "replayable checkpoint must survive runner stop")
			} else {
				require.True(t, os.IsNotExist(err), "user stop should retire replayable checkpoint")
			}
		})
	}
}

func TestCompletedTerminalCallbackCannotBecomeRunnerStop(t *testing.T) {
	for _, tc := range []struct {
		name   string
		events []llm.Event
		kind   StreamEventType
	}{
		{"done", []llm.Event{{Type: llm.EventTypeText, Text: "answer"}, {Type: llm.EventTypeDone}}, StreamEventDone},
		{"error", []llm.Event{{Type: llm.EventTypeError, Error: errors.New("fake provider failure")}}, StreamEventError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setTestDataDir(t)
			provider := &sequenceProvider{responses: [][]llm.Event{tc.events}}
			r := NewAgentRunner(&domain.Agent{ID: "terminal", Name: "terminal", Model: "test/model"},
				&config.AgentConfig{Name: "terminal"}, provider, nil)
			terminal := make(chan struct{}, 1)
			release := make(chan struct{})
			admission := r.Prompt(WithSessionID(context.Background(), "session"), "request", func(e StreamEvent) {
				if e.Type == tc.kind {
					terminal <- struct{}{}
					<-release // hold the terminal callback across runner shutdown
				}
			})
			require.Equal(t, AdmissionAccepted, admission.Status)
			select {
			case <-terminal:
			case <-time.After(time.Second):
				t.Fatal("terminal callback did not begin")
			}
			path := store.CheckpointPath("terminal", admission.RunID)
			require.FileExists(t, path)
			r.Stop()
			require.True(t, checkpointIsLive(path), "terminal callback still owns checkpoint")
			require.FileExists(t, path)
			close(release)
			r.Wait()
			require.NoFileExists(t, path, "completed terminal must not be replayed after stop")
		})
	}
}

func TestRecoveryWaitsForOldManagerLiveRun(t *testing.T) {
	setTestDataDir(t)
	provider := newBlockingProvider()
	old := NewAgentRunner(&domain.Agent{ID: "shared", Name: "shared"}, &config.AgentConfig{Name: "shared"}, provider, nil)
	terminal := make(chan struct{})
	release := make(chan struct{})
	admission := old.Prompt(WithSessionID(context.Background(), "session"), "request", func(e StreamEvent) {
		if e.Type == StreamEventStop {
			close(terminal)
			<-release
		}
	})
	select {
	case <-provider.ready:
	case <-time.After(time.Second):
		t.Fatal("old run did not start")
	}
	old.Stop()
	select {
	case <-terminal:
	case <-time.After(time.Second):
		t.Fatal("old terminal callback did not start")
	}
	path := store.CheckpointPath("shared", admission.RunID)
	newProvider := &sequenceProvider{}
	manager := NewManager(nil)
	replacement := newTestRunner(manager, "shared", "shared", newProvider)
	manager.recoverCheckpoints(replacement)
	require.Equal(t, 0, newProvider.callCount())
	_, err := os.Stat(path)
	require.NoError(t, err)
	close(release)
	old.Wait()
	deadline := time.After(2 * time.Second)
	for newProvider.callCount() != 1 {
		select {
		case <-deadline:
			t.Fatal("recovery did not resume after old owner exited")
		case <-time.After(time.Millisecond):
		}
	}
	replacement.Wait()
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err), "recovered run should retire checkpoint")
}

func TestManagerDrainDeadlineDoesNotReleaseLiveRun(t *testing.T) {
	setTestDataDir(t)
	provider := newBlockingProvider()
	manager := NewManager(nil)
	runner := newTestRunner(manager, "draining", "draining", provider)
	manager.owned[runner] = struct{}{}
	terminal := make(chan struct{})
	release := make(chan struct{})
	admission := runner.Prompt(WithSessionID(context.Background(), "session"), "request", func(e StreamEvent) {
		if e.Type == StreamEventStop {
			close(terminal)
			<-release
		}
	})
	require.Equal(t, AdmissionAccepted, admission.Status)
	select {
	case <-provider.ready:
	case <-time.After(time.Second):
		t.Fatal("run did not start")
	}
	runner.Stop()
	select {
	case <-terminal:
	case <-time.After(time.Second):
		t.Fatal("terminal callback did not start")
	}
	path := store.CheckpointPath("draining", admission.RunID)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, manager.Drain(ctx), context.DeadlineExceeded)
	require.True(t, checkpointIsLive(path))
	close(release)
	require.NoError(t, manager.Drain(context.Background()))
	require.False(t, checkpointIsLive(path))
}

func TestCheckpointRecoveryClaimWakesEveryWaitingManager(t *testing.T) {
	path := store.CheckpointPath("claim", "run-1")
	release, claimed := ClaimCheckpointRecovery(path, nil)
	require.True(t, claimed)
	firstPending := make(chan struct{}, 1)
	_, claimed = ClaimCheckpointRecovery(path, func() { firstPending <- struct{}{} })
	require.False(t, claimed)
	resumed := make(chan struct{}, 1)
	_, claimed = ClaimCheckpointRecovery(path, func() { resumed <- struct{}{} })
	require.False(t, claimed)
	release()
	select {
	case <-resumed:
	case <-time.After(time.Second):
		t.Fatal("newer manager was not resumed")
	}
	select {
	case <-firstPending:
	case <-time.After(time.Second):
		t.Fatal("older waiter was lost")
	}
	nextRelease, claimed := ClaimCheckpointRecovery(path, nil)
	require.True(t, claimed)
	nextRelease()
}

func TestStoppedManagerWakeCannotReplaceActiveRecovery(t *testing.T) {
	setTestDataDir(t)
	const agentID = "wake-owner"
	path := store.CheckpointPath(agentID, "run-1")
	require.NoError(t, store.WriteJSON(path, RunCheckpoint{AgentName: agentID, SessionID: "session", Message: "request", CreatedAt: time.Now()}))
	old := NewManager(nil)
	oldRunner := newTestRunner(old, agentID, agentID, &sequenceProvider{})
	old.Stop()
	newManager := NewManager(nil)
	newProvider := &sequenceProvider{}
	newRunner := newTestRunner(newManager, agentID, agentID, newProvider)
	release, claimed := ClaimCheckpointRecovery(path, nil)
	require.True(t, claimed)
	_, claimed = ClaimCheckpointRecovery(path, func() { newManager.wakeCheckpointRecovery(newRunner, "run-1.json", path, time.Hour) })
	require.False(t, claimed)
	_, claimed = ClaimCheckpointRecovery(path, func() { old.wakeCheckpointRecovery(oldRunner, "run-1.json", path, time.Hour) })
	require.False(t, claimed)
	release()
	deadline := time.After(2 * time.Second)
	for newProvider.callCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("active manager recovery was lost to stale callback")
		case <-time.After(time.Millisecond):
		}
	}
	newRunner.Wait()
	require.NoError(t, newManager.Drain(context.Background()))
}

func TestCurrentManagerRetriesAfterStaleRecoveryRejects(t *testing.T) {
	setTestDataDir(t)
	const agentID = "stale-recovery-winner"
	path := store.CheckpointPath(agentID, "run-1")
	require.NoError(t, store.WriteJSON(path, RunCheckpoint{
		AgentName: agentID, SessionID: "session", Message: "request", CreatedAt: time.Now(),
	}))
	old := NewManager(nil)
	oldRunner := newTestRunner(old, agentID, agentID, &sequenceProvider{})
	current := NewManager(nil)
	provider := &sequenceProvider{}
	currentRunner := newTestRunner(current, agentID, agentID, provider)

	oldRelease, claimed := ClaimCheckpointRecovery(path, nil)
	require.True(t, claimed)
	// The current manager loses the first wake to a stale recovery claim.
	current.wakeCheckpointRecovery(currentRunner, "run-1.json", path, time.Hour)
	oldRunner.Stop()
	old.recoverCheckpoint(oldRunner, "run-1.json", path, time.Hour)
	require.FileExists(t, path, "rejected stale handoff must preserve the checkpoint")
	oldRelease()

	deadline := time.After(2 * time.Second)
	for provider.callCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("current manager was not woken after stale handoff rejected")
		case <-time.After(time.Millisecond):
		}
	}
	currentRunner.Wait()
	require.NoError(t, current.Drain(context.Background()))
}

func TestConcurrentRecoveryWakesQuiesceOnRetainedCheckpoint(t *testing.T) {
	setTestDataDir(t)
	const agentID = "retained-wake"
	path := store.CheckpointPath(agentID, "run-1")
	require.NoError(t, store.WriteJSON(path, RunCheckpoint{
		AgentName: agentID, SessionID: "session", Message: "request", CreatedAt: time.Now(),
		RetryCount: 1, LastRecoveredAt: time.Now(),
	}))
	m := NewManager(nil)
	runner := newTestRunner(m, agentID, agentID, &sequenceProvider{})
	release, claimed := ClaimCheckpointRecovery(path, nil)
	require.True(t, claimed)
	woke := make(chan struct{}, 2)
	for range 2 {
		_, claimed = ClaimCheckpointRecovery(path, func() {
			m.wakeCheckpointRecovery(runner, "run-1.json", path, time.Hour)
			woke <- struct{}{}
		})
		require.False(t, claimed)
	}
	release()
	for range 2 {
		select {
		case <-woke:
		case <-time.After(time.Second):
			t.Fatal("pending recovery wake did not finish")
		}
	}
	liveCheckpoints.Lock()
	pending := len(liveCheckpoints.pending[checkpointKey(path)])
	liveCheckpoints.Unlock()
	require.Zero(t, pending, "retained checkpoint must not queue recursive wakes")
	require.FileExists(t, path)
}

func TestManagerCannotAdmitNewRunnerAfterStop(t *testing.T) {
	setTestDataDir(t)
	m := NewManager(nil)
	m.Reconcile(&config.Config{Agents: []config.AgentConfig{{Name: "old", Model: "test/x"}}})
	m.recoveries.Wait()
	m.Stop()
	m.Reconcile(&config.Config{Agents: []config.AgentConfig{{Name: "new", Model: "test/x"}}})
	_, exists := m.Get("new")
	require.False(t, exists)
	old, exists := m.Get("old")
	require.True(t, exists)
	require.Equal(t, AdmissionRejectedStopping, old.Prompt(context.Background(), "request").Status)
}

func TestCallerCancellationKeepsUserCauseAfterRunnerStops(t *testing.T) {
	setTestDataDir(t)
	provider := newBlockingProvider()
	r := NewAgentRunner(&domain.Agent{ID: "caller", Name: "caller"}, &config.AgentConfig{Name: "caller"}, provider, nil)
	ctx, cancel := context.WithCancel(WithSessionID(context.Background(), "session"))
	terminal := make(chan StreamEvent, 1)
	release := make(chan struct{})
	admission := r.Prompt(ctx, "request", func(e StreamEvent) {
		if e.Type == StreamEventStop {
			terminal <- e
			<-release
		}
	})
	select {
	case <-provider.ready:
	case <-time.After(time.Second):
		t.Fatal("run did not start")
	}
	cancel()
	select {
	case event := <-terminal:
		require.Equal(t, StopCauseUser, event.StopCause)
	case <-time.After(time.Second):
		t.Fatal("caller stop event did not arrive")
	}
	r.Stop()
	close(release)
	r.Wait()
	require.NoFileExists(t, store.CheckpointPath("caller", admission.RunID),
		"runner stop must not change an earlier user-canceled checkpoint policy")
}

func TestUserStopRetiresOnlySelectedStaleCheckpoints(t *testing.T) {
	setTestDataDir(t)
	first := store.CheckpointPath("stale", "first")
	second := store.CheckpointPath("stale", "second")
	require.NoError(t, store.WriteJSON(first, RunCheckpoint{SessionID: "one", Message: "request one"}))
	require.NoError(t, store.WriteJSON(second, RunCheckpoint{SessionID: "two", Message: "request two"}))
	require.NoError(t, RetireCheckpointsForUserStop("stale", "one"))
	require.NoFileExists(t, first)
	require.FileExists(t, second)
}

func TestScopedUserStopIgnoresVanishedCheckpointAndRetainsCorruption(t *testing.T) {
	setTestDataDir(t)
	dir := store.CheckpointDir("stale")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	missing := store.CheckpointPath("stale", "missing")
	require.NoError(t, os.Symlink("not-present.json", missing))
	require.NoError(t, RetireCheckpointsForUserStop("stale", "session"),
		"a checkpoint removed after listing must not make an already requested stop fail")
	bad := store.CheckpointPath("stale", "corrupt")
	require.NoError(t, os.WriteFile(bad, []byte("not json"), 0o600))
	require.NoError(t, RetireCheckpointsForUserStop("stale", "session"))
	require.FileExists(t, bad, "corrupt checkpoint remains for inspection")
}

func TestAgentWideUserStopRetiresReadableRunsAndRetainsUnclassifiedRecords(t *testing.T) {
	setTestDataDir(t)
	valid := store.CheckpointPath("mixed-stop", "valid")
	slack := store.CheckpointPath("mixed-stop", "slack")
	corrupt := store.CheckpointPath("mixed-stop", "corrupt")
	require.NoError(t, store.WriteJSON(valid, RunCheckpoint{SessionID: "session", Message: "request"}))
	require.NoError(t, store.WriteJSON(slack, RunCheckpoint{SessionID: "session", Slack: &SlackCheckpoint{
		InstallationID: "bot-fake", WorkspaceID: "team-fake", ChannelID: "C123",
		RootThreadTS: "123.456", Disposition: SlackDispositionHandled, CleanupPending: true,
	}}))
	require.NoError(t, os.WriteFile(corrupt, []byte("invalid checkpoint json"), 0o600))
	require.NoError(t, RetireCheckpointsForUserStop("mixed-stop", ""))
	require.NoFileExists(t, valid)
	require.FileExists(t, slack, "Slack terminal cleanup remains owned by authenticated recovery")
	require.FileExists(t, corrupt, "unclassified record remains for inspection")
}

func TestUserStopSuppressesClaimedRecoveryUntilRelease(t *testing.T) {
	setTestDataDir(t)
	path := store.CheckpointPath("claimed", "run-1")
	require.NoError(t, store.WriteJSON(path, RunCheckpoint{SessionID: "session", Message: "request"}))
	release, claimed := ClaimCheckpointRecovery(path, nil)
	require.True(t, claimed)
	require.NoError(t, RetireCheckpointsForUserStop("claimed", "session"))
	require.FileExists(t, path, "current recovery holds ownership until release")
	_, claimed = ClaimCheckpointRecovery(path, nil)
	require.False(t, claimed, "user stop must suppress new recovery claims")
	release()
	require.NoFileExists(t, path)
}

func TestStopByUserAllowsLaterPrompt(t *testing.T) {
	setTestDataDir(t)
	r := NewAgentRunner(&domain.Agent{ID: "later", Name: "later"}, &config.AgentConfig{Name: "later"}, nil, nil)
	r.StopByUser()
	admission := r.Prompt(context.Background(), "new request")
	require.Equal(t, AdmissionAccepted, admission.Status)
	r.Wait()
}
