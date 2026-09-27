package server

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/agent"
	"github.com/lsegal/aviary/internal/channels"
	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/domain"
	"github.com/lsegal/aviary/internal/store"
)

func liveSlackMessage() channels.IncomingMessage {
	return channels.IncomingMessage{
		Type: "slack", InstallationID: "bot-fake", WorkspaceID: "team-fake",
		Channel: "C123", ThreadTS: "1700000000.000001", From: "U123", Text: "fake-private-request",
	}
}

func liveSlackCheckpoint(t *testing.T) (string, agent.RunCheckpoint) {
	t.Helper()
	entries, err := os.ReadDir(store.CheckpointDir("bot"))
	require.NoError(t, err)
	var paths []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			paths = append(paths, filepath.Join(store.CheckpointDir("bot"), entry.Name()))
		}
	}
	require.Len(t, paths, 1)
	cp, err := store.ReadJSON[agent.RunCheckpoint](paths[0])
	require.NoError(t, err)
	return paths[0], cp
}

func liveSlackConfig(model string, progress bool) *config.Config {
	return &config.Config{Models: config.ModelsConfig{Providers: map[string]config.ProviderConfig{}},
		Agents: []config.AgentConfig{{Name: "bot", Model: model, Channels: []config.ChannelConfig{{
			Type: "slack", ID: "alerts", ToolProgress: &progress,
		}}}}}
}

func TestLiveSlackProgressOffRunWritesTrustedCheckpointBeforeExecution(t *testing.T) {
	setupServerDataDir(t)
	resetSlogForTest()
	entered, release := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		writeDeliveryText(w, "safe answer")
	}))
	t.Cleanup(model.Close)
	cfg := liveSlackConfig("vllm/test", false)
	cfg.Models.Providers["vllm"] = config.ProviderConfig{BaseURI: model.URL}
	srv := New(cfg, "fake-token")
	ch := &deliveryTestChannel{}
	srv.handleIncomingChannelMessage(context.Background(), "bot", "slack", "alerts", ch, liveSlackMessage())
	runner, ok := srv.agents.Get("bot")
	require.True(t, ok)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("model execution did not start")
	}
	path, cp := liveSlackCheckpoint(t)
	require.Equal(t, agent.SlackDispositionPending, cp.Slack.Disposition)
	require.Equal(t, "bot-fake", cp.Slack.InstallationID)
	require.Equal(t, "team-fake", cp.Slack.WorkspaceID)
	require.Equal(t, "alerts", cp.Slack.ConfiguredID)
	require.Equal(t, "C123", cp.Slack.ChannelID)
	require.Equal(t, "1700000000.000001", cp.Slack.RootThreadTS)
	require.Empty(t, cp.Message)
	require.Empty(t, cp.MediaURL)
	require.Nil(t, cp.Overrides)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "fake-private-request")
	require.NotContains(t, string(raw), `"overrides"`)
	close(release)
	runner.Wait()
	require.Equal(t, []string{"safe answer"}, ch.posted())
	require.NoFileExists(t, path)
}

func TestLiveSlackInitialCheckpointFailureStopsExecutionAndPostsFixedNotice(t *testing.T) {
	setupServerDataDir(t)
	resetSlogForTest()
	dir := store.CheckpointDir("bot")
	require.NoError(t, os.MkdirAll(filepath.Dir(dir), 0o700))
	require.NoError(t, os.WriteFile(dir, []byte("blocking file"), 0o600))
	var calls atomic.Int32
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeDeliveryText(w, "unexpected")
	}))
	t.Cleanup(model.Close)
	cfg := liveSlackConfig("vllm/test", true)
	cfg.Models.Providers["vllm"] = config.ProviderConfig{BaseURI: model.URL}
	srv := New(cfg, "fake-token")
	ch := &deliveryTestChannel{}
	srv.handleIncomingChannelMessage(context.Background(), "bot", "slack", "alerts", ch, liveSlackMessage())
	runner, ok := srv.agents.Get("bot")
	require.True(t, ok)
	runner.Wait()
	require.Zero(t, calls.Load(), "no untracked model or tool execution is allowed")
	require.Equal(t, []string{"Unable to complete this request."}, ch.posted())
}

type blockingLiveAnswerChannel struct {
	deliveryTestChannel
	entered chan struct{}
	release chan struct{}
}

func (c *blockingLiveAnswerChannel) PostThreadTextContext(_ context.Context, channel, thread, text string) (string, error) {
	close(c.entered)
	<-c.release
	return c.deliveryTestChannel.PostThreadTextContext(context.Background(), channel, thread, text)
}

func TestLiveSlackSelectedAnswerSurvivesRunnerStopDuringDelivery(t *testing.T) {
	setupServerDataDir(t)
	resetSlogForTest()
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeDeliveryText(w, "selected answer")
	}))
	t.Cleanup(model.Close)
	cfg := liveSlackConfig("vllm/test", false)
	cfg.Models.Providers["vllm"] = config.ProviderConfig{BaseURI: model.URL}
	srv := New(cfg, "fake-token")
	ch := &blockingLiveAnswerChannel{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-ch.release:
		default:
			close(ch.release)
		}
	})
	srv.handleIncomingChannelMessage(context.Background(), "bot", "slack", "alerts", ch, liveSlackMessage())
	runner, ok := srv.agents.Get("bot")
	require.True(t, ok)
	select {
	case <-ch.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("selected answer did not reach Slack delivery")
	}
	path, cp := liveSlackCheckpoint(t)
	require.Equal(t, agent.SlackDispositionPending, cp.Slack.Disposition,
		"callback must hold the checkpoint until accepted delivery is recorded")
	runner.Stop()
	close(ch.release)
	runner.Wait()
	require.Equal(t, []string{"selected answer"}, ch.posted())
	require.NoFileExists(t, path)
}

func TestLiveSlackExpiredDrainDefersSecondServerRecoveryUntilOldCallbackFinishes(t *testing.T) {
	setupServerDataDir(t)
	resetSlogForTest()
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeDeliveryText(w, "selected answer")
	}))
	t.Cleanup(model.Close)
	cfg := liveSlackConfig("vllm/test", false)
	cfg.Models.Providers["vllm"] = config.ProviderConfig{BaseURI: model.URL}
	old := New(cfg, "fake-token")
	oldChannel := &blockingLiveAnswerChannel{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-oldChannel.release:
		default:
			close(oldChannel.release)
		}
	})
	old.handleIncomingChannelMessage(context.Background(), "bot", "slack", "alerts", oldChannel, liveSlackMessage())
	oldRunner, ok := old.agents.Get("bot")
	require.True(t, ok)
	select {
	case <-oldChannel.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("old terminal callback did not reach its first answer post")
	}
	path, _ := liveSlackCheckpoint(t)
	old.agents.Stop()
	drainCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, old.agents.Drain(drainCtx), context.DeadlineExceeded)
	require.FileExists(t, path, "expired drain cannot retire live terminal work")

	// A replacement Server has a matching authenticated outgoing route, but
	// process-wide live ownership keeps its recovery claim pending until the
	// original synchronous terminal callback and checkpoint teardown finish.
	replacement := New(cfg, "fake-token")
	sender := &recoverySender{}
	wakeSeen := make(chan struct{}, 1)
	var oldReleased atomic.Bool
	route := channels.SlackAuthenticatedRoute{
		AgentName: "bot", ConfiguredID: "alerts", InstallationID: "bot-fake", WorkspaceID: "team-fake",
		Channel: sender, StopBeforeDispatch: make(chan struct{}),
		CurrentCheck: func() bool {
			if oldReleased.Load() {
				select {
				case wakeSeen <- struct{}{}:
				default:
				}
			}
			return true
		},
	}
	replacement.recoverSlackCheckpoint(context.Background(), route, path)
	posts, edits, deletes := sender.snapshot()
	require.Empty(t, posts)
	require.Empty(t, edits)
	require.Empty(t, deletes)
	require.FileExists(t, path)

	oldReleased.Store(true)
	close(oldChannel.release)
	oldRunner.Wait()
	require.NoError(t, old.agents.Drain(context.Background()))
	select {
	case <-wakeSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("pending replacement recovery did not wake after old ownership ended")
	}
	require.Equal(t, []string{"selected answer"}, oldChannel.posted())
	require.NoFileExists(t, path, "old accepted delivery retires its own checkpoint")
	require.Never(t, func() bool {
		posts, edits, deletes := sender.snapshot()
		return len(posts)+len(edits)+len(deletes) != 0
	}, 100*time.Millisecond, time.Millisecond, "replacement must not add a duplicate notice or cleanup")
	require.NoError(t, replacement.agents.Drain(context.Background()))
}

func TestLiveSlackSelectedAnswerSurvivesRunnerStopDuringSummary(t *testing.T) {
	setupServerDataDir(t)
	resetSlogForTest()
	answer := strings.Repeat("A", 450)
	summaryEntered, releaseSummary := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseSummary:
		default:
			close(releaseSummary)
		}
	})
	var requests atomic.Int32
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			writeDeliveryText(w, answer)
			return
		}
		close(summaryEntered)
		<-releaseSummary
		writeDeliveryText(w, "safe summary")
	}))
	t.Cleanup(model.Close)
	cfg := liveSlackConfig("vllm/test", false)
	cfg.Models.Providers["vllm"] = config.ProviderConfig{BaseURI: model.URL}
	srv := New(cfg, "fake-token")
	ch := &deliveryTestChannel{}
	srv.handleIncomingChannelMessage(context.Background(), "bot", "slack", "alerts", ch, liveSlackMessage())
	runner, ok := srv.agents.Get("bot")
	require.True(t, ok)
	select {
	case <-summaryEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("selected answer did not reach optional summary")
	}
	path, _ := liveSlackCheckpoint(t)
	require.Empty(t, ch.posted(), "summary has not yet dispatched the file share")
	runner.Stop()
	close(releaseSummary)
	runner.Wait()
	require.Equal(t, int32(2), requests.Load())
	require.Equal(t, []string{answer}, ch.posted(), "later stop must not replace selected answer")
	require.NoFileExists(t, path)
}

type blockingFileLiveChannel struct {
	deliveryTestChannel
	entered chan struct{}
	release chan struct{}
}

func (c *blockingFileLiveChannel) ShareThreadMarkdownFileContext(_ context.Context, _, _, _, answer string) error {
	close(c.entered)
	<-c.release
	_, err := c.SendThreadMessageAndGetID("C123", "1700000000.000001", answer)
	return err
}

func TestLiveSlackSelectedAnswerSurvivesRunnerStopDuringFileShare(t *testing.T) {
	setupServerDataDir(t)
	resetSlogForTest()
	answer := strings.Repeat("B", 450)
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeDeliveryText(w, answer)
	}))
	t.Cleanup(model.Close)
	cfg := liveSlackConfig("vllm/test", false)
	cfg.Models.Providers["vllm"] = config.ProviderConfig{BaseURI: model.URL}
	srv := New(cfg, "fake-token")
	ch := &blockingFileLiveChannel{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-ch.release:
		default:
			close(ch.release)
		}
	})
	srv.handleIncomingChannelMessage(context.Background(), "bot", "slack", "alerts", ch, liveSlackMessage())
	runner, ok := srv.agents.Get("bot")
	require.True(t, ok)
	select {
	case <-ch.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("selected answer did not reach file share")
	}
	path, _ := liveSlackCheckpoint(t)
	runner.Stop()
	close(ch.release)
	runner.Wait()
	require.Equal(t, []string{answer}, ch.posted())
	require.NoFileExists(t, path)
}

type multipartFallbackLiveChannel struct {
	deliveryTestChannel
	secondEntered chan struct{}
	releaseSecond chan struct{}
	posts         atomic.Int32
	shares        atomic.Int32
}

func (c *multipartFallbackLiveChannel) ShareThreadMarkdownFileContext(context.Context, string, string, string, string) error {
	c.shares.Add(1)
	return &channels.SlackFileShareError{Cause: errors.New("fake allocation rejection"), SafeFallback: true}
}

func (c *multipartFallbackLiveChannel) PostThreadTextContext(_ context.Context, channel, thread, text string) (string, error) {
	if c.posts.Add(1) == 2 {
		close(c.secondEntered)
		<-c.releaseSecond
	}
	return c.deliveryTestChannel.PostThreadTextContext(context.Background(), channel, thread, text)
}

func TestLiveSlackSelectedAnswerPreservesAcceptedPartAcrossRunnerStop(t *testing.T) {
	setupServerDataDir(t)
	resetSlogForTest()
	answer := strings.Repeat("C", 4500)
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeDeliveryText(w, answer)
	}))
	t.Cleanup(model.Close)
	cfg := liveSlackConfig("vllm/test", false)
	cfg.Models.Providers["vllm"] = config.ProviderConfig{BaseURI: model.URL}
	srv := New(cfg, "fake-token")
	ch := &multipartFallbackLiveChannel{secondEntered: make(chan struct{}), releaseSecond: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-ch.releaseSecond:
		default:
			close(ch.releaseSecond)
		}
	})
	srv.handleIncomingChannelMessage(context.Background(), "bot", "slack", "alerts", ch, liveSlackMessage())
	runner, ok := srv.agents.Get("bot")
	require.True(t, ok)
	select {
	case <-ch.secondEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("multipart fallback did not reach its second post")
	}
	path, _ := liveSlackCheckpoint(t)
	require.Equal(t, []string{answer[:3900]}, ch.posted(), "first accepted part stays visible")
	runner.Stop()
	close(ch.releaseSecond)
	runner.Wait()
	require.Equal(t, int32(1), ch.shares.Load())
	require.Equal(t, []string{answer[:3900], answer[3900:]}, ch.posted(),
		"selected parts keep order and are never resent or replaced by interruption")
	require.NoFileExists(t, path)
}

func TestLivePrivateSlackHistoryAndFailedDeliveryCheckpoint(t *testing.T) {
	for _, failed := range []bool{false, true} {
		name := "accepted"
		if failed {
			name = "rejected"
		}
		t.Run(name, func(t *testing.T) {
			setupServerDataDir(t)
			resetSlogForTest()
			model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeDeliveryText(w, "private safe answer")
			}))
			t.Cleanup(model.Close)
			cfg := liveSlackConfig("vllm/test", false)
			cfg.Models.Providers["vllm"] = config.ProviderConfig{BaseURI: model.URL}
			srv := New(cfg, "fake-token")
			selectPrivateSlackDeliveryTarget(t, srv, privateSlackDeliveryScope())
			ch := &deliveryTestChannel{fail: failed}
			srv.handleIncomingChannelMessage(context.Background(), "bot", "slack", "alerts", ch, liveSlackMessage())
			runner, ok := srv.agents.Get("bot")
			require.True(t, ok)
			runner.Wait()
			sessions, err := agent.NewSessionManager().List("bot")
			require.NoError(t, err)
			var answers int
			for _, session := range sessions {
				if session.Name != "slack:C123" {
					continue
				}
				messages, err := store.ReadJSONL[domain.Message](store.SessionPath("bot", session.ID))
				require.NoError(t, err)
				for _, message := range messages {
					if message.Role == domain.MessageRoleAssistant {
						answers++
					}
				}
			}
			if failed {
				require.Zero(t, answers, "private answer must enter history only after confirmed delivery")
				_, cp := liveSlackCheckpoint(t)
				require.NotEqual(t, agent.SlackDispositionHandled, cp.Slack.Disposition)
			} else {
				require.Equal(t, 1, answers)
				require.Empty(t, checkpointFilesForLiveTest(t))
			}
		})
	}
}

func checkpointFilesForLiveTest(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(store.CheckpointDir("bot"))
	if os.IsNotExist(err) {
		return nil
	}
	require.NoError(t, err)
	var paths []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			paths = append(paths, entry.Name())
		}
	}
	return paths
}

type cleanupFailLiveChannel struct{ deliveryTestChannel }

func (*cleanupFailLiveChannel) DeleteThreadMessageContext(context.Context, string, string) error {
	return os.ErrPermission
}

type checkpointFailureSignalWriter struct {
	once   sync.Once
	failed chan struct{}
}

func (w *checkpointFailureSignalWriter) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("Slack progress checkpoint update failed")) {
		w.once.Do(func() { close(w.failed) })
	}
	return len(p), nil
}

type gatedLiveToolClient struct {
	entered chan struct{}
	release chan struct{}
}

func (*gatedLiveToolClient) ListTools(context.Context) ([]agent.ToolInfo, error) {
	return []agent.ToolInfo{{Name: "synthetic_tool", InputSchema: map[string]any{"type": "object"}}}, nil
}

func (c *gatedLiveToolClient) CallToolText(ctx context.Context, _ string, _ map[string]any) (string, error) {
	close(c.entered)
	select {
	case <-c.release:
		return "synthetic result", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (*gatedLiveToolClient) Close() error { return nil }

type transientProgressWriteLiveChannel struct {
	deliveryTestChannel
	blockedPath string
}

func (c *transientProgressWriteLiveChannel) PostThreadTextContext(ctx context.Context, channel, thread, text string) (string, error) {
	if strings.HasPrefix(text, "Tool progress") {
		entries, err := os.ReadDir(store.CheckpointDir("bot"))
		if err != nil {
			return "", err
		}
		for _, entry := range entries {
			if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
				c.blockedPath = filepath.Join(store.CheckpointDir("bot"), entry.Name())
				break
			}
		}
		if c.blockedPath == "" {
			return "", errors.New("missing synthetic run checkpoint")
		}
		if err := os.Rename(c.blockedPath, c.blockedPath+".saved"); err != nil {
			return "", err
		}
		if err := os.Mkdir(c.blockedPath, 0o700); err != nil {
			return "", err
		}
		_, err = c.deliveryTestChannel.PostThreadTextContext(ctx, channel, thread, text)
		return "progress-live", err
	}
	return c.deliveryTestChannel.PostThreadTextContext(ctx, channel, thread, text)
}

func (*transientProgressWriteLiveChannel) DeleteThreadMessageContext(context.Context, string, string) error {
	return os.ErrPermission
}

func TestLiveSlackTerminalRecoversTimestampAfterTransientProgressWriteFailure(t *testing.T) {
	setupServerDataDir(t)
	resetSlogForTest()
	var rounds atomic.Int32
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if rounds.Add(1) == 1 {
			writeDeliveryToolCall(w, "synthetic_tool", map[string]any{"path": "/fake-path"})
			return
		}
		writeDeliveryText(w, "safe final answer")
	}))
	t.Cleanup(model.Close)
	cfg := liveSlackConfig("vllm/test", true)
	cfg.Models.Providers["vllm"] = config.ProviderConfig{BaseURI: model.URL}
	srv := New(cfg, "fake-token")
	signal := &checkpointFailureSignalWriter{failed: make(chan struct{})}
	slog.SetDefault(slog.New(slog.NewTextHandler(signal, nil)))
	t.Cleanup(resetSlogForTest)
	tool := &gatedLiveToolClient{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-tool.release:
		default:
			close(tool.release)
		}
	})
	agent.SetToolClientFactory(func(context.Context) (agent.ToolClient, error) { return tool, nil })
	t.Cleanup(func() { agent.SetToolClientFactory(nil) })
	ch := &transientProgressWriteLiveChannel{}
	srv.handleIncomingChannelMessage(context.Background(), "bot", "slack", "alerts", ch, liveSlackMessage())
	runner, ok := srv.agents.Get("bot")
	require.True(t, ok)
	select {
	case <-tool.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("synthetic tool did not start")
	}
	select {
	case <-signal.failed:
	case <-time.After(3 * time.Second):
		t.Fatal("accepted progress did not encounter the transient checkpoint write failure")
	}
	path := ch.blockedPath
	require.NotEmpty(t, path)
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Rename(path+".saved", path))
	close(tool.release)
	runner.Wait()
	cp, err := store.ReadJSON[agent.RunCheckpoint](path)
	require.NoError(t, err)
	require.Equal(t, agent.SlackDispositionHandled, cp.Slack.Disposition)
	require.True(t, cp.Slack.CleanupPending)
	require.Equal(t, "progress-live", cp.Slack.ProgressTS,
		"terminal write must durably merge the progress timestamp known to the presenter")
	require.Contains(t, ch.posted(), "safe final answer")

	sender := &recoverySender{}
	route := channels.SlackAuthenticatedRoute{AgentName: "bot", ConfiguredID: "alerts",
		InstallationID: "bot-fake", WorkspaceID: "team-fake", Channel: sender,
		StopBeforeDispatch: make(chan struct{}), CurrentCheck: func() bool { return true }}
	(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
	posts, edits, deletes := sender.snapshot()
	require.Empty(t, posts)
	require.Empty(t, edits)
	require.Equal(t, []string{"C123/progress-live"}, deletes)
	require.NoFileExists(t, path)
}

type blockingCleanupLiveChannel struct {
	deliveryTestChannel
	entered chan struct{}
	release chan struct{}
}

func (c *blockingCleanupLiveChannel) DeleteThreadMessageContext(ctx context.Context, channel, ts string) error {
	close(c.entered)
	<-c.release
	return c.deliveryTestChannel.DeleteThreadMessageContext(ctx, channel, ts)
}

func TestLiveSlackSelectedSilenceSurvivesRunnerStopDuringCleanup(t *testing.T) {
	setupServerDataDir(t)
	resetSlogForTest()
	var rounds atomic.Int32
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if rounds.Add(1) == 1 {
			writeDeliveryToolCall(w, "synthetic_tool", map[string]any{"path": "/fake-path"})
			return
		}
		writeDeliveryText(w, "NO_REPLY")
	}))
	t.Cleanup(model.Close)
	cfg := liveSlackConfig("vllm/test", true)
	cfg.Models.Providers["vllm"] = config.ProviderConfig{BaseURI: model.URL}
	srv := New(cfg, "fake-token")
	agent.SetToolClientFactory(func(context.Context) (agent.ToolClient, error) {
		return &slowDeliveryToolClient{delay: 2 * time.Second}, nil
	})
	t.Cleanup(func() { agent.SetToolClientFactory(nil) })
	ch := &blockingCleanupLiveChannel{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-ch.release:
		default:
			close(ch.release)
		}
	})
	srv.handleIncomingChannelMessage(context.Background(), "bot", "slack", "alerts", ch, liveSlackMessage())
	runner, ok := srv.agents.Get("bot")
	require.True(t, ok)
	select {
	case <-ch.entered:
	case <-time.After(4 * time.Second):
		t.Fatal("selected silence did not reach progress cleanup")
	}
	path, cp := liveSlackCheckpoint(t)
	require.Equal(t, agent.SlackDispositionHandled, cp.Slack.Disposition)
	require.True(t, cp.Slack.CleanupPending)
	runner.Stop()
	close(ch.release)
	runner.Wait()
	require.Equal(t, int32(2), rounds.Load())
	require.Len(t, ch.posted(), 1, "NO_REPLY must not add a terminal post")
	require.Contains(t, ch.posted()[0], "Tool progress")
	require.NoFileExists(t, path)
}

func TestLiveSlackAcceptedAnswerRetainsCheckpointForFailedProgressCleanup(t *testing.T) {
	setupServerDataDir(t)
	resetSlogForTest()
	var rounds atomic.Int32
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if rounds.Add(1) == 1 {
			writeDeliveryToolCall(w, "synthetic_tool", map[string]any{"path": "/fake-path"})
			return
		}
		writeDeliveryText(w, "safe final answer")
	}))
	t.Cleanup(model.Close)
	cfg := liveSlackConfig("vllm/test", true)
	cfg.Models.Providers["vllm"] = config.ProviderConfig{BaseURI: model.URL}
	srv := New(cfg, "fake-token")
	agent.SetToolClientFactory(func(context.Context) (agent.ToolClient, error) {
		return &slowDeliveryToolClient{delay: 2 * time.Second}, nil
	})
	t.Cleanup(func() { agent.SetToolClientFactory(nil) })
	ch := &cleanupFailLiveChannel{}
	srv.handleIncomingChannelMessage(context.Background(), "bot", "slack", "alerts", ch, liveSlackMessage())
	runner, ok := srv.agents.Get("bot")
	require.True(t, ok)
	runner.Wait()
	require.Equal(t, int32(2), rounds.Load(), "tool must run before the final answer")
	_, cp := liveSlackCheckpoint(t)
	require.Equal(t, agent.SlackDispositionHandled, cp.Slack.Disposition)
	require.True(t, cp.Slack.CleanupPending)
	require.NotEmpty(t, cp.Slack.ProgressTS)
	require.Contains(t, ch.posted(), "safe final answer")
}
