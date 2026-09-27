package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/agent"
	"github.com/lsegal/aviary/internal/channels"
	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/store"
)

type recoverySender struct {
	mu                                sync.Mutex
	posts, edits, deletes             []string
	postError, editError, deleteError error
	deleteErrorByTS                   map[string]error
}

func (f *recoverySender) PostThreadTextContext(ctx context.Context, channel, thread, text string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.posts = append(f.posts, channel+"/"+thread+":"+text)
	return "notice", f.postError
}

func (f *recoverySender) EditThreadTextContext(ctx context.Context, channel, ts, text string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.edits = append(f.edits, channel+"/"+ts+":"+text)
	return f.editError
}

func (f *recoverySender) DeleteThreadMessageContext(ctx context.Context, channel, ts string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes = append(f.deletes, channel+"/"+ts)
	if err := f.deleteErrorByTS[ts]; err != nil {
		return err
	}
	return f.deleteError
}

func (f *recoverySender) snapshot() (posts, edits, deletes []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.posts...), append([]string(nil), f.edits...), append([]string(nil), f.deletes...)
}

func recoveryFixture(t *testing.T, meta agent.SlackCheckpoint) (string, channels.SlackAuthenticatedRoute, *recoverySender) {
	t.Helper()
	setupServerDataDir(t)
	if meta.InstallationID == "" {
		meta.InstallationID = "install"
	}
	if meta.WorkspaceID == "" {
		meta.WorkspaceID = "workspace"
	}
	if meta.ChannelID == "" {
		meta.ChannelID = "C-original"
	}
	if meta.RootThreadTS == "" {
		meta.RootThreadTS = "1700000000.000001"
	}
	if meta.Disposition == "" {
		meta.Disposition = agent.SlackDispositionPending
	}
	path := filepath.Join(store.CheckpointDir("bot"), "run.json")
	require.NoError(t, store.WriteJSON(path, &agent.RunCheckpoint{AgentName: "bot", SessionID: "slack:C-original", Slack: &meta}))
	sender := &recoverySender{}
	stop := make(chan struct{})
	route := channels.SlackAuthenticatedRoute{AgentName: "bot", ConfiguredID: meta.ConfiguredID,
		InstallationID: "install", WorkspaceID: "workspace", Channel: sender,
		StopBeforeDispatch: stop, CurrentCheck: func() bool {
			select {
			case <-stop:
				return false
			default:
				return true
			}
		}}
	return path, route, sender
}

func readRecovery(t *testing.T, path string) agent.RunCheckpoint {
	t.Helper()
	cp, err := store.ReadJSON[agent.RunCheckpoint](path)
	require.NoError(t, err)
	return cp
}

func TestSlackRecoveryUsesOriginalTargetAndRetiresAcceptedNotice(t *testing.T) {
	path, route, sender := recoveryFixture(t, agent.SlackCheckpoint{ConfiguredID: "", Disposition: agent.SlackDispositionPending})
	(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
	posts, edits, deletes := sender.snapshot()
	require.Equal(t, []string{"C-original/1700000000.000001:Interrupted; please resend your request."}, posts)
	require.Empty(t, edits)
	require.Empty(t, deletes)
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err))
}

func TestSlackRecoveryKnownProgressEditsWithoutFreshPost(t *testing.T) {
	path, route, sender := recoveryFixture(t, agent.SlackCheckpoint{ProgressTS: "progress", CleanupPending: true})
	(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
	posts, edits, deletes := sender.snapshot()
	require.Empty(t, posts)
	require.Equal(t, []string{"C-original/progress:Interrupted; please resend your request."}, edits)
	require.Empty(t, deletes)
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err))
}

func TestSlackRecoveryHandledOnlyCleansKnownProgress(t *testing.T) {
	path, route, sender := recoveryFixture(t, agent.SlackCheckpoint{ProgressTS: "progress", CleanupPending: true, Disposition: agent.SlackDispositionHandled})
	(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
	posts, edits, deletes := sender.snapshot()
	require.Empty(t, posts)
	require.Empty(t, edits)
	require.Equal(t, []string{"C-original/progress"}, deletes)
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err))
}

func TestSlackRecoveryRetainsDeniedCleanup(t *testing.T) {
	path, route, sender := recoveryFixture(t, agent.SlackCheckpoint{ProgressTS: "progress", CleanupPending: true, Disposition: agent.SlackDispositionHandled})
	sender.deleteError = &channels.SlackDeliveryError{Rejected: true, Cause: errors.New("fake denied")}
	(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
	meta := readRecovery(t, path).Slack
	require.Equal(t, agent.SlackDispositionHandled, meta.Disposition)
	require.True(t, meta.CleanupPending)
	posts, edits, _ := sender.snapshot()
	require.Empty(t, posts)
	require.Empty(t, edits)
}

func TestSlackRecoveryEditsLastPageAndDeletesEarlierPages(t *testing.T) {
	path, route, sender := recoveryFixture(t, agent.SlackCheckpoint{
		ProgressTS: "page-1", ProgressExtraTS: []string{"page-2", "page-3"}, CleanupPending: true,
	})
	(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
	posts, edits, deletes := sender.snapshot()
	require.Empty(t, posts)
	require.Equal(t, []string{"C-original/page-3:Interrupted; please resend your request."}, edits)
	require.Equal(t, []string{"C-original/page-1", "C-original/page-2"}, deletes)
	require.NoFileExists(t, path)
}

func TestSlackRecoveryPersistsEachPageDeletedBeforeLaterFailure(t *testing.T) {
	path, route, sender := recoveryFixture(t, agent.SlackCheckpoint{
		ProgressTS: "page-1", ProgressExtraTS: []string{"page-2", "page-3"},
		CleanupPending: true, Disposition: agent.SlackDispositionHandled,
	})
	sender.deleteErrorByTS = map[string]error{
		"page-2": &channels.SlackDeliveryError{Rejected: true, Cause: errors.New("fake denied")},
	}
	(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
	meta := readRecovery(t, path).Slack
	require.Equal(t, agent.SlackDispositionHandled, meta.Disposition)
	require.Equal(t, []string{"page-2", "page-3"}, meta.ProgressTimestamps(),
		"a crash after the first deletion must resume at the next page")
	require.True(t, meta.CleanupPending)
	_, _, deletes := sender.snapshot()
	require.Equal(t, []string{"C-original/page-1", "C-original/page-2"}, deletes)
	sender.deleteErrorByTS = nil
	(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
	_, _, deletes = sender.snapshot()
	require.Equal(t, []string{"C-original/page-1", "C-original/page-2", "C-original/page-2", "C-original/page-3"}, deletes)
	require.NoFileExists(t, path)
}

func TestSlackRecoveryMissingPageCountsAsDeleted(t *testing.T) {
	path, route, sender := recoveryFixture(t, agent.SlackCheckpoint{
		ProgressTS: "missing-page", ProgressExtraTS: []string{"page-2"},
		CleanupPending: true, Disposition: agent.SlackDispositionHandled,
	})
	// SlackChannel normalizes message_not_found to nil; the fake sender
	// models that successful idempotent delete on the first page.
	(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
	_, _, deletes := sender.snapshot()
	require.Equal(t, []string{"C-original/missing-page", "C-original/page-2"}, deletes)
	require.NoFileExists(t, path)
}

func TestSlackRecoveryNoticePromotionKeepsEarlierPagesOnDeleteFailure(t *testing.T) {
	path, route, sender := recoveryFixture(t, agent.SlackCheckpoint{
		ProgressTS: "page-1", ProgressExtraTS: []string{"page-2", "notice-page"}, CleanupPending: true,
	})
	sender.deleteErrorByTS = map[string]error{
		"page-2": &channels.SlackDeliveryError{Rejected: true, Cause: errors.New("fake denied")},
	}
	(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
	meta := readRecovery(t, path).Slack
	require.Equal(t, agent.SlackDispositionHandled, meta.Disposition)
	require.Equal(t, []string{"page-2"}, meta.ProgressTimestamps())
	require.True(t, meta.CleanupPending)
	_, edits, deletes := sender.snapshot()
	require.Equal(t, []string{"C-original/notice-page:Interrupted; please resend your request."}, edits)
	require.Equal(t, []string{"C-original/page-1", "C-original/page-2"}, deletes)
	sender.deleteErrorByTS = nil
	(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
	_, edits, deletes = sender.snapshot()
	require.Len(t, edits, 1, "handled recovery must not rewrite the fixed notice")
	require.Equal(t, []string{"C-original/page-1", "C-original/page-2", "C-original/page-2"}, deletes)
	require.NoFileExists(t, path)
}

func TestSlackRecoveryUncertainLastPageEditKeepsAllPages(t *testing.T) {
	path, route, sender := recoveryFixture(t, agent.SlackCheckpoint{
		ProgressTS: "page-1", ProgressExtraTS: []string{"page-2"}, CleanupPending: true,
	})
	sender.editError = errors.New("fake connection lost after edit dispatch")
	(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
	meta := readRecovery(t, path).Slack
	require.Equal(t, agent.SlackDispositionUnconfirmed, meta.Disposition)
	require.Equal(t, []string{"page-1", "page-2"}, meta.ProgressTimestamps())
	posts, edits, deletes := sender.snapshot()
	require.Empty(t, posts)
	require.Equal(t, []string{"C-original/page-2:Interrupted; please resend your request."}, edits)
	require.Empty(t, deletes)
}

func TestSlackRecoveryNoticeMarkerPreventsDuplicatePost(t *testing.T) {
	path, route, sender := recoveryFixture(t, agent.SlackCheckpoint{Disposition: agent.SlackDispositionUnconfirmed, NoticeAttempted: true})
	(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
	posts, edits, deletes := sender.snapshot()
	require.Empty(t, posts)
	require.Empty(t, edits)
	require.Empty(t, deletes)
	require.True(t, readRecovery(t, path).Slack.NoticeAttempted)
}

func TestSlackRecoveryDefiniteRejectionCanRetryLater(t *testing.T) {
	path, route, sender := recoveryFixture(t, agent.SlackCheckpoint{})
	sender.postError = &channels.SlackDeliveryError{Rejected: true, Cause: errors.New("fake channel missing")}
	(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
	meta := readRecovery(t, path).Slack
	require.Equal(t, agent.SlackDispositionPending, meta.Disposition)
	require.False(t, meta.NoticeAttempted)
	sender.postError = nil
	(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
	posts, _, _ := sender.snapshot()
	require.Len(t, posts, 2)
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err))
}

func TestSlackRecoveryUncertainPostIsNeverRepeated(t *testing.T) {
	for _, disposition := range []agent.SlackDisposition{agent.SlackDispositionPending, agent.SlackDispositionUnconfirmed} {
		t.Run(string(disposition), func(t *testing.T) {
			path, route, sender := recoveryFixture(t, agent.SlackCheckpoint{Disposition: disposition})
			sender.postError = errors.New("fake transport uncertainty")
			(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
			meta := readRecovery(t, path).Slack
			require.Equal(t, agent.SlackDispositionUnconfirmed, meta.Disposition)
			require.True(t, meta.NoticeAttempted)
			sender.postError = nil
			(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
			posts, _, _ := sender.snapshot()
			require.Len(t, posts, 1)
		})
	}
}

func TestSlackRecoveryRejectedUncertaintyNoticeKeepsUnconfirmedAnswer(t *testing.T) {
	path, route, sender := recoveryFixture(t, agent.SlackCheckpoint{Disposition: agent.SlackDispositionUnconfirmed})
	sender.postError = &channels.SlackDeliveryError{Rejected: true, Cause: errors.New("fake rate limit rejected")}
	(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
	meta := readRecovery(t, path).Slack
	require.Equal(t, agent.SlackDispositionUnconfirmed, meta.Disposition)
	require.False(t, meta.NoticeAttempted)
	posts, _, _ := sender.snapshot()
	require.Equal(t, []string{"C-original/1700000000.000001:Delivery could not be confirmed. Please check this thread before retrying."}, posts)
	sender.postError = nil
	(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
	posts, _, _ = sender.snapshot()
	require.Len(t, posts, 2)
	require.Equal(t, posts[0], posts[1], "recovery must retain uncertainty wording")
	require.NoFileExists(t, path)
}

type uncertainAnswerNoticeChannel struct {
	deliveryTestChannel
	calls atomic.Int32
}

func (c *uncertainAnswerNoticeChannel) PostThreadTextContext(ctx context.Context, channel, thread, body string) (string, error) {
	_, _ = c.deliveryTestChannel.PostThreadTextContext(ctx, channel, thread, body)
	if c.calls.Add(1) == 1 {
		return "", errors.New("fake uncertain answer acceptance")
	}
	return "", &channels.SlackDeliveryError{Rejected: true, Cause: errors.New("fake rejected notice")}
}

func TestLiveSlackUncertainAnswerRejectedNoticePersistsUnconfirmedRecovery(t *testing.T) {
	setupServerDataDir(t)
	resetSlogForTest()
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeDeliveryText(w, "fake answer")
	}))
	t.Cleanup(model.Close)
	cfg := liveSlackConfig("vllm/test", false)
	cfg.Models.Providers["vllm"] = config.ProviderConfig{BaseURI: model.URL}
	srv := New(cfg, "fake-token")
	ch := &uncertainAnswerNoticeChannel{}
	srv.handleIncomingChannelMessage(context.Background(), "bot", "slack", "alerts", ch, liveSlackMessage())
	runner, ok := srv.agents.Get("bot")
	require.True(t, ok)
	runner.Wait()
	path, cp := liveSlackCheckpoint(t)
	require.Equal(t, agent.SlackDispositionUnconfirmed, cp.Slack.Disposition)
	require.False(t, cp.Slack.NoticeAttempted)
	require.Equal(t, []string{"fake answer", "Answer delivery could not be confirmed."}, ch.posted())
	recoverySender := &recoverySender{}
	route := channels.SlackAuthenticatedRoute{AgentName: "bot", ConfiguredID: "alerts", InstallationID: "bot-fake",
		WorkspaceID: "team-fake", Channel: recoverySender, StopBeforeDispatch: make(chan struct{}),
		CurrentCheck: func() bool { return true }}
	srv.recoverSlackCheckpoint(context.Background(), route, path)
	posts, _, _ := recoverySender.snapshot()
	require.Equal(t, []string{"C123/1700000000.000001:Delivery could not be confirmed. Please check this thread before retrying."}, posts)
	require.NoFileExists(t, path)
}

func TestSlackRecoveryRequiresMatchingAuthenticatedRoute(t *testing.T) {
	path, route, sender := recoveryFixture(t, agent.SlackCheckpoint{ConfiguredID: ""})
	route.ConfiguredID = "other"
	(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
	route.ConfiguredID = ""
	route.WorkspaceID = "other"
	(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
	route.CurrentCheck = func() bool { return false }
	(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
	posts, edits, deletes := sender.snapshot()
	require.Empty(t, posts)
	require.Empty(t, edits)
	require.Empty(t, deletes)
	require.NotNil(t, readRecovery(t, path).Slack)
}

func TestSlackRecoveryRetainsInvalidStateWithoutPosting(t *testing.T) {
	for _, meta := range []agent.SlackCheckpoint{
		{Disposition: "bogus"},
		{Disposition: agent.SlackDispositionHandled, CleanupPending: true},
		{Disposition: agent.SlackDispositionHandled, ProgressExtraTS: []string{"orphan"}, CleanupPending: true},
		{Disposition: agent.SlackDispositionHandled, ProgressTS: "duplicate", ProgressExtraTS: []string{"duplicate"}, CleanupPending: true},
	} {
		t.Run(string(meta.Disposition), func(t *testing.T) {
			path, route, sender := recoveryFixture(t, meta)
			(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
			posts, edits, deletes := sender.snapshot()
			require.Empty(t, posts)
			require.Empty(t, edits)
			require.Empty(t, deletes)
			require.NotNil(t, readRecovery(t, path).Slack)
		})
	}
}

func TestSlackRecoveryScanRetainsMissingRouteAndDropsTargetlessLegacyRecord(t *testing.T) {
	path, _, _ := recoveryFixture(t, agent.SlackCheckpoint{})
	oldPath := filepath.Join(store.CheckpointDir("removed-agent"), "old.json")
	require.NoError(t, store.WriteJSON(oldPath, &agent.RunCheckpoint{AgentName: "removed-agent",
		Overrides: &agent.RunOverrides{SuppressDelivery: true}}))
	s := &Server{channels: channels.NewManager()}
	s.RecoverSlackCheckpoints(context.Background())
	require.NotNil(t, readRecovery(t, path).Slack, "removed route keeps its original-target checkpoint")
	_, err := os.Stat(oldPath)
	require.True(t, os.IsNotExist(err), "old targetless Slack work cannot be replayed")
}
