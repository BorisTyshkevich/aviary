package server

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/agent"
	"github.com/lsegal/aviary/internal/channels"
	"github.com/lsegal/aviary/internal/store"
)

type recoverySender struct {
	mu                                sync.Mutex
	posts, edits, deletes             []string
	postError, editError, deleteError error
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
	path, route, sender := recoveryFixture(t, agent.SlackCheckpoint{})
	sender.postError = errors.New("fake transport uncertainty")
	(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
	meta := readRecovery(t, path).Slack
	require.Equal(t, agent.SlackDispositionUnconfirmed, meta.Disposition)
	require.True(t, meta.NoticeAttempted)
	sender.postError = nil
	(&Server{}).recoverSlackCheckpoint(context.Background(), route, path)
	posts, _, _ := sender.snapshot()
	require.Len(t, posts, 1)
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
