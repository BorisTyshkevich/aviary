package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/agent"
	"github.com/lsegal/aviary/internal/channels"
)

type presenterTestSender struct {
	mu          sync.Mutex
	posts       []string
	postTimes   []time.Time
	edits       []string
	editTimes   []time.Time
	deletes     []string
	files       []string
	postErrors  []error
	editError   error
	deleteError error
	fileError   error
	editStarted chan struct{}
	editRelease chan struct{}
	postStarted chan struct{}
	postRelease chan struct{}
}

type preDispatchRejectSender struct {
	*presenterTestSender
	started chan struct{}
	release chan struct{}
	mu      sync.Mutex
	first   bool
}

func (s *preDispatchRejectSender) PostThreadTextContext(ctx context.Context, channel, threadTS, body string) (string, error) {
	s.mu.Lock()
	first := !s.first
	s.first = true
	s.mu.Unlock()
	if first {
		close(s.started)
		<-s.release
		return "", &channels.SlackDeliveryError{Cause: context.Canceled, Rejected: true}
	}
	return s.presenterTestSender.PostThreadTextContext(ctx, channel, threadTS, body)
}

func (s *presenterTestSender) PostThreadTextContext(ctx context.Context, _, _, body string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.Lock()
	started, release := s.postStarted, s.postRelease
	s.postStarted = nil
	s.mu.Unlock()
	if started != nil {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.posts = append(s.posts, body)
	s.postTimes = append(s.postTimes, time.Now())
	n := len(s.posts)
	if n <= len(s.postErrors) && s.postErrors[n-1] != nil {
		return "", s.postErrors[n-1]
	}
	return "ts-" + string(rune('0'+n)), nil
}

func TestSlackPresenterNoticeGetsFreshBudgetAfterAnswerTimeout(t *testing.T) {
	sender := &presenterTestSender{postStarted: make(chan struct{}), postRelease: make(chan struct{})}
	p := presenterForTest(sender, false)
	p.terminalTimeout = 10 * time.Millisecond
	srv := &Server{}
	var budgets []time.Duration
	p.terminalContextFactory = func(budget time.Duration) (context.Context, context.CancelFunc) {
		budgets = append(budgets, budget)
		return srv.terminalContext(context.Background(), budget)
	}
	result, _ := p.Terminal(nil, "done", "", "answer", false)
	require.Equal(t, slackOutcomeUnconfirmed, result.Outcome)
	require.Equal(t, slackDispositionHandled, result.Disposition,
		"the fixed notice must retain its own budget after the answer expires")
	posts, _, _, _ := sender.snapshot()
	require.Equal(t, []string{"Answer delivery could not be confirmed."}, posts)
	require.Equal(t, []time.Duration{10 * time.Millisecond, slackAnswerCallTimeout}, budgets)
}

func TestSlackPresenterDoesNotStartWriteAfterSharedDrainDeadline(t *testing.T) {
	sender := &presenterTestSender{}
	p := presenterForTest(sender, false)
	srv := &Server{}
	srv.terminalDrainUntil.Store(time.Now().Add(-time.Second).UnixNano())
	p.terminalContextFactory = func(budget time.Duration) (context.Context, context.CancelFunc) {
		return srv.terminalContext(context.Background(), budget)
	}
	result, _ := p.Terminal(nil, "error", "", "", false)
	require.False(t, result.ConfirmedNewReply)
	posts, _, _, _ := sender.snapshot()
	require.Empty(t, posts, "expired shared drain must prevent a new Slack write")
}

func (s *presenterTestSender) EditThreadTextContext(ctx context.Context, _, _, body string) error {
	if s.editStarted != nil {
		close(s.editStarted)
		select {
		case <-s.editRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.edits = append(s.edits, body)
	s.editTimes = append(s.editTimes, time.Now())
	return s.editError
}

func (s *presenterTestSender) DeleteThreadMessageContext(_ context.Context, _, ts string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletes = append(s.deletes, ts)
	return s.deleteError
}

func (s *presenterTestSender) ShareThreadMarkdownFileContext(_ context.Context, _, _, intro, answer string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files = append(s.files, intro+"\n"+answer)
	return s.fileError
}

func (s *presenterTestSender) snapshot() (posts, edits, deletes, files []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.posts...), append([]string(nil), s.edits...), append([]string(nil), s.deletes...), append([]string(nil), s.files...)
}

func presenterForTest(sender *presenterTestSender, progress bool) *slackPresenter {
	p := newSlackPresenter(sender, "C123", "1710000000.123456", progress)
	p.delay = time.Millisecond
	p.editGap = time.Millisecond
	return p
}

func TestSlackPresenterFastRunFlushesProgressBeforeFinal(t *testing.T) {
	sender := &presenterTestSender{}
	p := newSlackPresenter(sender, "C123", "1710000000.123456", true)
	p.Tool(agent.PublicToolEvent{Name: "web_search", InvocationID: "id-1", State: agent.ToolState("started")})
	result, first := p.Terminal(nil, "done", "", "final", false)
	require.True(t, first)
	require.Equal(t, slackOutcomeAnswer, result.Outcome)
	posts, _, deletes, _ := sender.snapshot()
	require.Equal(t, []string{"Tool progress\n• 1. web_search started", "final"}, posts)
	require.Equal(t, []string{"ts-1"}, deletes)
}

func TestSlackPresenterRetriesDefinitelyRejectedPreDispatchProgressAtTerminal(t *testing.T) {
	sender := &preDispatchRejectSender{presenterTestSender: &presenterTestSender{},
		started: make(chan struct{}), release: make(chan struct{})}
	p := newSlackPresenter(sender, "C123", "1710000000.123456", true)
	p.delay = time.Millisecond
	p.editGap = time.Millisecond
	p.Tool(agent.PublicToolEvent{Name: "web_search", InvocationID: "id-1", State: agent.ToolStateStarted})
	select {
	case <-sender.started:
	case <-time.After(time.Second):
		t.Fatal("progress post did not reach pre-dispatch boundary")
	}
	p.progressCancel()
	close(sender.release)
	result, _ := p.Terminal(nil, "done", "", "final", false)
	require.Equal(t, slackOutcomeAnswer, result.Outcome)
	posts, _, deletes, _ := sender.snapshot()
	require.Equal(t, []string{"Tool progress\n• 1. web_search started", "final"}, posts)
	require.Equal(t, []string{"ts-1"}, deletes)
}

func TestSlackPresenterFastRunFlushesAllQueuedPages(t *testing.T) {
	sender := &presenterTestSender{}
	p := presenterForTest(sender, true)
	p.delay = time.Second
	p.maxChars = 500
	for i := range 20 {
		p.Tool(agent.PublicToolEvent{Name: "clickhouse_query", InvocationID: fmt.Sprintf("call-%d", i),
			State: agent.ToolStateStarted, Detail: "SQL: SELECT count() FROM events WHERE tenant = ? AND day = ?"})
	}
	result, _ := p.Terminal(nil, "done", "", "final", false)
	posts, _, deletes, _ := sender.snapshot()
	require.Greater(t, len(posts), 2)
	require.Equal(t, "final", posts[len(posts)-1])
	require.Len(t, deletes, len(result.ProgressTimestamps))
	allProgress := strings.Join(posts[:len(posts)-1], "\n")
	for i := 1; i <= 20; i++ {
		require.Contains(t, allProgress, fmt.Sprintf("• %d. clickhouse_query", i))
	}
}

func TestSlackPresenterTerminalFlushIsBounded(t *testing.T) {
	sender := &presenterTestSender{}
	p := newSlackPresenter(sender, "C123", "1710000000.123456", true)
	p.delay = time.Second
	p.maxChars = 500
	p.flushBudget = 20 * time.Millisecond
	for i := range 20 {
		p.Tool(agent.PublicToolEvent{Name: "clickhouse_query", InvocationID: fmt.Sprintf("call-%d", i),
			State: agent.ToolStateStarted, Detail: "SQL: SELECT count() FROM events WHERE tenant = ? AND day = ?"})
	}
	start := time.Now()
	result, _ := p.Terminal(nil, "done", "", "final", false)
	require.Less(t, time.Since(start), time.Second)
	require.Equal(t, slackOutcomeAnswer, result.Outcome)
	posts, _, deletes, _ := sender.snapshot()
	require.Equal(t, []string{"final"}, posts, "a short budget must not start a post it cannot observe")
	require.Empty(t, deletes)
}

func TestSlackPresenterTerminalFlushKeepsDispatchedPostAliveAfterBudget(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	sender := &presenterTestSender{postStarted: started, postRelease: release}
	p := newSlackPresenter(sender, "C123", "1710000000.123456", true)
	p.delay = time.Second
	p.editGap = time.Millisecond
	p.flushBudget = time.Second
	var cancelFlush context.CancelFunc
	p.terminalContextFactory = func(budget time.Duration) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		if budget == p.flushBudget {
			cancelFlush = cancel
		}
		return ctx, cancel
	}
	p.Tool(agent.PublicToolEvent{Name: "web_search", InvocationID: "id-1", State: agent.ToolStateStarted})
	done := make(chan slackTerminalResult, 1)
	go func() { result, _ := p.Terminal(nil, "done", "", "final", false); done <- result }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("terminal flush did not begin progress post")
	}
	cancelFlush()
	close(release)
	select {
	case result := <-done:
		require.Equal(t, slackOutcomeAnswer, result.Outcome)
		require.Equal(t, []string{"ts-1"}, result.ProgressTimestamps)
	case <-time.After(time.Second):
		t.Fatal("terminal delivery stalled after flush budget expired")
	}
	posts, _, deletes, _ := sender.snapshot()
	require.Equal(t, []string{"Tool progress\n• 1. web_search started", "final"}, posts)
	require.Equal(t, []string{"ts-1"}, deletes)
}

func TestSlackPresenterProgressOffIgnoresToolEvents(t *testing.T) {
	sender := &presenterTestSender{}
	p := presenterForTest(sender, false)
	p.Tool(agent.PublicToolEvent{Name: "registered_tool", InvocationID: "id-1", State: agent.ToolState("started")})
	result, _ := p.Terminal(nil, "done", "", "final", false)
	require.Equal(t, slackOutcomeAnswer, result.Outcome)
	posts, edits, deletes, _ := sender.snapshot()
	require.Equal(t, []string{"final"}, posts)
	require.Empty(t, edits)
	require.Empty(t, deletes)
}

func TestSlackPresenterProgressCoalescesAndCleansUp(t *testing.T) {
	sender := &presenterTestSender{}
	p := presenterForTest(sender, true)
	for i := range 10 {
		id := "id-" + string(rune('a'+i))
		p.Tool(agent.PublicToolEvent{Name: "registered_tool", InvocationID: id, State: agent.ToolState("started")})
		p.Tool(agent.PublicToolEvent{Name: "registered_tool", InvocationID: id, State: agent.ToolState("succeeded")})
	}
	require.Eventually(t, func() bool { posts, _, _, _ := sender.snapshot(); return len(posts) == 1 }, time.Second, time.Millisecond)
	result, _ := p.Terminal(nil, "done", "", "final", false)
	require.Equal(t, slackOutcomeAnswer, result.Outcome)
	posts, _, deletes, _ := sender.snapshot()
	require.Len(t, posts, 2)
	require.Contains(t, posts[0], "registered_tool")
	require.Contains(t, posts[0], "10. registered_tool succeeded")
	require.NotContains(t, posts[0], "additional calls beyond configured cap")
	require.LessOrEqual(t, len(posts[0]), slackProgressMaxText)
	require.Equal(t, []string{"ts-1"}, deletes)
}

func TestSlackPresenterPaginatesOneHundredCallsAndDeletesEveryPage(t *testing.T) {
	sender := &presenterTestSender{}
	p := presenterForTest(sender, true)
	p.maxCalls, p.maxChars = 100, 500
	for i := range 100 {
		p.Tool(agent.PublicToolEvent{Name: "clickhouse_query", InvocationID: fmt.Sprintf("call-%03d", i),
			State: agent.ToolStateStarted, Detail: "SQL: SELECT count() FROM events WHERE tenant = ? AND day = ?"})
	}
	p.mu.Lock()
	wantPages := len(p.pages)
	p.mu.Unlock()
	require.Greater(t, wantPages, 1)
	require.Eventually(t, func() bool { posts, _, _, _ := sender.snapshot(); return len(posts) == wantPages },
		5*time.Second, time.Millisecond)
	result, _ := p.Terminal(nil, "done", "", "final", false)
	require.Equal(t, slackOutcomeAnswer, result.Outcome)
	require.Len(t, result.ProgressTimestamps, wantPages)
	posts, _, deletes, _ := sender.snapshot()
	require.Len(t, deletes, wantPages)
	allProgress := strings.Join(posts[:len(posts)-1], "\n")
	for i := 1; i <= 100; i++ {
		require.Contains(t, allProgress, fmt.Sprintf("• %d. clickhouse_query", i))
	}
	for _, post := range posts[:len(posts)-1] {
		require.LessOrEqual(t, len(post), p.maxChars)
	}
}

func TestSlackPresenterConfiguredCallCapReportsOverflow(t *testing.T) {
	sender := &presenterTestSender{}
	p := presenterForTest(sender, true)
	p.maxCalls = 3
	for i := range 5 {
		p.Tool(agent.PublicToolEvent{Name: "registered_tool", InvocationID: fmt.Sprintf("call-%d", i), State: agent.ToolStateStarted})
	}
	require.Eventually(t, func() bool { posts, _, _, _ := sender.snapshot(); return len(posts) == 1 }, time.Second, time.Millisecond)
	posts, _, _, _ := sender.snapshot()
	require.Contains(t, posts[0], "3. registered_tool")
	require.Contains(t, posts[0], "2 additional calls beyond configured cap")
	p.Terminal(nil, "done", "", "final", false)
}

func TestSlackPresenterProgressEditRespectsMinimumGap(t *testing.T) {
	sender := &presenterTestSender{}
	p := presenterForTest(sender, true)
	p.editGap = 35 * time.Millisecond
	p.Tool(agent.PublicToolEvent{Name: "registered_tool", InvocationID: "id-1", State: agent.ToolState("started")})
	require.Eventually(t, func() bool { posts, _, _, _ := sender.snapshot(); return len(posts) == 1 }, time.Second, time.Millisecond)
	p.Tool(agent.PublicToolEvent{Name: "registered_tool", InvocationID: "id-1", State: agent.ToolState("succeeded")})
	require.Eventually(t, func() bool { _, edits, _, _ := sender.snapshot(); return len(edits) == 1 }, time.Second, time.Millisecond)
	p.Terminal(nil, "done", "", "final", false)
	sender.mu.Lock()
	gap := sender.editTimes[0].Sub(sender.postTimes[0])
	sender.mu.Unlock()
	require.GreaterOrEqual(t, gap, 30*time.Millisecond)
}

func TestSlackPresenterFailedCreationIsNeverRepeated(t *testing.T) {
	sender := &presenterTestSender{postErrors: []error{errors.New("synthetic uncertain post")}}
	p := presenterForTest(sender, true)
	p.Tool(agent.PublicToolEvent{Name: "registered_tool", InvocationID: "id-1", State: agent.ToolState("started")})
	require.Eventually(t, func() bool { posts, _, _, _ := sender.snapshot(); return len(posts) == 1 }, time.Second, time.Millisecond)
	p.Tool(agent.PublicToolEvent{Name: "registered_tool", InvocationID: "id-1", State: agent.ToolState("succeeded")})
	p.Tool(agent.PublicToolEvent{Name: "registered_tool", InvocationID: "id-2", State: agent.ToolState("started")})
	result, _ := p.Terminal(nil, "done", "", "NO_REPLY", false)
	require.Equal(t, slackOutcomeSilence, result.Outcome)
	posts, _, _, _ := sender.snapshot()
	require.Len(t, posts, 1)
}

func TestSlackPresenterTerminalOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name, kind, answer string
		already            bool
		want               slackTerminalOutcome
		posts              []string
	}{
		{"silence", "done", "NO_REPLY", false, slackOutcomeSilence, nil},
		{"already answered", "done", "", true, slackOutcomeAlreadyDone, nil},
		{"unexpected empty", "done", "", false, slackOutcomeEmpty, []string{"Unable to complete this request."}},
		{"error", "error", "", false, slackOutcomeNotice, []string{"Unable to complete this request."}},
		{"stop", "stop", "", false, slackOutcomeStopped, []string{"Stopped."}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sender := &presenterTestSender{}
			p := presenterForTest(sender, false)
			result, first := p.Terminal(nil, tc.kind, "", tc.answer, tc.already)
			require.True(t, first)
			require.Equal(t, tc.want, result.Outcome)
			posts, _, _, _ := sender.snapshot()
			require.Equal(t, tc.posts, posts)
			_, first = p.Terminal(nil, "error", "", "", false)
			require.False(t, first)
			posts, _, _, _ = sender.snapshot()
			require.Equal(t, tc.posts, posts)
		})
	}
}

func TestSlackPresenterFileFallbackOnlyBeforeAcceptedShare(t *testing.T) {
	answer := "# Result\n" + strings.Repeat("x", 8000)
	for _, tc := range []struct {
		name      string
		safe      bool
		wantPosts int
		want      slackTerminalOutcome
	}{
		{"safe", true, 3, slackOutcomeAnswer},
		{"uncertain", false, 1, slackOutcomeUnconfirmed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sender := &presenterTestSender{fileError: &channels.SlackFileShareError{Cause: errors.New("synthetic share failure"), SafeFallback: tc.safe}}
			p := presenterForTest(sender, false)
			result, _ := p.Terminal(nil, "done", "", answer, false)
			require.Equal(t, tc.want, result.Outcome)
			posts, _, _, files := sender.snapshot()
			require.Len(t, files, 1)
			require.Len(t, posts, tc.wantPosts)
			if tc.safe {
				require.Equal(t, answer, strings.Join(posts, ""))
			} else {
				require.Equal(t, "Answer delivery could not be confirmed.", posts[0])
			}
		})
	}
}

func TestSlackPresenterAcceptedFileShareNeedsNoTextFallback(t *testing.T) {
	sender := &presenterTestSender{}
	p := presenterForTest(sender, false)
	p.summarize = func(context.Context, string, string) (string, error) { return "Synthetic summary.", nil }
	result, _ := p.Terminal(nil, "done", "test/model", "# Result\nFull details", false)
	require.Equal(t, slackOutcomeAnswer, result.Outcome)
	require.True(t, result.ConfirmedNewReply)
	posts, _, _, files := sender.snapshot()
	require.Empty(t, posts)
	require.Equal(t, []string{"Synthetic summary.\n# Result\nFull details"}, files)
}

func TestSlackPresenterPartialAnswerLeavesPartsAndNotifies(t *testing.T) {
	answer := "# Result\n" + strings.Repeat("x", 8000)
	sender := &presenterTestSender{fileError: &channels.SlackFileShareError{Cause: errors.New("synthetic allocation rejection"), SafeFallback: true},
		postErrors: []error{nil, &channels.SlackDeliveryError{Cause: errors.New("synthetic rejection"), Rejected: true}}}
	p := presenterForTest(sender, false)
	result, _ := p.Terminal(nil, "done", "", answer, false)
	require.Equal(t, slackOutcomePartial, result.Outcome)
	require.True(t, result.ConfirmedNewReply)
	posts, _, _, _ := sender.snapshot()
	require.Len(t, posts, 3)
	require.Equal(t, "Answer incomplete.", posts[2])
}

func TestSlackPresenterPartialAnswerWithProgressPostsNoticeAfterParts(t *testing.T) {
	answer := "# Result\n" + strings.Repeat("x", 8000)
	rejected := &channels.SlackDeliveryError{Cause: errors.New("synthetic rejection"), Rejected: true}
	sender := &presenterTestSender{fileError: &channels.SlackFileShareError{Cause: errors.New("synthetic allocation rejection"), SafeFallback: true},
		postErrors: []error{nil, nil, rejected}}
	p := presenterForTest(sender, true)
	p.Tool(agent.PublicToolEvent{Name: "registered_tool", InvocationID: "id-1", State: agent.ToolState("started")})
	require.Eventually(t, func() bool { posts, _, _, _ := sender.snapshot(); return len(posts) == 1 }, time.Second, time.Millisecond)
	result, _ := p.Terminal(nil, "done", "", answer, false)
	require.Equal(t, slackOutcomePartial, result.Outcome)
	require.True(t, result.ConfirmedNewReply)
	posts, edits, deletes, _ := sender.snapshot()
	require.Len(t, posts, 4)
	require.Equal(t, "Answer incomplete.", posts[3])
	require.Empty(t, edits)
	require.Equal(t, []string{"ts-1"}, deletes)
}

func TestSlackPresenterPartialAnswerKeepsPromotedNoticeWhenPostFails(t *testing.T) {
	answer := "# Result\n" + strings.Repeat("x", 8000)
	rejected := &channels.SlackDeliveryError{Cause: errors.New("synthetic rejection"), Rejected: true}
	sender := &presenterTestSender{fileError: &channels.SlackFileShareError{Cause: errors.New("synthetic allocation rejection"), SafeFallback: true},
		postErrors: []error{nil, nil, rejected, rejected}}
	p := presenterForTest(sender, true)
	p.Tool(agent.PublicToolEvent{Name: "registered_tool", InvocationID: "id-1", State: agent.ToolState("started")})
	require.Eventually(t, func() bool { posts, _, _, _ := sender.snapshot(); return len(posts) == 1 }, time.Second, time.Millisecond)
	result, _ := p.Terminal(nil, "done", "", answer, false)
	require.Equal(t, slackOutcomePartial, result.Outcome)
	require.Equal(t, slackDispositionHandled, result.Disposition)
	require.True(t, result.ConfirmedNewReply)
	posts, edits, deletes, _ := sender.snapshot()
	require.Len(t, posts, 4)
	require.Equal(t, "Answer incomplete.", edits[len(edits)-1])
	require.Empty(t, deletes)
}

func TestSlackPresenterFailedAnswerPromotesKnownProgress(t *testing.T) {
	sender := &presenterTestSender{postErrors: []error{nil, errors.New("synthetic uncertain answer")}}
	p := presenterForTest(sender, true)
	p.Tool(agent.PublicToolEvent{Name: "registered_tool", InvocationID: "id-1", State: agent.ToolState("started")})
	require.Eventually(t, func() bool { posts, _, _, _ := sender.snapshot(); return len(posts) == 1 }, time.Second, time.Millisecond)
	result, _ := p.Terminal(nil, "done", "", "final", false)
	require.Equal(t, slackOutcomeUnconfirmed, result.Outcome)
	require.False(t, result.ConfirmedNewReply)
	posts, edits, deletes, _ := sender.snapshot()
	require.Len(t, posts, 2)
	require.Equal(t, []string{"Answer delivery could not be confirmed."}, edits)
	require.Empty(t, deletes)
}

func TestSlackPresenterStandaloneNoticeFailureIsNotRepeated(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want slackTerminalDisposition
	}{
		{"definite rejection", &channels.SlackDeliveryError{Cause: errors.New("synthetic rejection"), Rejected: true}, slackDispositionPending},
		{"uncertain acceptance", errors.New("synthetic uncertain write"), slackDispositionUnconfirmed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sender := &presenterTestSender{postErrors: []error{tc.err}}
			p := presenterForTest(sender, false)
			result, first := p.Terminal(nil, "error", "", "", false)
			require.True(t, first)
			require.Equal(t, slackOutcomeNotice, result.Outcome)
			require.Equal(t, tc.want, result.Disposition)
			require.Equal(t, tc.want == slackDispositionUnconfirmed, result.NoticeAttempted)
			_, first = p.Terminal(nil, "done", "", "late answer", false)
			require.False(t, first)
			posts, _, _, _ := sender.snapshot()
			require.Len(t, posts, 1)
		})
	}
}

func TestSlackPresenterUncertainAnswerKeepsDispositionAfterRejectedNotice(t *testing.T) {
	rejected := &channels.SlackDeliveryError{Cause: errors.New("fake rejected follow-up"), Rejected: true}
	sender := &presenterTestSender{postErrors: []error{errors.New("fake uncertain answer"), rejected}}
	p := presenterForTest(sender, false)
	result, first := p.Terminal(nil, "done", "", "answer", false)
	require.True(t, first)
	require.Equal(t, slackOutcomeUnconfirmed, result.Outcome)
	require.Equal(t, slackDispositionUnconfirmed, result.Disposition)
	require.False(t, result.NoticeAttempted, "a rejected notice permits a later fixed notice")
	posts, _, _, _ := sender.snapshot()
	require.Equal(t, []string{"answer", "Answer delivery could not be confirmed."}, posts)
}

func TestSlackPresenterKnownProgressRetainedWhenNoticeEditFails(t *testing.T) {
	sender := &presenterTestSender{postErrors: []error{nil, errors.New("synthetic uncertain answer")}, editError: errors.New("synthetic uncertain edit")}
	p := presenterForTest(sender, true)
	p.Tool(agent.PublicToolEvent{Name: "registered_tool", InvocationID: "id-1", State: agent.ToolState("started")})
	require.Eventually(t, func() bool { posts, _, _, _ := sender.snapshot(); return len(posts) == 1 }, time.Second, time.Millisecond)
	result, _ := p.Terminal(nil, "done", "", "answer", false)
	require.Equal(t, slackOutcomeUnconfirmed, result.Outcome)
	require.Equal(t, slackDispositionUnconfirmed, result.Disposition)
	require.Equal(t, "ts-1", result.ProgressTimestamps[0])
	require.True(t, result.CleanupPending)
	posts, edits, deletes, _ := sender.snapshot()
	require.Len(t, posts, 2)
	require.Len(t, edits, 1)
	require.Empty(t, deletes)
}

func TestSlackPresenterRejectedNoticeEditPostsFixedNotice(t *testing.T) {
	rejected := &channels.SlackDeliveryError{Cause: errors.New("synthetic rejected edit"), Rejected: true}
	sender := &presenterTestSender{editError: rejected}
	p := presenterForTest(sender, true)
	p.Tool(agent.PublicToolEvent{Name: "registered_tool", InvocationID: "id-1", State: agent.ToolState("started")})
	require.Eventually(t, func() bool { posts, _, _, _ := sender.snapshot(); return len(posts) == 1 }, time.Second, time.Millisecond)
	result, _ := p.Terminal(nil, "error", "", "", false)
	require.Equal(t, slackOutcomeNotice, result.Outcome)
	require.Equal(t, slackDispositionHandled, result.Disposition)
	require.True(t, result.ConfirmedNewReply)
	posts, edits, deletes, _ := sender.snapshot()
	require.Equal(t, "Unable to complete this request.", posts[1])
	require.Equal(t, []string{"Unable to complete this request."}, edits)
	require.Equal(t, []string{"ts-1"}, deletes)
}

func TestSlackPresenterNoticeHasFreshBudgetAfterAnswerTimeout(t *testing.T) {
	sender := &presenterTestSender{postStarted: make(chan struct{}), postRelease: make(chan struct{})}
	p := presenterForTest(sender, false)
	p.terminalTimeout = 5 * time.Millisecond
	result, _ := p.Terminal(nil, "done", "", "answer", false)
	require.Equal(t, slackOutcomeUnconfirmed, result.Outcome)
	require.Equal(t, slackDispositionHandled, result.Disposition)
	posts, _, _, _ := sender.snapshot()
	require.Equal(t, []string{"Answer delivery could not be confirmed."}, posts)
}

func TestSlackPresenterPersistenceHooksBracketWrites(t *testing.T) {
	sender := &presenterTestSender{}
	p := presenterForTest(sender, true)
	createdCh := make(chan string, 1)
	var finalized slackTerminalResult
	p.hooks.ProgressCreated = func(ts string) error { createdCh <- ts; return nil }
	p.hooks.TerminalFinalized = func(result slackTerminalResult) { finalized = result }
	p.Tool(agent.PublicToolEvent{Name: "registered_tool", InvocationID: "id-1", State: agent.ToolState("started")})
	var created string
	select {
	case created = <-createdCh:
	case <-time.After(time.Second):
		t.Fatal("progress timestamp hook not called")
	}
	result, _ := p.Terminal(nil, "done", "", "final", false)
	require.Equal(t, "ts-1", created)
	require.Equal(t, result, finalized)
	posts, _, deletes, _ := sender.snapshot()
	require.Equal(t, []string{"Tool progress\n• 1. registered_tool started", "final"}, posts)
	require.Equal(t, []string{"ts-1"}, deletes)
}

func TestSlackPresenterNoticeHookFailurePreventsUntrackedPost(t *testing.T) {
	sender := &presenterTestSender{}
	p := presenterForTest(sender, false)
	p.hooks.NoticeAttempting = func() error { return errors.New("synthetic checkpoint failure") }
	result, _ := p.Terminal(nil, "error", "", "", false)
	require.Equal(t, slackDispositionPending, result.Disposition)
	require.False(t, result.NoticeAttempted)
	posts, _, _, _ := sender.snapshot()
	require.Empty(t, posts)
}

func TestSlackPresenterProgressCleanupFailureRemainsPending(t *testing.T) {
	sender := &presenterTestSender{deleteError: errors.New("synthetic cleanup denial")}
	p := presenterForTest(sender, true)
	p.Tool(agent.PublicToolEvent{Name: "registered_tool", InvocationID: "id-1", State: agent.ToolState("started")})
	require.Eventually(t, func() bool { posts, _, _, _ := sender.snapshot(); return len(posts) == 1 }, time.Second, time.Millisecond)
	result, _ := p.Terminal(nil, "done", "", "final", false)
	require.Equal(t, slackDispositionHandled, result.Disposition)
	require.True(t, result.CleanupPending)
	_, _, deletes, _ := sender.snapshot()
	require.Equal(t, []string{"ts-1"}, deletes)
}

func TestSlackPresenterDrainsInFlightEditBeforeFinal(t *testing.T) {
	sender := &presenterTestSender{editStarted: make(chan struct{}), editRelease: make(chan struct{})}
	p := presenterForTest(sender, true)
	p.Tool(agent.PublicToolEvent{Name: "registered_tool", InvocationID: "id-1", State: agent.ToolState("started")})
	require.Eventually(t, func() bool { posts, _, _, _ := sender.snapshot(); return len(posts) == 1 }, time.Second, time.Millisecond)
	p.Tool(agent.PublicToolEvent{Name: "registered_tool", InvocationID: "id-1", State: agent.ToolState("succeeded")})
	select {
	case <-sender.editStarted:
	case <-time.After(time.Second):
		t.Fatal("progress edit did not start")
	}
	resultCh := make(chan slackTerminalResult, 1)
	go func() { result, _ := p.Terminal(nil, "done", "", "final", false); resultCh <- result }()
	select {
	case <-resultCh:
		t.Fatal("terminal completed while progress edit was in flight")
	case <-time.After(20 * time.Millisecond):
	}
	close(sender.editRelease)
	result := <-resultCh
	require.Equal(t, slackOutcomeAnswer, result.Outcome)
	posts, edits, _, _ := sender.snapshot()
	require.Equal(t, []string{"Tool progress\n• 1. registered_tool started", "final"}, posts)
	require.Equal(t, []string{"Tool progress\n• 1. registered_tool succeeded"}, edits)
}

func TestSlackPresenterDrainsInFlightCreationAndCleansItUp(t *testing.T) {
	sender := &presenterTestSender{postStarted: make(chan struct{}), postRelease: make(chan struct{})}
	p := presenterForTest(sender, true)
	started := sender.postStarted
	p.Tool(agent.PublicToolEvent{Name: "registered_tool", InvocationID: "id-1", State: agent.ToolState("started")})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("progress creation did not start")
	}
	resultCh := make(chan slackTerminalResult, 1)
	go func() { result, _ := p.Terminal(nil, "done", "", "final", false); resultCh <- result }()
	select {
	case <-resultCh:
		t.Fatal("terminal completed while progress creation was in flight")
	case <-time.After(20 * time.Millisecond):
	}
	close(sender.postRelease)
	result := <-resultCh
	require.Equal(t, slackOutcomeAnswer, result.Outcome)
	posts, _, deletes, _ := sender.snapshot()
	require.Equal(t, []string{"Tool progress\n• 1. registered_tool started", "final"}, posts)
	require.Equal(t, []string{"ts-1"}, deletes)
}
