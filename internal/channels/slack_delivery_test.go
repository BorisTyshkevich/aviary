package channels

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/slack-go/slack"
	"github.com/stretchr/testify/require"
)

func deliveryTestSlackClient(server *httptest.Server) *SlackChannel {
	return &SlackChannel{client: slack.New("fake-bot-token", slack.OptionAPIURL(server.URL+"/"),
		slack.OptionHTTPClient(slackStatusHTTPClient{base: server.Client()}))}
}

func TestSlackDeliveryPreservesObserved429WithoutValidHeader(t *testing.T) {
	for _, header := range []string{"", "malformed"} {
		t.Run(fmt.Sprintf("header_%q", header), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				if header != "" {
					w.Header().Set("Retry-After", header)
				}
				w.WriteHeader(http.StatusTooManyRequests)
			}))
			defer server.Close()
			ch := deliveryTestSlackClient(server)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := ch.PostThreadTextContext(ctx, "C123", "1710000000.123456", "synthetic")
			var delivery *SlackDeliveryError
			require.ErrorAs(t, err, &delivery)
			require.True(t, delivery.Rejected)
			require.Equal(t, http.StatusTooManyRequests, delivery.Status)
			require.EqualValues(t, 1, calls.Load())
		})
	}
}

func TestSlackDeliveryDoesNotRetryServerError(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	ch := deliveryTestSlackClient(server)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := ch.PostThreadTextContext(ctx, "C123", "1710000000.123456", "synthetic")
	var delivery *SlackDeliveryError
	require.ErrorAs(t, err, &delivery)
	require.False(t, delivery.Rejected)
	require.EqualValues(t, 1, calls.Load())
}

func TestSlackDeleteMissingMessageCountsAsCleanup(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/chat.delete", r.URL.Path)
		_, _ = w.Write([]byte(`{"ok":false,"error":"message_not_found"}`))
	}))
	defer server.Close()
	ch := deliveryTestSlackClient(server)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, ch.DeleteThreadMessageContext(ctx, "C123", "1710000001.123456"))
}

func TestSlackDeliveryRetryClassifiesLastDispatchedAttempt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	calls := 0
	_, err := slackDeliveryCall(ctx, func(attempt context.Context) (struct{}, error) {
		calls++
		if calls == 1 {
			return struct{}{}, &slack.RateLimitedError{RetryAfter: time.Millisecond}
		}
		attempt.Value(slackObservedStatusKey{}).(*slackObservedStatus).dispatched.Store(true)
		return struct{}{}, errors.New("synthetic transport failure")
	})
	var delivery *SlackDeliveryError
	require.ErrorAs(t, err, &delivery)
	require.False(t, delivery.Rejected)
	require.Equal(t, 2, calls)
}

func TestSlackDeliveryRetains429IfNextAttemptNeverDispatches(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	calls := 0
	_, err := slackDeliveryCall(ctx, func(_ context.Context) (struct{}, error) {
		calls++
		if calls == 1 {
			return struct{}{}, &slack.RateLimitedError{RetryAfter: time.Millisecond}
		}
		cancel()
		return struct{}{}, context.Canceled
	})
	var delivery *SlackDeliveryError
	require.ErrorAs(t, err, &delivery)
	require.True(t, delivery.Rejected)
	require.Equal(t, 2, calls)
}

func TestSlackDeliveryRetriesOnly429WithinDeadline(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		deadline, retryAfter time.Duration
		wantCalls            int
	}{
		{"retry fits", time.Second, time.Millisecond, 2},
		{"retry exceeds deadline", 20 * time.Millisecond, time.Second, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), tc.deadline)
			defer cancel()
			calls := 0
			_, err := slackDeliveryCall(ctx, func(attempt context.Context) (struct{}, error) {
				calls++
				attempt.Value(slackObservedStatusKey{}).(*slackObservedStatus).dispatched.Store(true)
				if calls == 1 {
					return struct{}{}, &slack.RateLimitedError{RetryAfter: tc.retryAfter}
				}
				return struct{}{}, nil
			})
			require.Equal(t, tc.wantCalls, calls)
			if tc.wantCalls == 1 {
				var delivery *SlackDeliveryError
				require.ErrorAs(t, err, &delivery)
				require.True(t, delivery.Rejected)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSlackDeliveryCancellationDuring429WaitRetainsRejection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	called := make(chan struct{})
	go func() { <-called; cancel() }()
	calls := 0
	_, err := slackDeliveryCall(ctx, func(attempt context.Context) (struct{}, error) {
		calls++
		attempt.Value(slackObservedStatusKey{}).(*slackObservedStatus).dispatched.Store(true)
		close(called)
		return struct{}{}, &slack.RateLimitedError{RetryAfter: 100 * time.Millisecond}
	})
	var delivery *SlackDeliveryError
	require.ErrorAs(t, err, &delivery)
	require.True(t, delivery.Rejected)
	require.Equal(t, 1, calls)
}

func TestSlackDeliveryPreDispatchStopCancels429Wait(t *testing.T) {
	stop := make(chan struct{})
	ctx, cancel := context.WithTimeout(WithSlackPreDispatchStop(context.Background(), stop), time.Second)
	defer cancel()
	calls := 0
	started := time.Now()
	_, err := slackDeliveryCall(ctx, func(attempt context.Context) (struct{}, error) {
		calls++
		attempt.Value(slackObservedStatusKey{}).(*slackObservedStatus).dispatched.Store(true)
		close(stop)
		return struct{}{}, &slack.RateLimitedError{RetryAfter: 500 * time.Millisecond}
	})
	var delivery *SlackDeliveryError
	require.ErrorAs(t, err, &delivery)
	require.True(t, delivery.Rejected)
	require.Equal(t, 1, calls)
	require.Less(t, time.Since(started), 200*time.Millisecond)
}

func TestSlackDeliveryPreDispatchErrorIsRejected(t *testing.T) {
	_, err := slackDeliveryCall(context.Background(), func(context.Context) (struct{}, error) {
		return struct{}{}, errors.New("synthetic pre-dispatch failure")
	})
	var delivery *SlackDeliveryError
	require.ErrorAs(t, err, &delivery)
	require.True(t, delivery.Rejected)
}

func TestSlackGenericThreadSendKeepsMarkdownEnabled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/chat.postMessage", r.URL.Path)
		require.Equal(t, "", r.FormValue("mrkdwn"))
		require.Equal(t, "1710000000.123456", r.FormValue("thread_ts"))
		_, _ = w.Write([]byte(`{"ok":true,"channel":"C123","ts":"1710000001.123456"}`))
	}))
	defer server.Close()
	ch := deliveryTestSlackClient(server)
	ts, err := ch.SendThreadMessageAndGetID("C123", "1710000000.123456", "*synthetic*")
	require.NoError(t, err)
	require.Equal(t, "1710000001.123456", ts)
}

func TestSlackFileShareFallbackDependsOnCompletionAcceptance(t *testing.T) {
	for _, tc := range []struct {
		name, failPath, errorCode string
		status                    int
		safe                      bool
	}{
		{"allocation failed", "/files.getUploadURLExternal", "", 500, true},
		{"bytes failed", "/upload", "", 500, true},
		{"completion definite rejection", "/files.completeUploadExternal", "missing_scope", 200, true},
		{"completion uncertain API error", "/files.completeUploadExternal", "internal_error", 200, false},
		{"completion uncertain HTTP error", "/files.completeUploadExternal", "", 500, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == tc.failPath {
					w.WriteHeader(tc.status)
					if tc.status == 200 {
						_, _ = fmt.Fprintf(w, `{"ok":false,"error":%q}`, tc.errorCode)
					}
					return
				}
				switch r.URL.Path {
				case "/files.getUploadURLExternal":
					_, _ = fmt.Fprintf(w, `{"ok":true,"upload_url":%q,"file_id":"F1"}`, server.URL+"/upload")
				case "/upload":
					_, _ = w.Write([]byte("ok"))
				case "/files.completeUploadExternal":
					_, _ = w.Write([]byte(`{"ok":true,"files":[{"id":"F1"}]}`))
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			ch := deliveryTestSlackClient(server)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := ch.ShareThreadMarkdownFileContext(ctx, "C123", "1710000000.123456", "Full answer attached.", "# Synthetic answer")
			var share *SlackFileShareError
			require.ErrorAs(t, err, &share)
			require.Equal(t, tc.safe, share.SafeFallback)
		})
	}
}
