package channels

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/slack-go/slack"
)

// SlackDeliveryError records whether Slack definitely rejected a write. All
// other dispatched failures have uncertain acceptance and must not be resent.
type SlackDeliveryError struct {
	Cause    error
	Rejected bool
	Status   int
}

func (e *SlackDeliveryError) Error() string { return e.Cause.Error() }
func (e *SlackDeliveryError) Unwrap() error { return e.Cause }

type slackObservedStatus struct {
	code       atomic.Int32
	dispatched atomic.Bool
}
type slackObservedStatusKey struct{}
type slackPreDispatchStopKey struct{}

// WithSlackPreDispatchStop prevents a queued write or rate-limit retry from
// starting after stop closes. A request already dispatched keeps its deadline.
func WithSlackPreDispatchStop(ctx context.Context, stop <-chan struct{}) context.Context {
	return context.WithValue(ctx, slackPreDispatchStopKey{}, stop)
}

func slackPreDispatchStopped(ctx context.Context) bool {
	stop, _ := ctx.Value(slackPreDispatchStopKey{}).(<-chan struct{})
	if stop == nil {
		return false
	}
	select {
	case <-stop:
		return true
	default:
		return false
	}
}

// slackStatusHTTPClient records only the response status for the operation
// that sent a request. It never retains a body, header, or credential.
type slackStatusHTTPClient struct{ base *http.Client }

func (c slackStatusHTTPClient) Do(req *http.Request) (*http.Response, error) {
	if slackPreDispatchStopped(req.Context()) {
		return nil, context.Canceled
	}
	if observed, ok := req.Context().Value(slackObservedStatusKey{}).(*slackObservedStatus); ok {
		observed.dispatched.Store(true)
	}
	resp, err := c.base.Do(req)
	if resp != nil {
		if observed, ok := req.Context().Value(slackObservedStatusKey{}).(*slackObservedStatus); ok {
			observed.code.Store(int32(resp.StatusCode))
		}
	}
	return resp, err
}

func slackKnownRejection(err error) bool {
	var apiErr slack.SlackErrorResponse
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.Err {
	case "invalid_auth", "not_authed", "account_inactive", "missing_scope", "not_in_channel", "channel_not_found", "invalid_arguments", "file_not_found", "cant_update_message", "cant_delete_message":
		return true
	default:
		return false
	}
}

func slackClassifyError(err error, status int) *SlackDeliveryError {
	if err == nil {
		return nil
	}
	var rate *slack.RateLimitedError
	var code slack.StatusCodeError
	rejected := status == http.StatusTooManyRequests || errors.As(err, &rate) ||
		(errors.As(err, &code) && code.Code == http.StatusTooManyRequests) || slackKnownRejection(err)
	return &SlackDeliveryError{Cause: err, Rejected: rejected, Status: status}
}

// slackDeliveryCall retries only a valid Slack 429 with a delay that fits
// the caller's deadline. It never retries transport errors or server errors.
func slackDeliveryCall[T any](ctx context.Context, call func(context.Context) (T, error)) (T, error) {
	var zero T
	var lastRejected error
	for retries := 0; ; retries++ {
		if slackPreDispatchStopped(ctx) {
			if lastRejected != nil {
				return zero, lastRejected
			}
			return zero, &SlackDeliveryError{Cause: context.Canceled, Rejected: true}
		}
		if err := ctx.Err(); err != nil {
			if lastRejected != nil {
				return zero, lastRejected
			}
			return zero, &SlackDeliveryError{Cause: err, Rejected: true}
		}
		observed := &slackObservedStatus{}
		attemptCtx := context.WithValue(ctx, slackObservedStatusKey{}, observed)
		value, err := call(attemptCtx)
		if err == nil {
			return value, nil
		}
		classified := slackClassifyError(err, int(observed.code.Load()))
		if lastRejected != nil && !observed.dispatched.Load() {
			return zero, lastRejected
		}
		if !observed.dispatched.Load() {
			classified.Rejected = true
		}
		var rate *slack.RateLimitedError
		if !errors.As(err, &rate) || rate.RetryAfter <= 0 || retries >= 3 {
			return zero, classified
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) <= rate.RetryAfter+250*time.Millisecond {
			return zero, classified
		}
		lastRejected = classified
		timer := time.NewTimer(rate.RetryAfter)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return zero, classified
		case <-slackStopChannel(ctx):
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return zero, classified
		}
	}
}

func slackStopChannel(ctx context.Context) <-chan struct{} {
	stop, _ := ctx.Value(slackPreDispatchStopKey{}).(<-chan struct{})
	return stop
}

// PostThreadTextContext posts an answer or progress message to the original thread.
func (c *SlackChannel) PostThreadTextContext(ctx context.Context, channel, threadTS, body string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	resolved, err := c.resolveDeliveryTarget(ctx, channel)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(threadTS) == "" {
		return "", &SlackDeliveryError{Cause: errors.New("slack thread timestamp is required"), Rejected: true}
	}
	return slackDeliveryCall(ctx, func(attempt context.Context) (string, error) {
		_, ts, err := c.client.PostMessageContext(attempt, resolved,
			slack.MsgOptionText(body, false), slack.MsgOptionDisableMarkdown(), slack.MsgOptionTS(threadTS))
		return ts, err
	})
}

// EditThreadTextContext edits one known Slack message without creating another.
func (c *SlackChannel) EditThreadTextContext(ctx context.Context, channel, ts, body string) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	resolved, err := c.resolveDeliveryTarget(ctx, channel)
	if err != nil {
		return err
	}
	_, err = slackDeliveryCall(ctx, func(attempt context.Context) (struct{}, error) {
		_, _, _, err := c.client.UpdateMessageContext(attempt, resolved, ts, slack.MsgOptionText(body, false), slack.MsgOptionDisableMarkdown())
		return struct{}{}, err
	})
	return err
}

// DeleteThreadMessageContext removes a known temporary message.
func (c *SlackChannel) DeleteThreadMessageContext(ctx context.Context, channel, ts string) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	resolved, err := c.resolveDeliveryTarget(ctx, channel)
	if err != nil {
		return err
	}
	_, err = slackDeliveryCall(ctx, func(attempt context.Context) (struct{}, error) {
		_, _, err := c.client.DeleteMessageContext(attempt, resolved, ts)
		return struct{}{}, err
	})
	var apiErr slack.SlackErrorResponse
	if errors.As(err, &apiErr) && apiErr.Err == "message_not_found" {
		return nil
	}
	return err
}

// SlackFileShareError describes the stage at which a file share failed.
// SafeFallback is true only when no completion was attempted or Slack gave
// definite rejection evidence for that attempt.
type SlackFileShareError struct {
	Cause        error
	SafeFallback bool
}

func (e *SlackFileShareError) Error() string { return e.Cause.Error() }
func (e *SlackFileShareError) Unwrap() error { return e.Cause }

// ShareThreadMarkdownFileContext stages a Markdown file and completes its share.
func (c *SlackChannel) ShareThreadMarkdownFileContext(ctx context.Context, channel, threadTS, introduction, answer string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resolved, err := c.resolveDeliveryTarget(ctx, channel)
	if err != nil {
		return &SlackFileShareError{Cause: err, SafeFallback: true}
	}
	if strings.TrimSpace(threadTS) == "" || answer == "" {
		return &SlackFileShareError{Cause: errors.New("slack file share target and content are required"), SafeFallback: true}
	}
	filename := "aviary-answer-" + time.Now().UTC().Format("20060102T150405.000000000Z") + ".md"
	upload, err := slackDeliveryCall(ctx, func(attempt context.Context) (*slack.GetUploadURLExternalResponse, error) {
		return c.client.GetUploadURLExternalContext(attempt, slack.GetUploadURLExternalParameters{FileName: filename, FileSize: len(answer)})
	})
	if err != nil {
		return &SlackFileShareError{Cause: err, SafeFallback: true}
	}
	_, err = slackDeliveryCall(ctx, func(attempt context.Context) (struct{}, error) {
		return struct{}{}, c.client.UploadToURL(attempt, slack.UploadToURLParameters{UploadURL: upload.UploadURL, Content: answer, Filename: filename})
	})
	if err != nil {
		return &SlackFileShareError{Cause: err, SafeFallback: true}
	}
	_, err = slackDeliveryCall(ctx, func(attempt context.Context) (*slack.CompleteUploadExternalResponse, error) {
		return c.client.CompleteUploadExternalContext(attempt, slack.CompleteUploadExternalParameters{
			Files: []slack.FileSummary{{ID: upload.FileID, Title: filename}}, Channel: resolved,
			ThreadTimestamp: threadTS, InitialComment: introduction,
		})
	})
	if err != nil {
		var delivery *SlackDeliveryError
		return &SlackFileShareError{Cause: err, SafeFallback: errors.As(err, &delivery) && delivery.Rejected}
	}
	return nil
}
