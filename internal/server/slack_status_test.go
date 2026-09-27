package server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type delayedStatusChannel struct {
	mu       sync.Mutex
	statuses []string
	started  chan struct{}
	finished chan struct{}
	release  chan struct{}
	blockOn  int
	err      error
}

func (c *delayedStatusChannel) SendAssistantStatusContext(ctx context.Context, _, _, status string) error {
	c.mu.Lock()
	c.statuses = append(c.statuses, status)
	n := len(c.statuses)
	c.mu.Unlock()
	if n == c.blockOn {
		close(c.started)
		select {
		case <-c.release:
		case <-ctx.Done():
		}
		close(c.finished)
		return ctx.Err()
	}
	return c.err
}

func (c *delayedStatusChannel) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.statuses...)
}

func TestSlackStatusRefreshDrainsBeforeTerminalWrite(t *testing.T) {
	for _, tc := range []struct {
		name      string
		confirmed bool
		want      []string
	}{
		{"confirmed reply", true, []string{"is thinking", "is thinking"}},
		{"silent outcome", false, []string{"is thinking", "is thinking", ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ch := &delayedStatusChannel{started: make(chan struct{}), finished: make(chan struct{}), release: make(chan struct{}), blockOn: 2}
			status := newSlackRunStatus(ch, "C123", "1710000000.123456")
			status.refreshEvery = time.Millisecond
			status.Start()
			select {
			case <-ch.started:
			case <-time.After(time.Second):
				t.Fatal("status refresh did not start")
			}
			terminalReady := make(chan struct{})
			go func() {
				status.BeforeTerminal()
				close(terminalReady)
			}()
			select {
			case <-terminalReady:
				t.Fatal("terminal write began while status refresh was in flight")
			case <-time.After(25 * time.Millisecond):
			}
			close(ch.release)
			select {
			case <-terminalReady:
			case <-time.After(time.Second):
				t.Fatal("terminal write did not resume after status refresh finished")
			}
			select {
			case <-ch.finished:
			default:
				t.Fatal("terminal write began before status refresh finished")
			}
			status.Finish(tc.confirmed)
			require.Equal(t, tc.want, ch.snapshot())
		})
	}
}

func TestSlackStatusClearsForSilentTerminal(t *testing.T) {
	ch := &delayedStatusChannel{}
	status := newSlackRunStatus(ch, "C123", "1710000000.123456")
	status.Start()
	status.Finish(false)
	require.Equal(t, []string{"is thinking", ""}, ch.snapshot())
}

func TestSlackStatusStopsOnUnsupportedSurface(t *testing.T) {
	ch := &delayedStatusChannel{err: errors.New("unsupported_conversation_type")}
	status := newSlackRunStatus(ch, "C123", "1710000000.123456")
	status.Start()
	status.Finish(false)
	require.Equal(t, []string{"is thinking"}, ch.snapshot())
}
