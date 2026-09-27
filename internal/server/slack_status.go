package server

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/lsegal/aviary/internal/channels"
)

const (
	slackStatusText    = "is thinking"
	slackStatusTimeout = 5 * time.Second
	slackStatusRefresh = 60 * time.Second
)

// slackRunStatus owns the native status calls for one routed turn. The refresh
// loop exits before the terminal callback writes to Slack.
type slackRunStatus struct {
	sender         channels.AssistantStatusSender
	channel        string
	threadTS       string
	contextFactory func(time.Duration) (context.Context, context.CancelFunc)
	cancel         context.CancelFunc
	done           chan struct{}
	once           sync.Once
	refreshEvery   time.Duration
	unsupported    bool
}

func newSlackRunStatus(sender channels.AssistantStatusSender, channel, threadTS string) *slackRunStatus {
	return &slackRunStatus{sender: sender, channel: channel, threadTS: threadTS, refreshEvery: slackStatusRefresh}
}

func (s *slackRunStatus) send(status string) error {
	var ctx context.Context
	var cancel context.CancelFunc
	if s.contextFactory != nil {
		ctx, cancel = s.contextFactory(slackStatusTimeout)
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), slackStatusTimeout)
	}
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.sender.SendAssistantStatusContext(ctx, s.channel, s.threadTS, status)
}

func (s *slackRunStatus) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done = make(chan struct{})
	if err := s.send(slackStatusText); err != nil {
		if strings.Contains(err.Error(), "unsupported_conversation_type") {
			s.unsupported = true
			slog.Debug("server: Slack native status unsupported for this conversation")
		} else {
			slog.Debug("server: Slack native status unavailable")
		}
	}
	go func() {
		defer close(s.done)
		if s.unsupported {
			return
		}
		ticker := time.NewTicker(s.refreshEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if ctx.Err() != nil {
					return
				}
				// Once dispatched, let the request finish under its own
				// deadline before a terminal reply is written.
				if err := s.send(slackStatusText); err != nil {
					if strings.Contains(err.Error(), "unsupported_conversation_type") {
						s.unsupported = true
						slog.Debug("server: Slack native status unsupported for this conversation")
						return
					}
					slog.Debug("server: Slack native status refresh unavailable")
				}
			}
		}
	}()
}

func (s *slackRunStatus) BeforeTerminal() {
	s.once.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		if s.done != nil {
			<-s.done
		}
	})
}

func (s *slackRunStatus) Finish(confirmedNewReply bool) {
	s.BeforeTerminal()
	if confirmedNewReply || s.unsupported {
		return
	}
	if err := s.send(""); err != nil {
		slog.Debug("server: failed to clear Slack native status")
	}
}
