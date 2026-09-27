package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/lsegal/aviary/internal/agent"
	"github.com/lsegal/aviary/internal/channels"
)

const (
	slackProgressDelay     = time.Second
	slackProgressEditGap   = 2 * time.Second
	slackProgressTimeout   = 5 * time.Second
	slackTerminalTimeout   = 45 * time.Second
	slackAnswerCallTimeout = 15 * time.Second
	slackProgressMaxCalls  = 8
	slackProgressMaxText   = 2800
)

type slackPresenterSender interface {
	PostThreadTextContext(context.Context, string, string, string) (string, error)
	EditThreadTextContext(context.Context, string, string, string) error
	DeleteThreadMessageContext(context.Context, string, string) error
	ShareThreadMarkdownFileContext(context.Context, string, string, string, string) error
}

type slackPublicCall struct {
	name, id, state string
}

type slackTerminalOutcome string
type slackTerminalDisposition string

const (
	slackDispositionPending     slackTerminalDisposition = "pending"
	slackDispositionHandled     slackTerminalDisposition = "handled"
	slackDispositionUnconfirmed slackTerminalDisposition = "unconfirmed"
)

const (
	slackOutcomeAnswer         slackTerminalOutcome = "answer"
	slackOutcomeSilence        slackTerminalOutcome = "silence"
	slackOutcomeAlreadyDone    slackTerminalOutcome = "already_answered"
	slackOutcomeNotice         slackTerminalOutcome = "notice"
	slackOutcomeStopped        slackTerminalOutcome = "stopped"
	slackOutcomeEmpty          slackTerminalOutcome = "empty"
	slackOutcomeDeliveryFailed slackTerminalOutcome = "delivery_failed"
	slackOutcomePartial        slackTerminalOutcome = "partial"
	slackOutcomeUnconfirmed    slackTerminalOutcome = "unconfirmed"
)

// slackTerminalResult is the handoff seam for checkpoint disposition in PR3b.
type slackTerminalResult struct {
	Outcome           slackTerminalOutcome
	Disposition       slackTerminalDisposition
	ConfirmedNewReply bool
	ProgressTimestamp string
	CleanupPending    bool
	NoticeAttempted   bool
}

// slackPresenterHooks mark the persistence boundaries needed by recovery.
// PR2 uses the presenter without hooks.
type slackPresenterHooks struct {
	ProgressCreated   func(string) error
	NoticeAttempting  func() error
	TerminalFinalized func(slackTerminalResult)
}

type slackPresenter struct {
	sender          slackPresenterSender
	channel         string
	threadTS        string
	progressOn      bool
	summarize       func(context.Context, string, string) (string, error)
	terminalContext context.Context
	delay           time.Duration
	editGap         time.Duration
	terminalTimeout time.Duration

	mu              sync.Mutex
	calls           []slackPublicCall
	more            int
	version         int
	progressTS      string
	creationAttempt bool
	closed          bool
	terminalSet     bool
	terminalDone    chan struct{}
	result          slackTerminalResult
	wake            chan struct{}
	progressCancel  context.CancelFunc
	progressDone    chan struct{}
	hooks           slackPresenterHooks
}

func newSlackPresenter(sender slackPresenterSender, channel, threadTS string, progressOn bool) *slackPresenter {
	return &slackPresenter{sender: sender, channel: channel, threadTS: threadTS,
		progressOn: progressOn, delay: slackProgressDelay, editGap: slackProgressEditGap,
		terminalTimeout: slackTerminalTimeout,
		wake:            make(chan struct{}, 1)}
}

func (p *slackPresenter) Tool(name, id, state string) {
	if p == nil || !p.progressOn || name == "" || id == "" || (state != "started" && state != "succeeded" && state != "failed") {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	for i := range p.calls {
		if p.calls[i].id == id {
			p.calls[i].state = state
			p.version++
			p.signal()
			return
		}
	}
	if len(p.calls) < slackProgressMaxCalls {
		p.calls = append(p.calls, slackPublicCall{name: name, id: id, state: state})
	} else if state == "started" {
		p.more++
	}
	p.version++
	if p.progressDone == nil {
		ctx, cancel := context.WithCancel(context.Background())
		p.progressCancel = cancel
		p.progressDone = make(chan struct{})
		go p.runProgress(ctx)
	}
	p.signal()
}

func (p *slackPresenter) signal() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func slackEscapeToolName(name string) string {
	name = strings.ReplaceAll(name, "&", "&amp;")
	name = strings.ReplaceAll(name, "<", "&lt;")
	name = strings.ReplaceAll(name, ">", "&gt;")
	name = strings.ReplaceAll(name, "`", "'")
	return name
}

func (p *slackPresenter) progressBodyLocked() string {
	var b strings.Builder
	b.WriteString("Tool progress")
	for _, call := range p.calls {
		line := fmt.Sprintf("\n• %s %s", slackEscapeToolName(call.name), call.state)
		if b.Len()+len(line) > slackProgressMaxText {
			break
		}
		b.WriteString(line)
	}
	if p.more > 0 {
		line := fmt.Sprintf("\n… %d more", p.more)
		if b.Len()+len(line) <= slackProgressMaxText {
			b.WriteString(line)
		}
	}
	return b.String()
}

func (p *slackPresenter) runProgress(ctx context.Context) {
	defer close(p.progressDone)
	timer := time.NewTimer(p.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}
	var sentVersion int
	var lastWrite time.Time
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return
		}
		version, body, ts, attempted := p.version, p.progressBodyLocked(), p.progressTS, p.creationAttempt
		if !attempted {
			p.creationAttempt = true
		}
		p.mu.Unlock()
		if !attempted {
			// A dispatched write must finish before terminal delivery. Canceling
			// the scheduling context stops future writes, not this HTTP call.
			if ctx.Err() != nil {
				return
			}
			callCtx, cancel := context.WithTimeout(channels.WithSlackPreDispatchStop(context.Background(), ctx.Done()), slackProgressTimeout)
			created, err := p.sender.PostThreadTextContext(callCtx, p.channel, p.threadTS, body)
			cancel()
			p.mu.Lock()
			if err == nil {
				p.progressTS = created
			}
			p.mu.Unlock()
			if err != nil {
				return
			}
			if p.hooks.ProgressCreated != nil {
				if err := p.hooks.ProgressCreated(created); err != nil {
					slog.Warn("server: Slack progress checkpoint update failed")
					return
				}
			}
			sentVersion, lastWrite = version, time.Now()
			continue
		}
		if ts == "" {
			return
		}
		if version > sentVersion {
			if wait := time.Until(lastWrite.Add(p.editGap)); wait > 0 {
				timer.Reset(wait)
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
				}
				continue
			}
			if ctx.Err() != nil {
				return
			}
			callCtx, cancel := context.WithTimeout(channels.WithSlackPreDispatchStop(context.Background(), ctx.Done()), slackProgressTimeout)
			err := p.sender.EditThreadTextContext(callCtx, p.channel, ts, body)
			cancel()
			if err != nil {
				slog.Debug("server: Slack progress edit unavailable")
			}
			sentVersion, lastWrite = version, time.Now()
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-p.wake:
		}
	}
}

func (p *slackPresenter) beforeTerminal(status *slackRunStatus) bool {
	p.mu.Lock()
	if p.terminalSet {
		done := p.terminalDone
		p.mu.Unlock()
		<-done
		return false
	}
	p.terminalSet = true
	p.terminalDone = make(chan struct{})
	p.closed = true
	cancel, done := p.progressCancel, p.progressDone
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if status != nil {
		status.BeforeTerminal()
	}
	if done != nil {
		<-done
	}
	return true
}

func (p *slackPresenter) finishStatus(status *slackRunStatus, result slackTerminalResult) slackTerminalResult {
	if p.hooks.TerminalFinalized != nil {
		p.hooks.TerminalFinalized(result)
	}
	if status != nil {
		status.Finish(result.ConfirmedNewReply)
	}
	p.mu.Lock()
	p.result = result
	close(p.terminalDone)
	p.mu.Unlock()
	return result
}

func (p *slackPresenter) progressTimestamp() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.progressTS
}

func (p *slackPresenter) terminalParent() context.Context {
	if p.terminalContext != nil {
		return p.terminalContext
	}
	return context.Background()
}

func (p *slackPresenter) cleanup() bool {
	ts := p.progressTimestamp()
	if ts == "" {
		return false
	}
	callCtx, cancel := context.WithTimeout(p.terminalParent(), slackProgressTimeout)
	defer cancel()
	if err := p.sender.DeleteThreadMessageContext(callCtx, p.channel, ts); err != nil {
		slog.Warn("server: Slack progress cleanup remains pending")
		return true
	}
	return false
}

func (p *slackPresenter) post(ctx context.Context, body string) error {
	callCtx, cancel := context.WithTimeout(ctx, slackAnswerCallTimeout)
	defer cancel()
	_, err := p.sender.PostThreadTextContext(callCtx, p.channel, p.threadTS, body)
	return err
}

func (p *slackPresenter) standaloneNotice(body string, outcome slackTerminalOutcome) slackTerminalResult {
	result := slackTerminalResult{Outcome: outcome, ProgressTimestamp: p.progressTimestamp()}
	if p.hooks.NoticeAttempting != nil {
		if err := p.hooks.NoticeAttempting(); err != nil {
			slog.Warn("server: Slack notice checkpoint update failed")
			result.Disposition = slackDispositionPending
			return result
		}
	}
	ctx, cancel := context.WithTimeout(p.terminalParent(), slackAnswerCallTimeout)
	defer cancel()
	result.NoticeAttempted = true
	if err := p.post(ctx, body); err != nil {
		result.Disposition = slackErrorDisposition(err)
		return result
	}
	result.Disposition = slackDispositionHandled
	result.ConfirmedNewReply = true
	return result
}

func (p *slackPresenter) editNotice(body string, outcome slackTerminalOutcome) slackTerminalResult {
	ts := p.progressTimestamp()
	if ts == "" {
		return p.standaloneNotice(body, outcome)
	}
	ctx, cancel := context.WithTimeout(p.terminalParent(), slackAnswerCallTimeout)
	err := p.sender.EditThreadTextContext(ctx, p.channel, ts, body)
	cancel()
	if err == nil {
		return slackTerminalResult{Outcome: outcome, Disposition: slackDispositionHandled, ProgressTimestamp: ts}
	}
	if slackErrorDisposition(err) == slackDispositionPending {
		result := p.standaloneNotice(body, outcome)
		if !result.ConfirmedNewReply {
			result.CleanupPending = true
		}
		return result
	}
	return slackTerminalResult{Outcome: outcome, Disposition: slackDispositionUnconfirmed, ProgressTimestamp: ts, CleanupPending: true}
}

func slackErrorDisposition(err error) slackTerminalDisposition {
	var delivery *channels.SlackDeliveryError
	if errors.As(err, &delivery) && delivery.Rejected {
		return slackDispositionPending
	}
	return slackDispositionUnconfirmed
}

func (p *slackPresenter) Terminal(status *slackRunStatus, kind, model, answer string, alreadyAnswered bool) (slackTerminalResult, bool) {
	if !p.beforeTerminal(status) {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.result, false
	}
	ctx, cancel := context.WithTimeout(p.terminalParent(), p.terminalTimeout)
	defer cancel()
	var result slackTerminalResult
	switch {
	case alreadyAnswered:
		result = slackTerminalResult{Outcome: slackOutcomeAlreadyDone, Disposition: slackDispositionHandled, ProgressTimestamp: p.progressTimestamp(), CleanupPending: p.cleanup()}
	case kind == "done" && strings.TrimSpace(answer) != "" && !agent.ShouldDeliverReply(answer):
		result = slackTerminalResult{Outcome: slackOutcomeSilence, Disposition: slackDispositionHandled, ProgressTimestamp: p.progressTimestamp(), CleanupPending: p.cleanup()}
	case kind == "done" && strings.TrimSpace(answer) != "":
		result = p.answer(ctx, model, answer)
	default:
		body := "Unable to complete this request."
		outcome := slackOutcomeEmpty
		switch kind {
		case "stop":
			body = "Stopped."
			outcome = slackOutcomeStopped
		case "error":
			outcome = slackOutcomeNotice
		}
		result = p.editNotice(body, outcome)
		if result.ConfirmedNewReply {
			result.CleanupPending = p.cleanup()
		}
	}
	return p.finishStatus(status, result), true
}

func (p *slackPresenter) answer(ctx context.Context, model, answer string) slackTerminalResult {
	var deliveredParts int
	var deliveryErr error
	if shouldAttachSlackAnswer(answer) {
		intro := "Full answer attached."
		if p.summarize != nil && time.Until(deadlineOf(ctx)) > 25*time.Second {
			summaryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			if summary, err := p.summarize(summaryCtx, model, answer); err == nil && summary != "" {
				intro = summary
			}
			cancel()
		}
		callCtx, cancel := context.WithTimeout(ctx, slackAnswerCallTimeout)
		deliveryErr = p.sender.ShareThreadMarkdownFileContext(callCtx, p.channel, p.threadTS, intro, answer)
		cancel()
		if deliveryErr == nil {
			deliveredParts = 1
		}
		if deliveryErr != nil {
			var fileErr *channels.SlackFileShareError
			if errors.As(deliveryErr, &fileErr) && fileErr.SafeFallback {
				deliveryErr = nil
			} else {
				return p.failedAnswer(ctx, 0, deliveryErr)
			}
		}
	}
	if deliveredParts == 0 {
		for _, part := range splitSlackPlainText(answer, 3900) {
			if err := p.post(ctx, part); err != nil {
				deliveryErr = err
				break
			}
			deliveredParts++
		}
	}
	if deliveryErr != nil {
		return p.failedAnswer(ctx, deliveredParts, deliveryErr)
	}
	return slackTerminalResult{Outcome: slackOutcomeAnswer, Disposition: slackDispositionHandled, ConfirmedNewReply: true,
		ProgressTimestamp: p.progressTimestamp(), CleanupPending: p.cleanup()}
}

func deadlineOf(ctx context.Context) time.Time { deadline, _ := ctx.Deadline(); return deadline }

func (p *slackPresenter) failedAnswer(_ context.Context, deliveredParts int, err error) slackTerminalResult {
	body := "Unable to deliver the answer."
	outcome := slackOutcomeDeliveryFailed
	if deliveredParts > 0 {
		body, outcome = "Answer incomplete.", slackOutcomePartial
	}
	var delivery *channels.SlackDeliveryError
	if !errors.As(err, &delivery) || !delivery.Rejected {
		body = "Answer delivery could not be confirmed."
		outcome = slackOutcomeUnconfirmed
		if deliveredParts > 0 {
			body = "Answer incomplete; delivery could not be confirmed."
		}
	}
	var result slackTerminalResult
	if deliveredParts > 0 {
		// The notice belongs after the accepted parts. A known progress
		// message can only be promoted when the new notice did not land.
		result = p.standaloneNotice(body, outcome)
		result.ConfirmedNewReply = true
		if result.Disposition == slackDispositionHandled {
			result.CleanupPending = p.cleanup()
		} else if p.progressTimestamp() != "" {
			// Editing a known message cannot create another reply, even when
			// acceptance of the standalone notice remains uncertain.
			ts := p.progressTimestamp()
			editCtx, cancel := context.WithTimeout(p.terminalParent(), slackAnswerCallTimeout)
			editErr := p.sender.EditThreadTextContext(editCtx, p.channel, ts, body)
			cancel()
			result.CleanupPending = editErr != nil
			if editErr == nil && result.Disposition == slackDispositionPending {
				result.Disposition = slackDispositionHandled
			}
		}
		return result
	}
	result = p.editNotice(body, outcome)
	if result.ConfirmedNewReply {
		result.CleanupPending = p.cleanup()
	}
	return result
}
