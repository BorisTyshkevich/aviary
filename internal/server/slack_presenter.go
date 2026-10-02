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
	slackProgressDelay       = time.Second
	slackProgressEditGap     = 2 * time.Second
	slackProgressTimeout     = 5 * time.Second
	slackProgressFlushBudget = 10 * time.Second
	slackTerminalTimeout     = 45 * time.Second
	slackAnswerCallTimeout   = 15 * time.Second
	slackProgressMaxCalls    = 100
	slackProgressMaxText     = 2800
)

type slackPresenterSender = channels.SlackReplySender

type slackPublicCall struct {
	name, id, state, detail string
	duration                time.Duration
	page                    int
}

type slackProgressPage struct {
	calls     []int
	ts        string
	attempted bool
	sentBody  string
	version   int
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
	slackOutcomeInterrupted    slackTerminalOutcome = "interrupted"
	slackOutcomeEmpty          slackTerminalOutcome = "empty"
	slackOutcomeDeliveryFailed slackTerminalOutcome = "delivery_failed"
	slackOutcomePartial        slackTerminalOutcome = "partial"
	slackOutcomeUnconfirmed    slackTerminalOutcome = "unconfirmed"
)

// slackTerminalResult records delivery certainty and temporary-message cleanup.
type slackTerminalResult struct {
	Outcome            slackTerminalOutcome
	Disposition        slackTerminalDisposition
	ConfirmedNewReply  bool
	ProgressTimestamps []string
	PromotedProgressTS string
	CleanupPending     bool
	// NoticeAttempted is the durable uncertainty marker for a standalone post.
	// A definite rejection clears it even though an HTTP attempt occurred.
	NoticeAttempted bool
}

// slackPresenterHooks mark the persistence boundaries needed by recovery.
// Hooks persist accepted progress and terminal outcomes around Slack writes.
type slackPresenterHooks struct {
	ProgressCreated   func(string) error
	NoticeAttempting  func() error
	TerminalAccepted  func(slackTerminalResult) error
	TerminalFinalized func(slackTerminalResult)
}

type slackPresenter struct {
	sender                 slackPresenterSender
	channel                string
	threadTS               string
	progressOn             bool
	maxCalls               int
	maxChars               int
	summarize              func(context.Context, string, string) (string, error)
	terminalContextFactory func(time.Duration) (context.Context, context.CancelFunc)
	delay                  time.Duration
	editGap                time.Duration
	terminalTimeout        time.Duration
	flushBudget            time.Duration

	mu                sync.Mutex
	calls             []slackPublicCall
	pages             []slackProgressPage
	more              int
	progressFailed    bool
	lastProgressWrite time.Time
	closed            bool
	terminalSet       bool
	terminalDone      chan struct{}
	result            slackTerminalResult
	wake              chan struct{}
	progressCancel    context.CancelFunc
	progressDone      chan struct{}
	hooks             slackPresenterHooks
}

func newSlackPresenter(sender slackPresenterSender, channel, threadTS string, progressOn bool) *slackPresenter {
	return &slackPresenter{sender: sender, channel: channel, threadTS: threadTS,
		progressOn: progressOn, maxCalls: slackProgressMaxCalls, maxChars: slackProgressMaxText,
		delay: slackProgressDelay, editGap: slackProgressEditGap,
		terminalTimeout: slackTerminalTimeout, flushBudget: slackProgressFlushBudget,
		wake: make(chan struct{}, 1)}
}

func (p *slackPresenter) Tool(event agent.PublicToolEvent) {
	if p == nil || !p.progressOn || event.Name == "" || event.InvocationID == "" ||
		(event.State != agent.ToolStateStarted && event.State != agent.ToolStateSucceeded && event.State != agent.ToolStateFailed) {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.progressFailed {
		return
	}
	for i := range p.calls {
		if p.calls[i].id == event.InvocationID {
			p.calls[i].state = string(event.State)
			p.calls[i].duration = event.Duration
			p.pages[p.calls[i].page].version++
			p.signal()
			return
		}
	}
	if len(p.calls) >= p.maxCalls {
		if event.State == agent.ToolStateStarted {
			p.more++
			if len(p.pages) > 0 {
				p.pages[len(p.pages)-1].version++
				p.signal()
			}
		}
		return
	}
	call := slackPublicCall{
		name: trimSlackProgress(slackEscapeToolName(event.Name), 180), id: event.InvocationID,
		state: string(event.State), detail: trimSlackProgress(slackEscapeToolName(event.Detail), min(1000, max(0, p.maxChars-350))),
		duration: event.Duration,
	}
	if len(p.pages) == 0 {
		p.pages = append(p.pages, slackProgressPage{})
	}
	page := len(p.pages) - 1
	if len(p.pages[page].calls) > 0 && p.pageBudgetLocked(page)+p.callBudget(call, len(p.calls)+1) > p.maxChars {
		p.pages = append(p.pages, slackProgressPage{})
		page++
	}
	call.page = page
	p.calls = append(p.calls, call)
	p.pages[page].calls = append(p.pages[page].calls, len(p.calls)-1)
	p.pages[page].version++
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

func trimSlackProgress(value string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(value) <= maxBytes {
		return value
	}
	var b strings.Builder
	for _, r := range value {
		if b.Len()+len(string(r))+3 > maxBytes {
			break
		}
		b.WriteRune(r)
	}
	b.WriteString("...")
	return b.String()
}

func (p *slackPresenter) progressLine(call slackPublicCall, number int) string {
	line := fmt.Sprintf("\n• %d. %s %s", number, call.name, call.state)
	if call.duration > 0 && call.state != string(agent.ToolStateStarted) {
		line += " (" + call.duration.Round(time.Millisecond).String() + ")"
	}
	if call.detail != "" {
		line += "\n  " + call.detail
	}
	return line
}

func (p *slackPresenter) callBudget(call slackPublicCall, number int) int {
	return len(p.progressLine(call, number)) + 40 // reserve for a later state and duration
}

func (p *slackPresenter) pageBudgetLocked(page int) int {
	size := len("Tool progress (continued)") + 70 // reserve the configured-cap notice
	for _, i := range p.pages[page].calls {
		size += p.callBudget(p.calls[i], i+1)
	}
	return size
}

func (p *slackPresenter) progressBodyLocked(page int) string {
	var b strings.Builder
	b.WriteString("Tool progress")
	if page > 0 {
		b.WriteString(" (continued)")
	}
	for _, i := range p.pages[page].calls {
		b.WriteString(p.progressLine(p.calls[i], i+1))
	}
	if p.more > 0 && page == len(p.pages)-1 {
		fmt.Fprintf(&b, "\n… %d additional calls beyond configured cap", p.more)
	}
	return trimSlackProgress(b.String(), p.maxChars)
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
	var lastWrite time.Time
	for {
		p.mu.Lock()
		if p.closed || p.progressFailed {
			p.mu.Unlock()
			return
		}
		pageIndex := -1
		for i := range p.pages {
			if !p.pages[i].attempted || p.pages[i].version > 0 && p.progressBodyLocked(i) != p.pages[i].sentBody {
				pageIndex = i
				break
			}
		}
		if pageIndex < 0 {
			p.mu.Unlock()
			select {
			case <-ctx.Done():
				return
			case <-p.wake:
				continue
			}
		}
		body := p.progressBodyLocked(pageIndex)
		ts, attempted := p.pages[pageIndex].ts, p.pages[pageIndex].attempted
		p.mu.Unlock()
		if wait := time.Until(lastWrite.Add(p.editGap)); wait > 0 {
			timer.Reset(wait)
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
		}
		if ctx.Err() != nil {
			return
		}
		if !attempted {
			// A dispatched write must finish before terminal delivery. Canceling
			// the scheduling context stops future writes, not this HTTP call.
			if ctx.Err() != nil {
				return
			}
			p.mu.Lock()
			p.pages[pageIndex].attempted = true
			p.mu.Unlock()
			callCtx, cancel := context.WithTimeout(channels.WithSlackPreDispatchStop(context.Background(), ctx.Done()), slackProgressTimeout)
			created, err := p.sender.PostThreadTextContext(callCtx, p.channel, p.threadTS, body)
			cancel()
			if err != nil {
				p.mu.Lock()
				// The stop gate can reject a queued request after we mark it
				// attempted. No write reached Slack, so terminal flush may try it.
				if ctx.Err() != nil && slackErrorDisposition(err) == slackDispositionPending {
					p.pages[pageIndex].attempted = false
				} else {
					p.progressFailed = true
				}
				p.mu.Unlock()
				return
			}
			p.mu.Lock()
			p.pages[pageIndex].ts = created
			p.pages[pageIndex].sentBody = body
			p.lastProgressWrite = time.Now()
			p.mu.Unlock()
			if p.hooks.ProgressCreated != nil {
				if err := p.hooks.ProgressCreated(created); err != nil {
					slog.Warn("server: Slack progress checkpoint update failed")
					p.mu.Lock()
					p.progressFailed = true
					p.mu.Unlock()
					return
				}
			}
			lastWrite = time.Now()
			continue
		}
		callCtx, cancel := context.WithTimeout(channels.WithSlackPreDispatchStop(context.Background(), ctx.Done()), slackProgressTimeout)
		err := p.sender.EditThreadTextContext(callCtx, p.channel, ts, body)
		cancel()
		if err != nil {
			slog.Debug("server: Slack progress edit unavailable")
		}
		p.mu.Lock()
		p.pages[pageIndex].sentBody = body // retry only after a subsequent tool event
		p.lastProgressWrite = time.Now()
		p.mu.Unlock()
		lastWrite = time.Now()
	}
}

// flushTerminalProgress gives queued pages a finite chance to appear before
// the final reply. An attempted page is never posted again after uncertainty.
func (p *slackPresenter) flushTerminalProgress() {
	if !p.progressOn {
		return
	}
	budgetCtx, budgetCancel := p.operationContext(p.flushBudget)
	defer budgetCancel()
	for {
		p.mu.Lock()
		if p.progressFailed {
			p.mu.Unlock()
			return
		}
		index := -1
		for i := range p.pages {
			if !p.pages[i].attempted {
				index = i
				break
			}
		}
		if index < 0 {
			p.mu.Unlock()
			return
		}
		body := p.progressBodyLocked(index)
		lastWrite := p.lastProgressWrite
		p.mu.Unlock()
		if wait := time.Until(lastWrite.Add(min(time.Second, p.editGap))); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-budgetCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
		if budgetCtx.Err() != nil {
			return
		}
		if deadline, ok := budgetCtx.Deadline(); ok && time.Until(deadline) < slackProgressTimeout {
			return
		}
		p.mu.Lock()
		p.pages[index].attempted = true
		p.mu.Unlock()
		// The flush budget limits new posts. Once dispatched, this call keeps
		// its own deadline so terminal delivery can capture an accepted TS.
		callCtx, cancel := context.WithTimeout(channels.WithSlackPreDispatchStop(context.Background(), budgetCtx.Done()), slackProgressTimeout)
		ts, err := p.sender.PostThreadTextContext(callCtx, p.channel, p.threadTS, body)
		cancel()
		if err != nil {
			p.mu.Lock()
			p.progressFailed = true
			p.mu.Unlock()
			return
		}
		p.mu.Lock()
		p.pages[index].ts = ts
		p.pages[index].sentBody = body
		p.lastProgressWrite = time.Now()
		p.mu.Unlock()
		if p.hooks.ProgressCreated != nil {
			if err := p.hooks.ProgressCreated(ts); err != nil {
				slog.Warn("server: Slack progress checkpoint update failed")
				p.mu.Lock()
				p.progressFailed = true
				p.mu.Unlock()
				return
			}
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
	p.flushTerminalProgress()
	return true
}

func (p *slackPresenter) finishStatus(status *slackRunStatus, result slackTerminalResult) slackTerminalResult {
	if result.ProgressTimestamps == nil {
		result.ProgressTimestamps = p.progressTimestamps()
	}
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
	for i := len(p.pages) - 1; i >= 0; i-- {
		if p.pages[i].ts != "" {
			return p.pages[i].ts
		}
	}
	return ""
}

func (p *slackPresenter) progressTimestamps() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var timestamps []string
	for _, page := range p.pages {
		if page.ts != "" {
			timestamps = append(timestamps, page.ts)
		}
	}
	return timestamps
}

func (p *slackPresenter) operationContext(budget time.Duration) (context.Context, context.CancelFunc) {
	if p.terminalContextFactory != nil {
		return p.terminalContextFactory(budget)
	}
	return context.WithTimeout(context.Background(), budget)
}

func (p *slackPresenter) cleanup(skip string) bool {
	timestamps := p.progressTimestamps()
	if len(timestamps) == 0 {
		return false
	}
	budgetCtx, budgetCancel := p.operationContext(slackTerminalTimeout)
	defer budgetCancel()
	pending := false
	for _, ts := range timestamps {
		if ts == skip {
			continue
		}
		if budgetCtx.Err() != nil {
			pending = true
			break
		}
		callCtx, cancel := context.WithTimeout(budgetCtx, slackProgressTimeout)
		err := p.sender.DeleteThreadMessageContext(callCtx, p.channel, ts)
		cancel()
		if err != nil {
			slog.Warn("server: Slack progress cleanup remains pending")
			pending = true
		}
	}
	return pending
}

func (p *slackPresenter) accepted(result slackTerminalResult, cleanupProgress bool) slackTerminalResult {
	result.ProgressTimestamps = p.progressTimestamps()
	if cleanupProgress || result.PromotedProgressTS != "" {
		for _, ts := range result.ProgressTimestamps {
			if ts != result.PromotedProgressTS {
				result.CleanupPending = true
				break
			}
		}
	}
	if p.hooks.TerminalAccepted != nil {
		if err := p.hooks.TerminalAccepted(result); err != nil {
			slog.Warn("server: Slack accepted terminal checkpoint update failed")
			return result // keep known progress for later cleanup/recovery
		}
	}
	if result.CleanupPending {
		result.CleanupPending = p.cleanup(result.PromotedProgressTS)
	}
	return result
}

func (p *slackPresenter) post(ctx context.Context, body string) error {
	callCtx, cancel := context.WithTimeout(ctx, slackAnswerCallTimeout)
	defer cancel()
	_, err := p.sender.PostThreadTextContext(callCtx, p.channel, p.threadTS, body)
	return err
}

func (p *slackPresenter) standaloneNotice(body string, outcome slackTerminalOutcome) slackTerminalResult {
	result := slackTerminalResult{Outcome: outcome, ProgressTimestamps: p.progressTimestamps()}
	if p.hooks.NoticeAttempting != nil {
		if err := p.hooks.NoticeAttempting(); err != nil {
			slog.Warn("server: Slack notice checkpoint update failed")
			result.Disposition = slackDispositionPending
			return result
		}
	}
	ctx, cancel := p.operationContext(slackAnswerCallTimeout)
	defer cancel()
	if err := p.post(ctx, body); err != nil {
		result.Disposition = slackErrorDisposition(err)
		result.NoticeAttempted = result.Disposition == slackDispositionUnconfirmed
		return result
	}
	result.Disposition = slackDispositionHandled
	result.ConfirmedNewReply = true
	return p.accepted(result, true)
}

func (p *slackPresenter) editNotice(body string, outcome slackTerminalOutcome) slackTerminalResult {
	ts := p.progressTimestamp()
	if ts == "" {
		return p.standaloneNotice(body, outcome)
	}
	ctx, cancel := p.operationContext(slackAnswerCallTimeout)
	err := p.sender.EditThreadTextContext(ctx, p.channel, ts, body)
	cancel()
	if err == nil {
		return p.accepted(slackTerminalResult{Outcome: outcome, Disposition: slackDispositionHandled, PromotedProgressTS: ts}, false)
	}
	if slackErrorDisposition(err) == slackDispositionPending {
		result := p.standaloneNotice(body, outcome)
		if !result.ConfirmedNewReply {
			result.CleanupPending = len(result.ProgressTimestamps) > 0
		}
		return result
	}
	return slackTerminalResult{Outcome: outcome, Disposition: slackDispositionUnconfirmed, ProgressTimestamps: p.progressTimestamps(), CleanupPending: true}
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
	ctx, cancel := p.operationContext(p.terminalTimeout)
	defer cancel()
	var result slackTerminalResult
	switch {
	case alreadyAnswered:
		result = p.accepted(slackTerminalResult{Outcome: slackOutcomeAlreadyDone, Disposition: slackDispositionHandled}, true)
	case kind == "done" && strings.TrimSpace(answer) != "" && !agent.ShouldDeliverReply(answer):
		result = p.accepted(slackTerminalResult{Outcome: slackOutcomeSilence, Disposition: slackDispositionHandled}, true)
	case kind == "done" && strings.TrimSpace(answer) != "":
		result = p.answer(ctx, model, answer)
	default:
		body := "Unable to complete this request."
		outcome := slackOutcomeEmpty
		switch kind {
		case "stop":
			body = "Stopped."
			outcome = slackOutcomeStopped
		case "interrupted":
			body = "Interrupted; please resend your request."
			outcome = slackOutcomeInterrupted
		case "error":
			outcome = slackOutcomeNotice
		}
		result = p.editNotice(body, outcome)
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
	return p.accepted(slackTerminalResult{Outcome: slackOutcomeAnswer, Disposition: slackDispositionHandled, ConfirmedNewReply: true}, true)
}

func deadlineOf(ctx context.Context) time.Time { deadline, _ := ctx.Deadline(); return deadline }

func (p *slackPresenter) failedAnswer(_ context.Context, deliveredParts int, err error) slackTerminalResult {
	answerUnconfirmed := slackErrorDisposition(err) == slackDispositionUnconfirmed
	body := "Unable to deliver the answer."
	outcome := slackOutcomeDeliveryFailed
	if deliveredParts > 0 {
		body, outcome = "Answer incomplete.", slackOutcomePartial
	}
	if answerUnconfirmed {
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
		if result.Disposition != slackDispositionHandled && p.progressTimestamp() != "" {
			// Editing a known message cannot create another reply, even when
			// acceptance of the standalone notice remains uncertain.
			ts := p.progressTimestamp()
			editCtx, cancel := p.operationContext(slackAnswerCallTimeout)
			editErr := p.sender.EditThreadTextContext(editCtx, p.channel, ts, body)
			cancel()
			result.CleanupPending = editErr != nil
			if editErr == nil {
				result.Disposition = slackDispositionHandled
				result.PromotedProgressTS = ts
				result = p.accepted(result, false)
			}
		}
		if answerUnconfirmed && result.Disposition != slackDispositionHandled {
			result.Disposition = slackDispositionUnconfirmed
		}
		return result
	}
	result = p.editNotice(body, outcome)
	if answerUnconfirmed && result.Disposition != slackDispositionHandled {
		result.Disposition = slackDispositionUnconfirmed
	}
	return result
}
