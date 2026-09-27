package agent

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lsegal/aviary/internal/store"
)

const (
	maxCheckpointRecoveryRetries = 3
	checkpointRecoveryCooldown   = time.Minute
)

// RunCheckpoint stores the state of an in-flight agent prompt so it can be
// resumed after a server restart or config reload. Replayable records are
// retired on completion; Slack records remain until terminal delivery and
// temporary-message cleanup are durably handled. Only replayable records use
// the configured failed_task_timeout recovery policy.
type RunCheckpoint struct {
	// AgentName is the name of the agent that owns this checkpoint.
	AgentName string `json:"agent_name"`
	// SessionID is the session the prompt was running in.
	SessionID string `json:"session_id"`
	// PromptMessageID identifies the user message for replayable runs. The
	// checkpoint filename is the independent run identity.
	PromptMessageID string `json:"prompt_message_id,omitempty"`
	// Message is the original user message to re-issue.
	Message string `json:"message,omitempty"`
	// MediaURL is an optional media attachment for the message.
	MediaURL string `json:"media_url,omitempty"`
	// Overrides holds any per-run model/tool overrides that were active.
	Overrides *RunOverrides `json:"overrides,omitempty"`
	// Slack records are non-replayable and carry only the original delivery target.
	Slack *SlackCheckpoint `json:"slack,omitempty"`
	// CreatedAt is when the checkpoint was written (prompt start time).
	CreatedAt time.Time `json:"created_at"`
	// RetryCount tracks how many times this checkpoint has been re-issued.
	RetryCount int `json:"retry_count,omitempty"`
	// LastRecoveredAt is when the checkpoint was last re-issued.
	LastRecoveredAt time.Time `json:"last_recovered_at,omitempty"`
}

// requiresTrustedIngress reports whether recovery would need the original
// channel consumer or caller identity, neither of which survives a restart.
func (cp RunCheckpoint) requiresTrustedIngress() bool {
	return cp.Overrides != nil && (cp.Overrides.DeferAnswerPersistence || cp.Overrides.SuppressDelivery)
}

// SlackDisposition records whether terminal delivery was handled, remains
// pending, or may have been accepted without a reliable acknowledgement.
type SlackDisposition string

const (
	// SlackDispositionPending means no terminal delivery is confirmed.
	SlackDispositionPending SlackDisposition = "pending"
	// SlackDispositionHandled means terminal delivery or intentional silence is confirmed.
	SlackDispositionHandled SlackDisposition = "handled"
	// SlackDispositionUnconfirmed means a write may have been accepted.
	SlackDispositionUnconfirmed SlackDisposition = "unconfirmed"
)

// SlackCheckpoint contains only routing and delivery bookkeeping. Prompt,
// media, answer, raw tool data, and credentials never belong in this record.
type SlackCheckpoint struct {
	InstallationID  string           `json:"installation_id"`
	WorkspaceID     string           `json:"workspace_id"`
	ConfiguredID    string           `json:"configured_id"`
	ChannelID       string           `json:"channel_id"`
	RootThreadTS    string           `json:"root_thread_ts"`
	ProgressTS      string           `json:"progress_ts,omitempty"`
	ProgressExtraTS []string         `json:"progress_extra_ts,omitempty"`
	Disposition     SlackDisposition `json:"disposition"`
	CleanupPending  bool             `json:"cleanup_pending,omitempty"`
	NoticeAttempted bool             `json:"notice_attempted,omitempty"`
}

// ProgressTimestamps returns accepted progress pages in their creation order.
func (meta *SlackCheckpoint) ProgressTimestamps() []string {
	if meta == nil || meta.ProgressTS == "" {
		return nil
	}
	result := make([]string, 0, 1+len(meta.ProgressExtraTS))
	result = append(result, meta.ProgressTS)
	return append(result, meta.ProgressExtraTS...)
}

// CheckpointHandle serializes updates to one run's existing checkpoint file.
// It is runtime-only and is never included in checkpoint JSON.
type CheckpointHandle struct {
	mu          sync.Mutex
	path        string
	checkpoint  RunCheckpoint
	initialized bool
}

// NewSlackCheckpointHandle prepares metadata for a non-replayable Slack run.
func NewSlackCheckpointHandle(meta SlackCheckpoint) *CheckpointHandle {
	meta.Disposition = SlackDispositionPending
	return &CheckpointHandle{checkpoint: RunCheckpoint{Slack: &meta}}
}

// Initialized distinguishes an initial checkpoint failure from later failures.
// The former has no durable notice marker and needs a fixed direct reply.
func (h *CheckpointHandle) Initialized() bool {
	if h == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.initialized
}

func (h *CheckpointHandle) bind(path string, cp RunCheckpoint) error {
	if h == nil {
		return errors.New("slack checkpoint handle is unavailable")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.checkpoint.Slack == nil {
		return errors.New("slack checkpoint handle is unavailable")
	}
	if h.initialized {
		return errors.New("slack checkpoint handle is already bound")
	}
	meta := *h.checkpoint.Slack
	if cp.AgentName == "" || cp.SessionID == "" {
		return errors.New("slack checkpoint run identity is incomplete")
	}
	if meta.InstallationID == "" || meta.WorkspaceID == "" || meta.ChannelID == "" || meta.RootThreadTS == "" {
		return errors.New("slack checkpoint target is incomplete")
	}
	cp.Message, cp.MediaURL, cp.Overrides = "", "", nil
	cp.Slack = &meta
	if err := store.WriteJSON(path, &cp); err != nil {
		return fmt.Errorf("writing Slack checkpoint: %w", err)
	}
	h.path, h.checkpoint, h.initialized = path, cp, true
	return nil
}

func (h *CheckpointHandle) update(change func(*SlackCheckpoint) error) error {
	if h == nil {
		return errors.New("slack checkpoint handle is unavailable")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.initialized || h.checkpoint.Slack == nil {
		return errors.New("slack checkpoint is not initialized")
	}
	next := h.checkpoint
	meta := *next.Slack
	meta.ProgressExtraTS = append([]string(nil), meta.ProgressExtraTS...)
	if err := change(&meta); err != nil {
		return err
	}
	next.Slack = &meta
	if err := store.WriteJSON(h.path, &next); err != nil {
		return fmt.Errorf("updating Slack checkpoint: %w", err)
	}
	h.checkpoint = next
	return nil
}

// RecordProgressTimestamp persists an accepted progress message ID in page order.
func (h *CheckpointHandle) RecordProgressTimestamp(ts string) error {
	return h.update(func(meta *SlackCheckpoint) error {
		if ts == "" {
			return errors.New("invalid Slack progress timestamp")
		}
		if meta.ProgressTS == "" {
			meta.ProgressTS = ts
		} else if meta.ProgressTS != ts {
			for _, existing := range meta.ProgressExtraTS {
				if existing == ts {
					return nil
				}
			}
			meta.ProgressExtraTS = append(meta.ProgressExtraTS, ts)
		}
		meta.CleanupPending = true
		return nil
	})
}

// RecordNoticeAttempt persists uncertainty before a standalone HTTP post.
func (h *CheckpointHandle) RecordNoticeAttempt() error {
	return h.update(func(meta *SlackCheckpoint) error {
		meta.NoticeAttempted = true
		if meta.Disposition != SlackDispositionHandled {
			// If the process dies after this write, acceptance is unknown.
			meta.Disposition = SlackDispositionUnconfirmed
		}
		return nil
	})
}

// RecordTerminal atomically merges known progress pages and terminal state.
// A page promoted into a confirmed fixed notice is removed from cleanup
// ownership in the same write. A handled outcome can never be downgraded.
func (h *CheckpointHandle) RecordTerminal(disposition SlackDisposition, cleanupPending bool, progressTimestamps []string, promotedProgressTS string, noticeAttempted bool) error {
	return h.update(func(meta *SlackCheckpoint) error {
		switch disposition {
		case SlackDispositionPending, SlackDispositionHandled, SlackDispositionUnconfirmed:
		default:
			return errors.New("invalid Slack terminal disposition")
		}
		if meta.Disposition == SlackDispositionHandled && disposition != SlackDispositionHandled {
			return nil
		}
		seen := make(map[string]bool, len(progressTimestamps)+1+len(meta.ProgressExtraTS))
		merged := make([]string, 0, len(progressTimestamps)+1+len(meta.ProgressExtraTS))
		for _, ts := range append(append([]string(nil), progressTimestamps...), meta.ProgressTimestamps()...) {
			if ts == "" {
				return errors.New("invalid Slack progress timestamp")
			}
			if seen[ts] {
				continue
			}
			merged = append(merged, ts)
			seen[ts] = true
		}
		if disposition == SlackDispositionHandled && promotedProgressTS != "" {
			if !seen[promotedProgressTS] {
				return errors.New("promoted Slack progress timestamp is unknown")
			}
			kept := merged[:0]
			for _, ts := range merged {
				if ts != promotedProgressTS {
					kept = append(kept, ts)
				}
			}
			merged = kept
		}
		meta.ProgressTS, meta.ProgressExtraTS = "", nil
		if len(merged) > 0 {
			meta.ProgressTS, meta.ProgressExtraTS = merged[0], merged[1:]
		}
		if cleanupPending && meta.ProgressTS == "" {
			return errors.New("slack cleanup is pending without a progress timestamp")
		}
		if meta.Disposition == SlackDispositionHandled {
			if disposition == SlackDispositionHandled {
				meta.NoticeAttempted = noticeAttempted
				if meta.CleanupPending {
					meta.CleanupPending = cleanupPending
				}
			}
			return nil
		}
		meta.Disposition = disposition
		meta.CleanupPending = cleanupPending
		meta.NoticeAttempted = noticeAttempted
		return nil
	})
}

func (h *CheckpointHandle) retireIfHandled() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.initialized || h.checkpoint.Slack.Disposition != SlackDispositionHandled || h.checkpoint.Slack.CleanupPending {
		return nil
	}
	return store.DeleteJSON(h.path)
}
