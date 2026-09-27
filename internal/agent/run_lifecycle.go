package agent

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/lsegal/aviary/internal/store"
)

// AdmissionStatus reports whether a runner took ownership of a prompt.
type AdmissionStatus string

// Admission status values.
const (
	AdmissionAccepted         AdmissionStatus = "accepted"
	AdmissionRejectedStopping AdmissionStatus = "rejected_stopping"
)

// RunAdmission records ownership. Stream callbacks may begin before the
// submission call returns, so callers must synchronize optional UI startup.
type RunAdmission struct {
	Status AdmissionStatus
	RunID  string
}

// StopCause distinguishes an explicit request stop from runner replacement or shutdown.
type StopCause string

// Stop cause values.
const (
	StopCauseUser   StopCause = "user_stop"
	StopCauseRunner StopCause = "runner_stop"
)

type runCancellation struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
}

var errRunnerStopped = errors.New("runner stopped")
var errUserStopped = errors.New("user stopped")

func (r *runCancellation) stop(cause StopCause) {
	if cause == StopCauseRunner {
		r.cancel(errRunnerStopped)
		return
	}
	r.cancel(errUserStopped)
}

func (r *runCancellation) stopCause() StopCause {
	if errors.Is(context.Cause(r.ctx), errRunnerStopped) {
		return StopCauseRunner
	}
	return StopCauseUser // caller/request context was canceled first
}

// liveCheckpoints excludes same-process recovery while an earlier Server still
// owns a run. Entries remain until its terminal callback and checkpoint teardown
// have returned, including after a bounded shutdown drain expires.
var liveCheckpoints = struct {
	sync.Mutex
	paths      map[string]*liveCheckpoint
	recovering map[string]struct{}
	pending    map[string]func()
	retired    map[string]struct{}
}{paths: make(map[string]*liveCheckpoint), recovering: make(map[string]struct{}), pending: make(map[string]func()), retired: make(map[string]struct{})}

type liveCheckpoint struct {
	owners int
}

func checkpointKey(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		return filepath.Clean(path)
	}
	return filepath.Clean(abs)
}

func claimLiveCheckpoint(path string) (func(), bool) {
	key := checkpointKey(path)
	liveCheckpoints.Lock()
	if _, retired := liveCheckpoints.retired[key]; retired {
		liveCheckpoints.Unlock()
		return nil, false
	}
	entry := liveCheckpoints.paths[key]
	if entry == nil {
		entry = &liveCheckpoint{}
		liveCheckpoints.paths[key] = entry
	}
	entry.owners++
	liveCheckpoints.Unlock()
	return func() {
		liveCheckpoints.Lock()
		entry.owners--
		if entry.owners == 0 {
			delete(liveCheckpoints.paths, key)
		}
		if err := retireCheckpointIfIdleLocked(key); err != nil {
			slog.Warn("agent: failed to retire stopped checkpoint", "path", key, "err", err)
		}
		wake := wakeCheckpointRecoveryLocked(key)
		liveCheckpoints.Unlock()
		if wake != nil {
			go wake()
		}
	}, true
}

// ClaimCheckpointRecovery makes the live check and recovery decision atomic.
// The caller holds the returned release function through recovery read and
// handoff. If an old run or recovery owns the checkpoint, the newest pending
// callback replaces an earlier one and runs after ownership ends.
func ClaimCheckpointRecovery(path string, resume func()) (func(), bool) {
	key := checkpointKey(path)
	liveCheckpoints.Lock()
	if _, retired := liveCheckpoints.retired[key]; retired {
		liveCheckpoints.Unlock()
		return nil, false
	}
	_, recovering := liveCheckpoints.recovering[key]
	if liveCheckpoints.paths[key] != nil || recovering {
		liveCheckpoints.pending[key] = resume
		liveCheckpoints.Unlock()
		return nil, false
	}
	liveCheckpoints.recovering[key] = struct{}{}
	liveCheckpoints.Unlock()
	return func() {
		liveCheckpoints.Lock()
		delete(liveCheckpoints.recovering, key)
		if err := retireCheckpointIfIdleLocked(key); err != nil {
			slog.Warn("agent: failed to retire stopped checkpoint", "path", key, "err", err)
		}
		wake := wakeCheckpointRecoveryLocked(key)
		liveCheckpoints.Unlock()
		if wake != nil {
			go wake()
		}
	}, true
}

func wakeCheckpointRecoveryLocked(key string) func() {
	if _, retired := liveCheckpoints.retired[key]; retired {
		return nil
	}
	if liveCheckpoints.paths[key] != nil {
		return nil
	}
	if _, recovering := liveCheckpoints.recovering[key]; recovering {
		return nil
	}
	wake := liveCheckpoints.pending[key]
	delete(liveCheckpoints.pending, key)
	return wake
}

// RetireCheckpointsForUserStop suppresses stale recovery before deleting the
// selected checkpoint files. Live owners perform the deletion after their
// terminal callback, and a current recovery claim cannot hand off a new run
// once a matching checkpoint is retired.
func RetireCheckpointsForUserStop(agentID, sessionID string) error {
	dir := store.CheckpointDir(agentID)
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var failures []error
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		if sessionID != "" {
			cp, readErr := store.ReadJSON[RunCheckpoint](path)
			if readErr != nil {
				failures = append(failures, readErr)
				continue
			}
			if cp.SessionID != sessionID {
				continue
			}
		}
		if err := retireCheckpoint(path); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func retireCheckpoint(path string) error {
	key := checkpointKey(path)
	liveCheckpoints.Lock()
	defer liveCheckpoints.Unlock()
	liveCheckpoints.retired[key] = struct{}{}
	delete(liveCheckpoints.pending, key)
	return retireCheckpointIfIdleLocked(key)
}

func retireCheckpointIfIdleLocked(key string) error {
	if _, retired := liveCheckpoints.retired[key]; !retired {
		return nil
	}
	if liveCheckpoints.paths[key] != nil {
		return nil
	}
	if _, recovering := liveCheckpoints.recovering[key]; recovering {
		return nil
	}
	if err := store.DeleteJSON(key); err != nil {
		return err
	}
	delete(liveCheckpoints.retired, key)
	return nil
}

func checkpointRetired(path string) bool {
	liveCheckpoints.Lock()
	defer liveCheckpoints.Unlock()
	_, retired := liveCheckpoints.retired[checkpointKey(path)]
	return retired
}

func checkpointIsLive(path string) bool {
	liveCheckpoints.Lock()
	defer liveCheckpoints.Unlock()
	return liveCheckpoints.paths[checkpointKey(path)] != nil
}
