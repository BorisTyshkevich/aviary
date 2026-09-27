package agent

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/connections"
	"github.com/lsegal/aviary/internal/domain"
	"github.com/lsegal/aviary/internal/llm"
	"github.com/lsegal/aviary/internal/preparation"
	"github.com/lsegal/aviary/internal/store"
)

// Manager maintains a registry of AgentRunners and reconciles them with config.
type Manager struct {
	mu          sync.RWMutex
	runners     map[string]*AgentRunner // keyed by agent name
	order       []string                // agent names in config entry order
	session     *SessionManager
	factory     *llm.Factory
	cfg         *config.Config // latest reconciled config, for checkpoint timeout
	connections *connections.Service
	preparation *preparation.Engine
	owned       map[*AgentRunner]struct{}
	recoveries  sync.WaitGroup
	stopped     bool
}

// SetPreparationEngine installs scoped artifact storage before reconciliation.
func (m *Manager) SetPreparationEngine(engine *preparation.Engine) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.preparation = engine
}

// SetConnectionService installs the thread lifecycle service before reconciliation.
func (m *Manager) SetConnectionService(service *connections.Service) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connections = service
	for _, runner := range m.runners {
		runner.mu.Lock()
		runner.connections = service
		runner.mu.Unlock()
	}
}

// NewManager creates a new Manager with an optional LLM factory.
func NewManager(factory *llm.Factory) *Manager {
	return &Manager{
		runners: make(map[string]*AgentRunner),
		owned:   make(map[*AgentRunner]struct{}),
		session: NewSessionManager(),
		factory: factory,
	}
}

// Reconcile idempotently adds, updates, or removes agents based on cfg.
// It is safe to call concurrently and from a config watcher goroutine.
func (m *Manager) Reconcile(cfg *config.Config) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return
	}

	m.cfg = cfg

	desired := make(map[string]*config.AgentConfig, len(cfg.Agents))
	for i := range cfg.Agents {
		ac := &cfg.Agents[i]
		desired[ac.Name] = ac
	}

	// Remove agents no longer in config.
	for name, runner := range m.runners {
		if _, ok := desired[name]; !ok {
			slog.Info("agent removed", "name", name)
			m.retireRunner(runner)
			delete(m.runners, name)
		}
	}

	// Rebuild order to match config entry order, dropping removed agents.
	newOrder := make([]string, 0, len(cfg.Agents))
	for i := range cfg.Agents {
		newOrder = append(newOrder, cfg.Agents[i].Name)
	}
	m.order = newOrder

	// Add or update agents.
	for name, ac := range desired {
		effectiveModel := config.EffectiveAgentModel(*ac, cfg.Models)
		effectiveFallbacks := config.EffectiveAgentFallbacks(*ac, cfg.Models)
		if existing, ok := m.runners[name]; ok {
			if existing.agent.Model == effectiveModel &&
				slices.Equal(existing.agent.Fallbacks, effectiveFallbacks) &&
				reflect.DeepEqual(existing.cfg, ac) {
				continue
			}
			slog.Info("agent updated", "name", name)
			m.retireRunner(existing)
		} else {
			slog.Info("agent started", "name", name)
		}
		a := &domain.Agent{
			ID:        name,
			Name:      name,
			Model:     effectiveModel,
			Fallbacks: effectiveFallbacks,
			State:     domain.AgentStateIdle,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		var provider llm.Provider
		if m.factory != nil {
			if p, err := m.factory.ForModel(effectiveModel); err == nil {
				provider = p
			} else if effectiveModel != "" {
				slog.Warn("failed to create LLM provider", "agent", name, "model", effectiveModel, "err", err)
			}
		}
		runner := NewAgentRunner(a, ac, provider, m.factory)
		runner.connections = m.connections
		runner.preparation = m.preparation
		m.runners[name] = runner
		m.owned[runner] = struct{}{}
		m.recoveries.Add(1)
		go func() {
			defer m.recoveries.Done()
			m.recoverCheckpoints(runner)
		}()
	}
}

// recoverCheckpoints scans the agent's running/ directory for interrupted
// prompt checkpoints and either re-issues them or sends a timeout notification.
func (m *Manager) recoverCheckpoints(runner *AgentRunner) {
	m.mu.RLock()
	stopped := m.stopped
	m.mu.RUnlock()
	if stopped || runner.Stopping() {
		return
	}
	timeout := config.DefaultFailedTaskTimeout
	m.mu.RLock()
	cfg := m.cfg
	m.mu.RUnlock()
	if cfg != nil {
		timeout = cfg.Server.EffectiveFailedTaskTimeout()
	}

	dir := store.CheckpointDir(runner.agent.ID)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return // no running/ dir or unreadable — nothing to recover
	}

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		m.mu.RLock()
		stopped = m.stopped
		m.mu.RUnlock()
		if stopped || runner.Stopping() {
			return
		}
		name := e.Name()
		release, claimed := ClaimCheckpointRecovery(path, func() {
			m.wakeCheckpointRecovery(runner, name, path, timeout)
		})
		if !claimed {
			continue
		}
		func() {
			defer release()
			m.recoverCheckpoint(runner, name, path, timeout)
		}()
	}
}

// wakeCheckpointRecovery retries one file once after a live owner exits. It
// does not rescan the directory or enqueue another wake behind a concurrent
// recovery, so retained checkpoints cannot cause an unbounded wake loop.
func (m *Manager) wakeCheckpointRecovery(runner *AgentRunner, name, path string, timeout time.Duration) {
	m.mu.Lock()
	if m.stopped || runner.Stopping() {
		m.mu.Unlock()
		return
	}
	m.recoveries.Add(1)
	m.mu.Unlock()
	defer m.recoveries.Done()
	release, claimed := ClaimCheckpointRecovery(path, nil)
	if !claimed {
		return
	}
	defer release()
	if runner.Stopping() {
		return
	}
	m.recoverCheckpoint(runner, name, path, timeout)
}

func (m *Manager) recoverCheckpoint(runner *AgentRunner, name, path string, timeout time.Duration) {
	cp, err := store.ReadJSON[RunCheckpoint](path)
	if err != nil {
		slog.Warn("agent: ignoring unreadable checkpoint", "path", path, "err", err)
		_ = store.DeleteJSON(path)
		return
	}
	if cp.requiresTrustedIngress() {
		// Deferred/suppressed turns depend on their original trusted channel
		// consumer. Replaying them here could expose private context or mark
		// an answer complete without delivering it.
		slog.Info("agent: checkpoint needs trusted ingress, notifying session",
			"agent", runner.agent.Name, "session", cp.SessionID)
		msg := "I was interrupted. Please resend your request if it is still needed."
		runner.appendSessionMessage(cp.SessionID, domain.MessageRoleAssistant, msg, "", "")
		deliverToSession(runner.agent.ID, cp.SessionID, msg)
		_ = store.DeleteJSON(path)
		return
	}

	age := time.Since(cp.CreatedAt)
	if age > timeout {
		slog.Info("agent: checkpoint timed out, notifying session",
			"agent", runner.agent.Name, "session", cp.SessionID, "age", age)
		msg := fmt.Sprintf("I was interrupted %s ago and the recovery window (%s) has passed. Please resend your request if it is still needed.", age.Round(time.Second), timeout)
		runner.appendSessionMessage(cp.SessionID, domain.MessageRoleAssistant, msg, "", "")
		deliverToSession(runner.agent.ID, cp.SessionID, msg)
		_ = store.DeleteJSON(path)
		return
	}
	if cp.RetryCount >= maxCheckpointRecoveryRetries {
		slog.Info("agent: checkpoint retry limit reached, notifying session",
			"agent", runner.agent.Name, "session", cp.SessionID,
			"retry", cp.RetryCount, "limit", maxCheckpointRecoveryRetries)
		msg := fmt.Sprintf("I was interrupted and retry recovery %d times without finishing, so I stopped retrying. Please resend your request if it is still needed.", cp.RetryCount)
		runner.appendSessionMessage(cp.SessionID, domain.MessageRoleAssistant, msg, "", "")
		deliverToSession(runner.agent.ID, cp.SessionID, msg)
		_ = store.DeleteJSON(path)
		return
	}
	if !cp.LastRecoveredAt.IsZero() {
		sinceLastRecovery := time.Since(cp.LastRecoveredAt)
		if sinceLastRecovery < checkpointRecoveryCooldown {
			slog.Debug("agent: skipping recent checkpoint recovery",
				"agent", runner.agent.Name, "session", cp.SessionID,
				"retry", cp.RetryCount, "since_last_recovery", sinceLastRecovery)
			return
		}
	}

	slog.Info("agent: recovering interrupted prompt",
		"agent", runner.agent.Name, "session", cp.SessionID,
		"age", age, "retry", cp.RetryCount)
	// Increment retry count and re-write checkpoint before re-issuing.
	original := cp
	cp.RetryCount++
	cp.LastRecoveredAt = time.Now()
	if err := store.WriteJSON(path, cp); err != nil {
		slog.Warn("agent: could not save recovery attempt", "path", path, "err", err)
		return
	}

	ctx := WithSessionID(context.Background(), cp.SessionID)
	checkpointID := strings.TrimSuffix(name, filepath.Ext(name))
	if admission := runner.recoverPrompt(ctx, checkpointID, path, cp); admission.Status == AdmissionRejectedStopping {
		if checkpointRetired(path) {
			return
		}
		if err := store.WriteJSON(path, original); err != nil {
			slog.Warn("agent: could not restore rejected recovery", "path", path, "err", err)
		}
	}
}

// Get returns the runner for the named agent.
func (m *Manager) Get(name string) (*AgentRunner, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.runners[name]
	return r, ok
}

// GetByID returns the runner for a concrete agent ID.
func (m *Manager) GetByID(agentID string) (*AgentRunner, bool) {
	return m.Get(agentID)
}

// List returns a snapshot of all agents in config entry order.
func (m *Manager) List() []*domain.Agent {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*domain.Agent, 0, len(m.runners))
	for _, name := range m.order {
		if r, ok := m.runners[name]; ok {
			out = append(out, r.Agent())
		}
	}
	return out
}

// Stop stops all agents.
func (m *Manager) Stop() {
	m.mu.Lock()
	m.stopped = true
	for _, r := range m.runners {
		r.Stop()
	}
	m.mu.Unlock()
}

// Drain waits for every runner this manager still owns, including replaced
// runners whose terminal callbacks may remain active after a config reload.
func (m *Manager) Drain(ctx context.Context) error {
	m.mu.RLock()
	runners := make([]*AgentRunner, 0, len(m.owned))
	for r := range m.owned {
		runners = append(runners, r)
	}
	m.mu.RUnlock()
	done := make(chan struct{})
	go func() {
		m.recoveries.Wait()
		for _, r := range runners {
			r.Wait()
		}
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) retireRunner(r *AgentRunner) {
	r.Stop()
	go func() {
		r.Wait()
		m.mu.Lock()
		delete(m.owned, r)
		m.mu.Unlock()
	}()
}
