package agent

import (
	"context"
	"errors"

	"github.com/lsegal/aviary/internal/connections"
)

// reserveConnectionTurn runs synchronously before the prompt goroutine starts.
// The caller holds r.mu so service replacement cannot race this reservation.
func (r *AgentRunner) reserveConnectionTurn(ctx context.Context) (context.Context, func(), error) {
	release := func() {}
	execution, present := connections.ExecutionFromContext(ctx)
	_, task := TaskIDFromContext(ctx)
	_, job := JobIDFromContext(ctx)
	if task || job {
		execution = connections.Execution{Kind: connections.Scheduled}
	} else if !present {
		execution = connections.Execution{Kind: connections.Control}
	}
	ctx = connections.WithExecution(ctx, execution)
	if execution.Kind != connections.Interactive {
		return ctx, release, nil
	}
	if !execution.Personal() || execution.Scope.AgentID != r.agent.ID {
		return ctx, release, errors.New("trusted connection identity is unavailable")
	}
	if r.connections == nil {
		return ctx, release, nil
	}
	lease, err := r.connections.Begin(execution.Scope)
	if err != nil {
		return ctx, release, err
	}
	if lease != nil {
		ctx = connections.WithLease(ctx, lease)
		release = lease.End
	}
	return ctx, release, nil
}

func privateConnectionTurn(ctx context.Context) bool {
	if state, ok := PreparationFromContext(ctx); ok && state.Input.Execution.Personal() {
		return true
	}
	e, ok := connections.ExecutionFromContext(ctx)
	lease, leased := connections.LeaseFromContext(ctx)
	return ok && e.Personal() && leased && lease.Target().Generation != ""
}

// connectedProgressTurn requires an actual leased target. A preparation-only
// context is private, but has no authorized channel progress projection.
func connectedProgressTurn(ctx context.Context) bool {
	e, ok := connections.ExecutionFromContext(ctx)
	lease, leased := connections.LeaseFromContext(ctx)
	return ok && e.Personal() && leased && lease.Target().Generation != ""
}

// PrivateDataContext reports whether shared persistence would expose turn evidence.
func PrivateDataContext(ctx context.Context) bool { return privateConnectionTurn(ctx) }
