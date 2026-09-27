package server

import (
	"context"
	"errors"

	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/connections"
	"github.com/lsegal/aviary/internal/preparation"
)

// collectAfterConnect runs trusted deployment code without starting an agent turn.
// Collection failure never reverses a successfully saved database login.
func (s *Server) collectAfterConnect(ctx context.Context, target connections.Target, principal connections.Principal, queryAllowed bool) (string, error) {
	if s.agents == nil {
		return "", preparation.ErrUnavailable
	}
	runner, ok := s.agents.Get(target.Scope.AgentID)
	if !ok || runner.Config() == nil || runner.Config().Hooks == nil || runner.Config().Hooks.PostConnect == nil {
		return "", nil
	}
	if s.connections == nil || s.preparation == nil {
		return "", preparation.ErrUnavailable
	}
	if !queryAllowed || !runner.AllowsTool("clickhouse_query") {
		return "", preparation.ErrForbidden
	}
	return collectConnectionEvidence(ctx, s.connections, s.preparation, *runner.Config().Hooks.PostConnect, target, principal)
}

func collectConnectionEvidence(ctx context.Context, service *connections.Service, engine *preparation.Engine, hook config.BeforeTurnHookConfig, target connections.Target, principal connections.Principal) (string, error) {
	lease, err := service.Begin(target.Scope)
	if err != nil {
		return "", err
	}
	defer lease.End()
	if lease.Target() != target {
		return "", connections.ErrStale
	}
	execution := connections.Execution{Kind: connections.Interactive, Scope: target.Scope, Principal: principal}
	credential, ok := service.CredentialFor(execution, target)
	if !ok {
		return "", connections.ErrStale
	}
	in := preparation.Input{Execution: execution, Target: target, CredentialVersion: credential.Version}
	result, err := engine.Run(ctx, hook, in, &credential)
	if err != nil {
		return "", err
	}
	defer func() { _ = engine.Discard(in, result.RunID) }()
	if len(result.Artifacts) != 1 || result.Artifacts[0] != "evidence.json" {
		return "", errors.New("post-connect evidence is unavailable")
	}
	data, err := engine.Read(ctx, in, result.RunID, "evidence.json", 1<<20)
	if err != nil {
		return "", err
	}
	snapshot := connections.EvidenceSnapshot{Status: result.Status, Summary: result.Summary, RunID: result.RunID,
		ObservedAt: result.ObservedAt, ProducerRevision: result.ProducerRevision, Path: "evidence.json", Content: data}
	if err := service.SaveEvidence(execution, target, credential.Version, snapshot); err != nil {
		return "", err
	}
	if result.Status != "complete" && result.Status != "partial" {
		return "", preparation.ErrUnavailable
	}
	return result.PublicSummary, nil
}
