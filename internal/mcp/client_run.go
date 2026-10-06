package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/lsegal/aviary/internal/agent"
	"github.com/lsegal/aviary/internal/clientauth"
)

// Client runs intentionally do not retain the inbound principal in execution
// context: the agent's in-process tools use the agent's configured policy.
func runClientAgent(ctx context.Context, req *sdkmcp.CallToolRequest, args agentRunArgs, p clientauth.Principal, registry *clientauth.Registry) (*sdkmcp.CallToolResult, struct{}, error) {
	d := GetDeps()
	if d.Agents == nil {
		return nil, struct{}{}, errors.New("agent manager unavailable")
	}
	name := strings.TrimSpace(args.Name)
	runner, ok := d.Agents.Get(name)
	if !ok {
		return nil, struct{}{}, clientauth.ErrDenied
	}
	// The configured runner identity is canonical; do not infer it from paths.
	agentID := runner.Agent().ID
	var mu sync.Mutex
	var answer strings.Builder
	done := make(chan error, 1)
	var stopReply string
	err := registry.Admit(p, agentID, func(execCtx context.Context, release func()) error {
		sess, err := agent.NewSessionManager().ClientSession(p.ID, agentID, args.Session, args.SessionID, !isStopCommand(args.Message))
		if err != nil {
			return err
		}
		if isStopCommand(args.Message) {
			stopped := agent.StopSession(agentID, sess.ID)
			stopReply = fmt.Sprintf("stopped %d run(s) in session %q", stopped, sess.ID)
			release()
			return nil
		}
		execCtx = agent.WithSessionID(execCtx, sess.ID)
		history := resolveAgentRunHistory(args)
		var terminal sync.Once
		finish := func(err error) { terminal.Do(func() { release(); done <- err }) }
		progress := 0.0
		admission := runner.PromptMediaWithOverrides(execCtx, args.Message, args.MediaURL, agent.RunOverrides{Bare: args.Bare, History: &history, DisableRecovery: true}, func(e agent.StreamEvent) {
			switch e.Type {
			case agent.StreamEventText:
				mu.Lock()
				answer.WriteString(e.Text)
				mu.Unlock()
				if req.Params.GetProgressToken() != nil {
					progress++
					_ = req.Session.NotifyProgress(ctx, &sdkmcp.ProgressNotificationParams{ProgressToken: req.Params.GetProgressToken(), Progress: progress, Message: e.Text})
				}
			case agent.StreamEventToolProgress:
				if args.IncludeToolProgress && e.PublicTool != nil && req.Params.GetProgressToken() != nil {
					progress++
					_ = req.Session.NotifyProgress(ctx, &sdkmcp.ProgressNotificationParams{ProgressToken: req.Params.GetProgressToken(), Progress: progress, Message: publicClientToolProgress(e.PublicTool)})
				}
			case agent.StreamEventMedia:
				if req.Params.GetProgressToken() != nil {
					progress++
					_ = req.Session.NotifyProgress(ctx, &sdkmcp.ProgressNotificationParams{ProgressToken: req.Params.GetProgressToken(), Progress: progress, Message: "[media]" + e.MediaURL})
				}
			case agent.StreamEventDone:
				finish(nil)
			case agent.StreamEventStop:
				finish(context.Canceled)
			case agent.StreamEventError:
				finish(e.Err)
			}
		})
		if admission.Status != agent.AdmissionAccepted {
			return errors.New("agent is restarting; retry the request")
		}
		slog.Info("mcp: client run admitted", "client_id", p.ID, "client_name", p.Name, "agent", agentID, "session", sess.ID)
		return nil
	})
	if err != nil {
		slog.Info("mcp: client run denied", "client_id", p.ID, "client_name", p.Name)
		return nil, struct{}{}, err
	}
	if stopReply != "" {
		return text(stopReply)
	}
	select {
	case err := <-done:
		if err != nil {
			return &sdkmcp.CallToolResult{IsError: true, Content: []sdkmcp.Content{&sdkmcp.TextContent{Text: "Agent run failed or was interrupted."}}}, struct{}{}, nil
		}
		mu.Lock()
		result := answer.String()
		mu.Unlock()
		return text(result)
	case <-ctx.Done():
		return nil, struct{}{}, ctx.Err()
	}
}

func publicClientToolProgress(event *agent.PublicToolEvent) string {
	data, _ := json.Marshal(map[string]string{"name": event.Name, "invocation_id": event.InvocationID, "state": string(event.State)})
	return "[tool]" + string(data)
}
