package mcp

import (
	"context"
	"fmt"
	"strings"
	"time"

	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/lsegal/aviary/internal/agent"
	"github.com/lsegal/aviary/internal/clickhouseconn"
	"github.com/lsegal/aviary/internal/connections"
)

type connectionQueryArgs struct {
	Generation string `json:"generation"`
	SQL        string `json:"sql,omitempty"`
	MaxRows    int    `json:"max_rows,omitempty"`
	MaxBytes   int    `json:"max_bytes,omitempty"`
	TimeoutMS  int    `json:"timeout_ms,omitempty"`
}

func connectionAuthority(ctx context.Context) (connections.Execution, connections.Target, connections.Credential, error) {
	denied := fmt.Errorf("personal connection authorization is unavailable")
	e, ok := connections.ExecutionFromContext(ctx)
	if !ok || !e.Personal() {
		return e, connections.Target{}, connections.Credential{}, denied
	}
	if _, ok := agent.TaskIDFromContext(ctx); ok {
		return e, connections.Target{}, connections.Credential{}, denied
	}
	if _, ok := agent.JobIDFromContext(ctx); ok {
		return e, connections.Target{}, connections.Credential{}, denied
	}
	id, ok := agent.SessionAgentIDFromContext(ctx)
	if !ok || id != e.Scope.AgentID {
		return e, connections.Target{}, connections.Credential{}, denied
	}
	lease, ok := connections.LeaseFromContext(ctx)
	if !ok || GetDeps().Connections == nil {
		return e, connections.Target{}, connections.Credential{}, denied
	}
	target := lease.Target()
	if target.Transport != "clickhouse" || target.Generation == "" || target.Scope != e.Scope {
		return e, target, connections.Credential{}, denied
	}
	c, ok := GetDeps().Connections.CredentialFor(e, target)
	if !ok {
		return e, target, c, denied
	}
	return e, target, c, nil
}

func registerConnectionTools(s *sdkmcp.Server) {
	for _, operation := range []string{"query", "inspect"} {
		name := "clickhouse_" + operation
		addTool(s, &sdkmcp.Tool{Name: name, Description: "Read from the current trusted thread connection. Personal authorization and the exact attachment generation are required."},
			func(ctx context.Context, _ *sdkmcp.CallToolRequest, args connectionQueryArgs) (*sdkmcp.CallToolResult, struct{}, error) {
				if err := agentToolPermitted(ctx, name); err != nil {
					return nil, struct{}{}, err
				}
				_, target, credential, err := connectionAuthority(ctx)
				if err != nil {
					return nil, struct{}{}, err
				}
				if args.Generation != target.Generation {
					return nil, struct{}{}, fmt.Errorf("connection generation is stale")
				}
				if args.MaxRows < 0 || args.MaxBytes < 0 || args.TimeoutMS < 0 {
					return nil, struct{}{}, fmt.Errorf("query bounds must be positive")
				}
				if args.MaxRows == 0 {
					args.MaxRows = 1000
				}
				if args.MaxBytes == 0 {
					args.MaxBytes = 64 << 10
				}
				if args.TimeoutMS == 0 || args.TimeoutMS > 30000 {
					args.TimeoutMS = 30000
				}
				if operation == "inspect" {
					args.SQL = "SELECT database, table, name, type FROM system.columns ORDER BY database, table, position"
				}
				if GetDeps().ClickHouse == nil {
					return nil, struct{}{}, fmt.Errorf("database adapter is unavailable")
				}
				adapter := GetDeps().ClickHouse()
				result, err := adapter.Query(ctx, clickhouseconn.Target{Endpoint: target.Endpoint, Username: credential.Username}, clickhouseconn.NewCredentials(credential.Password), clickhouseconn.Request{SQL: args.SQL, MaxRows: args.MaxRows, MaxBytes: args.MaxBytes, Timeout: time.Duration(args.TimeoutMS) * time.Millisecond})
				if err != nil {
					return nil, struct{}{}, err
				}
				return jsonResult(map[string]any{"target": target.Endpoint, "generation": target.Generation, "columns": result.Columns, "rows": result.Rows, "query_id": result.QueryID, "truncated": result.Truncated})
			})
	}
	addTool(s, &sdkmcp.Tool{Name: "artifact_read", Description: "Read a bounded evidence artifact from the current preparation scope. Run IDs and paths come from the evidence index."},
		func(ctx context.Context, _ *sdkmcp.CallToolRequest, args struct {
			RunID    string `json:"run_id"`
			Path     string `json:"path"`
			MaxBytes int64  `json:"max_bytes,omitempty"`
		}) (*sdkmcp.CallToolResult, struct{}, error) {
			if err := agentToolPermitted(ctx, "artifact_read"); err != nil {
				return nil, struct{}{}, err
			}
			state, ok := agent.PreparationFromContext(ctx)
			if !ok {
				return nil, struct{}{}, fmt.Errorf("preparation artifacts are unavailable")
			}
			if state.Input.CredentialVersion != "" {
				_, _, c, err := connectionAuthority(ctx)
				if err != nil || c.Version != state.Input.CredentialVersion {
					return nil, struct{}{}, fmt.Errorf("preparation authorization is stale")
				}
			}
			if args.MaxBytes == 0 {
				args.MaxBytes = 64 << 10
			}
			var data []byte
			if state.Snapshot != nil {
				if args.RunID != state.Result.RunID || len(state.Result.Artifacts) != 1 || args.Path != state.Result.Artifacts[0] || args.MaxBytes < 1 || int64(len(state.Snapshot)) > args.MaxBytes {
					return nil, struct{}{}, fmt.Errorf("preparation artifact is unavailable")
				}
				data = state.Snapshot
			} else {
				var err error
				data, err = state.Engine.Read(ctx, state.Input, args.RunID, args.Path, args.MaxBytes)
				if err != nil {
					return nil, struct{}{}, err
				}
			}
			return jsonResult(map[string]any{"run_id": args.RunID, "path": args.Path, "provenance": state.Input, "content": string(data)})
		})
}

func connectionToolName(target connections.Target, operation string) string {
	return "clickhouse_" + target.Generation + "__" + operation
}

func connectionToolOperation(name string, target connections.Target) (string, bool) {
	for _, operation := range []string{"query", "inspect"} {
		if name == connectionToolName(target, operation) {
			return operation, true
		}
	}
	return "", false
}

func connectionToolCandidate(name string) bool { return strings.HasPrefix(name, "clickhouse_") }
