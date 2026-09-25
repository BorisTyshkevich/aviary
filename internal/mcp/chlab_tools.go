package mcp

import (
	"context"
	"fmt"

	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/lsegal/aviary/internal/agent"
	"github.com/lsegal/aviary/internal/chlab"
)

type chlabStartArgs struct {
	Version string `json:"version"`
	Shape   string `json:"shape"`
}
type chlabQueryArgs struct {
	SQL  string `json:"sql"`
	Node string `json:"node,omitempty"`
}
type chlabNodeArgs struct {
	Node string `json:"node"`
}

func labRequest(ctx context.Context, action string) (chlab.Request, error) {
	agentID, ok := agent.SessionAgentIDFromContext(ctx)
	if !ok {
		return chlab.Request{}, fmt.Errorf("chlab requires an agent session")
	}
	sessionID, ok := agent.SessionIDFromContext(ctx)
	if !ok {
		return chlab.Request{}, fmt.Errorf("chlab requires a session")
	}
	return chlab.Request{Action: action, Agent: agentID, Session: sessionID}, nil
}

func registerCHLabTools(s *sdkmcp.Server) {
	addTool(s, &sdkmcp.Tool{Name: "chlab_start", Description: "Start a disposable ClickHouse lab only when the user explicitly requests runtime verification. Accepts any published official clickhouse/clickhouse-server version tag and shape 1x1, 1x2, or 2x2. Startup is asynchronous; poll chlab_status."}, func(ctx context.Context, _ *sdkmcp.CallToolRequest, args chlabStartArgs) (*sdkmcp.CallToolResult, struct{}, error) {
		r, err := labRequest(ctx, "start")
		if err != nil {
			return nil, struct{}{}, err
		}
		r.Version = args.Version
		r.Shape = args.Shape
		result, err := chlab.Call(ctx, r)
		if err != nil {
			return nil, struct{}{}, err
		}
		return jsonResult(result)
	})
	addTool(s, &sdkmcp.Tool{Name: "chlab_status", Description: "Get the lab status, actual ClickHouse version, resolved image digest, and node states for this session."}, func(ctx context.Context, _ *sdkmcp.CallToolRequest, _ struct{}) (*sdkmcp.CallToolResult, struct{}, error) {
		r, err := labRequest(ctx, "status")
		if err != nil {
			return nil, struct{}{}, err
		}
		result, err := chlab.Call(ctx, r)
		if err != nil {
			return nil, struct{}{}, err
		}
		return jsonResult(result)
	})
	addTool(s, &sdkmcp.Tool{Name: "chlab_query", Description: "Run SQL in the current session's disposable ClickHouse lab. Node defaults to ch1. Returns observed output or error."}, func(ctx context.Context, _ *sdkmcp.CallToolRequest, args chlabQueryArgs) (*sdkmcp.CallToolResult, struct{}, error) {
		r, err := labRequest(ctx, "query")
		if err != nil {
			return nil, struct{}{}, err
		}
		r.SQL = args.SQL
		r.Node = args.Node
		result, err := chlab.Call(ctx, r)
		if err != nil {
			return nil, struct{}{}, err
		}
		return jsonResult(result)
	})
	addTool(s, &sdkmcp.Tool{Name: "chlab_stop", Description: "Stop and delete the current session's ClickHouse lab."}, func(ctx context.Context, _ *sdkmcp.CallToolRequest, _ struct{}) (*sdkmcp.CallToolResult, struct{}, error) {
		r, err := labRequest(ctx, "stop")
		if err != nil {
			return nil, struct{}{}, err
		}
		result, err := chlab.Call(ctx, r)
		if err != nil {
			return nil, struct{}{}, err
		}
		return jsonResult(result)
	})
	for _, operation := range []string{"start", "stop", "restart"} {
		name := operation
		addTool(s, &sdkmcp.Tool{Name: "chlab_node_" + name, Description: "" + name + " a ClickHouse node (ch1 through ch4, depending on shape) in this session's lab."}, func(ctx context.Context, _ *sdkmcp.CallToolRequest, args chlabNodeArgs) (*sdkmcp.CallToolResult, struct{}, error) {
			r, err := labRequest(ctx, "node_"+name)
			if err != nil {
				return nil, struct{}{}, err
			}
			r.Node = args.Node
			result, err := chlab.Call(ctx, r)
			if err != nil {
				return nil, struct{}{}, err
			}
			return jsonResult(result)
		})
	}
}
