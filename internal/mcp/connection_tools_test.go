package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lsegal/aviary/internal/agent"
	"github.com/lsegal/aviary/internal/clickhouseconn"
	"github.com/lsegal/aviary/internal/connections"
	"github.com/lsegal/aviary/internal/endpointpolicy"
)

func TestConnectionToolsRequireTrustedPersonalLeaseAndFreshGeneration(t *testing.T) {
	s, err := connections.Open(filepath.Join(t.TempDir(), "connections"))
	if err != nil {
		t.Fatal(err)
	}
	scope := connections.Scope{AgentID: "bot", InstallationID: "i", WorkspaceID: "w", ChannelID: "c", RootThreadID: "t"}
	principal := connections.Principal{InstallationID: "i", WorkspaceID: "w", UserID: "a"}
	target, _, err := s.Select(scope, "clickhouse", "https://db.example:8443")
	if err != nil {
		t.Fatal(err)
	}
	prompt := connections.Prompt{Principal: principal, DMChannelID: "dm", DMRootID: "root", Target: target, Stage: "password", Username: "a", ExpiresAt: time.Now().Add(time.Hour)}
	if err := s.PutPrompt(prompt); err != nil {
		t.Fatal(err)
	}
	if err := s.CompletePassword(context.Background(), principal, "dm", "root", "100.000001", "fake-secret", func(context.Context, connections.Target, connections.Credential) error { return nil }); err != nil {
		t.Fatal(err)
	}
	lease, err := s.Begin(scope)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.End()
	ctx := agent.WithSessionAgentID(connections.WithLease(connections.WithExecution(context.Background(), connections.Execution{Kind: connections.Interactive, Scope: scope, Principal: principal}), lease), "bot")
	old := GetDeps()
	oldSet := depsSet
	SetDeps(&Deps{Connections: s})
	t.Cleanup(func() { globalDeps = old; depsSet = oldSet })
	_, got, _, err := connectionAuthority(ctx)
	if err != nil || got.Generation != target.Generation {
		t.Fatal("trusted lease was not authorized")
	}
	if _, _, _, err := connectionAuthority(agent.WithToolPolicy(ctx, func(string) bool { return false })); err != nil {
		t.Fatal("authority must not use model policy")
	}
	if _, _, _, err := connectionAuthority(agent.WithSessionAgentID(connections.WithExecution(context.Background(), connections.Execution{Kind: connections.Scheduled, Scope: scope}), "bot")); err == nil {
		t.Fatal("scheduled execution was authorized")
	}
	if _, _, _, err := connectionAuthority(agent.WithSessionAgentID(connections.WithExecution(context.Background(), connections.Execution{Kind: connections.Interactive, Scope: scope, Principal: connections.Principal{InstallationID: "i", WorkspaceID: "w", UserID: "b"}}), "bot")); err == nil {
		t.Fatal("missing principal credential was authorized")
	}
	for _, blocked := range []context.Context{
		context.Background(),
		agent.WithTaskID(ctx, "scheduled-task"),
		agent.WithJobID(ctx, "scheduled-job"),
		connections.WithExecution(ctx, connections.Execution{Kind: connections.Scheduled}),
		agent.WithToolPolicy(ctx, func(string) bool { return false }),
	} {
		client, err := NewInProcessClient(blocked, NewServer())
		if err != nil {
			t.Fatal(err)
		}
		result, err := client.CallTool(blocked, "clickhouse_query", map[string]any{"generation": target.Generation, "sql": "SELECT 1"})
		if err == nil && !result.IsError {
			t.Fatal("unauthorized canonical MCP invocation succeeded")
		}
		_ = client.Close()
	}
	client, err := NewInProcessClient(ctx, NewServer())
	if err != nil {
		t.Fatal(err)
	}
	defer func(c interface{ Close() error }) { _ = c.Close() }(client)
	for _, name := range []string{"agent_run", "session_send", "channel_send_file", "agent_file_write", "browser_open", "web_search", "chlab_start", "chlab_query", "chlab_node_add"} {
		if err := agentToolPermitted(ctx, name); err == nil {
			t.Fatalf("private turn allowed %s", name)
		}
	}
	result, err := client.CallTool(ctx, "clickhouse_query", map[string]any{"generation": "obsolete", "sql": "SELECT 1"})
	if err == nil && !result.IsError {
		t.Fatal("stale canonical generation accepted")
	}
	if _, _, err := s.Select(scope, "clickhouse", "https://next.example:8443"); err == nil {
		t.Fatal("active lease allowed target replacement")
	}
}

func TestLiveConnectionToolsInProcess(t *testing.T) {
	path := os.Getenv("AVIARY_CLICKHOUSE_SMOKE_FILE")
	if path == "" {
		t.Skip("AVIARY_CLICKHOUSE_SMOKE_FILE is not set")
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm()&0o077 != 0 {
		t.Fatal("smoke fixture unavailable")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("smoke fixture unavailable")
	}
	var fixture struct {
		Endpoint string `json:"endpoint"`
		Accounts []struct {
			Username string `json:"username"`
			Password string `json:"password"`
		} `json:"accounts"`
	}
	if json.Unmarshal(data, &fixture) != nil || fixture.Endpoint == "" || len(fixture.Accounts) < 2 {
		t.Fatal("smoke fixture invalid")
	}
	policy, err := smokePolicy(fixture.Endpoint)
	if err != nil {
		t.Fatal("smoke policy could not be derived")
	}
	s, err := connections.Open(filepath.Join(t.TempDir(), "connections"))
	if err != nil {
		t.Fatal(err)
	}
	scope := connections.Scope{AgentID: "bot", InstallationID: "i", WorkspaceID: "w", ChannelID: "c", RootThreadID: "t"}
	target, _, err := s.Select(scope, "clickhouse", fixture.Endpoint)
	if err != nil {
		t.Fatal("target selection failed")
	}
	old, oldSet := GetDeps(), depsSet
	SetDeps(&Deps{Connections: s, ClickHouse: func() clickhouseconn.Adapter { return clickhouseconn.Adapter{Policy: policy} }})
	t.Cleanup(func() { globalDeps = old; depsSet = oldSet })
	for index, account := range fixture.Accounts[:2] {
		principal := connections.Principal{InstallationID: "i", WorkspaceID: "w", UserID: string(rune('a' + index))}
		root := "prompt" + principal.UserID
		if err := s.PutPrompt(connections.Prompt{Principal: principal, DMChannelID: "dm", DMRootID: root, Target: target, Stage: "password", Username: account.Username, ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
			t.Fatal("prompt setup failed")
		}
		if err := s.CompletePassword(context.Background(), principal, "dm", root, "100.000001", account.Password, func(ctx context.Context, t connections.Target, c connections.Credential) error {
			return clickhouseconn.Adapter{Policy: policy}.ValidateReadOnly(ctx, clickhouseconn.Target{Endpoint: t.Endpoint, Username: c.Username}, clickhouseconn.NewCredentials(c.Password))
		}); err != nil {
			t.Fatal("credential validation failed")
		}
		lease, err := s.Begin(scope)
		if err != nil {
			t.Fatal("lease failed")
		}
		ctx := agent.WithSessionAgentID(connections.WithLease(connections.WithExecution(context.Background(), connections.Execution{Kind: connections.Interactive, Scope: scope, Principal: principal}), lease), "bot")
		client, err := NewAgentToolClient(ctx)
		if err != nil {
			lease.End()
			t.Fatal("tool client failed")
		}
		defer func(c interface{ Close() error }) { _ = c.Close() }(client)
		tools, err := client.ListTools(ctx)
		if err != nil {
			lease.End()
			t.Fatal("tool list failed")
		}
		name := ""
		for _, tool := range tools {
			if strings.HasSuffix(tool.Name, "__query") {
				name = tool.Name
				break
			}
		}
		if name == "" {
			lease.End()
			t.Fatal("connection query alias missing")
		}
		text, err := client.CallToolText(ctx, name, map[string]any{"sql": "SELECT currentUser()", "max_rows": 1, "max_bytes": 1024, "timeout_ms": 10000})
		if err != nil || !mcpIdentityMatches(text, account.Username) {
			lease.End()
			t.Fatal("live connection query failed")
		}
		inspect := strings.TrimSuffix(name, "__query") + "__inspect"
		text, err = client.CallToolText(ctx, inspect, map[string]any{"max_rows": 1, "max_bytes": 1024, "timeout_ms": 10000})
		if err != nil || !mcpHasRows(text) {
			lease.End()
			t.Fatal("live connection inspect failed")
		}
		if _, err := client.CallToolText(ctx, "clickhouse_old__query", map[string]any{"sql": "SELECT 1"}); err == nil {
			lease.End()
			t.Fatal("stale alias was accepted")
		}
		if _, err := client.CallToolText(agent.WithToolPolicy(ctx, func(string) bool { return false }), name, map[string]any{"sql": "SELECT 1"}); err == nil {
			lease.End()
			t.Fatal("tool policy denial bypassed")
		}
		lease.End()
	}
	// Exercise both owners concurrently through separate in-process MCP sessions.
	outcomes := make(chan bool, 2)
	for index, account := range fixture.Accounts[:2] {
		go func(index int, username string) {
			principal := connections.Principal{InstallationID: "i", WorkspaceID: "w", UserID: string(rune('a' + index))}
			lease, err := s.Begin(scope)
			if err != nil {
				outcomes <- false
				return
			}
			defer lease.End()
			ctx := agent.WithSessionAgentID(connections.WithLease(connections.WithExecution(context.Background(), connections.Execution{Kind: connections.Interactive, Scope: scope, Principal: principal}), lease), "bot")
			client, err := NewAgentToolClient(ctx)
			if err != nil {
				outcomes <- false
				return
			}
			defer func() { _ = client.Close() }()
			result, err := client.CallToolText(ctx, connectionToolName(target, "query"), map[string]any{"sql": "SELECT currentUser()", "max_rows": 1, "max_bytes": 1024})
			outcomes <- err == nil && mcpIdentityMatches(result, username)
		}(index, account.Username)
	}
	for range 2 {
		if !<-outcomes {
			t.Fatal("concurrent principal isolation failed")
		}
	}
	lease, _ := s.Begin(scope)
	defer lease.End()
	scheduled := agent.WithSessionAgentID(connections.WithLease(connections.WithExecution(context.Background(), connections.Execution{Kind: connections.Scheduled, Scope: scope}), lease), "bot")
	client, err := NewAgentToolClient(scheduled)
	if err != nil {
		t.Fatal("scheduled client failed")
	}
	defer func(c interface{ Close() error }) { _ = c.Close() }(client)
	tools, err := client.ListTools(scheduled)
	if err != nil {
		t.Fatal("scheduled list failed")
	}
	for _, tool := range tools {
		if strings.Contains(tool.Name, "__query") {
			t.Fatal("scheduled run exposed connection tool")
		}
	}
	control := agent.WithSessionAgentID(connections.WithLease(connections.WithExecution(context.Background(), connections.Execution{Kind: connections.Control, Scope: scope}), lease), "bot")
	client, err = NewAgentToolClient(control)
	if err != nil {
		t.Fatal("control client failed")
	}
	defer func(c interface{ Close() error }) { _ = c.Close() }(client)
	tools, err = client.ListTools(control)
	if err != nil {
		t.Fatal("control list failed")
	}
	for _, tool := range tools {
		if strings.Contains(tool.Name, "__query") {
			t.Fatal("control run exposed connection tool")
		}
	}
}

func smokePolicy(raw string) (endpointpolicy.Policy, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return endpointpolicy.Policy{}, fmt.Errorf("invalid endpoint")
	}
	port := 443
	if u.Port() != "" {
		port, err = strconv.Atoi(u.Port())
		if err != nil {
			return endpointpolicy.Policy{}, fmt.Errorf("invalid endpoint")
		}
	}
	ips, err := net.LookupIP(u.Hostname())
	if err != nil || len(ips) == 0 {
		return endpointpolicy.Policy{}, fmt.Errorf("DNS unavailable")
	}
	cidrs := make([]string, 0, len(ips))
	for _, ip := range ips {
		address, ok := netip.AddrFromSlice(ip)
		if !ok {
			continue
		}
		bits := 32
		if address.Is6() {
			bits = 128
		}
		cidrs = append(cidrs, address.String()+fmt.Sprintf("/%d", bits))
	}
	return endpointpolicy.Policy{Allow: []endpointpolicy.Rule{{Host: u.Hostname(), Ports: []int{port}, CIDRs: cidrs}}}, nil
}

func mcpIdentityMatches(text, username string) bool {
	var response struct {
		Rows [][]any `json:"rows"`
	}
	return json.Unmarshal([]byte(text), &response) == nil && len(response.Rows) == 1 && len(response.Rows[0]) == 1 && fmt.Sprint(response.Rows[0][0]) == username
}
func mcpHasRows(text string) bool {
	var response struct {
		Rows [][]any `json:"rows"`
	}
	return json.Unmarshal([]byte(text), &response) == nil && len(response.Rows) > 0
}
