package mcp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lsegal/aviary/internal/agent"
	"github.com/lsegal/aviary/internal/clickhouseconn"
	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/connections"
	"github.com/lsegal/aviary/internal/llm"
	"github.com/lsegal/aviary/internal/preparation"
	"github.com/lsegal/aviary/internal/store"
)

// TestLivePreparedConnectionThroughMCP joins a trusted preparation executable,
// scoped artifact reads, and the direct ClickHouse tool. The owner-only fixture
// supplies both the endpoint credentials and the administrator hook argv.
func TestLivePreparedConnectionThroughMCP(t *testing.T) {
	fixturePath := os.Getenv("AVIARY_CLICKHOUSE_SMOKE_FILE")
	if fixturePath == "" {
		t.Skip("AVIARY_CLICKHOUSE_SMOKE_FILE is not set")
	}
	info, err := os.Stat(fixturePath)
	if err != nil || info.Mode().Perm()&0o077 != 0 {
		t.Fatal("live fixture is unavailable")
	}
	data, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal("live fixture is unavailable")
	}
	var fixture struct {
		Endpoint        string   `json:"endpoint"`
		PreparationArgv []string `json:"preparation_argv"`
		Accounts        []struct {
			Username string `json:"username"`
			Password string `json:"password"`
		} `json:"accounts"`
	}
	if json.Unmarshal(data, &fixture) != nil || fixture.Endpoint == "" || len(fixture.Accounts) == 0 {
		t.Fatal("live fixture is invalid")
	}
	if len(fixture.PreparationArgv) == 0 || strings.TrimSpace(fixture.PreparationArgv[0]) == "" {
		t.Skip("live fixture has no preparation_argv")
	}
	policy, err := smokePolicy(fixture.Endpoint)
	if err != nil {
		t.Fatal("live policy could not be derived")
	}

	store.SetDataDir(t.TempDir())
	t.Cleanup(func() { store.SetDataDir("") })
	if err := store.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	connectionsStore, err := connections.Open(filepath.Join(t.TempDir(), "connections"))
	if err != nil {
		t.Fatal(err)
	}
	scope := connections.Scope{AgentID: "prepared-live", InstallationID: "test", WorkspaceID: "test", ChannelID: "test", RootThreadID: "test"}
	principal := connections.Principal{InstallationID: "test", WorkspaceID: "test", UserID: "owner"}
	target, _, err := connectionsStore.Select(scope, "clickhouse", fixture.Endpoint)
	if err != nil {
		t.Fatal("target selection failed")
	}
	account := fixture.Accounts[0]
	if err := connectionsStore.PutPrompt(connections.Prompt{Principal: principal, DMChannelID: "dm", DMRootID: "live", Target: target, Stage: "password", Username: account.Username, ExpiresAt: time.Now().Add(time.Minute)}); err != nil {
		t.Fatal("credential prompt setup failed")
	}
	adapter := clickhouseconn.Adapter{Policy: policy}
	if err := connectionsStore.CompletePassword(context.Background(), principal, "dm", "live", "100.000001", account.Password, func(ctx context.Context, selected connections.Target, credential connections.Credential) error {
		return adapter.ValidateReadOnly(ctx, clickhouseconn.Target{Endpoint: selected.Endpoint, Username: credential.Username}, clickhouseconn.NewCredentials(credential.Password))
	}); err != nil {
		t.Fatal("credential validation failed")
	}

	var (
		mu          sync.Mutex
		rounds      int
		runID       string
		artifactOK  bool
		identityOK  bool
		modelFailed bool
	)
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			http.NotFound(w, r)
			return
		}
		var request struct {
			Messages []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"messages"`
			Tools []struct {
				Function struct {
					Name string `json:"name"`
				} `json:"function"`
			} `json:"tools"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		rounds++
		contents := make([]string, 0, len(request.Messages))
		for _, message := range request.Messages {
			contents = append(contents, message.Content)
		}
		all := strings.Join(contents, "\n")
		switch rounds {
		case 1:
			match := regexp.MustCompile(`"run_id":"([a-f0-9]{32})"`).FindStringSubmatch(all)
			artifact := regexp.MustCompile(`"artifacts":\["([^"]+)"`).FindStringSubmatch(all)
			if len(match) != 2 || len(artifact) != 2 {
				modelFailed = true
				http.Error(w, "missing preparation index", http.StatusBadRequest)
				return
			}
			runID = match[1]
			writeOpenAIToolCall(t, w, "artifact_read", map[string]any{"run_id": runID, "path": artifact[1], "max_bytes": 4096})
		case 2:
			for _, message := range request.Messages {
				if message.Role != "tool" {
					continue
				}
				var artifact struct {
					RunID   string `json:"run_id"`
					Content string `json:"content"`
				}
				if json.Unmarshal([]byte(message.Content), &artifact) == nil && artifact.RunID == runID && strings.TrimSpace(artifact.Content) != "" {
					artifactOK = true
				}
			}
			queryTool := ""
			for _, tool := range request.Tools {
				if strings.HasPrefix(tool.Function.Name, "clickhouse_") && strings.HasSuffix(tool.Function.Name, "__query") {
					queryTool = tool.Function.Name
					break
				}
			}
			if !artifactOK || queryTool == "" {
				modelFailed = true
				http.Error(w, "artifact or connection tool unavailable", http.StatusBadRequest)
				return
			}
			writeOpenAIToolCall(t, w, queryTool, map[string]any{"sql": "SELECT currentUser()", "max_rows": 1, "max_bytes": 1024, "timeout_ms": 10000})
		case 3:
			for _, message := range request.Messages {
				if message.Role == "tool" && mcpIdentityMatches(message.Content, account.Username) {
					identityOK = true
				}
			}
			if !identityOK {
				modelFailed = true
				http.Error(w, "identity result missing", http.StatusBadRequest)
				return
			}
			writeOpenAIText(w, "live preparation and connection completed")
		default:
			modelFailed = true
			http.Error(w, "unexpected model round", http.StatusBadRequest)
		}
	}))
	t.Cleanup(model.Close)

	factory := llm.NewFactory(func(string) (string, error) { return "", nil }).WithProviderOptionsResolver(func(provider string) (llm.ProviderOptions, bool) {
		if provider != "vllm" {
			return llm.ProviderOptions{}, false
		}
		return llm.ProviderOptions{BaseURI: model.URL}, true
	})
	engine, err := preparation.Open(filepath.Join(t.TempDir(), "artifacts"))
	if err != nil {
		t.Fatal(err)
	}
	manager := agent.NewManager(factory)
	manager.SetConnectionService(connectionsStore)
	manager.SetPreparationEngine(engine)
	manager.Reconcile(&config.Config{Agents: []config.AgentConfig{{
		Name: "prepared-live", Model: "vllm/test-model",
		Hooks: &config.HooksConfig{BeforeTurn: &config.BeforeTurnHookConfig{Argv: fixture.PreparationArgv, Timeout: "30s", OnError: "stop", AllowCredential: true}},
	}}})
	runner, ok := manager.Get("prepared-live")
	if !ok {
		t.Fatal("prepared runner unavailable")
	}

	oldDeps, oldDepsSet := GetDeps(), depsSet
	SetDeps(&Deps{Agents: manager, Connections: connectionsStore, ClickHouse: func() clickhouseconn.Adapter { return adapter }})
	t.Cleanup(func() { globalDeps, depsSet = oldDeps, oldDepsSet })
	agent.SetToolClientFactory(NewAgentToolClient)
	t.Cleanup(func() { agent.SetToolClientFactory(nil) })

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	ctx = connections.WithExecution(ctx, connections.Execution{Kind: connections.Interactive, Scope: scope, Principal: principal})
	done := make(chan agent.StreamEvent, 1)
	history := false
	runner.PromptWithOverrides(ctx, "Read collected evidence, then query the attached database identity.", agent.RunOverrides{History: &history}, func(event agent.StreamEvent) {
		if event.Type == agent.StreamEventDone || event.Type == agent.StreamEventError || event.Type == agent.StreamEventStop {
			select {
			case done <- event:
			default:
			}
		}
	})
	select {
	case event := <-done:
		runner.Wait()
		if event.Type != agent.StreamEventDone || event.Text != "live preparation and connection completed" {
			t.Fatal("live prepared connection turn failed")
		}
	case <-ctx.Done():
		t.Fatal("live prepared connection turn timed out")
	}
	mu.Lock()
	if rounds != 3 || runID == "" || !artifactOK || !identityOK || modelFailed {
		mu.Unlock()
		t.Fatal("live preparation, artifact, and connection flow was incomplete")
	}
	mu.Unlock()

	session, err := agent.NewSessionManager().GetOrCreateNamed("prepared-live", "main")
	if err != nil {
		t.Fatal(err)
	}
	messages, err := store.ReadJSONL[map[string]any](store.SessionPath("prepared-live", session.ID))
	if err == nil {
		for _, message := range messages {
			if message["role"] == "tool" {
				t.Fatal("private preparation or connection tool result was persisted")
			}
		}
	}
}
