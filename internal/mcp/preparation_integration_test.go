package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/agent"
	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/domain"
	"github.com/lsegal/aviary/internal/llm"
	"github.com/lsegal/aviary/internal/preparation"
	"github.com/lsegal/aviary/internal/store"
)

// TestPreparationIntegrationHelper is the trusted test hook. It only runs in
// the child process selected by the hook argv below.
func TestPreparationIntegrationHelper(t *testing.T) {
	for i, arg := range os.Args {
		if arg != "--" || i+1 >= len(os.Args) || os.Args[i+1] != "mcp-preparation" {
			continue
		}
		var request struct {
			Version     int    `json:"version"`
			ArtifactDir string `json:"artifact_dir"`
		}
		require.NoError(t, json.NewDecoder(os.Stdin).Decode(&request))
		require.Equal(t, preparation.ProtocolVersion, request.Version)
		require.NoError(t, os.WriteFile(filepath.Join(request.ArtifactDir, "evidence.txt"), []byte("hook evidence for model"), 0o600))
		_, err := fmt.Fprint(os.Stdout, `{"status":"complete","summary":"test evidence index","artifacts":["evidence.txt"],"observed_at":"2026-09-27T00:00:00Z","producer_revision":"mcp-integration-test"}`)
		require.NoError(t, err)
		// The testing package prints its own PASS line on return. The hook protocol
		// requires stdout to contain exactly one JSON value.
		os.Exit(0)
	}
}

func TestAgentRunPreparationArtifactReadThroughMCP(t *testing.T) {
	store.SetDataDir(t.TempDir())
	t.Cleanup(func() { store.SetDataDir("") })
	require.NoError(t, store.EnsureDirs())

	var (
		mu             sync.Mutex
		requests       int
		indexedRunID   string
		toolResultSeen bool
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			http.NotFound(w, r)
			return
		}
		var request struct {
			Messages []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		requests++
		if requests == 1 {
			all := make([]string, 0, len(request.Messages))
			for _, message := range request.Messages {
				all = append(all, message.Content)
			}
			match := regexp.MustCompile(`"run_id":"([a-f0-9]{32})"`).FindStringSubmatch(strings.Join(all, "\n"))
			if len(match) != 2 {
				http.Error(w, "missing evidence index", http.StatusBadRequest)
				return
			}
			indexedRunID = match[1]
			writeOpenAIToolCall(t, w, "artifact_read", map[string]any{"run_id": indexedRunID, "path": "evidence.txt"})
			return
		}
		if requests != 2 {
			http.Error(w, "unexpected model round", http.StatusBadRequest)
			return
		}
		for _, message := range request.Messages {
			if message.Role == "tool" && strings.Contains(message.Content, "hook evidence for model") && strings.Contains(message.Content, indexedRunID) {
				toolResultSeen = true
			}
		}
		if !toolResultSeen {
			http.Error(w, "missing artifact tool result", http.StatusBadRequest)
			return
		}
		writeOpenAIText(w, "evidence read successfully")
	}))
	t.Cleanup(server.Close)

	factory := llm.NewFactory(func(string) (string, error) { return "", nil }).WithProviderOptionsResolver(func(provider string) (llm.ProviderOptions, bool) {
		if provider != "vllm" {
			return llm.ProviderOptions{}, false
		}
		return llm.ProviderOptions{BaseURI: server.URL}, true
	})
	engine, err := preparation.Open(filepath.Join(t.TempDir(), "artifacts"))
	require.NoError(t, err)
	manager := agent.NewManager(factory)
	manager.SetPreparationEngine(engine)
	manager.Reconcile(&config.Config{Agents: []config.AgentConfig{{
		Name:  "prepared",
		Model: "vllm/test-model",
		Hooks: &config.HooksConfig{BeforeTurn: &config.BeforeTurnHookConfig{
			Argv:    []string{os.Args[0], "-test.run=TestPreparationIntegrationHelper", "--", "mcp-preparation"},
			Timeout: "3s",
			OnError: "stop",
		}},
	}}})

	oldDeps, oldDepsSet := GetDeps(), depsSet
	SetDeps(&Deps{Agents: manager})
	t.Cleanup(func() {
		globalDeps, depsSet = oldDeps, oldDepsSet
	})
	agent.SetToolClientFactory(NewAgentToolClient)
	t.Cleanup(func() { agent.SetToolClientFactory(nil) })

	toolProgress := make(chan map[string]any, 4)
	client := sdkmcp.NewClient(&sdkmcp.Implementation{Name: "test-client", Version: "1"}, &sdkmcp.ClientOptions{
		ProgressNotificationHandler: func(_ context.Context, req *sdkmcp.ProgressNotificationClientRequest) {
			if !strings.HasPrefix(req.Params.Message, "[tool]") {
				return
			}
			var payload map[string]any
			if json.Unmarshal([]byte(strings.TrimPrefix(req.Params.Message, "[tool]")), &payload) == nil {
				toolProgress <- payload
			}
		},
	})
	clientTransport, serverTransport := sdkmcp.NewInMemoryTransports()
	connectionCtx, cancelConnection := context.WithCancel(context.Background())
	t.Cleanup(cancelConnection)
	go func() { _, _ = NewServer().Connect(connectionCtx, serverTransport, nil) }()
	mcpSession, err := client.Connect(connectionCtx, clientTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mcpSession.Close()) })
	result, err := mcpSession.CallTool(context.Background(), &sdkmcp.CallToolParams{
		Name:      "agent_run",
		Arguments: map[string]any{"name": "prepared", "message": "Read the indexed evidence and answer.", "history": false, "include_tool_progress": true},
		Meta:      sdkmcp.Meta{"progressToken": "tool-progress-test"},
	})
	require.NoError(t, err)
	require.False(t, result.IsError, extractText(result))
	require.Equal(t, "evidence read successfully", extractText(result))
	var progress []map[string]any
	for range 2 {
		select {
		case payload := <-toolProgress:
			progress = append(progress, payload)
		case <-time.After(3 * time.Second):
			t.Fatal("missing authenticated MCP tool progress")
		}
	}
	require.Equal(t, "artifact_read", progress[0]["name"])
	require.Equal(t, "started", progress[0]["state"])
	require.Equal(t, "succeeded", progress[1]["state"])
	require.NotEmpty(t, progress[0]["invocation_id"])
	require.Equal(t, progress[0]["invocation_id"], progress[1]["invocation_id"])
	require.Equal(t, "evidence.txt", progress[0]["args"].(map[string]any)["path"])
	session, err := agent.NewSessionManager().GetOrCreateNamed("prepared", "main")
	require.NoError(t, err)
	messages, err := store.ReadJSONL[domain.Message](store.SessionPath("prepared", session.ID))
	require.NoError(t, err)
	var persisted bool
	for _, message := range messages {
		if message.Role == domain.MessageRoleAssistant && message.Content == "evidence read successfully" {
			persisted = true
			break
		}
	}
	require.True(t, persisted, "control preparation must not suppress the ordinary agent answer")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 2, requests)
	require.NotEmpty(t, indexedRunID)
	require.True(t, toolResultSeen)
}

func writeOpenAIToolCall(t *testing.T, w http.ResponseWriter, name string, arguments map[string]any) {
	t.Helper()
	encoded, err := json.Marshal(arguments)
	require.NoError(t, err)
	w.Header().Set("Content-Type", "text/event-stream")
	_, err = fmt.Fprintf(w, "data: {\"id\":\"test\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_evidence\",\"type\":\"function\",\"function\":{\"name\":%q,\"arguments\":%q}}]},\"finish_reason\":null}]}\n\n", name, string(encoded))
	require.NoError(t, err)
	_, err = io.WriteString(w, "data: {\"id\":\"test\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n")
	require.NoError(t, err)
}

func writeOpenAIText(w http.ResponseWriter, text string) {
	w.Header().Set("Content-Type", "text/event-stream")
	_, _ = fmt.Fprintf(w, "data: {\"id\":\"test\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":%q},\"finish_reason\":null}]}\n\n", text)
	_, _ = io.WriteString(w, "data: {\"id\":\"test\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
}
