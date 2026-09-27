package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/connections"
	"github.com/lsegal/aviary/internal/domain"
	"github.com/lsegal/aviary/internal/llm"
	"github.com/lsegal/aviary/internal/preparation"
	"github.com/lsegal/aviary/internal/store"
)

// TestAgentPreparationHelper runs as the administrator-configured executable.
func TestAgentPreparationHelper(_ *testing.T) {
	mode, marker := "", ""
	for i, arg := range os.Args {
		if arg == "--" && i+2 < len(os.Args) {
			mode, marker = os.Args[i+1], os.Args[i+2]
			break
		}
	}
	if mode == "" {
		return
	}
	var req struct {
		Version     int    `json:"version"`
		ArtifactDir string `json:"artifact_dir"`
		Credential  *struct {
			Username string `json:"username"`
			Password string `json:"password"`
			Version  string `json:"version"`
		} `json:"credential"`
	}
	if json.NewDecoder(os.Stdin).Decode(&req) != nil || req.Version != preparation.ProtocolVersion {
		os.Exit(2)
	}
	file, err := os.OpenFile(marker, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		os.Exit(2)
	}
	_, _ = file.WriteString("run\n")
	_ = file.Close()
	if mode == "fail" {
		os.Exit(3)
	}
	if mode == "no-secret" && req.Credential != nil {
		os.Exit(4)
	}
	if mode == "expect-secret" && (req.Credential == nil || req.Credential.Username != "alice" || req.Credential.Password != "fake-agent-password") {
		os.Exit(4)
	}
	if strings.HasPrefix(mode, "status-") {
		status := strings.TrimPrefix(mode, "status-")
		_, _ = fmt.Fprintf(os.Stdout, `{"status":%q,"summary":"collector reported status","artifacts":[],"observed_at":"2026-09-27T00:00:00Z","producer_revision":"fake-agent-test"}`, status)
		os.Exit(0)
	}
	if err := os.WriteFile(filepath.Join(req.ArtifactDir, "evidence.txt"), []byte("private artifact"), 0o600); err != nil {
		os.Exit(2)
	}
	_, _ = fmt.Fprint(os.Stdout, `{"status":"complete","summary":"private index","artifacts":["evidence.txt"],"observed_at":"2026-09-27T00:00:00Z","producer_revision":"fake-agent-test"}`)
	os.Exit(0)
}

type preparationProvider struct {
	mu       sync.Mutex
	requests []llm.Request
	marker   string
	counts   []int
	artifact string
	err      error
}

func (p *preparationProvider) Stream(ctx context.Context, req llm.Request) (<-chan llm.Event, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = append(p.requests, req)
	b, _ := os.ReadFile(p.marker)
	p.counts = append(p.counts, strings.Count(string(b), "run\n"))
	if state, ok := PreparationFromContext(ctx); ok {
		data, err := state.Engine.Read(ctx, state.Input, state.Result.RunID, "evidence.txt", 1024)
		if err == nil {
			p.artifact = string(data)
		}
	}
	if p.err != nil {
		return nil, p.err
	}
	ch := make(chan llm.Event, 2)
	ch <- llm.Event{Type: llm.EventTypeText, Text: "answer"}
	ch <- llm.Event{Type: llm.EventTypeDone}
	close(ch)
	return ch, nil
}

func preparationRunner(t *testing.T, mode, policy string) (*AgentRunner, string) {
	t.Helper()
	setTestDataDir(t)
	engine, err := preparation.Open(filepath.Join(t.TempDir(), "artifacts"))
	require.NoError(t, err)
	marker := filepath.Join(t.TempDir(), "runs.txt")
	r := NewAgentRunner(&domain.Agent{ID: "prep-agent", Name: "bot", Model: "anthropic/test"}, &config.AgentConfig{Name: "bot", Hooks: &config.HooksConfig{BeforeTurn: &config.BeforeTurnHookConfig{Argv: []string{os.Args[0], "-test.run=TestAgentPreparationHelper", "--", mode, marker}, Timeout: "3s", OnError: policy, AllowCredential: true}}}, nil, nil)
	r.preparation = engine
	return r, marker
}

func runPreparationPrompt(ctx context.Context, t *testing.T, r *AgentRunner, overrides RunOverrides) StreamEvent {
	t.Helper()
	events := make(chan StreamEvent, 1)
	r.PromptWithOverrides(ctx, "question", overrides, func(e StreamEvent) {
		if e.Type == StreamEventDone || e.Type == StreamEventError || e.Type == StreamEventStop {
			select {
			case events <- e:
			default:
			}
		}
	})
	select {
	case e := <-events:
		r.Wait()
		return e
	case <-time.After(5 * time.Second):
		t.Fatal("preparation turn timed out")
		return StreamEvent{}
	}
}

func attachPreparationPrincipal(t *testing.T, r *AgentRunner) context.Context {
	t.Helper()
	s, err := connections.Open(filepath.Join(t.TempDir(), "connections"))
	require.NoError(t, err)
	scope := connections.Scope{AgentID: r.agent.ID, InstallationID: "install", WorkspaceID: "workspace", ChannelID: "channel", RootThreadID: "thread"}
	target, _, err := s.Select(scope, "clickhouse", "https://fake.example")
	require.NoError(t, err)
	p := connections.Principal{InstallationID: "install", WorkspaceID: "workspace", UserID: "alice"}
	require.NoError(t, s.PutPrompt(connections.Prompt{Principal: p, DMChannelID: "dm", DMRootID: "password-prompt", Target: target, Stage: "password", Username: "alice", ExpiresAt: time.Now().Add(time.Hour)}))
	require.NoError(t, s.CompletePassword(context.Background(), p, "dm", "password-prompt", "100.000001", "fake-agent-password", func(context.Context, connections.Target, connections.Credential) error { return nil }))
	r.connections = s
	return connections.WithExecution(WithSessionID(context.Background(), "shared"), connections.Execution{Kind: connections.Interactive, Scope: scope, Principal: p})
}

func TestPreparationRunsBeforeFirstModelAndOnceAcrossFallback(t *testing.T) {
	r, marker := preparationRunner(t, "expect-secret", "continue")
	ctx := attachPreparationPrincipal(t, r)
	primary := &preparationProvider{marker: marker, err: errors.New("429 rate limit")}
	fallback := &preparationProvider{marker: marker}
	r.provider = primary
	r.agent.Fallbacks = []string{"openai/fallback"}
	r.factory = &testProviderFactory{providers: map[string]llm.Provider{"openai/fallback": fallback}}
	event := runPreparationPrompt(ctx, t, r, RunOverrides{Bare: true})
	require.Equal(t, StreamEventDone, event.Type)
	require.Equal(t, []int{1}, primary.counts)
	require.Equal(t, []int{1}, fallback.counts)
	require.Equal(t, "private artifact", fallback.artifact)
	require.Len(t, fallback.requests, 1)
	require.Contains(t, fmt.Sprint(fallback.requests[0].Messages), "private index")
	require.NotContains(t, fmt.Sprint(fallback.requests[0].Messages), "fake-agent-password")
	b, err := os.ReadFile(store.SessionPath("prep-agent", "shared"))
	require.NoError(t, err)
	require.NotContains(t, string(b), "private index")
	require.NotContains(t, string(b), "private artifact")
	require.NotContains(t, string(b), "fake-agent-password")
}

func TestPreparationFailurePolicy(t *testing.T) {
	for _, tc := range []struct {
		policy    string
		wantCalls int
		wantEvent StreamEventType
	}{{"continue", 1, StreamEventDone}, {"stop", 0, StreamEventError}} {
		t.Run(tc.policy, func(t *testing.T) {
			r, marker := preparationRunner(t, "fail", tc.policy)
			provider := &preparationProvider{marker: marker}
			r.provider = provider
			event := runPreparationPrompt(context.Background(), t, r, RunOverrides{Bare: true})
			require.Equal(t, tc.wantEvent, event.Type)
			require.Len(t, provider.requests, tc.wantCalls)
			if tc.wantCalls > 0 {
				require.Contains(t, fmt.Sprint(provider.requests[0].Messages), "Preparation evidence is unavailable")
			}
		})
	}
}

func TestPreparationStructuredStatusRespectsFailurePolicy(t *testing.T) {
	for _, status := range []string{"unavailable", "denied", "timed_out", "partial"} {
		for _, policy := range []string{"continue", "stop"} {
			t.Run(status+"/"+policy, func(t *testing.T) {
				r, marker := preparationRunner(t, "status-"+status, policy)
				provider := &preparationProvider{marker: marker}
				r.provider = provider
				event := runPreparationPrompt(context.Background(), t, r, RunOverrides{Bare: true})
				if policy == "stop" && status != "partial" {
					require.Equal(t, StreamEventError, event.Type)
					require.Empty(t, provider.requests)
					return
				}
				require.Equal(t, StreamEventDone, event.Type)
				require.Len(t, provider.requests, 1)
				require.Contains(t, fmt.Sprint(provider.requests[0].Messages), `"status":"`+status+`"`)
				require.Contains(t, fmt.Sprint(provider.requests[0].Messages), "collector reported status")
			})
		}
	}
}

func TestPreparationCanonicalToolDenyWithholdsCredential(t *testing.T) {
	r, marker := preparationRunner(t, "no-secret", "stop")
	ctx := attachPreparationPrincipal(t, r)
	provider := &preparationProvider{marker: marker}
	r.provider = provider
	event := runPreparationPrompt(ctx, t, r, RunOverrides{Bare: true, DisabledTools: []string{"clickhouse_query"}})
	require.Equal(t, StreamEventDone, event.Type)
	require.Equal(t, []int{1}, provider.counts)
	require.Equal(t, "private artifact", provider.artifact)
}
