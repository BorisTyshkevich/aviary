package channels

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/slack-go/slack"

	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/connections"
)

func TestSlackConnectionIntakePrivateSetupAndRestartRedaction(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "connections")
	service, err := connections.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var posts []string
	postCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch r.URL.Path {
		case "/conversations.open":
			_, _ = w.Write([]byte(`{"ok":true,"channel":{"id":"D1"}}`))
		case "/users.info":
			_, _ = w.Write([]byte(`{"ok":true,"user":{"id":"U1","profile":{"email":"alice@example.test"}}}`))
		case "/chat.postMessage":
			mu.Lock()
			posts = append(posts, r.FormValue("text"))
			postCount++
			n := postCount
			mu.Unlock()
			root := "100.000001"
			if n == 3 {
				root = "100.000003"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "channel": r.FormValue("channel"), "ts": root})
		case "/chat.update":
			if strings.Contains(r.FormValue("text"), "password prompt") && !service.ClassifyReply("BOT", "TEAM", "D1", r.FormValue("ts")) {
				t.Error("actionable password prompt was posted before durable classification")
			}
			mu.Lock()
			posts = append(posts, r.FormValue("text"))
			mu.Unlock()
			_, _ = w.Write([]byte(`{"ok":true,"channel":"D1","ts":"100.000003"}`))
		default:
			t.Errorf("unexpected Slack API path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	ch := NewSlackChannel("xapp-fake", "xoxb-fake", nil, "", nil)
	ch.client = slack.New("xoxb-fake", slack.OptionAPIURL(server.URL+"/"))
	ch.botUserID, ch.teamID = "BOT", "TEAM"
	spec := channelSpec{agentName: "agent", channelConfig: config.ChannelConfig{Type: "slack", AllowFrom: []config.AllowFromEntry{{From: "U1", AllowedGroups: "C1", RespondToMentions: true}}}}
	intake := &slackConnectionIntake{channel: ch, service: service, specs: []channelSpec{spec},
		validateEndpoint: func(context.Context, string, string) error { return nil },
		validatePassword: func(_ context.Context, _ connections.Target, c connections.Credential) error {
			if c.Username != "alice@example.test" || c.Password != "  fake password !  " {
				t.Errorf("credential altered")
			}
			return nil
		}}
	ch.intake = intake.handle
	ch.redactReference = intake.redactReference
	if !intake.handle(slackIngress{UserID: "U1", ChannelID: "C1", RootTS: "50.000001", MessageTS: "50.000001", Text: "<@BOT> connect https://db.example"}) {
		t.Fatal("connect not consumed")
	}
	if _, ok := service.Current(connections.Scope{AgentID: "agent", InstallationID: "BOT", WorkspaceID: "TEAM", ChannelID: "C1", RootThreadID: "50.000001"}); !ok {
		t.Fatal("target not selected")
	}
	if !service.ClassifyReply("BOT", "TEAM", "D1", "100.000001") {
		t.Fatal("username prompt not durable")
	}
	if !intake.handle(slackIngress{UserID: "U1", ChannelID: "D1", RootTS: "100.000001", MessageTS: "100.000002", Text: "use proposed", IsDM: true}) {
		t.Fatal("username reply not consumed")
	}
	if !service.ClassifyReply("BOT", "TEAM", "D1", "100.000003") {
		t.Fatal("password prompt not durable")
	}
	if !intake.handle(slackIngress{UserID: "U2", ChannelID: "D1", RootTS: "100.000003", MessageTS: "100.000004", Text: "wrong principal fake secret", IsDM: true}) {
		t.Fatal("wrong principal reply not suppressed")
	}
	if !intake.handle(slackIngress{UserID: "U1", ChannelID: "D1", RootTS: "100.000003", MessageTS: "100.000005", Text: "  fake password !  ", IsDM: true}) {
		t.Fatal("password not consumed")
	}
	target, _ := service.Current(connections.Scope{AgentID: "agent", InstallationID: "BOT", WorkspaceID: "TEAM", ChannelID: "C1", RootThreadID: "50.000001"})
	exec := connections.Execution{Kind: connections.Interactive, Scope: target.Scope, Principal: connections.Principal{InstallationID: "BOT", WorkspaceID: "TEAM", UserID: "U1"}}
	credential, ok := service.CredentialFor(exec, target)
	if !ok || credential.Password != "  fake password !  " {
		t.Fatal("exact password not stored privately")
	}
	mu.Lock()
	for _, post := range posts {
		if strings.Contains(post, "fake password") {
			t.Fatal("password echoed to Slack")
		}
	}
	mu.Unlock()
	restarted, err := connections.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	intake.service = restarted
	if !intake.redactReference("D1", "100.000003") {
		t.Fatal("restart lost password classification")
	}
	if !intake.redactReference("D1", "100.000005") {
		t.Fatal("restart lost direct-reply classification")
	}
	if !intake.handle(slackIngress{UserID: "U1", ChannelID: "D1", RootTS: "100.000003", MessageTS: "100.000006", Text: "late fake secret", IsDM: true}) {
		t.Fatal("late password reached ordinary path")
	}
}

func TestSlackConnectionIntakeRejectsAmbiguousAndUnauthorizedCommands(t *testing.T) {
	var mu sync.Mutex
	var posted []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		mu.Lock()
		posted = append(posted, r.FormValue("text"))
		mu.Unlock()
		_, _ = w.Write([]byte(`{"ok":true,"channel":"C1","ts":"100.000002"}`))
	}))
	defer server.Close()
	ch := NewSlackChannel("xapp-fake", "xoxb-fake", nil, "", nil)
	ch.client = slack.New("xoxb-fake", slack.OptionAPIURL(server.URL+"/"))
	ch.botUserID, ch.teamID = "BOT", "TEAM"
	allowed := config.ChannelConfig{Type: "slack", AllowFrom: []config.AllowFromEntry{{From: "U1", AllowedGroups: "C1", RespondToMentions: true}}}
	intake := &slackConnectionIntake{channel: ch, specs: []channelSpec{{agentName: "a", channelConfig: allowed}, {agentName: "b", channelConfig: allowed}}}
	if !intake.handle(slackIngress{UserID: "U1", ChannelID: "C1", RootTS: "100.000001", MessageTS: "100.000001", Text: "<@BOT> connect https://db.example"}) {
		t.Fatal("ambiguous command not consumed")
	}
	mu.Lock()
	if len(posted) != 1 || !strings.Contains(posted[0], "multiple agents") {
		t.Fatalf("ambiguous response: %v", posted)
	}
	mu.Unlock()
	if !intake.handle(slackIngress{UserID: "U2", ChannelID: "C1", RootTS: "100.000001", MessageTS: "100.000003", Text: "<@BOT> disconnect"}) {
		t.Fatal("unauthorized command not consumed")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(posted) != 1 {
		t.Fatalf("unauthorized sender got response: %v", posted)
	}
}

func TestSlackPasswordPromptNeverBecomesActionableWhenPersistenceFails(t *testing.T) {
	service, err := connections.Open(filepath.Join(t.TempDir(), "connections"))
	if err != nil {
		t.Fatal(err)
	}
	scope := connections.Scope{AgentID: "agent", InstallationID: "BOT", WorkspaceID: "TEAM", ChannelID: "C1", RootThreadID: "50.000001"}
	target, _, err := service.Select(scope, "clickhouse", "https://db.example")
	if err != nil {
		t.Fatal(err)
	}
	principal := connections.Principal{InstallationID: "BOT", WorkspaceID: "TEAM", UserID: "U1"}
	var updated, deleted bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch r.URL.Path {
		case "/chat.postMessage":
			if strings.Contains(r.FormValue("text"), "password") {
				t.Error("secret request posted before classification")
			}
			if err := service.Disconnect(scope); err != nil {
				t.Error(err)
			}
			_, _ = w.Write([]byte(`{"ok":true,"channel":"D1","ts":"100.000001"}`))
		case "/chat.update":
			updated = true
			_, _ = w.Write([]byte(`{"ok":true,"channel":"D1","ts":"100.000001"}`))
		case "/chat.delete":
			deleted = true
			_, _ = w.Write([]byte(`{"ok":true,"channel":"D1","ts":"100.000001"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	ch := NewSlackChannel("xapp-fake", "xoxb-fake", nil, "", nil)
	ch.client = slack.New("xoxb-fake", slack.OptionAPIURL(server.URL+"/"))
	intake := &slackConnectionIntake{channel: ch, service: service}
	err = intake.postClassifiedPrompt("D1", "Reply with password", connections.Prompt{Principal: principal, DMChannelID: "D1", Target: target, Stage: "password", Username: "alice", ExpiresAt: time.Now().Add(time.Minute)})
	if err == nil || updated || !deleted {
		t.Fatalf("failed prompt was actionable: err=%v updated=%v deleted=%v", err, updated, deleted)
	}
	if !service.ClassifyReply("BOT", "TEAM", "D1", "100.000001") {
		t.Fatal("failed prompt lost tombstone")
	}
}

func TestSlackConnectionIntakeDoesNotBlockSocketEventLoop(t *testing.T) {
	service, err := connections.Open(filepath.Join(t.TempDir(), "connections"))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch r.URL.Path {
		case "/conversations.open":
			_, _ = w.Write([]byte(`{"ok":true,"channel":{"id":"D1"}}`))
		case "/users.info":
			_, _ = w.Write([]byte(`{"ok":true,"user":{"id":"U1","profile":{"email":"alice@example.test"}}}`))
		case "/chat.postMessage":
			_, _ = w.Write([]byte(`{"ok":true,"channel":"D1","ts":"100.000001"}`))
		case "/chat.update":
			_, _ = w.Write([]byte(`{"ok":true,"channel":"D1","ts":"100.000001"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	ch := NewSlackChannel("xapp-fake", "xoxb-fake", nil, "", nil)
	ch.client = slack.New("xoxb-fake", slack.OptionAPIURL(server.URL+"/"))
	ch.botUserID, ch.teamID = "BOT", "TEAM"
	entered, release := make(chan struct{}), make(chan struct{})
	intake := &slackConnectionIntake{channel: ch, service: service,
		validateEndpoint: func(ctx context.Context, _, _ string) error {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		specs: []channelSpec{{agentName: "agent", channelConfig: config.ChannelConfig{Type: "slack", AllowFrom: []config.AllowFromEntry{{From: "U1", AllowedGroups: "C1", RespondToMentions: true}}}}}}
	ctx, cancel := context.WithCancel(context.Background())
	intake.start(ctx)
	defer func() {
		cancel()
		intake.wait()
	}()
	returned := make(chan bool, 1)
	go func() {
		returned <- intake.handle(slackIngress{UserID: "U1", ChannelID: "C1", RootTS: "50.000001", MessageTS: "50.000001", Text: "<@BOT> connect <https://db.example>"})
	}()
	select {
	case ok := <-returned:
		if !ok {
			t.Fatal("command not consumed")
		}
	case <-time.After(time.Second):
		t.Fatal("socket dispatch blocked on validation")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	close(release)
	scope := connections.Scope{AgentID: "agent", InstallationID: "BOT", WorkspaceID: "TEAM", ChannelID: "C1", RootThreadID: "50.000001"}
	deadline := time.After(2 * time.Second)
	for {
		if _, ok := service.Current(scope); ok {
			break
		}
		select {
		case <-deadline:
			t.Fatal("async command did not attach target")
		case <-time.After(time.Millisecond):
		}
	}
}
