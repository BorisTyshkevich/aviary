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
	"github.com/slack-go/slack/slackevents"

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
	privateRoots := 0
	userInfoCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch r.URL.Path {
		case "/conversations.open":
			_, _ = w.Write([]byte(`{"ok":true,"channel":{"id":"D1"}}`))
		case "/users.info":
			mu.Lock()
			userInfoCalls++
			mu.Unlock()
			_, _ = w.Write([]byte(`{"ok":true,"user":{"id":"U1","profile":{"email":"alice@example.test"}}}`))
		case "/chat.postMessage":
			mu.Lock()
			posts = append(posts, r.FormValue("text"))
			if r.FormValue("channel") == "D1" && r.FormValue("thread_ts") == "" {
				privateRoots++
			}
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "channel": r.FormValue("channel"), "ts": "100.000001"})
		case "/chat.update":
			if strings.Contains(r.FormValue("text"), "database password") && !service.ClassifyReply("BOT", "TEAM", "D1", r.FormValue("ts")) {
				t.Error("actionable password prompt was posted before durable classification")
			}
			mu.Lock()
			posts = append(posts, r.FormValue("text"))
			mu.Unlock()
			_, _ = w.Write([]byte(`{"ok":true,"channel":"D1","ts":"100.000001"}`))
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
			if c.Username != "aviary_reader" || c.Password != "  fake password !  " {
				t.Errorf("credential altered")
			}
			return nil
		}}
	ch.intake = intake.handle
	ch.redactReference = intake.redactReference
	if !intake.handle(slackIngress{UserID: "U1", ChannelID: "C1", RootTS: "50.000001", MessageTS: "50.000001", Text: "<@BOT> connect https://db.example aviary_reader"}) {
		t.Fatal("connect not consumed")
	}
	if _, ok := service.Current(connections.Scope{AgentID: "agent", InstallationID: "BOT", WorkspaceID: "TEAM", ChannelID: "C1", RootThreadID: "50.000001"}); !ok {
		t.Fatal("target not selected")
	}
	if !service.ClassifyReply("BOT", "TEAM", "D1", "100.000001") {
		t.Fatal("password prompt not durable")
	}
	if !intake.handle(slackIngress{UserID: "U1", ChannelID: "C1", RootTS: "50.000001", MessageTS: "50.000006", Text: "<@BOT> connect https://db.example other_reader"}) {
		t.Fatal("username change command not consumed")
	}
	mu.Lock()
	changeRejected := false
	for _, post := range posts {
		if strings.Contains(post, "To change the username, disconnect and reconnect") {
			changeRejected = true
		}
	}
	mu.Unlock()
	if !changeRejected {
		t.Fatal("pending prompt username was silently reused")
	}
	if !intake.handle(slackIngress{UserID: "U2", ChannelID: "D1", RootTS: "100.000001", MessageTS: "100.000004", Text: "wrong principal fake secret", IsDM: true}) {
		t.Fatal("wrong principal reply not suppressed")
	}
	if !intake.handle(slackIngress{UserID: "U1", ChannelID: "D1", RootTS: "100.000001", MessageTS: "100.000005", Text: "  fake password !  ", IsDM: true}) {
		t.Fatal("password not consumed")
	}
	target, _ := service.Current(connections.Scope{AgentID: "agent", InstallationID: "BOT", WorkspaceID: "TEAM", ChannelID: "C1", RootThreadID: "50.000001"})
	exec := connections.Execution{Kind: connections.Interactive, Scope: target.Scope, Principal: connections.Principal{InstallationID: "BOT", WorkspaceID: "TEAM", UserID: "U1"}}
	credential, ok := service.CredentialFor(exec, target)
	if !ok || credential.Password != "  fake password !  " {
		t.Fatal("exact password not stored privately")
	}
	if !intake.handle(slackIngress{UserID: "U1", ChannelID: "C1", RootTS: "50.000001", MessageTS: "50.000007", Text: "<@BOT> connect https://db.example changed_reader"}) {
		t.Fatal("credential username change command not consumed")
	}
	mu.Lock()
	if privateRoots != 1 {
		t.Fatalf("private setup posted %d roots, want one", privateRoots)
	}
	if userInfoCalls != 0 {
		t.Fatal("explicit database username triggered email lookup")
	}
	credentialChangeRejected := false
	for _, post := range posts {
		if strings.Contains(post, "Existing credentials use another username") {
			credentialChangeRejected = true
		}
		if strings.Contains(post, "fake password") {
			t.Fatal("password echoed to Slack")
		}
	}
	mu.Unlock()
	if !credentialChangeRejected {
		t.Fatal("existing credential username was silently reused")
	}
	restarted, err := connections.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	intake.service = restarted
	if !intake.redactReference("D1", "100.000001") {
		t.Fatal("restart lost password classification")
	}
	if !intake.redactReference("D1", "100.000005") {
		t.Fatal("restart lost direct-reply classification")
	}
	if !intake.handle(slackIngress{UserID: "U1", ChannelID: "D1", RootTS: "100.000001", MessageTS: "100.000006", Text: "late fake secret", IsDM: true}) {
		t.Fatal("late password reached ordinary path")
	}
}

func TestSlackConnectionIntakeInfersEmailOrRequestsUsernameOverride(t *testing.T) {
	for _, tc := range []struct {
		name, email string
		wantPrompt  bool
	}{
		{"email", "reader@example.test", true},
		{"missing-email", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, err := connections.Open(filepath.Join(t.TempDir(), "connections"))
			if err != nil {
				t.Fatal(err)
			}
			privatePosts := 0
			var channelReplies []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = r.ParseForm()
				switch r.URL.Path {
				case "/users.info":
					_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "user": map[string]any{"id": "U1", "profile": map[string]any{"email": tc.email}}})
				case "/conversations.open":
					_, _ = w.Write([]byte(`{"ok":true,"channel":{"id":"D1"}}`))
				case "/chat.postMessage":
					if r.FormValue("channel") == "D1" {
						privatePosts++
					} else {
						channelReplies = append(channelReplies, r.FormValue("text"))
					}
					_, _ = w.Write([]byte(`{"ok":true,"channel":"D1","ts":"100.000001"}`))
				case "/chat.update":
					if !service.ClassifyReply("BOT", "TEAM", "D1", r.FormValue("ts")) {
						t.Error("password prompt became actionable before persistence")
					}
					_, _ = w.Write([]byte(`{"ok":true,"channel":"D1","ts":"100.000001"}`))
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			ch := NewSlackChannel("xapp-fake", "xoxb-fake", nil, "", nil)
			ch.client = slack.New("xoxb-fake", slack.OptionAPIURL(server.URL+"/"))
			ch.botUserID, ch.teamID = "BOT", "TEAM"
			spec := channelSpec{agentName: "agent", channelConfig: config.ChannelConfig{Type: "slack", AllowFrom: []config.AllowFromEntry{{From: "U1", AllowedGroups: "C1", RespondToMentions: true}}}}
			intake := &slackConnectionIntake{channel: ch, service: service, specs: []channelSpec{spec}, validateEndpoint: func(context.Context, string, string) error { return nil }}
			if !intake.handle(slackIngress{UserID: "U1", ChannelID: "C1", RootTS: "50.000001", MessageTS: "50.000001", Text: "<@BOT> connect https://db.example"}) {
				t.Fatal("connect not consumed")
			}
			scope := connections.Scope{AgentID: "agent", InstallationID: "BOT", WorkspaceID: "TEAM", ChannelID: "C1", RootThreadID: "50.000001"}
			target, ok := service.Current(scope)
			if !ok {
				t.Fatal("target not attached")
			}
			principal := connections.Principal{InstallationID: "BOT", WorkspaceID: "TEAM", UserID: "U1"}
			prompt, active := service.ActivePromptFor(principal, target)
			if active != tc.wantPrompt || privatePosts != 0 && !tc.wantPrompt {
				t.Fatalf("prompt=%v private posts=%d", active, privatePosts)
			}
			if tc.wantPrompt {
				if privatePosts != 1 || prompt.Stage != "password" || prompt.Username != tc.email {
					t.Fatal("email did not create one password-only prompt")
				}
			} else if len(channelReplies) != 1 || channelReplies[0] != slackEmailUnavailableMessage {
				t.Fatalf("missing email guidance: %v", channelReplies)
			}
		})
	}
}

func TestSlackConnectionIntakeDoesNotRepromptDuringPasswordValidation(t *testing.T) {
	service, err := connections.Open(filepath.Join(t.TempDir(), "connections"))
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	privateRoots := 0
	readyReplies := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch r.URL.Path {
		case "/conversations.open":
			_, _ = w.Write([]byte(`{"ok":true,"channel":{"id":"D1"}}`))
		case "/chat.postMessage":
			if strings.Contains(r.FormValue("text"), "Your credentials are ready") {
				mu.Lock()
				readyReplies++
				mu.Unlock()
			}
			if r.FormValue("channel") == "D1" && r.FormValue("thread_ts") == "" {
				mu.Lock()
				privateRoots++
				mu.Unlock()
			}
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
	spec := channelSpec{agentName: "agent", channelConfig: config.ChannelConfig{Type: "slack", AllowFrom: []config.AllowFromEntry{{From: "U1", AllowedGroups: "C1", RespondToMentions: true}}}}
	entered, release := make(chan struct{}), make(chan struct{})
	intake := &slackConnectionIntake{channel: ch, service: service, specs: []channelSpec{spec},
		validateEndpoint: func(context.Context, string, string) error { return nil },
		validatePassword: func(_ context.Context, _ connections.Target, c connections.Credential) error {
			if c.Username != "reader" || c.Password != "fake-secret-test-only" {
				return connections.ErrValidation
			}
			close(entered)
			<-release
			return nil
		}}
	connect := slackIngress{UserID: "U1", ChannelID: "C1", RootTS: "50.000001", MessageTS: "50.000001", Text: "<@BOT> connect https://db.example reader"}
	if !intake.handle(connect) {
		t.Fatal("connect not consumed")
	}
	passwordDone := make(chan bool, 1)
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	doneRead := false
	defer func() {
		unblock()
		if !doneRead {
			select {
			case <-passwordDone:
			case <-time.After(time.Second):
				t.Error("password validation did not finish during cleanup")
			}
		}
	}()
	go func() {
		passwordDone <- intake.handle(slackIngress{UserID: "U1", ChannelID: "D1", RootTS: "100.000001", MessageTS: "100.000002", Text: "fake-secret-test-only", IsDM: true})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("password validation did not start")
	}
	intake.ensureSetupNow("agent", IncomingMessage{From: "U1", Channel: "C1", ThreadTS: "50.000001"})
	connect.MessageTS = "50.000003"
	if !intake.handle(connect) {
		t.Fatal("repeat connect not consumed")
	}
	mu.Lock()
	count := privateRoots
	mu.Unlock()
	if count != 1 {
		t.Fatalf("validation created %d private prompts", count)
	}
	unblock()
	consumed := <-passwordDone
	doneRead = true
	if !consumed {
		t.Fatal("password reply not consumed")
	}
	connect.MessageTS = "50.000004"
	if !intake.handle(connect) {
		t.Fatal("post-validation connect not consumed")
	}
	mu.Lock()
	count = privateRoots
	ready := readyReplies
	mu.Unlock()
	if count != 1 || ready == 0 {
		t.Fatalf("saved credential was not reused: prompts=%d ready replies=%d", count, ready)
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

func TestClassifiedReplyLookupCancelsWithIntake(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(entered)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	ch := NewSlackChannel("xapp-fake", "xoxb-fake", nil, "", nil)
	ch.client = slack.New("xoxb-fake", slack.OptionAPIURL(server.URL+"/"))
	intake := &slackConnectionIntake{channel: ch}
	ctx, cancel := context.WithCancel(context.Background())
	intake.start(ctx)
	defer func() { cancel(); intake.wait() }()
	ch.intakeDeferred = intake.enqueue
	ch.intakeContext = intake.baseContext
	ch.redactReference = func(channel, root string) bool { return channel == "D1" && root == "100.000001" }
	ch.handleMessageEvent(&slackevents.MessageEvent{
		SubType: slack.MsgSubTypeMessageReplied, Channel: "D1", ThreadTimeStamp: "100.000001", TimeStamp: "100.000001",
		Message: &slack.Msg{Timestamp: "100.000001", LatestReply: "100.000002"},
	})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("classified reply lookup did not start")
	}
	cancel()
	done := make(chan struct{})
	go func() { intake.wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("classified reply lookup delayed intake shutdown")
	}
}
