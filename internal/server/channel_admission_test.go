package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/agent"
	"github.com/lsegal/aviary/internal/channels"
	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/domain"
	"github.com/lsegal/aviary/internal/store"
)

type admissionChannel struct {
	stubChannel
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.Mutex
	posts   []string
	threads []string
}

type drainingStatusChannel struct {
	admissionChannel
	clearEntered chan struct{}
	releaseClear chan struct{}
	clearOnce    sync.Once
}

func (c *drainingStatusChannel) ShowAssistantStatus() bool { return true }

func (c *drainingStatusChannel) SendAssistantStatus(_, _, status string) error {
	if status == "" {
		c.clearOnce.Do(func() { close(c.clearEntered) })
		<-c.releaseClear
	}
	return nil
}

func (c *admissionChannel) ShowTyping() bool {
	if c.entered != nil {
		c.once.Do(func() { close(c.entered) })
		<-c.release
	}
	return false
}

func (c *admissionChannel) Send(_, text string) error {
	c.mu.Lock()
	c.posts = append(c.posts, text)
	c.mu.Unlock()
	return nil
}

func (c *admissionChannel) SendThreadMessageAndGetID(_, threadTS, text string) (string, error) {
	c.mu.Lock()
	c.threads = append(c.threads, threadTS+":"+text)
	c.mu.Unlock()
	return "notice", nil
}

func (c *admissionChannel) SendTyping(string, bool) error { return nil }

func (c *admissionChannel) snapshot() ([]string, []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string{}, c.posts...), append([]string{}, c.threads...)
}

func signalAdmissionConfig(model string) *config.Config {
	return &config.Config{Agents: []config.AgentConfig{{Name: "bot", Model: model,
		Channels: []config.ChannelConfig{{Type: "signal", ID: "route"}}}}}
}

func TestChannelAdmissionReloadRetriesOnlyRejectedRun(t *testing.T) {
	setupServerDataDir(t)
	resetSlogForTest()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := signalAdmissionConfig("stub-one")
	srv := New(cfg, "tok")
	srv.channels.Reconcile(ctx, cfg, func(string, string, string, channels.Channel, channels.IncomingMessage) {})
	defer srv.channels.Stop()
	old, ok := srv.agents.Get("bot")
	require.True(t, ok)
	ch := &admissionChannel{entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		srv.handleIncomingChannelMessage(ctx, "bot", "signal", "route", ch, channels.IncomingMessage{
			Type: "signal", Channel: "+15550001111", From: "+15550002222", Text: "synthetic request",
		})
		close(done)
	}()
	select {
	case <-ch.entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not reach pre-admission gate")
	}
	updated := signalAdmissionConfig("stub-two")
	srv.cfg = updated
	srv.agents.Reconcile(updated)
	srv.channels.Reconcile(ctx, updated, func(string, string, string, channels.Channel, channels.IncomingMessage) {})
	close(ch.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("rejected admission did not hand off to replacement")
	}
	old.Wait()
	replacement, ok := srv.agents.Get("bot")
	require.True(t, ok)
	replacement.Wait()
	posts, threads := ch.snapshot()
	require.Empty(t, posts)
	require.Empty(t, threads, "accepted replacement must not send a resend notice")
	sessions, err := agent.NewSessionManager().List("bot")
	require.NoError(t, err)
	var users int
	for _, session := range sessions {
		messages, err := store.ReadJSONL[domain.Message](store.SessionPath("bot", session.ID))
		if err != nil {
			continue
		}
		for _, message := range messages {
			if message.Role == domain.MessageRoleUser && message.Content == "synthetic request" {
				users++
			}
		}
	}
	require.Equal(t, 1, users, "rejected run must not persist or replay the request")
}

func TestChannelAdmissionRepeatedRejectionRepliesInOriginalThread(t *testing.T) {
	setupServerDataDir(t)
	resetSlogForTest()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := signalAdmissionConfig("stub")
	srv := New(cfg, "tok")
	srv.channels.Reconcile(ctx, cfg, func(string, string, string, channels.Channel, channels.IncomingMessage) {})
	defer srv.channels.Stop()
	runner, ok := srv.agents.Get("bot")
	require.True(t, ok)
	runner.Stop()
	ch := &admissionChannel{}
	srv.handleIncomingChannelMessage(ctx, "bot", "signal", "route", ch, channels.IncomingMessage{
		Type: "signal", Channel: "+15550001111", ThreadTS: "original", From: "+15550002222", Text: "request",
	})
	posts, threads := ch.snapshot()
	require.Empty(t, posts)
	require.Equal(t, []string{"original:Restarting; please resend your request."}, threads)
	require.Zero(t, checkpointCountForServerTest("bot"))
}

func TestNonSlackIngressStopsBeforeRunnerAdmission(t *testing.T) {
	setupServerDataDir(t)
	resetSlogForTest()
	srv := New(signalAdmissionConfig("stub"), "tok")
	srv.nonSlackIngressClosed.Store(true)
	ch := &admissionChannel{}
	srv.handleIncomingChannelMessage(context.Background(), "bot", "signal", "route", ch, channels.IncomingMessage{
		Type: "signal", Channel: "+15550001111", From: "+15550002222", Text: "late request",
	})
	posts, threads := ch.snapshot()
	require.Empty(t, posts)
	require.Empty(t, threads)
	require.Zero(t, checkpointCountForServerTest("bot"))
}

func TestServerRootCancellationClosesAdmissionAndTransports(t *testing.T) {
	setupServerDataDir(t)
	resetSlogForTest()
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := probe.Addr().(*net.TCPAddr).Port
	require.NoError(t, probe.Close())
	cfg := signalAdmissionConfig("stub")
	cfg.Server.Port = port
	cfg.Server.NoTLS = true
	srv := New(cfg, "tok")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.ListenAndServe(ctx) }()
	defer cancel()
	deadline := time.After(2 * time.Second)
	for {
		connection, dialErr := net.DialTimeout("tcp", "127.0.0.1:"+fmt.Sprint(port), 20*time.Millisecond)
		if dialErr == nil {
			_ = connection.Close()
			break
		}
		select {
		case err := <-done:
			t.Fatalf("server exited before listener was ready: %v", err)
		case <-deadline:
			t.Fatal("server listener did not start")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("server did not finish bounded shutdown")
	}
	require.True(t, srv.nonSlackIngressClosed.Load())
	runner, ok := srv.agents.Get("bot")
	require.True(t, ok)
	require.Equal(t, agent.AdmissionRejectedStopping, runner.Prompt(context.Background(), "late").Status)
}

func TestServerRootCancellationDrainsAdmittedChannelRun(t *testing.T) {
	setupServerDataDir(t)
	resetSlogForTest()
	modelEntered := make(chan struct{}, 1)
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {\"id\":\"test\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"},\"finish_reason\":null}]}\n\n")
		w.(http.Flusher).Flush()
		modelEntered <- struct{}{}
		<-r.Context().Done()
	}))
	defer model.Close()
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := probe.Addr().(*net.TCPAddr).Port
	require.NoError(t, probe.Close())
	cfg := signalAdmissionConfig("vllm/test")
	cfg.Models.Providers = map[string]config.ProviderConfig{"vllm": {BaseURI: model.URL}}
	cfg.Server.Port, cfg.Server.NoTLS = port, true
	srv := New(cfg, "tok")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.ListenAndServe(ctx) }()
	defer cancel()
	deadline := time.After(2 * time.Second)
	for {
		connection, dialErr := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 20*time.Millisecond)
		if dialErr == nil {
			_ = connection.Close()
			break
		}
		select {
		case err := <-done:
			t.Fatalf("server exited before listener was ready: %v", err)
		case <-deadline:
			t.Fatal("server listener did not start")
		case <-time.After(time.Millisecond):
		}
	}
	ch := &drainingStatusChannel{clearEntered: make(chan struct{}), releaseClear: make(chan struct{})}
	<-srv.routerReady
	srv.msgFn("bot", "signal", "route", ch, channels.IncomingMessage{
		Type: "signal", Channel: "+15550001111", ThreadTS: "original", From: "+15550002222", Text: "request",
	})
	select {
	case <-modelEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("admitted model request did not start")
	}
	cancel()
	select {
	case <-ch.clearEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("runner stop did not enter terminal callback")
	}
	select {
	case err := <-done:
		t.Fatalf("server returned before terminal callback completed: %v", err)
	default:
	}
	close(ch.releaseClear)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("server did not finish after terminal callback")
	}
	require.Positive(t, checkpointCountForServerTest("bot"), "runner stop must retain replayable channel checkpoint")
}

func checkpointCountForServerTest(agentID string) int {
	entries, err := store.ListJSON[agent.RunCheckpoint](store.CheckpointDir(agentID), ".json")
	if err != nil {
		return -1
	}
	return len(entries)
}
