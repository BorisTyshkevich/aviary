package channels

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/socketmode"
	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/config"
)

func TestSlackIngressGateDrainsAcceptedEnvelopeAndKeepsOutgoingClient(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/chat.postMessage", r.URL.Path)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "channel": "C1", "ts": "1.000001"})
	}))
	defer api.Close()
	ch := NewSlackChannel("xapp-fake", "xoxb-fake", nil, "", nil)
	ch.client = slack.New("xoxb-fake", slack.OptionAPIURL(api.URL+"/"))
	entered := make(chan struct{})
	release := make(chan struct{})
	var acknowledgements atomic.Int32
	ch.ackEnvelope = func(context.Context, *socketmode.Request) error {
		acknowledgements.Add(1)
		close(entered)
		<-release
		return nil
	}
	event := socketmode.Event{Type: socketmode.EventTypeEventsAPI, Request: &socketmode.Request{}}
	handled := make(chan struct{})
	go func() { ch.dispatch(event); close(handled) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("envelope was not acknowledged")
	}
	stopped := make(chan struct{})
	go func() { ch.Stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("gate closed while an acknowledgement was still in flight")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("gate did not close after acknowledgement finished")
	}
	ch.dispatch(event)
	require.Equal(t, int32(1), acknowledgements.Load(), "gate acknowledged a new envelope")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	require.NoError(t, ch.SendThreadPlainTextContext(ctx, "C1", "1.000000", "still outgoing"))
	cancel()
	select {
	case <-handled:
	case <-time.After(time.Second):
		t.Fatal("acknowledged envelope did not finish")
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	require.NoError(t, ch.WaitIngress(ctx))
	cancel()
}

func TestSlackStopBeforeStartPreventsSocketOpening(t *testing.T) {
	ch := NewSlackChannel("xapp-fake", "xoxb-fake", nil, "", nil)
	ch.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, ch.WaitSocketClosed(ctx))
	require.NoError(t, ch.Start(ctx))
	require.False(t, ch.ingressStarted)
}

func TestSlackWaitSocketClosedHonorsDeadline(t *testing.T) {
	ch := NewSlackChannel("xapp-fake", "xoxb-fake", nil, "", nil)
	ch.ingressMu.Lock()
	ch.ingressStarted = true
	ch.ingressMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.True(t, errors.Is(ch.WaitSocketClosed(ctx), context.DeadlineExceeded))
	close(ch.socketDone)
	require.NoError(t, ch.WaitSocketClosed(context.Background()))
}

func TestManagerRestartWaitsForOldSocketClosure(t *testing.T) {
	manager := NewManager()
	old := NewSlackChannel("xapp-fake", "xoxb-fake", nil, "", nil)
	old.ingressMu.Lock()
	old.ingressStarted = true
	old.ingressMu.Unlock()
	cc := config.ChannelConfig{Type: "slack", ID: "route", URL: "xapp-fake", Token: "xoxb-fake",
		AllowFrom: []config.AllowFromEntry{{From: "U1"}}}
	old.botUserID, old.teamID = "BOT", "TEAM"
	spec := channelSpec{agentName: "agent", channelConfig: cc}
	key := channelKey("agent", "slack", "route")
	connKey := slackConnectionKey(cc)
	manager.channels[key] = old
	manager.specs[key] = spec
	manager.slackAlias[key] = connKey
	manager.slack[connKey] = &sharedSlackChannel{connKey: connKey, keys: []string{key}, specs: []channelSpec{spec}, ch: old}
	old.ingressActive.Add(1)
	handoff := make(chan struct{})
	routed := make(chan struct{}, 1)
	go func() {
		defer old.ingressActive.Done()
		<-handoff
		manager.routeSlackMessage(old, IncomingMessage{Type: "slack", Channel: "D1", From: "U1", Text: "hello",
			OriginalText: "hello", ThreadTS: "1.000001"}, &slackConnectionIntake{},
			func(string, string, string, Channel, IncomingMessage) { routed <- struct{}{} })
	}()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // replacement's auth.test exits without a live network call
	done := make(chan error, 1)
	go func() { done <- manager.Restart(ctx, key, func(string, string, string, Channel, IncomingMessage) {}) }()
	deadline := time.After(time.Second)
	for {
		manager.mu.Lock()
		_, retained := manager.slack[connKey]
		manager.mu.Unlock()
		if !old.ingressOpen() {
			require.True(t, retained, "old route was removed before ingress handoff")
			break
		}
		select {
		case <-deadline:
			t.Fatal("old ingress was not quiesced")
		case <-time.After(time.Millisecond):
		}
	}
	select {
	case err := <-done:
		t.Fatalf("replacement started before old socket closed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(old.socketDone)
	select {
	case err := <-done:
		t.Fatalf("replacement started before acknowledged handoff: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(handoff)
	select {
	case <-routed:
	case <-time.After(time.Second):
		t.Fatal("old acknowledged message lost during replacement")
	}
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("replacement did not start after old socket closed")
	}
	manager.Stop()
}

func TestManagerReconcileKeepsOldRouteUntilAcknowledgedHandoff(t *testing.T) {
	manager := NewManager()
	old := NewSlackChannel("xapp-fake", "xoxb-fake", nil, "", nil)
	old.botUserID, old.teamID = "BOT", "TEAM"
	old.ingressMu.Lock()
	old.ingressStarted = true
	old.ingressMu.Unlock()
	cc := config.ChannelConfig{Type: "slack", ID: "route", URL: "xapp-fake", Token: "xoxb-fake",
		AllowFrom: []config.AllowFromEntry{{From: "U1"}}}
	key := channelKey("agent", "slack", "route")
	connKey := slackConnectionKey(cc)
	oldSpec := channelSpec{agentName: "agent", agentModel: "old", channelConfig: cc}
	manager.channels[key] = old
	manager.specs[key] = oldSpec
	manager.slackAlias[key] = connKey
	manager.slack[connKey] = &sharedSlackChannel{connKey: connKey, keys: []string{key}, specs: []channelSpec{oldSpec}, ch: old}
	old.ingressActive.Add(1)
	handoff := make(chan struct{})
	routed := make(chan struct{}, 1)
	go func() {
		defer old.ingressActive.Done()
		<-handoff
		manager.routeSlackMessage(old, IncomingMessage{Type: "slack", Channel: "D1", From: "U1", Text: "hello",
			OriginalText: "hello", ThreadTS: "1.000001"}, &slackConnectionIntake{},
			func(string, string, string, Channel, IncomingMessage) { routed <- struct{}{} })
	}()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // replacement's auth.test exits without a live network call
	done := make(chan struct{})
	go func() {
		manager.Reconcile(ctx, &config.Config{Agents: []config.AgentConfig{{Name: "agent", Model: "new",
			Channels: []config.ChannelConfig{cc}}}}, func(string, string, string, Channel, IncomingMessage) {})
		close(done)
	}()
	deadline := time.After(time.Second)
	for old.ingressOpen() {
		select {
		case <-deadline:
			t.Fatal("old ingress was not quiesced")
		case <-time.After(time.Millisecond):
		}
	}
	manager.mu.Lock()
	_, retained := manager.slack[connKey]
	manager.mu.Unlock()
	require.True(t, retained, "old route was removed before handoff")
	close(old.socketDone)
	select {
	case <-done:
		t.Fatal("replacement started before acknowledged handoff")
	case <-time.After(20 * time.Millisecond):
	}
	close(handoff)
	select {
	case <-routed:
	case <-time.After(time.Second):
		t.Fatal("old acknowledged message lost during reconcile")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reconcile did not finish after handoff")
	}
	manager.Stop()
}

func TestManagerQuiescenceRejectsNewSocketStarts(t *testing.T) {
	manager := NewManager()
	require.NoError(t, manager.QuiesceSlack(context.Background()))
	cc := config.ChannelConfig{Type: "slack", ID: "route", URL: "xapp-fake", Token: "xoxb-fake"}
	manager.Reconcile(context.Background(), &config.Config{Agents: []config.AgentConfig{{Name: "agent",
		Channels: []config.ChannelConfig{cc}}}}, func(string, string, string, Channel, IncomingMessage) {})
	manager.mu.Lock()
	defer manager.mu.Unlock()
	require.Empty(t, manager.slack)
	require.Empty(t, manager.channels)
}
