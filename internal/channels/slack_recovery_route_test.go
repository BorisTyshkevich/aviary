package channels

import (
	"context"
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

func TestSlackRecoveryRoutePublishesAuthenticatedGenerationAndInvalidatesOnRetirement(t *testing.T) {
	m := NewManager()
	ch := &SlackChannel{}
	shared := &sharedSlackChannel{ch: ch, recoveryStop: make(chan struct{}), specs: []channelSpec{
		{agentName: "bot", channelConfig: config.ChannelConfig{Type: "slack", ID: "first"}},
		{agentName: "bot", channelConfig: config.ChannelConfig{Type: "slack", ID: "second"}},
	}}
	m.slack["connection"] = shared
	var notified []SlackAuthenticatedRoute
	m.SetSlackAuthenticatedHook(func(route SlackAuthenticatedRoute) { notified = append(notified, route) })
	m.publishSlackAuthentication("connection", shared, "bot-user", "workspace")
	require.Len(t, notified, 2)
	require.Equal(t, []string{"first", "second"}, []string{notified[0].ConfiguredID, notified[1].ConfiguredID})
	for _, route := range notified {
		require.True(t, route.Current())
		require.Equal(t, "bot-user", route.InstallationID)
		require.Equal(t, "workspace", route.WorkspaceID)
	}
	require.Len(t, m.AuthenticatedSlackRoutes(), 2)
	shared.invalidateRecovery()
	for _, route := range notified {
		require.False(t, route.Current())
		select {
		case <-route.PreDispatchStop():
		default:
			t.Fatal("retired route did not close its pre-dispatch gate")
		}
	}
	require.True(t, ch.ingressOpen(), "recovery retirement must not stop the live presenter's outgoing client")
	require.Empty(t, m.AuthenticatedSlackRoutes())
	m.publishSlackAuthentication("connection", shared, "bot-user", "workspace")
	require.Empty(t, m.AuthenticatedSlackRoutes(), "retired generation cannot become ready again")
}

func TestSlackRecoveryInvalidatesOnUnexpectedChannelExit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	m := NewManager()
	ch := deliveryTestSlackClient(server)
	ch.socketDone = make(chan struct{})
	ch.dispatchDone = make(chan struct{})
	shared := &sharedSlackChannel{ch: ch, recoveryStop: make(chan struct{}),
		recoveryReady: true, userID: "bot-user", teamID: "workspace", specs: []channelSpec{{agentName: "bot"}}}
	m.slack["connection"] = shared
	route := m.AuthenticatedSlackRoutes()[0]
	require.True(t, route.Current())
	m.runSharedSlack(context.Background(), "connection", shared)
	require.False(t, route.Current())
	select {
	case <-route.PreDispatchStop():
	default:
		t.Fatal("unexpected channel exit left recovery pre-dispatch gate open")
	}
}

func TestSlackRecoveryGateClosesBeforeOutstandingIngressDrains(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"ok":true,"ts":"1700000001.000001"}`))
	}))
	defer server.Close()
	m := NewManager()
	ch := deliveryTestSlackClient(server)
	shared := &sharedSlackChannel{ch: ch, recoveryStop: make(chan struct{}), recoveryReady: true,
		userID: "bot-user", teamID: "workspace", specs: []channelSpec{{agentName: "bot"}}}
	ch.onIngressClosed = shared.stopRecoveryDispatch
	m.slack["connection"] = shared
	route := m.AuthenticatedSlackRoutes()[0]
	require.True(t, route.Current())
	ch.ingressActive.Add(1) // acknowledged handler still owns its handoff
	ch.Stop()
	select {
	case <-route.PreDispatchStop():
	default:
		t.Fatal("recovery dispatch remained open while ingress handler drained")
	}
	require.False(t, route.Current())
	_, err := ch.PostThreadTextContext(WithSlackPreDispatchStop(context.Background(), route.PreDispatchStop()),
		"C0ORIGINAL1", "1700000000.000001", "fixed notice")
	require.Error(t, err)
	require.Zero(t, calls.Load(), "no Slack HTTP request may start after ingress closes")
	waitCtx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, ch.WaitIngress(waitCtx), "handler is deliberately still active")
	ch.ingressActive.Done()
}

func TestSlackRecoveryRawChannelIDCannotBeShadowedByAlias(t *testing.T) {
	const original = "C0ORIGINAL1"
	const shadow = "C0SHADOW01"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/chat.postMessage", r.URL.Path)
		require.NoError(t, r.ParseForm())
		require.Equal(t, original, r.Form.Get("channel"))
		_, _ = w.Write([]byte(`{"ok":true,"channel":"C0ORIGINAL1","ts":"1700000001.000001"}`))
	}))
	defer server.Close()
	ch := deliveryTestSlackClient(server)
	ch.channelAliases = map[string]string{normalizeSlackAlias(original): shadow, "ordinary-name": shadow}
	_, err := ch.PostThreadTextContext(context.Background(), original, "1700000000.000001", "fixed notice")
	require.NoError(t, err)
	resolved, err := ch.resolveDeliveryTarget(context.Background(), "ordinary-name")
	require.NoError(t, err)
	require.Equal(t, shadow, resolved)
}

func TestSlackAuthenticationReadinessPrecedesSlowIdentityCache(t *testing.T) {
	cacheEntered := make(chan struct{})
	releaseCache := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth.test":
			_, _ = w.Write([]byte(`{"ok":true,"user_id":"U-BOT","team_id":"T-WORKSPACE"}`))
		case "/users.list":
			close(cacheEntered)
			<-releaseCache
			_, _ = w.Write([]byte(`{"ok":true,"members":[]}`))
		default:
			_, _ = w.Write([]byte(`{"ok":true,"channels":[]}`))
		}
	}))
	defer server.Close()
	api := slack.New("fake-bot-token", slack.OptionAppLevelToken("fake-app-token"),
		slack.OptionAPIURL(server.URL+"/"), slack.OptionHTTPClient(slackStatusHTTPClient{base: server.Client()}))
	ch := NewSlackChannel("fake-app-token", "fake-bot-token", nil, "", nil)
	ch.client, ch.sm = api, socketmode.New(api)
	ready := make(chan [2]string, 1)
	ch.onAuthenticated = func(userID, teamID string) {
		ready <- [2]string{userID, teamID}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ch.Start(ctx) }()
	select {
	case <-cacheEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("identity cache request did not start")
	}
	select {
	case identity := <-ready:
		require.Equal(t, [2]string{"U-BOT", "T-WORKSPACE"}, identity)
	default:
		t.Fatal("authentication readiness waited for identity cache")
	}
	cancel()
	close(releaseCache)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Slack channel did not stop after cache cancellation")
	}
}

func TestSlackRecoveryRejectsStaleAuthenticationCallback(t *testing.T) {
	m := NewManager()
	old := &sharedSlackChannel{ch: &SlackChannel{}, recoveryStop: make(chan struct{}), specs: []channelSpec{{agentName: "bot"}}}
	current := &sharedSlackChannel{ch: &SlackChannel{}, recoveryStop: make(chan struct{}), specs: []channelSpec{{agentName: "bot"}}}
	m.slack["connection"] = current
	called := false
	m.SetSlackAuthenticatedHook(func(SlackAuthenticatedRoute) { called = true })
	m.publishSlackAuthentication("connection", old, "old-user", "old-workspace")
	require.False(t, called)
	require.Empty(t, m.AuthenticatedSlackRoutes())
}
