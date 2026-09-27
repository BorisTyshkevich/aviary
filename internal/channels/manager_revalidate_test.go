package channels

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/store"
)

type blockingManagerSender struct {
	entered chan struct{}
	release chan struct{}
}

func (*blockingManagerSender) Start(context.Context) error     { return nil }
func (*blockingManagerSender) Stop()                           {}
func (*blockingManagerSender) OnMessage(func(IncomingMessage)) {}
func (s *blockingManagerSender) Send(string, string) error {
	close(s.entered)
	<-s.release
	return nil
}

func TestManagerDeliveryDoesNotHoldLifecycleLock(t *testing.T) {
	m := NewManager()
	sender := &blockingManagerSender{entered: make(chan struct{}), release: make(chan struct{})}
	m.channels[channelKey("bot", "signal", "route")] = sender
	deliveryDone := make(chan error, 1)
	go func() { deliveryDone <- m.RouteDelivery("signal", "+15550001111", "synthetic") }()
	select {
	case <-sender.entered:
	case <-time.After(time.Second):
		t.Fatal("send did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	inspectionDone := make(chan struct{})
	go func() {
		_ = m.List()
		_ = m.QuiesceSlack(ctx)
		close(inspectionDone)
	}()
	select {
	case <-inspectionDone:
	case <-ctx.Done():
		t.Fatal("lifecycle lock was held by a delivery")
	}
	close(sender.release)
	require.NoError(t, <-deliveryDone)
}

func TestRevalidateRoutedSignalMessageUsesCurrentAccessAndOverrides(t *testing.T) {
	m := NewManager()
	key := channelKey("bot", "signal", "route")
	ch := &SignalChannel{allowFrom: []config.AllowFromEntry{{From: "+15550002222", RestrictTools: []string{"safe_tool"}}},
		model: "new-model", disabledTools: []string{"denied_tool"}}
	m.channels[key] = ch
	m.specs[key] = channelSpec{agentName: "bot", channelConfig: config.ChannelConfig{Type: "signal", ID: "route"}}
	msg := IncomingMessage{Type: "signal", From: "+15550002222", Channel: "+15550002222", Text: "synthetic",
		Model: "old-model", RestrictTools: []string{"old_tool"}}
	_, routed, ok := m.RevalidateRoutedMessage("bot", "signal", "route", msg)
	require.True(t, ok)
	require.Equal(t, "new-model", routed.Model)
	require.Equal(t, []string{"safe_tool"}, routed.RestrictTools)
	require.Equal(t, []string{"denied_tool"}, routed.DisabledTools)
	ch.allowFrom = []config.AllowFromEntry{{From: "+15550003333"}}
	_, _, ok = m.RevalidateRoutedMessage("bot", "signal", "route", msg)
	require.False(t, ok)
}

func TestRevalidateRoutedDiscordMessageUsesCurrentMentionPolicy(t *testing.T) {
	m := NewManager()
	key := channelKey("bot", "discord", "route")
	ch := &DiscordChannel{allowFrom: []config.AllowFromEntry{{From: "U1", AllowedGroups: "C1", RespondToMentions: true,
		RestrictTools: []string{"new_tool"}}}, model: "new-model"}
	m.channels[key] = ch
	m.specs[key] = channelSpec{agentName: "bot", channelConfig: config.ChannelConfig{Type: "discord", ID: "route"}}
	msg := IncomingMessage{Type: "discord", From: "U1", Channel: "C1", Text: "synthetic", IsGroup: true,
		WasMentioned: true, RestrictTools: []string{"old_tool"}}
	_, routed, ok := m.RevalidateRoutedMessage("bot", "discord", "route", msg)
	require.True(t, ok)
	require.Equal(t, []string{"new_tool"}, routed.RestrictTools)
	require.Equal(t, "new-model", routed.Model)
	msg.WasMentioned = false
	_, _, ok = m.RevalidateRoutedMessage("bot", "discord", "route", msg)
	require.False(t, ok)
}

func TestRevalidateRoutedSlackMessageUsesClaimedOwnerPolicy(t *testing.T) {
	store.SetDataDir(t.TempDir())
	t.Cleanup(func() { store.SetDataDir("") })
	m := NewManager()
	ch := NewSlackChannel("xapp-fake", "xoxb-fake", nil, "", nil)
	ch.botUserID, ch.teamID = "BOT", "TEAM"
	key := channelKey("bot", "slack", "route")
	spec := channelSpec{agentName: "bot", channelConfig: config.ChannelConfig{Type: "slack", ID: "route",
		AllowFrom: []config.AllowFromEntry{{From: "U1", AllowedGroups: "C1", RespondToMentions: true,
			RestrictTools: []string{"new_tool"}}}}}
	m.channels[key] = ch
	m.specs[key] = spec
	m.slack["conn"] = &sharedSlackChannel{ch: ch, specs: []channelSpec{spec}}
	root := formatSlackTestTS(time.Now().Add(-time.Minute))
	msg := IncomingMessage{Type: "slack", InstallationID: "BOT", WorkspaceID: "TEAM", From: "U1", Channel: "C1",
		ThreadTS: root, IsThreadReply: true, Text: "continue", OriginalText: "continue", RestrictTools: []string{"old_tool"}}
	_, _, ok := m.RevalidateRoutedMessage("bot", "slack", "route", msg)
	require.False(t, ok, "unclaimed replies must still pass ordinary mention policy")
	_, err := m.affinity.claim(slackThreadOwner{TeamID: "TEAM", ChannelID: "C1", RootTS: root, BotUserID: "BOT",
		AgentName: "bot", ConfiguredID: "route", EnabledAt: spec.metadata.EnabledAt})
	require.NoError(t, err)
	_, routed, ok := m.RevalidateRoutedMessage("bot", "slack", "route", msg)
	require.True(t, ok)
	require.Equal(t, []string{"new_tool"}, routed.RestrictTools)
	noReplies := false
	spec.channelConfig.ReplyToReplies = &noReplies
	m.specs[key] = spec
	m.slack["conn"].specs[0] = spec
	_, _, ok = m.RevalidateRoutedMessage("bot", "slack", "route", msg)
	require.False(t, ok)
}
