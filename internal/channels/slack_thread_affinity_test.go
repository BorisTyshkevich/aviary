package channels

import (
	"encoding/json"
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
	"github.com/lsegal/aviary/internal/store"
)

func TestSlackThreadAffinityFirstClaimPersistsAndExpires(t *testing.T) {
	store.SetDataDir(t.TempDir())
	t.Cleanup(func() { store.SetDataDir("") })
	root := formatSlackTestTS(time.Now().Add(-time.Minute))
	a := &slackThreadAffinity{}
	first := slackThreadOwner{TeamID: "T1", ChannelID: "C1", RootTS: root, BotUserID: "BOT1", AgentName: "one", ConfiguredID: "bot-one"}
	got, err := a.claim(first)
	require.NoError(t, err)
	require.Equal(t, first, *got)
	other := first
	other.BotUserID, other.AgentName = "BOT2", "two"
	var wg sync.WaitGroup
	errors := make(chan error, 8)
	for n := 0; n < 8; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			owner, err := (&slackThreadAffinity{}).claim(other)
			if err != nil {
				errors <- err
			} else if owner == nil || *owner != first {
				errors <- fmt.Errorf("concurrent claim changed owner: %+v", owner)
			}
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	restarted, err := (&slackThreadAffinity{}).lookup("T1", "C1", root)
	require.NoError(t, err)
	require.Equal(t, first, *restarted)

	require.NoError(t, os.WriteFile(a.path("T1", "C1", root), []byte("broken"), 0o600))
	_, err = a.lookup("T1", "C1", root)
	require.Error(t, err)

	oldRoot := formatSlackTestTS(time.Now().Add(-slackAffinityRetention - time.Hour))
	other.RootTS = oldRoot
	owner, err := a.claim(other)
	require.NoError(t, err)
	require.Nil(t, owner)
	require.NoFileExists(t, a.path("T1", "C1", oldRoot))
	oldPath := a.path("T1", "C1", oldRoot)
	require.NoError(t, os.MkdirAll(filepath.Dir(oldPath), 0o700))
	oldData, err := json.Marshal(other)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(oldPath, oldData, 0o600))
	require.NoError(t, a.prune())
	require.NoFileExists(t, oldPath)
}

func formatSlackTestTS(ts time.Time) string {
	return fmt.Sprintf("%d.%06d", ts.Unix(), ts.Nanosecond()/1000)
}

func TestSlackThreadAffinitySharedBotsAndCurrentPolicy(t *testing.T) {
	store.SetDataDir(t.TempDir())
	t.Cleanup(func() { store.SetDataDir("") })
	m := NewManager()
	a := NewSlackChannel("xapp-a", "xoxb-a", nil, "", nil)
	b := NewSlackChannel("xapp-b", "xoxb-b", nil, "", nil)
	otherWorkspace := NewSlackChannel("xapp-other", "xoxb-other", nil, "", nil)
	a.botUserID, b.botUserID = "BOTA", "BOTB"
	a.teamID, b.teamID = "TEAM", "TEAM"
	sink := newLogSink()
	a.SetLogSink(sink)
	otherWorkspace.botUserID, otherWorkspace.teamID = "BOTA", "OTHERTEAM"
	makeSpec := func(name string) channelSpec {
		return channelSpec{agentName: name, channelConfig: config.ChannelConfig{Type: "slack", ID: name,
			AllowFrom: []config.AllowFromEntry{{From: "*", AllowedGroups: "C1", RespondToMentions: true,
				ExcludePrefixes: []string{"!"}, RestrictTools: []string{"tool_one"}}}}}
	}
	otherChannelSpec := makeSpec("A-other-channel")
	otherChannelSpec.channelConfig.AllowFrom[0].AllowedGroups = "C2"
	disabledSpec := makeSpec("A-disabled")
	falseValue := false
	disabledSpec.channelConfig.AllowFrom[0].Enabled = &falseValue
	m.slack["a"] = &sharedSlackChannel{ch: a, specs: []channelSpec{makeSpec("A"), otherChannelSpec, disabledSpec}}
	m.slack["b"] = &sharedSlackChannel{ch: b, specs: []channelSpec{makeSpec("B")}}
	m.slack["other"] = &sharedSlackChannel{ch: otherWorkspace, specs: []channelSpec{makeSpec("other")}}
	root := formatSlackTestTS(time.Now().Add(-time.Minute))
	var routed []string
	deliver := func(name, _, _ string, _ Channel, msg IncomingMessage) {
		routed = append(routed, name+":"+msg.OriginalText)
		require.Equal(t, []string{"tool_one"}, msg.RestrictTools)
	}
	message := func(text string, reply bool) IncomingMessage {
		return IncomingMessage{Type: "slack", From: "U1", Channel: "C1", ThreadTS: root,
			IsThreadReply: reply, OriginalText: text, Text: text, ReceivedAt: time.Now()}
	}
	m.routeSlackMessage(a, message("<@BOTA> start", false), &slackConnectionIntake{}, deliver)
	require.Equal(t, []string{"A:<@BOTA> start"}, routed)
	routed = nil
	m.routeSlackMessage(a, message("continue", true), &slackConnectionIntake{}, deliver)
	m.routeSlackMessage(b, message("continue", true), &slackConnectionIntake{}, deliver)
	require.Equal(t, []string{"A:continue"}, routed)
	routed = nil
	m.routeSlackMessage(a, message("thanks <@USER>", true), &slackConnectionIntake{}, deliver)
	require.Equal(t, []string{"A:thanks <@USER>"}, routed)
	routed = nil
	denied := makeSpec("B")
	denied.channelConfig.AllowFrom[0].From = "U2"
	denied.channelConfig.AllowFrom[0].MentionPrefixes = []string{"bb"}
	m.slack["b"].specs = []channelSpec{denied}
	for _, targeted := range []string{"<@BOTB> denied", "bb denied"} {
		m.routeSlackMessage(a, message(targeted, true), &slackConnectionIntake{}, deliver)
		m.routeSlackMessage(b, message(targeted, true), &slackConnectionIntake{}, deliver)
	}
	require.Empty(t, routed)
	history, _, unsub := sink.Subscribe()
	defer unsub()
	require.Contains(t, strings.Join(history, "\n"), "denied explicit target")
	m.slack["b"].specs = []channelSpec{makeSpec("B")}
	m.routeSlackMessage(a, message("<@BOTB> only B", true), &slackConnectionIntake{}, deliver)
	m.routeSlackMessage(b, message("<@BOTB> only B", true), &slackConnectionIntake{}, deliver)
	require.Equal(t, []string{"B:<@BOTB> only B"}, routed)
	routed = nil
	m.routeSlackMessage(a, message("!excluded", true), &slackConnectionIntake{}, deliver)
	require.Empty(t, routed)
	owner, err := m.affinity.lookup("TEAM", "C1", root)
	require.NoError(t, err)
	require.Equal(t, "A", owner.AgentName)

	spec := makeSpec("A")
	spec.channelConfig.ReplyToReplies = &falseValue
	m.slack["a"].specs = []channelSpec{spec}
	m.routeSlackMessage(a, message("continue", true), &slackConnectionIntake{}, deliver)
	require.Empty(t, routed)
	m.routeSlackMessage(a, message("<@BOTA> fresh", true), &slackConnectionIntake{}, deliver)
	require.Equal(t, []string{"A:<@BOTA> fresh"}, routed)
	routed = nil
	spec = makeSpec("A")
	spec.metadata.EnabledAt = time.Now().Add(-30 * time.Second)
	m.slack["a"].specs = []channelSpec{spec}
	m.routeSlackMessage(a, message("stale generation", true), &slackConnectionIntake{}, deliver)
	require.Empty(t, routed)
}

func TestSlackThreadAffinityUnclaimedAndAmbiguous(t *testing.T) {
	store.SetDataDir(t.TempDir())
	t.Cleanup(func() { store.SetDataDir("") })
	m := NewManager()
	ch := NewSlackChannel("xapp", "xoxb", nil, "", nil)
	ch.botUserID, ch.teamID = "BOT", "TEAM"
	sink := newLogSink()
	ch.SetLogSink(sink)
	makeSpec := func(name string) channelSpec {
		return channelSpec{agentName: name, channelConfig: config.ChannelConfig{Type: "slack", ID: name,
			AllowFrom: []config.AllowFromEntry{{From: "*", AllowedGroups: "C1", RespondToMentions: true}}}}
	}
	m.slack["one"] = &sharedSlackChannel{ch: ch, specs: []channelSpec{makeSpec("one")}}
	root := formatSlackTestTS(time.Now().Add(-time.Minute))
	var count int
	fn := func(string, string, string, Channel, IncomingMessage) { count++ }
	msg := IncomingMessage{Type: "slack", From: "U", Channel: "C1", ThreadTS: root, IsThreadReply: true,
		OriginalText: "continue", Text: "<@BOT> quoted history", ReceivedAt: time.Now()}
	m.routeSlackMessage(ch, msg, &slackConnectionIntake{}, fn)
	require.Zero(t, count)
	m.slack["two"] = &sharedSlackChannel{ch: ch, specs: []channelSpec{makeSpec("two")}}
	msg.OriginalText = "<@BOT> ambiguous"
	m.routeSlackMessage(ch, msg, &slackConnectionIntake{}, fn)
	require.Zero(t, count)
	history, _, unsub := sink.Subscribe()
	defer unsub()
	require.Contains(t, strings.Join(history, "\n"), "ambiguous explicit target")
	require.NoFileExists(t, m.affinity.path("TEAM", "C1", root))
	delete(m.slack, "two")
	msg.IsEdited = true
	m.routeSlackMessage(ch, msg, &slackConnectionIntake{}, fn)
	require.Equal(t, 1, count)
	require.NoFileExists(t, m.affinity.path("TEAM", "C1", root))
	catchAll := makeSpec("one")
	catchAll.channelConfig.AllowFrom[0].RespondToMentions = false
	falseValue := false
	catchAll.channelConfig.ReplyToReplies = &falseValue
	m.slack["one"].specs = []channelSpec{catchAll}
	msg.ThreadTS = formatSlackTestTS(time.Now().Add(-2 * time.Minute))
	msg.IsEdited = false
	msg.OriginalText = "continue"
	m.routeSlackMessage(ch, msg, &slackConnectionIntake{}, fn)
	require.Equal(t, 1, count)
	msg.OriginalText = "<@BOT> fresh"
	m.routeSlackMessage(ch, msg, &slackConnectionIntake{}, fn)
	require.Equal(t, 2, count)
	require.NoFileExists(t, m.affinity.path("TEAM", "C1", msg.ThreadTS))
}

func TestSlackConnectCommandClaimsBeforeExecution(t *testing.T) {
	store.SetDataDir(t.TempDir())
	t.Cleanup(func() { store.SetDataDir("") })
	service, err := connections.Open(filepath.Join(t.TempDir(), "connections"))
	require.NoError(t, err)
	ch := NewSlackChannel("xapp", "xoxb", nil, "", nil)
	ch.botUserID, ch.teamID = "BOT", "TEAM"
	spec := channelSpec{agentName: "agent", channelConfig: config.ChannelConfig{Type: "slack", ID: "configured",
		AllowFrom: []config.AllowFromEntry{{From: "U1", AllowedGroups: "C1", RespondToMentions: true}}}}
	other := spec
	other.agentName, other.channelConfig.ID = "other", "other-configured"
	other.channelConfig.AllowFrom = []config.AllowFromEntry{{From: "U2", AllowedGroups: "C1", RespondToMentions: true}}
	m := NewManager()
	m.slack["bot"] = &sharedSlackChannel{ch: ch, specs: []channelSpec{spec, other}}
	claimed := false
	intake := &slackConnectionIntake{channel: ch, service: service, specs: []channelSpec{spec, other},
		claimCommand: func(in slackIngress, selected channelSpec) bool {
			claimed = m.claimSlackCommand(ch, in, selected)
			return false // Stop before posting a Slack response; claim precedes execution.
		}}
	intake.stopped = true // Suppress test-only Slack responses.
	root := formatSlackTestTS(time.Now().Add(-time.Minute))
	require.True(t, intake.handle(slackIngress{UserID: "U1", ChannelID: "C1", RootTS: root,
		MessageTS: root, CommandText: "<@BOT> status"}))
	require.True(t, claimed)
	owner, err := (&slackThreadAffinity{}).lookup("TEAM", "C1", root)
	require.NoError(t, err)
	require.Equal(t, "agent", owner.AgentName)
	malformedRoot := formatSlackTestTS(time.Now().Add(-5 * time.Minute))
	require.True(t, intake.handle(slackIngress{UserID: "U1", ChannelID: "C1", RootTS: malformedRoot,
		MessageTS: malformedRoot, CommandText: "<@BOT> connect http://invalid"}))
	require.NoFileExists(t, m.affinity.path("TEAM", "C1", malformedRoot))
	ambiguous := other
	ambiguous.channelConfig.AllowFrom[0].From = "U1"
	intake.specs = []channelSpec{spec, ambiguous}
	ambiguousRoot := formatSlackTestTS(time.Now().Add(-6 * time.Minute))
	require.True(t, intake.handle(slackIngress{UserID: "U1", ChannelID: "C1", RootTS: ambiguousRoot,
		MessageTS: ambiguousRoot, CommandText: "<@BOT> status"}))
	require.NoFileExists(t, m.affinity.path("TEAM", "C1", ambiguousRoot))

	for _, tc := range []slackIngress{
		{UserID: "U1", ChannelID: "D1", RootTS: formatSlackTestTS(time.Now().Add(-2 * time.Minute)), CommandText: "<@BOT> status", IsDM: true},
		{UserID: "U1", ChannelID: "C1", RootTS: formatSlackTestTS(time.Now().Add(-3 * time.Minute)), CommandText: "<@BOT> status", IsEdited: true},
	} {
		selected := spec
		require.True(t, m.claimSlackCommand(ch, tc, selected))
		require.NoFileExists(t, m.affinity.path("TEAM", tc.ChannelID, tc.RootTS))
	}
	noReplies := spec
	falseValue := false
	noReplies.channelConfig.ReplyToReplies = &falseValue
	noReplyRoot := formatSlackTestTS(time.Now().Add(-4 * time.Minute))
	require.True(t, m.claimSlackCommand(ch, slackIngress{UserID: "U1", ChannelID: "C1", RootTS: noReplyRoot, CommandText: "<@BOT> status"}, noReplies))
	require.NoFileExists(t, m.affinity.path("TEAM", "C1", noReplyRoot))
	catchAll := spec
	catchAll.channelConfig.AllowFrom = []config.AllowFromEntry{{From: "U1", AllowedGroups: "C1"}}
	m.slack["bot"].specs = []channelSpec{catchAll}
	intake.specs = []channelSpec{catchAll}
	catchAllRoot := formatSlackTestTS(time.Now().Add(-7 * time.Minute))
	require.True(t, intake.handle(slackIngress{UserID: "U1", ChannelID: "C1", RootTS: catchAllRoot,
		MessageTS: catchAllRoot, CommandText: "<@BOT> status"}))
	owner, err = m.affinity.lookup("TEAM", "C1", catchAllRoot)
	require.NoError(t, err)
	require.Equal(t, "agent", owner.AgentName)
}

func TestSlackCatchAllBotMentionClaimsButPrefixGateStaysStrict(t *testing.T) {
	store.SetDataDir(t.TempDir())
	t.Cleanup(func() { store.SetDataDir("") })
	m := NewManager()
	ch := NewSlackChannel("xapp", "xoxb", nil, "", nil)
	ch.botUserID, ch.teamID = "BOT", "TEAM"
	spec := channelSpec{agentName: "catchall", channelConfig: config.ChannelConfig{Type: "slack", ID: "catchall",
		AllowFrom: []config.AllowFromEntry{{From: "*", AllowedGroups: "C1"}}}}
	m.slack["one"] = &sharedSlackChannel{ch: ch, specs: []channelSpec{spec}}
	root := formatSlackTestTS(time.Now().Add(-time.Minute))
	msg := IncomingMessage{Type: "slack", From: "U", Channel: "C1", ThreadTS: root,
		OriginalText: "<@BOT> start", Text: "<@BOT> start", ReceivedAt: time.Now()}
	var routed []string
	fn := func(name, _, _ string, _ Channel, _ IncomingMessage) { routed = append(routed, name) }
	m.routeSlackMessage(ch, msg, &slackConnectionIntake{}, fn)
	require.Equal(t, []string{"catchall"}, routed)
	owner, err := m.affinity.lookup("TEAM", "C1", root)
	require.NoError(t, err)
	require.Equal(t, "catchall", owner.AgentName)

	prefixOnly := spec
	prefixOnly.agentName, prefixOnly.channelConfig.ID = "prefix", "prefix"
	prefixOnly.channelConfig.AllowFrom = []config.AllowFromEntry{{From: "*", AllowedGroups: "C1", MentionPrefixes: []string{"prefix:"}}}
	m.slack["one"].specs = []channelSpec{prefixOnly}
	msg.ThreadTS = formatSlackTestTS(time.Now().Add(-2 * time.Minute))
	routed = nil
	m.routeSlackMessage(ch, msg, &slackConnectionIntake{}, fn)
	require.Empty(t, routed)
	require.NoFileExists(t, m.affinity.path("TEAM", "C1", msg.ThreadTS))
}

func TestSlackDMUsesOriginalTextForPolicy(t *testing.T) {
	m := NewManager()
	ch := NewSlackChannel("xapp", "xoxb", nil, "", nil)
	ch.botUserID, ch.teamID = "BOT", "TEAM"
	groupOnly := false
	spec := channelSpec{agentName: "agent", channelConfig: config.ChannelConfig{Type: "slack", ID: "agent",
		AllowFrom: []config.AllowFromEntry{{From: "U1", RespondToMentions: true, MentionPrefixGroupOnly: &groupOnly}}}}
	m.slack["one"] = &sharedSlackChannel{ch: ch, specs: []channelSpec{spec}}
	msg := IncomingMessage{Type: "slack", From: "U1", Channel: "D1", OriginalText: "plain DM",
		Text: "plain DM\nquoted history: <@BOT>"}
	called := false
	m.routeSlackMessage(ch, msg, &slackConnectionIntake{}, func(string, string, string, Channel, IncomingMessage) { called = true })
	require.False(t, called)
}

func TestSlackThreadAffinitySenderPartitionedSameBot(t *testing.T) {
	store.SetDataDir(t.TempDir())
	t.Cleanup(func() { store.SetDataDir("") })
	m := NewManager()
	ch := NewSlackChannel("xapp", "xoxb", nil, "", nil)
	ch.botUserID, ch.teamID = "BOT", "TEAM"
	makeSpec := func(name, sender string) channelSpec {
		return channelSpec{agentName: name, channelConfig: config.ChannelConfig{Type: "slack", ID: name,
			AllowFrom: []config.AllowFromEntry{{From: sender, AllowedGroups: "C1", RespondToMentions: true}}}}
	}
	m.slack["shared"] = &sharedSlackChannel{ch: ch, specs: []channelSpec{makeSpec("alice", "U1"), makeSpec("bob", "U2")}}
	root := formatSlackTestTS(time.Now().Add(-time.Minute))
	var routed []string
	fn := func(name, _, _ string, _ Channel, _ IncomingMessage) { routed = append(routed, name) }
	message := func(sender, text string, reply bool) IncomingMessage {
		return IncomingMessage{Type: "slack", From: sender, Channel: "C1", ThreadTS: root,
			OriginalText: text, Text: text, IsThreadReply: reply, ReceivedAt: time.Now()}
	}
	m.routeSlackMessage(ch, message("U1", "<@BOT> start", false), &slackConnectionIntake{}, fn)
	require.Equal(t, []string{"alice"}, routed)
	owner, err := m.affinity.lookup("TEAM", "C1", root)
	require.NoError(t, err)
	require.Equal(t, "alice", owner.AgentName)
	routed = nil
	m.routeSlackMessage(ch, message("U2", "<@BOT> one message", true), &slackConnectionIntake{}, fn)
	require.Equal(t, []string{"bob"}, routed)
	owner, err = m.affinity.lookup("TEAM", "C1", root)
	require.NoError(t, err)
	require.Equal(t, "alice", owner.AgentName)
	routed = nil
	m.routeSlackMessage(ch, message("U2", "continue", true), &slackConnectionIntake{}, fn)
	require.Empty(t, routed)
}

func TestSlackWildcardPrefixCannotClaimOrSteal(t *testing.T) {
	store.SetDataDir(t.TempDir())
	t.Cleanup(func() { store.SetDataDir("") })
	m := NewManager()
	a := NewSlackChannel("xapp-a", "xoxb-a", nil, "", nil)
	b := NewSlackChannel("xapp-b", "xoxb-b", nil, "", nil)
	a.botUserID, b.botUserID = "BOTA", "BOTB"
	a.teamID, b.teamID = "TEAM", "TEAM"
	specA := channelSpec{agentName: "A", channelConfig: config.ChannelConfig{Type: "slack", ID: "a",
		AllowFrom: []config.AllowFromEntry{{From: "*", AllowedGroups: "C1", RespondToMentions: true}}}}
	specB := channelSpec{agentName: "B", channelConfig: config.ChannelConfig{Type: "slack", ID: "b",
		AllowFrom: []config.AllowFromEntry{{From: "*", AllowedGroups: "C1", MentionPrefixes: []string{"*", "help*"}}}}}
	m.slack["a"] = &sharedSlackChannel{ch: a, specs: []channelSpec{specA}}
	m.slack["b"] = &sharedSlackChannel{ch: b, specs: []channelSpec{specB}}
	root := formatSlackTestTS(time.Now().Add(-time.Minute))
	var routed []string
	fn := func(name, _, _ string, _ Channel, _ IncomingMessage) { routed = append(routed, name) }
	msg := IncomingMessage{Type: "slack", From: "U", Channel: "C1", ThreadTS: root,
		OriginalText: "<@BOTA> claim", Text: "<@BOTA> claim", ReceivedAt: time.Now()}
	m.routeSlackMessage(a, msg, &slackConnectionIntake{}, fn)
	require.Equal(t, []string{"A"}, routed)
	routed = nil
	literal := specB
	literal.channelConfig.AllowFrom = []config.AllowFromEntry{{From: "*", AllowedGroups: "C1", MentionPrefixes: []string{"b:"}}}
	m.slack["b"].specs = []channelSpec{literal}
	msg.IsThreadReply, msg.OriginalText, msg.Text = true, "b: answer", "b: answer"
	m.routeSlackMessage(a, msg, &slackConnectionIntake{}, fn)
	m.routeSlackMessage(b, msg, &slackConnectionIntake{}, fn)
	require.Equal(t, []string{"B"}, routed)
	owner, err := m.affinity.lookup("TEAM", "C1", root)
	require.NoError(t, err)
	require.Equal(t, "A", owner.AgentName)
	m.slack["b"].specs = []channelSpec{specB}
	routed = nil
	msg.IsThreadReply, msg.OriginalText, msg.Text = true, "continue", "continue"
	m.routeSlackMessage(a, msg, &slackConnectionIntake{}, fn)
	m.routeSlackMessage(b, msg, &slackConnectionIntake{}, fn)
	require.Equal(t, []string{"A"}, routed)
	routed = nil
	msg.ThreadTS = formatSlackTestTS(time.Now().Add(-2 * time.Minute))
	m.routeSlackMessage(b, msg, &slackConnectionIntake{}, fn)
	require.Equal(t, []string{"B"}, routed)
	require.NoFileExists(t, m.affinity.path("TEAM", "C1", msg.ThreadTS))
}
