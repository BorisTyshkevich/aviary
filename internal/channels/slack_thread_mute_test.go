package channels

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/store"
)

func TestIsSlackStopRequest(t *testing.T) {
	for text, want := range map[string]bool{
		"<@BOT> stop":                           true,
		"<@BOT|clicksearch> stop":               true,
		"<@BOT> Stop.":                          true,
		"<@BOT> please stop replying":           true,
		"stop <@BOT>":                           true,
		"<@BOT> be quiet now":                   true,
		"<@BOT> enough!":                        true,
		"stop":                                  false,
		"<@OTHER> stop":                         false,
		"<@BOT> stop the merge and check parts": false,
		"<@BOT> what does stop do":              false,
		"<@BOT> check parts":                    false,
	} {
		require.Equal(t, want, isSlackStopRequest(text, "BOT"), text)
	}
}

func TestSlackThreadMuteAndUnmute(t *testing.T) {
	store.SetDataDir(t.TempDir())
	t.Cleanup(func() { store.SetDataDir("") })
	root := formatSlackTestTS(time.Now().Add(-time.Minute))
	a := &slackThreadAffinity{}
	_, err := a.claim(slackThreadOwner{TeamID: "T", ChannelID: "C", RootTS: root, BotUserID: "B", AgentName: "a", ConfiguredID: "a", ClaimedBy: "U1"})
	require.NoError(t, err)
	require.NoError(t, a.setMuted("T", "C", root, true))
	got, err := (&slackThreadAffinity{}).lookup("T", "C", root)
	require.NoError(t, err)
	require.False(t, got.MutedAt.IsZero())
	require.Equal(t, "U1", got.ClaimedBy)
	require.NoError(t, a.setMuted("T", "C", root, false))
	got, err = a.lookup("T", "C", root)
	require.NoError(t, err)
	require.True(t, got.MutedAt.IsZero())
	require.NoError(t, a.setMuted("T", "C", formatSlackTestTS(time.Now().Add(-2*time.Minute)), true), "unclaimed thread is a no-op")
}

func TestSlackClaimedThreadMuteAndScope(t *testing.T) {
	for _, tc := range []struct {
		name  string
		scope string
		steps []struct {
			from, text string
			want       bool
		}
	}{
		{name: "stop mutes untagged replies from anyone, mention resumes", steps: []struct {
			from, text string
			want       bool
		}{
			{"U1", "<@BOTA> start", true},
			{"U2", "following up", true},
			{"U1", "<@BOTA> stop", false},
			{"U2", "ok broken on start we leave rest", false},
			{"U1", "continue without mention", false},
			{"U2", "<@BOTA> stop", false},
			{"U2", "<@BOTA> are you there", true},
			{"U1", "untagged again", true},
		}},
		{name: "stop is not a question", steps: []struct {
			from, text string
			want       bool
		}{
			{"U1", "<@BOTA> start", true},
			{"U1", "<@BOTA> stop the merge and check parts", true},
			{"U2", "untagged", true},
		}},
		{name: "claimer scope", scope: "claimer", steps: []struct {
			from, text string
			want       bool
		}{
			{"U1", "<@BOTA> start", true},
			{"U2", "untagged from another human", false},
			{"U1", "untagged from claimer", true},
			{"U2", "<@BOTA> mention from another human", true},
		}},
		{name: "mentions scope", scope: "mentions", steps: []struct {
			from, text string
			want       bool
		}{
			{"U1", "<@BOTA> start", true},
			{"U1", "untagged from claimer", false},
			{"U2", "<@BOTA> mention", true},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store.SetDataDir(t.TempDir())
			t.Cleanup(func() { store.SetDataDir("") })
			m := NewManager()
			a := NewSlackChannel("xapp-a", "xoxb-a", nil, "", nil)
			a.botUserID, a.teamID = "BOTA", "TEAM"
			spec := channelSpec{agentName: "A", channelConfig: config.ChannelConfig{Type: "slack", ID: "A", ClaimedReplyScope: tc.scope,
				AllowFrom: []config.AllowFromEntry{{From: "*", AllowedGroups: "C1", RespondToMentions: true}}}}
			m.slack["A"] = &sharedSlackChannel{ch: a, specs: []channelSpec{spec}}
			root := formatSlackTestTS(time.Now().Add(-time.Minute))
			var routed int
			deliver := func(_, _, _ string, _ Channel, _ IncomingMessage) { routed++ }
			for i, step := range tc.steps {
				routed = 0
				m.routeSlackMessage(a, IncomingMessage{Type: "slack", From: step.from, Channel: "C1", ThreadTS: root,
					IsThreadReply: i > 0, OriginalText: step.text, Text: step.text, ReceivedAt: time.Now()}, &slackConnectionIntake{}, deliver)
				require.Equal(t, step.want, routed == 1, "step %d %q", i, step.text)
			}
		})
	}
}
