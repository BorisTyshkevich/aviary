package channels

import (
	"testing"

	"github.com/lsegal/aviary/internal/config"
	"github.com/stretchr/testify/require"
)

func dmSpec(agent, id string, allow ...config.AllowFromEntry) channelSpec {
	return channelSpec{agentName: agent, channelConfig: config.ChannelConfig{Type: "slack", ID: id, AllowFrom: allow}}
}

// routeDM sends one plain DM through a shared connection holding specs and
// returns the "agent/configuredID" of every route that received it.
func routeDM(t *testing.T, specs ...channelSpec) []string {
	t.Helper()
	m := NewManager()
	ch := NewSlackChannel("xapp", "xoxb", nil, "", nil)
	ch.botUserID, ch.teamID = "BOT", "TEAM"
	m.slack["one"] = &sharedSlackChannel{ch: ch, specs: specs}
	msg := IncomingMessage{Type: "slack", From: "U1", Channel: "D1", OriginalText: "plain DM", Text: "plain DM"}
	var got []string
	m.routeSlackMessage(ch, msg, &slackConnectionIntake{}, func(agent, _, id string, _ Channel, _ IncomingMessage) {
		got = append(got, agent+"/"+id)
	})
	return got
}

// Two channel entries of one agent share a Slack connection and both allow every
// sender. A DM must start one run, not one per entry.
func TestSlackDMRoutesOncePerAgent(t *testing.T) {
	got := routeDM(t,
		dmSpec("expert", "main", config.AllowFromEntry{From: "*", AllowedGroups: "C1", RespondToMentions: true}),
		dmSpec("expert", "triage", config.AllowFromEntry{From: "*", AllowedGroups: "C2", RespondToMentions: true}),
	)
	require.Equal(t, []string{"expert/main"}, got)
}

// Different agents on one connection are independent: each still gets the DM.
func TestSlackDMStillReachesEveryAgent(t *testing.T) {
	got := routeDM(t,
		dmSpec("alice", "a", config.AllowFromEntry{From: "*"}),
		dmSpec("bob", "b", config.AllowFromEntry{From: "*"}),
	)
	require.ElementsMatch(t, []string{"alice/a", "bob/b"}, got)
}

// When the first entry of an agent does not allow the sender, a later one does.
func TestSlackDMUsesFirstEntryThatAllows(t *testing.T) {
	got := routeDM(t,
		dmSpec("expert", "main", config.AllowFromEntry{From: "U9"}),
		dmSpec("expert", "triage", config.AllowFromEntry{From: "*"}),
	)
	require.Equal(t, []string{"expert/triage"}, got)
}

// The config-only workaround: mention_prefix_group_only=false on the second entry
// makes it require a mention in DMs too, so a plain DM reaches only the first.
func TestSlackDMEntryRequiringMentionIsSkipped(t *testing.T) {
	groupOnly := false
	got := routeDM(t,
		dmSpec("expert", "triage", config.AllowFromEntry{From: "*", AllowedGroups: "C2", RespondToMentions: true, MentionPrefixGroupOnly: &groupOnly}),
		dmSpec("expert", "main", config.AllowFromEntry{From: "*", AllowedGroups: "C1", RespondToMentions: true}),
	)
	require.Equal(t, []string{"expert/main"}, got)
}

// Revalidating an already routed DM names the entry it went to; only that one
// may pass, and the other entry of the same agent must not.
func TestSlackDMRevalidationMatchesTheRoutedEntry(t *testing.T) {
	m := NewManager()
	ch := NewSlackChannel("xapp", "xoxb", nil, "", nil)
	ch.botUserID, ch.teamID = "BOT", "TEAM"
	m.slack["one"] = &sharedSlackChannel{ch: ch, specs: []channelSpec{
		dmSpec("expert", "main", config.AllowFromEntry{From: "*"}),
		dmSpec("expert", "triage", config.AllowFromEntry{From: "*"}),
	}}
	msg := IncomingMessage{Type: "slack", From: "U1", Channel: "D1", OriginalText: "plain DM", Text: "plain DM"}
	for key, want := range map[string]int{"expert/slack/main": 1, "expert/slack/triage": 0} {
		n := 0
		m.routeSlackMessageFor(ch, msg, nil, key, func(string, string, string, Channel, IncomingMessage) { n++ })
		require.Equal(t, want, n, key)
	}
}
