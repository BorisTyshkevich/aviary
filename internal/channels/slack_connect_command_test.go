package channels

import (
	"testing"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
)

func TestParseSlackConnectionCommand(t *testing.T) {
	tests := []struct {
		text, transport, endpoint string
		recognized, invalid       bool
		isDM                      bool
	}{
		{"<@BOT> connect https://cluster.example:8443", "clickhouse", "https://cluster.example:8443", true, false, false},
		{"<@BOT> connect <https://cluster.example:8443>", "clickhouse", "https://cluster.example:8443", true, false, false},
		{"<@BOT> connect <https://cluster.example:8443|https://cluster.example:8443>", "clickhouse", "https://cluster.example:8443", true, false, false},
		{"<@BOT> connect https://CLUSTER.EXAMPLE:443/", "clickhouse", "https://cluster.example", true, false, false},
		{"<@BOT> connect https://CLUSTER.EXAMPLE:8443/a%2Fb", "clickhouse", "https://cluster.example:8443/a%2Fb", true, false, false},
		{"<@BOT> connect https://cluster.example?", "", "", true, true, false},
		{"<@BOT> connect https://cluster.example#", "", "", true, true, false},
		{"connect https://mcp.example", "mcp", "https://mcp.example", true, false, true},
		{"connect https://host.example/mcp/", "mcp", "https://host.example/mcp/", true, false, true},
		{"connect https://notmcp.example/path?mcp=true", "", "", true, true, true},
		{"connect https://notmcp.example/mcp2", "clickhouse", "https://notmcp.example/mcp2", true, false, true},
		{"connect clickhouse https://mcp.example", "clickhouse", "https://mcp.example", true, false, true},
		{"connect mcp https://cluster.example", "mcp", "https://cluster.example", true, false, true},
		{"connect https://user:fake-secret@cluster.example", "", "", true, true, true},
		{"connect https://cluster.example extra", "", "", true, true, true},
		{"status extra", "", "", true, true, true},
		{"disconnect", "", "", false, false, false},
		{"please connect https://cluster.example", "", "", false, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.text, func(t *testing.T) {
			got, recognized, err := parseSlackConnectionCommand(tt.text, "BOT", tt.isDM)
			if recognized != tt.recognized || (err != nil) != tt.invalid {
				t.Fatalf("recognized=%v err=%v", recognized, err)
			}
			if !tt.invalid && (got.transport != tt.transport || got.endpoint != tt.endpoint) {
				t.Fatalf("transport=%q endpoint=%q", got.transport, got.endpoint)
			}
		})
	}
}

func TestSlackCredentialIngressPrecedesObserversAndPreservesWhitespace(t *testing.T) {
	ch := &SlackChannel{}
	called := 0
	ch.intake = func(in slackIngress) bool {
		called++
		if in.Text != "  fake password !  " || in.RootTS != "100.000001" || !in.IsDM {
			t.Fatalf("raw credential event was changed: %#v", in)
		}
		return true
	}
	ch.OnMessage(func(IncomingMessage) { t.Fatal("credential reached message handler") })
	ch.OnGroupChatMessage(func(IncomingMessage) { t.Fatal("credential reached observer") })
	ch.handleMessageEvent(&slackevents.MessageEvent{
		User: "U1", Channel: "D1", Text: "  fake password !  ",
		TimeStamp: "100.000002", ThreadTimeStamp: "100.000001",
	})
	if called != 1 {
		t.Fatalf("intake calls=%d", called)
	}
}

func TestSlackHistoryRedactsCredentialPromptThread(t *testing.T) {
	ch := &SlackChannel{redactReference: func(channel, thread string) bool {
		return channel == "D1" && thread == "100.000001"
	}}
	got := ch.formatSlackMessages(slackMessageReference{ChannelID: "D1", Timestamp: "100.000001"}, []slack.Message{
		{Msg: slack.Msg{User: "U1", Text: "fake-secret", ThreadTimestamp: "100.000001", Timestamp: "100.000002"}},
	})
	if got != "" {
		t.Fatalf("credential history was exposed: %q", got)
	}
}
