package channels

import (
	"strings"
	"testing"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
)

func TestParseSlackConnectionCommand(t *testing.T) {
	tests := []struct {
		text, transport, endpoint, username string
		recognized, invalid                 bool
		isDM                                bool
	}{
		{"<@BOT> connect https://cluster.example:8443", "clickhouse", "https://cluster.example:8443", "", true, false, false},
		{"<@BOT> connect <https://cluster.example:8443>", "clickhouse", "https://cluster.example:8443", "", true, false, false},
		{"<@BOT> connect <https://cluster.example:8443|https://cluster.example:8443>", "clickhouse", "https://cluster.example:8443", "", true, false, false},
		{"<@BOT> connect https://CLUSTER.EXAMPLE:443/", "clickhouse", "https://cluster.example", "", true, false, false},
		{"<@BOT> connect https://CLUSTER.EXAMPLE:8443/a%2Fb", "clickhouse", "https://cluster.example:8443/a%2Fb", "", true, false, false},
		{"<@BOT> connect <https://cluster.example:8443> reader_1", "clickhouse", "https://cluster.example:8443", "reader_1", true, false, false},
		{"<@BOT> connect clickhouse https://cluster.example:8443 aviary_reader", "clickhouse", "https://cluster.example:8443", "aviary_reader", true, false, false},
		{"<@BOT> connect https://cluster.example:8443 研究/reader:prod", "clickhouse", "https://cluster.example:8443", "研究/reader:prod", true, false, false},
		{"<@BOT> connect https://cluster.example:8443 <mailto:reader@example.test|reader@example.test>", "clickhouse", "https://cluster.example:8443", "reader@example.test", true, false, false},
		{"<@BOT> connect https://cluster.example?", "", "", "", true, true, false},
		{"<@BOT> connect https://cluster.example#", "", "", "", true, true, false},
		{"connect https://mcp.example", "mcp", "https://mcp.example", "", true, false, true},
		{"connect https://host.example/mcp/", "mcp", "https://host.example/mcp/", "", true, false, true},
		{"connect mcp https://cluster.example reader_1", "mcp", "https://cluster.example", "reader_1", true, false, true},
		{"connect https://notmcp.example/path?mcp=true", "", "", "", true, true, true},
		{"connect https://notmcp.example/mcp2", "clickhouse", "https://notmcp.example/mcp2", "", true, false, true},
		{"connect clickhouse https://mcp.example", "clickhouse", "https://mcp.example", "", true, false, true},
		{"connect mcp https://cluster.example", "mcp", "https://cluster.example", "", true, false, true},
		{"connect https://user:fake-secret@cluster.example", "", "", "", true, true, true},
		{"connect https://cluster.example extra another", "", "", "", true, true, true},
		{"connect clickhouse https://cluster.example reader extra", "", "", "", true, true, true},
		{"connect https://cluster.example user:reader", "clickhouse", "https://cluster.example", "user:reader", true, false, true},
		{"connect https://cluster.example pass=guess", "", "", "", true, true, true},
		{"connect https://cluster.example password=guess", "", "", "", true, true, true},
		{"connect https://cluster.example <@U123>", "", "", "", true, true, true},
		{"connect https://cluster.example <mailto:reader@example.test|another@example.test>", "", "", "", true, true, true},
		{"status extra", "", "", "", true, true, true},
		{"disconnect", "", "", "", false, false, false},
		{"please connect https://cluster.example", "", "", "", false, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.text, func(t *testing.T) {
			got, recognized, err := parseSlackConnectionCommand(tt.text, "BOT", tt.isDM)
			if recognized != tt.recognized || (err != nil) != tt.invalid {
				t.Fatalf("recognized=%v err=%v", recognized, err)
			}
			if !tt.invalid && (got.transport != tt.transport || got.endpoint != tt.endpoint || got.username != tt.username) {
				t.Fatalf("transport=%q endpoint=%q username=%q", got.transport, got.endpoint, got.username)
			}
		})
	}
}

func TestSlackDBUsernameTokenBounds(t *testing.T) {
	if !validSlackDBUsername(strings.Repeat("r", 256)) || validSlackDBUsername(strings.Repeat("r", 257)) {
		t.Fatal("username byte bound incorrect")
	}
	for _, username := range []string{"", "reader name", "reader\nname", "reader\x00name", "reader\u200bname", "reader\u202ename", "reader|name", "reader<name>", "password=guess"} {
		if validSlackDBUsername(username) {
			t.Fatalf("ambiguous username accepted: %q", username)
		}
	}
	if !validSlackDBUsername("研究/reader:prod") {
		t.Fatal("valid Unicode database username rejected")
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
