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
		{"<@BOT> connect <https://cluster.example:8443|cluster.example>", "clickhouse", "https://cluster.example:8443", "", true, false, false},
		{"<@BOT> connect <https://cluster.example:8443|Production database cluster>", "clickhouse", "https://cluster.example:8443", "", true, false, false},
		{"<@BOT> connect <https://cluster.example:8443|mcp.example>", "clickhouse", "https://cluster.example:8443", "", true, false, false},
		{"<@BOT> connect <https://cluster.example:8443/a&amp;b|Production database cluster> reader&amp;team", "clickhouse", "https://cluster.example:8443/a&b", "reader&team", true, false, false},
		{"<@BOT> connect clickhouse <https://cluster.example:8443|Production database cluster> reader_1", "clickhouse", "https://cluster.example:8443", "reader_1", true, false, false},
		{"connect mcp <https://cluster.example:8443|MCP gateway> reader_1", "mcp", "https://cluster.example:8443", "reader_1", true, false, true},
		{"<@BOT> connect https://CLUSTER.EXAMPLE:443/", "clickhouse", "https://cluster.example", "", true, false, false},
		{"<@BOT> connect https://CLUSTER.EXAMPLE:8443/a%2Fb", "clickhouse", "https://cluster.example:8443/a%2Fb", "", true, false, false},
		{"<@BOT> connect <https://cluster.example:8443> reader_1", "clickhouse", "https://cluster.example:8443", "reader_1", true, false, false},
		{"<@BOT> connect <https://cluster.example:8443> <http://reader.team.io|reader.team.io>", "clickhouse", "https://cluster.example:8443", "reader.team.io", true, false, false},
		{"<@BOT> connect <https://cluster.example:8443> <https://reader.team.io|reader.team.io>", "clickhouse", "https://cluster.example:8443", "reader.team.io", true, false, false},
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
		{"connect <http://cluster.example|https://cluster.example>", "", "", "", true, true, true},
		{"connect <https://user:fake-secret@cluster.example|cluster.example>", "", "", "", true, true, true},
		{"connect <https://cluster.example?token=fake|cluster.example>", "", "", "", true, true, true},
		{"connect <https://cluster.example#fragment|cluster.example>", "", "", "", true, true, true},
		{"connect <https://cluster.example?token=fake&amp;more=fake|cluster.example>", "", "", "", true, true, true},
		{"connect <https://user&amp;team@cluster.example|cluster.example>", "", "", "", true, true, true},
		{"connect <https://cluster.example|safe> reader&lt;admin", "", "", "", true, true, true},
		{"connect <https://cluster.example|Production database cluster> extra another", "", "", "", true, true, true},
		{"connect <https://cluster.example|Production database cluster> reader extra", "", "", "", true, true, true},
		{"connect <https://cluster.example|Production <database> cluster>", "", "", "", true, true, true},
		{"connect <https://cluster.example|Production|database>", "", "", "", true, true, true},
		{"connect <https://cluster.example|Production database", "", "", "", true, true, true},
		{"connect <https://cluster.example|Production database>suffix", "", "", "", true, true, true},
		{"connect https://cluster.example extra another", "", "", "", true, true, true},
		{"connect clickhouse https://cluster.example reader extra", "", "", "", true, true, true},
		{"connect https://cluster.example user:reader", "clickhouse", "https://cluster.example", "user:reader", true, false, true},
		{"connect https://cluster.example pass=guess", "", "", "", true, true, true},
		{"connect https://cluster.example password=guess", "", "", "", true, true, true},
		{"connect https://cluster.example <@U123>", "", "", "", true, true, true},
		{"connect https://cluster.example <http://other.team.io|reader.team.io>", "", "", "", true, true, true},
		{"connect https://cluster.example <https://reader.team.io/path|reader.team.io>", "", "", "", true, true, true},
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

func TestSlackEventConnectUsesLinkDestination(t *testing.T) {
	richBlocks := slack.Blocks{BlockSet: []slack.Block{slack.NewRichTextBlock("", slack.NewRichTextSection(
		slack.NewRichTextSectionUserElement("BOT", nil),
		slack.NewRichTextSectionTextElement(" connect ", nil),
		slack.NewRichTextSectionLinkElement("https://cluster.example:8443", "cluster.example", nil),
	))}}
	for _, tt := range []struct {
		name   string
		blocks slack.Blocks
	}{
		{"rich text", richBlocks},
		{"raw fallback", slack.Blocks{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ch := &SlackChannel{botUserID: "BOT"}
			called := false
			ch.intake = func(in slackIngress) bool {
				called = true
				cmd, recognized, err := parseSlackConnectionCommand(in.CommandText, "BOT", in.IsDM)
				if !recognized || err != nil {
					t.Fatalf("command rejected: recognized=%v err=%v text=%q", recognized, err, in.Text)
				}
				if cmd.endpoint != "https://cluster.example:8443" || cmd.transport != "clickhouse" {
					t.Fatalf("display label changed target: %#v", cmd)
				}
				return true
			}
			ch.handleMessageEvent(&slackevents.MessageEvent{
				User: "U1", Channel: "C1", TimeStamp: "100.000001",
				Text: "<@BOT> connect <https://cluster.example:8443|cluster.example>", Blocks: tt.blocks,
			})
			if !called {
				t.Fatal("connection command did not reach intake")
			}
		})
	}
}

func TestSlackEventWithoutRawTextCannotConnectFromLinkLabel(t *testing.T) {
	ch := &SlackChannel{botUserID: "BOT"}
	intake := &slackConnectionIntake{channel: ch}
	called := false
	ch.intake = func(in slackIngress) bool {
		called = true
		if in.CommandText != "" || in.Text != "<@BOT> connect https://safe.example" {
			t.Fatalf("unexpected command views: raw=%q visible=%q", in.CommandText, in.Text)
		}
		if intake.handle(in) {
			t.Fatal("visible link label was parsed as a connection command")
		}
		return false
	}
	ch.handleMessageEvent(&slackevents.MessageEvent{
		User: "U1", Channel: "C1", TimeStamp: "100.000001",
		Blocks: slack.Blocks{BlockSet: []slack.Block{slack.NewRichTextBlock("", slack.NewRichTextSection(
			slack.NewRichTextSectionUserElement("BOT", nil),
			slack.NewRichTextSectionTextElement(" connect ", nil),
			slack.NewRichTextSectionLinkElement("https://cluster.example", "https://safe.example", nil),
		))}},
	})
	if !called {
		t.Fatal("event did not reach command intake")
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
