package channels

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
)

func TestSlackVisibleTextPreservesPasswordFromRichText(t *testing.T) {
	blocks := slack.Blocks{BlockSet: []slack.Block{slack.NewRichTextBlock("",
		slack.NewRichTextSection(
			slack.NewRichTextSectionTextElement("  literal &amp; <tag> ", nil),
			slack.NewRichTextSectionLinkElement("https://db.example", "", nil),
			slack.NewRichTextSectionTextElement("  ", nil),
		))}}
	got := slackVisibleText("  literal &amp;amp; &lt;tag&gt; <https://db.example>  ", blocks)
	if got != "  literal &amp; <tag> https://db.example  " {
		t.Fatalf("rich text password changed: %q", got)
	}
}

func TestSlackVisibleTextFallbackUnescapesOnceAndUnwrapsAutoLinks(t *testing.T) {
	raw := "  &amp;amp; &lt;tag&gt; <mailto:alice@example.test|alice@example.test> <https://db.example>  "
	got := slackVisibleText(raw, slack.Blocks{})
	if got != "  &amp; <tag> alice@example.test https://db.example  " {
		t.Fatalf("fallback password changed: %q", got)
	}
}

func TestSlackVisibleTextPreservesStyledPasswordMarkersFromRawText(t *testing.T) {
	tests := []struct {
		raw   string
		style *slack.RichTextSectionTextStyle
	}{
		{"*fakeBold*", &slack.RichTextSectionTextStyle{Bold: true}},
		{"_fakeItalic_", &slack.RichTextSectionTextStyle{Italic: true}},
		{"~fakeStrike~", &slack.RichTextSectionTextStyle{Strike: true}},
		{"`fakeCode`", &slack.RichTextSectionTextStyle{Code: true}},
	}
	for _, tc := range tests {
		blocks := slack.Blocks{BlockSet: []slack.Block{slack.NewRichTextBlock("", slack.NewRichTextSection(slack.NewRichTextSectionTextElement(tc.raw[1:len(tc.raw)-1], tc.style)))}}
		if got := slackVisibleText(tc.raw, blocks); got != tc.raw {
			t.Fatalf("styled password altered: %q became %q", tc.raw, got)
		}
	}
}

func TestSlackEventCredentialTextUsesVisibleRichTextBeforeObservers(t *testing.T) {
	ch := &SlackChannel{}
	ch.redactReference = func(channel, root string) bool { return channel == "D1" && root == "100.000001" }
	ch.intake = func(in slackIngress) bool {
		if in.Text != "  fake &amp; https://db.example  " {
			t.Fatalf("event password changed: %q", in.Text)
		}
		return true
	}
	ch.OnMessage(func(IncomingMessage) { t.Fatal("password reached ordinary handler") })
	ch.handleMessageEvent(&slackevents.MessageEvent{User: "U1", Channel: "D1", TimeStamp: "100.000002", ThreadTimeStamp: "100.000001",
		Text: "  fake &amp;amp; <https://db.example>  ", Blocks: slack.Blocks{BlockSet: []slack.Block{slack.NewRichTextBlock("", slack.NewRichTextSection(
			slack.NewRichTextSectionTextElement("  fake &amp; ", nil), slack.NewRichTextSectionLinkElement("https://db.example", "", nil), slack.NewRichTextSectionTextElement("  ", nil)))}}})
	if len(ch.seenMessages) != 0 {
		t.Fatal("password was retained in dedup keys")
	}
}

func TestSlackStartRejectsMissingTrustedIdentity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/auth.test" {
			t.Errorf("unexpected Slack API call: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"ok":true,"user_id":"BOT"}`))
	}))
	defer server.Close()
	ch := NewSlackChannel("xapp-fake", "xoxb-fake", nil, "", nil)
	ch.client = slack.New("xoxb-fake", slack.OptionAPIURL(server.URL+"/"))
	if err := ch.Start(context.Background()); err == nil || err.Error() != "slack identity unavailable" {
		t.Fatalf("missing team accepted: %v", err)
	}
}
