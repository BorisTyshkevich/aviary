package server

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/slack-go/slack"

	"github.com/lsegal/aviary/internal/channels"
)

// TestSlackPresenterLiveSmoke is opt-in and uses one synthetic thread in the
// explicitly configured Slack test channel. The root and final answer remain
// for inspection; temporary progress is deleted by the presenter.
func TestSlackPresenterLiveSmoke(t *testing.T) {
	fixturePath := os.Getenv("AVIARY_SLACK_SMOKE_FILE")
	if fixturePath == "" {
		t.Skip("AVIARY_SLACK_SMOKE_FILE is unset")
	}
	data, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal("cannot read protected Slack smoke fixture")
	}
	var fixture struct {
		BotToken string `json:"bot_token"`
		Channel  string `json:"channel"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal("invalid Slack smoke fixture")
	}
	if fixture.BotToken == "" || fixture.Channel == "" {
		t.Fatal("Slack smoke fixture lacks bot token or channel")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	api := slack.New(fixture.BotToken)
	_, rootTS, err := api.PostMessageContext(ctx, fixture.Channel, slack.MsgOptionText("Aviary synthetic PR2 progress demonstration (no live agent or tool run)", false))
	if err != nil || rootTS == "" {
		t.Fatal("failed to create synthetic Slack smoke thread")
	}
	ch := channels.NewSlackChannel("", fixture.BotToken, nil, "", nil)
	p := newSlackPresenter(ch, fixture.Channel, rootTS, true)
	p.delay = 100 * time.Millisecond
	p.Tool("synthetic_registered_tool", "smoke-call-1", "started")
	deadline := time.Now().Add(10 * time.Second)
	for p.progressTimestamp() == "" && time.Now().Before(deadline) {
		time.Sleep(25 * time.Millisecond)
	}
	if p.progressTimestamp() == "" {
		t.Fatal("synthetic Slack progress was not accepted")
	}
	p.Tool("synthetic_registered_tool", "smoke-call-1", "succeeded")
	updated := false
	for !updated && time.Now().Before(deadline) {
		messages, _, _, fetchErr := api.GetConversationRepliesContext(ctx, &slack.GetConversationRepliesParameters{ChannelID: fixture.Channel, Timestamp: rootTS, Limit: 20})
		if fetchErr == nil {
			for _, message := range messages {
				if strings.Contains(message.Text, "Tool progress") && strings.Contains(message.Text, "synthetic_registered_tool succeeded") {
					updated = true
					break
				}
			}
		}
		if !updated {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if !updated {
		t.Fatal("synthetic Slack progress edit was not visible")
	}
	// Leave the edited state visible long enough to inspect in the test channel.
	time.Sleep(8 * time.Second)
	result, first := p.Terminal(nil, "done", "", "Synthetic final answer.", false)
	if !first || result.Outcome != slackOutcomeAnswer || result.CleanupPending {
		t.Fatal("synthetic Slack answer or progress cleanup failed")
	}
	for time.Now().Before(deadline.Add(10 * time.Second)) {
		messages, _, _, fetchErr := api.GetConversationRepliesContext(ctx, &slack.GetConversationRepliesParameters{ChannelID: fixture.Channel, Timestamp: rootTS, Limit: 20})
		if fetchErr == nil {
			foundAnswer, foundProgress := false, false
			for _, message := range messages {
				foundAnswer = foundAnswer || strings.Contains(message.Text, "Synthetic final answer.")
				foundProgress = foundProgress || strings.Contains(message.Text, "Tool progress")
			}
			if foundAnswer && !foundProgress {
				t.Logf("synthetic Slack smoke thread channel=%s root_ts=%s", fixture.Channel, rootTS)
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("synthetic Slack thread did not show final answer without progress")
}
