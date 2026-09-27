package channels

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"github.com/stretchr/testify/require"
)

func TestSlackReplyFetchRequiresExactMessageAndThread(t *testing.T) {
	for _, tc := range []struct {
		name, timestamp, thread string
		accepted                bool
		includeParent           bool
	}{
		{"requested reply", "100.000002", "100.000001", true, false},
		{"parent alongside reply", "100.000002", "100.000001", true, true},
		{"different reply", "100.000003", "100.000001", false, false},
		{"parent instead of reply", "100.000001", "100.000001", false, false},
		{"wrong thread", "100.000002", "99.000001", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.NoError(t, r.ParseForm())
				require.Equal(t, "100.000002", r.FormValue("oldest"))
				require.Equal(t, "100.000002", r.FormValue("latest"))
				messages := []map[string]string{{"ts": tc.timestamp, "thread_ts": tc.thread, "user": "U1", "text": "fake private reply"}}
				if tc.includeParent {
					messages = append([]map[string]string{{"ts": "100.000001", "text": "parent"}}, messages...)
					messages = append(messages, map[string]string{"ts": "100.000003", "thread_ts": tc.thread, "text": "another reply"})
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "messages": messages})
			}))
			defer server.Close()
			ch := NewSlackChannel("xapp-fake", "xoxb-fake", nil, "", nil)
			ch.client = slack.New("xoxb-fake", slack.OptionAPIURL(server.URL+"/"))
			event := &slackevents.MessageEvent{SubType: slack.MsgSubTypeMessageReplied, Channel: "D1", ThreadTimeStamp: "100.000001", Message: &slack.Msg{Timestamp: "100.000001", LatestReply: "100.000002"}}
			normalized, ok := ch.normalizeMessageRepliedEvent(context.Background(), event)
			require.Equal(t, tc.accepted, ok)
			if tc.accepted {
				require.Equal(t, "fake private reply", normalized.Text)
			} else {
				require.Same(t, event, normalized)
			}
		})
	}
}
