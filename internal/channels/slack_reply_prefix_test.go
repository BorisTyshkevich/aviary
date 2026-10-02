package channels

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/config"
)

type recordingReplySender struct {
	posts, edits, deletes, intros, files []string
}

func (s *recordingReplySender) PostThreadTextContext(_ context.Context, _, _, body string) (string, error) {
	s.posts = append(s.posts, body)
	return "1700000001.000001", nil
}

func (s *recordingReplySender) EditThreadTextContext(_ context.Context, _, _, body string) error {
	s.edits = append(s.edits, body)
	return nil
}

func (s *recordingReplySender) DeleteThreadMessageContext(_ context.Context, _, ts string) error {
	s.deletes = append(s.deletes, ts)
	return nil
}

func (s *recordingReplySender) ShareThreadMarkdownFileContext(_ context.Context, _, _, introduction, answer string) error {
	s.intros = append(s.intros, introduction)
	s.files = append(s.files, answer)
	return nil
}

func TestPrefixSlackRepliesLeadsEveryVisibleBody(t *testing.T) {
	inner := &recordingReplySender{}
	sender := PrefixSlackReplies(inner, "🔒")
	ctx := context.Background()
	_, err := sender.PostThreadTextContext(ctx, "C1", "1700000000.000001", "answer")
	require.NoError(t, err)
	require.NoError(t, sender.EditThreadTextContext(ctx, "C1", "1700000001.000001", "Stopped."))
	require.NoError(t, sender.DeleteThreadMessageContext(ctx, "C1", "1700000001.000001"))
	require.NoError(t, sender.ShareThreadMarkdownFileContext(ctx, "C1", "1700000000.000001", "Summary.", "# full answer"))
	require.Equal(t, []string{"🔒 answer"}, inner.posts)
	require.Equal(t, []string{"🔒 Stopped."}, inner.edits)
	require.Equal(t, []string{"1700000001.000001"}, inner.deletes)
	require.Equal(t, []string{"🔒 Summary."}, inner.intros)
	require.Equal(t, []string{"# full answer"}, inner.files, "attached file content stays unchanged")
}

func TestPrefixSlackRepliesWithoutPrefixReturnsSender(t *testing.T) {
	inner := &recordingReplySender{}
	require.Same(t, inner, PrefixSlackReplies(inner, ""))
	require.Equal(t, "answer", SlackReplyText("", "answer"))
}

func TestSlackReplyPrefixMarkersSelectQuestions(t *testing.T) {
	always := config.ChannelConfig{Type: "slack", ReplyPrefix: "🔒"}
	marked := config.ChannelConfig{Type: "slack", ReplyPrefix: "🔒", ReplyPrefixMarkers: []string{":lock:", "🔒"}}
	for _, tc := range []struct {
		name     string
		cc       config.ChannelConfig
		question string
		want     string
	}{
		{"no prefix", config.ChannelConfig{Type: "slack"}, "<@BOT> :lock: question", ""},
		{"always without markers", always, "<@BOT> question", "🔒"},
		{"Slack shortcode marker", marked, "<@BOT> :lock: verify", "🔒"},
		{"shortcode adjoining text", marked, "<@BOT> test:lock:", "🔒"},
		{"unicode marker", marked, "🔒 <@BOT> question", "🔒"},
		{"unmarked question", marked, "<@BOT> question", ""},
		{"different lock emoji", marked, "<@BOT> :closed_lock_with_key: question", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, SlackReplyPrefix(tc.cc, tc.question))
		})
	}
}
