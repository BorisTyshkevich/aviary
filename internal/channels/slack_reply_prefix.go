package channels

import (
	"context"
	"strings"

	"github.com/lsegal/aviary/internal/config"
)

// SlackReplySender posts, edits, deletes and attaches agent-run replies in a
// Slack thread.
type SlackReplySender interface {
	SlackRecoverySender
	ShareThreadMarkdownFileContext(ctx context.Context, channel, threadTS, introduction, answer string) error
}

// SlackReplyPrefix returns the reply prefix a route applies to one question.
// Configured markers limit the prefix to questions whose own text contains one.
func SlackReplyPrefix(cc config.ChannelConfig, question string) string {
	if cc.ReplyPrefix == "" || len(cc.ReplyPrefixMarkers) == 0 {
		return cc.ReplyPrefix
	}
	for _, marker := range cc.ReplyPrefixMarkers {
		if marker != "" && strings.Contains(question, marker) {
			return cc.ReplyPrefix
		}
	}
	return ""
}

// SlackReplyText leads body with a route's configured reply prefix and one
// space. An empty prefix leaves body unchanged.
func SlackReplyText(prefix, body string) string {
	if prefix == "" {
		return body
	}
	return prefix + " " + body
}

// PrefixSlackReplies returns sender with prefix leading every message body it
// posts or edits, including the visible introduction of an attached answer.
func PrefixSlackReplies(sender SlackReplySender, prefix string) SlackReplySender {
	if prefix == "" || sender == nil {
		return sender
	}
	return slackPrefixedSender{inner: sender, prefix: prefix}
}

type slackPrefixedSender struct {
	inner  SlackReplySender
	prefix string
}

func (s slackPrefixedSender) PostThreadTextContext(ctx context.Context, channel, threadTS, body string) (string, error) {
	return s.inner.PostThreadTextContext(ctx, channel, threadTS, SlackReplyText(s.prefix, body))
}

func (s slackPrefixedSender) EditThreadTextContext(ctx context.Context, channel, ts, body string) error {
	return s.inner.EditThreadTextContext(ctx, channel, ts, SlackReplyText(s.prefix, body))
}

func (s slackPrefixedSender) DeleteThreadMessageContext(ctx context.Context, channel, ts string) error {
	return s.inner.DeleteThreadMessageContext(ctx, channel, ts)
}

func (s slackPrefixedSender) ShareThreadMarkdownFileContext(ctx context.Context, channel, threadTS, introduction, answer string) error {
	return s.inner.ShareThreadMarkdownFileContext(ctx, channel, threadTS, SlackReplyText(s.prefix, introduction), answer)
}
