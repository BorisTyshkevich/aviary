package channels

import (
	"regexp"
	"strings"
)

// slackMuteReaction acknowledges a thread mute without adding a message.
const slackMuteReaction = "mute"

var slackStopRequest = regexp.MustCompile(
	`^(?:(?:please|pls)[ ,]+)?(?:stop(?: (?:replying|responding|answering|talking))?|be quiet|quiet|shut up|mute|enough|go away|silence)(?:[ ,]+(?:now|please|pls|thanks))*$`)

// isSlackStopRequest reports whether text is a bare request, addressed to the
// bot by a direct mention, to stop replying. The whole message must be the
// request, so "@bot stop the merge and check parts" is an ordinary question.
func isSlackStopRequest(text, botUserID string) bool {
	if !slackBotMention(text, botUserID) {
		return false
	}
	bare := strings.ToLower(strings.TrimSpace(slackUserMention.ReplaceAllString(text, " ")))
	bare = strings.Join(strings.Fields(strings.Trim(bare, " .!,:;-")), " ")
	return slackStopRequest.MatchString(bare)
}

// slackClaimedReplyAllowed applies claimed_reply_scope to an unaddressed reply
// in a claimed thread. Unknown scopes behave like the default.
func slackClaimedReplyAllowed(scope string, owner *slackThreadOwner, msg IncomingMessage) bool {
	switch scope {
	case "mentions":
		return false
	case "claimer":
		return owner.ClaimedBy != "" && msg.From == owner.ClaimedBy
	default:
		return true
	}
}

// muteSlackThread silences the owning bot for unaddressed replies until it is
// mentioned again, and acknowledges with a reaction instead of a message.
func (m *Manager) muteSlackThread(ch *SlackChannel, msg IncomingMessage) {
	if err := m.affinity.setMuted(ch.teamID, msg.Channel, msg.ThreadTS, true); err != nil {
		ch.logf("slack: could not mute thread: %v", err)
		return
	}
	if msg.MessageTS == "" {
		return
	}
	if err := ch.addReaction(msg.Channel, msg.MessageTS, slackMuteReaction); err != nil && !strings.Contains(err.Error(), "already_reacted") {
		ch.logf("slack: could not react to mute request: %v", err)
	}
}
