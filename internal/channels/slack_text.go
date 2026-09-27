package channels

import (
	"regexp"
	"strings"

	"github.com/slack-go/slack"
)

var slackAutoLink = regexp.MustCompile(`<((?:https?://|mailto:)[^>|]+)(?:\|([^>]*))?>`)

// slackVisibleText reconstructs the message as entered in Slack. Rich text
// carries literal text without Slack's mrkdwn escapes. The fallback undoes
// only Slack's three documented entity substitutions, exactly once.
func slackVisibleText(raw string, blocks slack.Blocks) string {
	if visible, ok := richTextVisible(blocks); ok {
		return visible
	}
	plain := slackAutoLink.ReplaceAllStringFunc(raw, func(match string) string {
		parts := slackAutoLink.FindStringSubmatch(match)
		if len(parts) < 3 {
			return match
		}
		if parts[2] != "" {
			return parts[2]
		}
		return strings.TrimPrefix(parts[1], "mailto:")
	})
	return strings.NewReplacer("&amp;", "&", "&lt;", "<", "&gt;", ">").Replace(plain)
}

func richTextVisible(blocks slack.Blocks) (string, bool) {
	if len(blocks.BlockSet) != 1 {
		return "", false
	}
	rich, ok := blocks.BlockSet[0].(*slack.RichTextBlock)
	if !ok || len(rich.Elements) == 0 {
		return "", false
	}
	var out strings.Builder
	for _, elem := range rich.Elements {
		section, ok := elem.(*slack.RichTextSection)
		if !ok {
			return "", false
		}
		for _, fragment := range section.Elements {
			switch part := fragment.(type) {
			case *slack.RichTextSectionTextElement:
				if part.Style != nil {
					return "", false
				}
				out.WriteString(part.Text)
			case *slack.RichTextSectionLinkElement:
				if part.Style != nil {
					return "", false
				}
				if part.Text != "" {
					out.WriteString(part.Text)
				} else {
					out.WriteString(strings.TrimPrefix(part.URL, "mailto:"))
				}
			case *slack.RichTextSectionUserElement:
				if part.Style != nil {
					return "", false
				}
				out.WriteString("<@" + part.UserID + ">")
			default:
				return "", false
			}
		}
	}
	return out.String(), true
}
