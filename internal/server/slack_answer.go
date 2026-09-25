package server

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lsegal/aviary/internal/llm"
)

const slackAnswerInlineLimit = 400

var (
	slackMarkdownLine   = regexp.MustCompile(`^(?:#{1,6}[ \t]+|[*+-][ \t]+|[0-9]+[.)][ \t]+|>[ \t]+|(?:-{3,}|\*{3,}|_{3,})$)`)
	slackInlineEmphasis = regexp.MustCompile(`(?:^|[^\pL\pN])(?:\*[^*\s](?:[^*]*[^*\s])?\*|_[^_\s](?:[^_]*[^_\s])?_)(?:$|[^\pL\pN])`)
)

func shouldAttachSlackAnswer(answer string) bool {
	answer = strings.TrimSpace(answer)
	if answer == "" {
		return false
	}
	if utf8.RuneCountInString(answer) > slackAnswerInlineLimit || strings.ContainsAny(answer, "\r\n") {
		return true
	}
	return strings.Contains(answer, "**") || strings.Contains(answer, "__") ||
		strings.Contains(answer, "~~") || strings.ContainsAny(answer, "`[") ||
		slackMarkdownLine.MatchString(answer) || slackInlineEmphasis.MatchString(answer)
}

func splitSlackPlainText(answer string, limit int) []string {
	if answer == "" || limit <= 0 {
		return nil
	}
	runes := []rune(answer)
	parts := make([]string, 0, (len(runes)+limit-1)/limit)
	for len(runes) > 0 {
		n := min(limit, len(runes))
		parts = append(parts, string(runes[:n]))
		runes = runes[n:]
	}
	return parts
}

func summarizeSlackAnswer(ctx context.Context, factory *llm.Factory, model, answer string) (string, error) {
	if factory == nil || model == "" {
		return "", fmt.Errorf("slack summary model is unavailable")
	}
	if len(answer) > 24000 {
		return "", fmt.Errorf("slack answer is too long to summarize in one request")
	}
	provider, err := factory.ForModel(model)
	if err != nil {
		return "", err
	}
	_, modelName, ok := strings.Cut(model, "/")
	if !ok {
		return "", fmt.Errorf("invalid Slack summary model %q", model)
	}
	summaryCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	stream, err := provider.Stream(summaryCtx, llm.Request{
		Model:    modelName,
		System:   "Summarize the supplied answer for a Slack reader in one or two concise plain-text sentences. State only facts in the answer. Do not use Markdown, links, mentions, headings, lists, or code. Stay under 350 characters.",
		Messages: []llm.Message{{Role: llm.RoleUser, Content: answer}},
		MaxToks:  180,
		Stream:   true,
	})
	if err != nil {
		return "", err
	}
	var out strings.Builder
	for event := range stream {
		switch event.Type {
		case llm.EventTypeText:
			out.WriteString(event.Text)
		case llm.EventTypeError:
			return "", event.Error
		}
	}
	if err := summaryCtx.Err(); err != nil {
		return "", err
	}
	summary := strings.Join(strings.Fields(out.String()), " ")
	if summary == "" {
		return "", fmt.Errorf("slack summary model returned empty text")
	}
	runes := []rune(summary)
	if len(runes) > 350 {
		summary = strings.TrimSpace(string(runes[:350]))
	}
	summary = strings.ReplaceAll(summary, "&", "&amp;")
	summary = strings.ReplaceAll(summary, "<", "&lt;")
	summary = strings.ReplaceAll(summary, ">", "&gt;")
	return summary, nil
}
