package server

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShouldAttachSlackAnswer(t *testing.T) {
	tests := []struct {
		name   string
		answer string
		want   bool
	}{
		{name: "empty", answer: "  ", want: false},
		{name: "short plain paragraph", answer: "Looks good.", want: false},
		{name: "inline length boundary", answer: strings.Repeat("a", 400), want: false},
		{name: "over inline length", answer: strings.Repeat("a", 401), want: true},
		{name: "multiple paragraphs", answer: "First paragraph.\nSecond paragraph.", want: true},
		{name: "level one heading", answer: "# Heading", want: true},
		{name: "level two heading", answer: "## Heading", want: true},
		{name: "level three heading", answer: "### Heading", want: true},
		{name: "plus list", answer: "+ list item", want: true},
		{name: "dash list", answer: "- list item", want: true},
		{name: "star list", answer: "* list item", want: true},
		{name: "ordered list", answer: "12. list item", want: true},
		{name: "parenthesized ordered list", answer: "2) list item", want: true},
		{name: "blockquote", answer: "> quoted", want: true},
		{name: "bold", answer: "**bold text**", want: true},
		{name: "star italic", answer: "*italic text*", want: true},
		{name: "underscore italic", answer: "_emphasis_", want: true},
		{name: "inline emphasis", answer: "This is *important*.", want: true},
		{name: "strikethrough", answer: "~~removed~~", want: true},
		{name: "inline code", answer: "Run `go test`.", want: true},
		{name: "link", answer: "Read [the guide](https://example.com).", want: true},
		{name: "horizontal rule", answer: "---", want: true},
		{name: "plain hashtag", answer: "Use #general for updates.", want: false},
		{name: "plain identifier", answer: "Use foo_bar_baz.", want: false},
		{name: "plain multiplication", answer: "2 * 3 is 6.", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, shouldAttachSlackAnswer(tt.answer))
		})
	}
}
