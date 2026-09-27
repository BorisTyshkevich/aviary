package agent

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	maxPublicSQLInput  = 64 << 10
	maxPublicSQLDetail = 1000
)

// publicToolDetail uses tool-specific, typed input allowlists. Tool arguments,
// results, and errors must never be copied directly into public progress.
func publicToolDetail(name string, args map[string]any) string {
	if len(args) == 0 {
		return ""
	}
	var parts []string
	if isPublicSQLTool(name) {
		if raw, ok := args["sql"].(string); ok && raw != "" {
			if sql, ok := redactPublicSQL(raw); ok {
				parts = append(parts, "SQL: "+sql)
			} else {
				parts = append(parts, "SQL details unavailable")
			}
		}
	}
	for _, key := range []string{"count", "limit", "max_bytes", "max_rows", "offset", "timeout_ms"} {
		if value, ok := publicNumericArg(args[key]); ok {
			parts = append(parts, key+"="+value)
		}
	}
	for _, key := range []string{"dry_run", "include_hidden", "recursive"} {
		if value, ok := args[key].(bool); ok {
			parts = append(parts, fmt.Sprintf("%s=%t", key, value))
		}
	}
	return strings.Join(parts, "; ")
}

func isPublicSQLTool(name string) bool {
	if name == "chlab_query" || name == "clickhouse_query" {
		return true
	}
	const prefix, suffix = "clickhouse_", "__query"
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return false
	}
	generation := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
	if generation == "" {
		return false
	}
	for i := 0; i < len(generation); i++ {
		if !isSQLIdentByte(generation[i]) && generation[i] != '-' {
			return false
		}
	}
	return true
}

func publicNumericArg(raw any) (string, bool) {
	switch value := raw.(type) {
	case int:
		if value >= 0 && value <= 1_000_000_000 {
			return strconv.Itoa(value), true
		}
	case int64:
		if value >= 0 && value <= 1_000_000_000 {
			return strconv.FormatInt(value, 10), true
		}
	case float64:
		if !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0 && value <= 1_000_000_000 && math.Trunc(value) == value {
			return strconv.FormatFloat(value, 'f', 0, 64), true
		}
	}
	return "", false
}

// redactPublicSQL keeps SQL structure and unquoted identifiers while replacing
// literals and removing comments. Unknown syntax is refused in full so that a
// new quoting form cannot accidentally publish its contents.
func redactPublicSQL(input string) (string, bool) {
	if len(input) == 0 || len(input) > maxPublicSQLInput {
		return "", false
	}
	var out strings.Builder
	out.Grow(min(len(input), maxPublicSQLDetail))
	space := func() {
		if out.Len() > 0 && out.String()[out.Len()-1] != ' ' {
			out.WriteByte(' ')
		}
	}
	for i := 0; i < len(input); {
		c := input[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			quote := c
			i++
			closed := false
			for i < len(input) {
				if input[i] == '\\' {
					i += 2
					continue
				}
				if input[i] == quote {
					i++
					if i < len(input) && input[i] == quote {
						i++
						continue
					}
					closed = true
					break
				}
				i++
			}
			if !closed || i > len(input) {
				return "", false
			}
			out.WriteByte('?')
		case c == '-' && i+1 < len(input) && input[i+1] == '-':
			i += 2
			for i < len(input) && input[i] != '\n' {
				i++
			}
			space()
		case c == '#' || (c == '/' && i+1 < len(input) && input[i+1] == '/'):
			if c == '/' {
				i += 2
			} else {
				i++
			}
			for i < len(input) && input[i] != '\n' {
				i++
			}
			space()
		case c == '/' && i+1 < len(input) && input[i+1] == '*':
			i += 2
			closed := false
			for i+1 < len(input) {
				if input[i] == '/' && input[i+1] == '*' {
					return "", false
				}
				if input[i] == '*' && input[i+1] == '/' {
					i += 2
					closed = true
					break
				}
				i++
			}
			if !closed {
				return "", false
			}
			space()
		case c >= '0' && c <= '9':
			if i > 0 && isSQLIdentByte(input[i-1]) {
				out.WriteByte(c)
				i++
				continue
			}
			start := i
			for i < len(input) && (isSQLIdentByte(input[i]) || input[i] == '.') {
				i++
			}
			literal := input[start:i]
			if strings.ContainsAny(literal, "_.") {
				for _, b := range []byte(literal) {
					if b == '_' {
						return "", false
					}
				}
			}
			out.WriteByte('?')
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			space()
			i++
		case isSQLIdentByte(c) || strings.ContainsRune("(),;.=<>!+-*/%|&^[]?", rune(c)):
			out.WriteByte(c)
			i++
		default:
			return "", false
		}
	}
	redacted := strings.TrimSpace(out.String())
	if redacted == "" {
		return "", false
	}
	if len(redacted) > maxPublicSQLDetail {
		redacted = strings.TrimSpace(redacted[:maxPublicSQLDetail-3]) + "..."
	}
	return redacted, true
}

func isSQLIdentByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_'
}
