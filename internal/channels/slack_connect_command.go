package channels

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

type slackConnectionCommand struct {
	verb      string
	transport string
	endpoint  string
	username  string
}

func validSlackDBUsername(username string) bool {
	if username == "" || len(username) > 256 || !utf8.ValidString(username) {
		return false
	}
	lower := strings.ToLower(username)
	if strings.HasPrefix(lower, "password=") || strings.HasPrefix(lower, "pass=") || strings.HasPrefix(lower, "pwd=") {
		return false
	}
	for _, r := range username {
		if unicode.IsSpace(r) || unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || r == '<' || r == '>' || r == '|' {
			return false
		}
	}
	return true
}

func unwrapSlackEmail(raw string) string {
	if !strings.HasPrefix(raw, "<mailto:") || !strings.HasSuffix(raw, ">") {
		return raw
	}
	inner := strings.TrimSuffix(strings.TrimPrefix(raw, "<mailto:"), ">")
	address, label, hasLabel := strings.Cut(inner, "|")
	if hasLabel && address != label {
		return raw
	}
	return address
}

// parseSlackConnectionCommand recognizes only a whole message command. URLs in
// ordinary conversation, quoted history and tool results never select a target.
func parseSlackConnectionCommand(raw, botID string, isDM bool) (slackConnectionCommand, bool, error) {
	fields := strings.Fields(raw)
	if len(fields) == 0 {
		return slackConnectionCommand{}, false, nil
	}
	mentioned := botID != "" && (fields[0] == "<@"+botID+">" ||
		strings.HasPrefix(fields[0], "<@"+botID+"|")) && strings.HasSuffix(fields[0], ">")
	if mentioned {
		fields = fields[1:]
	}
	if !isDM && !mentioned {
		return slackConnectionCommand{}, false, nil
	}
	if len(fields) == 0 {
		return slackConnectionCommand{}, false, nil
	}
	verb := strings.ToLower(fields[0])
	if verb != "connect" && verb != "status" && verb != "disconnect" {
		return slackConnectionCommand{}, false, nil
	}
	cmd := slackConnectionCommand{verb: verb}
	if verb != "connect" {
		if len(fields) != 1 {
			return cmd, true, fmt.Errorf("use %s without arguments", verb)
		}
		return cmd, true, nil
	}
	args := fields[1:]
	if len(args) > 0 && (strings.EqualFold(args[0], "mcp") || strings.EqualFold(args[0], "clickhouse")) {
		cmd.transport = strings.ToLower(args[0])
		args = args[1:]
	}
	if len(args) < 1 || len(args) > 2 {
		return cmd, true, fmt.Errorf("use connect [mcp|clickhouse] HTTPS_URL [username]")
	}
	cmd.endpoint = args[0]
	if len(args) == 2 {
		username := unwrapSlackEmail(args[1])
		if !validSlackDBUsername(username) {
			return cmd, true, fmt.Errorf("username must be one nonempty token of at most 256 bytes without whitespace, controls or Slack markup")
		}
		cmd.username = username
	}
	cmd.endpoint = unwrapSlackURL(cmd.endpoint)
	u, err := url.Parse(cmd.endpoint)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" || u.RawQuery != "" || u.ForceQuery || strings.Contains(cmd.endpoint, "#") {
		return cmd, true, fmt.Errorf("connect requires an HTTPS URL without credentials, query or fragment")
	}
	port := u.Port()
	if port == "" && strings.HasSuffix(u.Host, ":") {
		return cmd, true, fmt.Errorf("connect requires a valid HTTPS port")
	}
	if port != "" {
		parsed, err := strconv.Atoi(port)
		if err != nil || parsed < 1 || parsed > 65535 {
			return cmd, true, fmt.Errorf("connect requires a valid HTTPS port")
		}
	}
	if cmd.transport == "" {
		if strings.HasPrefix(strings.ToLower(u.Hostname()), "mcp.") || u.EscapedPath() == "/mcp" || u.EscapedPath() == "/mcp/" {
			cmd.transport = "mcp"
		} else {
			cmd.transport = "clickhouse"
		}
	}
	host := strings.ToLower(u.Hostname())
	if port == "" || port == "443" {
		if strings.Contains(host, ":") {
			u.Host = "[" + host + "]"
		} else {
			u.Host = host
		}
	} else {
		u.Host = net.JoinHostPort(host, port)
	}
	if u.Path == "/" && (u.RawPath == "" || u.RawPath == "/") {
		u.Path = ""
		u.RawPath = ""
	}
	cmd.endpoint = u.String()
	return cmd, true, nil
}

func unwrapSlackURL(raw string) string {
	if !strings.HasPrefix(raw, "<") || !strings.HasSuffix(raw, ">") {
		return raw
	}
	inner := raw[1 : len(raw)-1]
	urlPart, label, hasLabel := strings.Cut(inner, "|")
	if !strings.HasPrefix(urlPart, "https://") {
		return raw
	}
	if hasLabel && label != urlPart {
		return raw
	}
	return urlPart
}
