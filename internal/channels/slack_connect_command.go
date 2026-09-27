package channels

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

type slackConnectionCommand struct {
	verb      string
	transport string
	endpoint  string
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
	if len(fields) == 3 {
		cmd.transport = strings.ToLower(fields[1])
		if cmd.transport != "mcp" && cmd.transport != "clickhouse" {
			return cmd, true, fmt.Errorf("transport must be mcp or clickhouse")
		}
		cmd.endpoint = fields[2]
	} else if len(fields) == 2 {
		cmd.endpoint = fields[1]
	} else {
		return cmd, true, fmt.Errorf("use connect [mcp|clickhouse] HTTPS_URL")
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
