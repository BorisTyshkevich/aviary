// Package mcp implements the MCP server and client dispatch for Aviary.
package mcp

import (
	"bytes"
	"io"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/lsegal/aviary/internal/agent"
	"github.com/lsegal/aviary/internal/buildinfo"
	"github.com/lsegal/aviary/internal/clientauth"
)

// NewServer creates and configures an MCP server with all Aviary tools registered.
func NewServer() *mcp.Server {
	s := mcp.NewServer(&mcp.Implementation{
		Name:    "aviary",
		Version: buildinfo.Version,
	}, nil)

	Register(s)

	return s
}

// HTTPHandler returns an http.Handler for the MCP server using the
// Streamable HTTP transport (MCP spec compliant).
func HTTPHandler(s *mcp.Server) http.Handler {
	base := mcp.NewStreamableHTTPHandler(func(_ *http.Request) *mcp.Server {
		return s
	}, &mcp.StreamableHTTPOptions{
		// Aviary authenticates MCP requests, and reverse proxies such as Tailscale
		// Serve forward their public Host to this loopback listener.
		DisableLocalhostProtection: true,
	})

	return withHTTPRequestContext(base)
}

// withHTTPRequestContext preserves administrator routing/logging and strips
// client impersonation headers without logging client arguments.
func withHTTPRequestContext(base http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _, client := clientauth.FromContext(r.Context())
		if client {
			r = r.Clone(r.Context())
			r.Header.Del("X-Aviary-Agent-ID")
		}
		if agentID := r.Header.Get("X-Aviary-Agent-ID"); !client && agentID != "" {
			r = r.WithContext(agent.WithSessionAgentID(r.Context(), agentID))
		}
		if !client && r.Method == http.MethodPost && r.Body != nil {
			body, err := io.ReadAll(r.Body)
			if err == nil {
				r.Body = io.NopCloser(bytes.NewReader(body))
				if name, args, ok := extractToolCallFromPayload(body); ok {
					logToolCall("http", name, args)
				}
			}
		}
		base.ServeHTTP(w, r)
	})
}
