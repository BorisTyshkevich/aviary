package mcp

import (
	"context"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"time"

	sdkauth "github.com/modelcontextprotocol/go-sdk/auth"
	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/lsegal/aviary/internal/buildinfo"
	"github.com/lsegal/aviary/internal/clientauth"
	"github.com/lsegal/aviary/internal/config"
)

type clientServer struct {
	principal clientauth.Principal
	policy    config.ClientConfig
	server    *sdkmcp.Server
}

// ClientHTTPHandler retains one SDK transport, so administrator and client
// session IDs share the SDK's principal ownership checks across every HTTP verb.
type ClientHTTPHandler struct {
	registry      *clientauth.Registry
	administrator *sdkmcp.Server
	base          http.Handler
	mu            sync.Mutex
	servers       map[string]*clientServer
}

// NewClientHTTPHandler creates shared transport ownership with separate catalogs.
func NewClientHTTPHandler(admin *sdkmcp.Server, registry *clientauth.Registry) *ClientHTTPHandler {
	h := &ClientHTTPHandler{registry: registry, administrator: admin, servers: map[string]*clientServer{}}
	h.base = withHTTPRequestContext(sdkmcp.NewStreamableHTTPHandler(h.selectServer, &sdkmcp.StreamableHTTPOptions{DisableLocalhostProtection: true}))
	return h
}

func (h *ClientHTTPHandler) selectServer(r *http.Request) *sdkmcp.Server {
	p, _, client := clientauth.FromContext(r.Context())
	if !client {
		return h.administrator
	}
	policy, ok := h.registry.Policy(p)
	if !ok {
		return nil
	}
	key := p.ID + ":" + config.ClientPolicyRevision([]config.ClientConfig{policy})
	h.mu.Lock()
	defer h.mu.Unlock()
	if existing := h.servers[key]; existing != nil {
		return existing.server
	}
	s := sdkmcp.NewServer(&sdkmcp.Implementation{Name: "aviary", Version: buildinfo.Version}, nil)
	if slices.Contains(policy.Tools, "ping") {
		registerPingTool(s)
	}
	if slices.Contains(policy.Tools, "agent_run") {
		registerAgentRunTool(s)
	}
	s.AddReceivingMiddleware(func(next sdkmcp.MethodHandler) sdkmcp.MethodHandler {
		return func(ctx context.Context, method string, req sdkmcp.Request) (sdkmcp.Result, error) {
			if _, ok := h.registry.Policy(p); !ok {
				return nil, clientauth.ErrDenied
			}
			if method == "tools/call" {
				params, ok := req.GetParams().(*sdkmcp.CallToolParamsRaw)
				if !ok || !h.registry.Allowed(p, params.Name) {
					return nil, clientauth.ErrDenied
				}
				slog.Info("mcp: client tool call", "client_id", p.ID, "client_name", p.Name, "tool", params.Name)
			}
			callCtx, release, err := h.registry.TrackStream(ctx, p)
			if err != nil {
				return nil, err
			}
			defer release()
			result, err := next(clientauth.WithPrincipal(callCtx, p, h.registry), method, req)
			if list, ok := result.(*sdkmcp.ListToolsResult); ok {
				list.Tools = slices.DeleteFunc(list.Tools, func(t *sdkmcp.Tool) bool { return !h.registry.Allowed(p, t.Name) })
			}
			return result, err
		}
	})
	h.servers[key] = &clientServer{p, policy, s}
	return s
}

// Reconcile closes every transport session whose credential or grants changed.
func (h *ClientHTTPHandler) Reconcile() {
	h.mu.Lock()
	var closeServers []*sdkmcp.Server
	for key, s := range h.servers {
		current, ok := h.registry.Policy(s.principal)
		if !ok || config.ClientPolicyRevision([]config.ClientConfig{current}) != config.ClientPolicyRevision([]config.ClientConfig{s.policy}) {
			delete(h.servers, key)
			closeServers = append(closeServers, s.server)
		}
	}
	h.mu.Unlock()
	for _, s := range closeServers {
		for session := range s.Sessions() {
			_ = session.Close()
		}
	}
}

func (h *ClientHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if p, _, ok := clientauth.FromContext(r.Context()); ok {
		ctx, release, err := h.registry.TrackStream(r.Context(), p)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		defer release()
		r = r.WithContext(ctx)
	}
	h.base.ServeHTTP(w, r)
}

// PrincipalMiddleware authenticates client bearers and supplies the SDK transport
// owner. Administrator cookie/query requests use AdminPrincipalHandler separately.
func PrincipalMiddleware(registry *clientauth.Registry, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bearer := r.Header.Get("Authorization")
		raw := ""
		if len(bearer) > 7 && bearer[:7] == "Bearer " {
			raw = bearer[7:]
		}
		p, client := registry.Authenticate(raw)
		if client && raw != "" {
			verifier := func(context.Context, string, *http.Request) (*sdkauth.TokenInfo, error) {
				return &sdkauth.TokenInfo{UserID: p.ID, Expiration: time.Now().Add(time.Hour)}, nil
			}
			sdkauth.RequireBearerToken(verifier, nil)(next).ServeHTTP(w, r.WithContext(clientauth.WithPrincipal(r.Context(), p, registry)))
			return
		}
		// Reuse the administrator-only middleware at the server boundary by passing
		// an authenticated owner via an SDK bearer verifier in AdminPrincipalHandler.
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
	})
}

// AdminPrincipalHandler is applied after administrator-only authentication.
func AdminPrincipalHandler(token string, next http.Handler) http.Handler {
	verifier := func(context.Context, string, *http.Request) (*sdkauth.TokenInfo, error) {
		return &sdkauth.TokenInfo{UserID: "aviary:administrator", Expiration: time.Now().Add(time.Hour)}, nil
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.Clone(r.Context())
		r.Header.Set("Authorization", "Bearer "+token)
		sdkauth.RequireBearerToken(verifier, nil)(next).ServeHTTP(w, r)
	})
}
