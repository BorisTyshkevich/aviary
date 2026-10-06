// Package clientauth owns inbound identities, admission and credential revocation.
package clientauth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"slices"
	"sync"

	"github.com/lsegal/aviary/internal/config"
)

// ErrDenied is the fixed response for an unauthorized client operation.
var ErrDenied = errors.New("client operation is not authorized")

// Principal is server-established identity; its credential generation is private.
type Principal struct {
	ID, Name string
	hash     string
}

type run struct {
	clientID, agent string
	cancel          context.CancelFunc
}
type stream struct {
	principal Principal
	cancel    context.CancelFunc
}

// Registry installs coherent policy snapshots and tracks accepted work separately
// from transport lifetimes. Its lock linearizes revocation with run admission.
type Registry struct {
	mu       sync.Mutex
	clients  map[string]config.ClientConfig
	runs     map[*run]struct{}
	streams  map[*stream]struct{}
	revision string
}

// New creates an empty registry; install a validated configuration before ingress.
func New() *Registry {
	return &Registry{clients: map[string]config.ClientConfig{}, runs: map[*run]struct{}{}, streams: map[*stream]struct{}{}}
}

// EqualToken compares fixed-length digests, including administrator credentials.
func EqualToken(a, b string) bool {
	x, y := sha256.Sum256([]byte(a)), sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(x[:], y[:]) == 1
}

// Authenticate resolves a bearer credential without retaining the raw token.
func (r *Registry) Authenticate(token string) (Principal, bool) {
	hash := config.ClientTokenHash(token)
	r.mu.Lock()
	defer r.mu.Unlock()
	var found Principal
	for _, c := range r.clients {
		if subtle.ConstantTimeCompare([]byte(c.TokenHash), []byte(hash)) == 1 {
			found = Principal{c.ID, c.Name, c.TokenHash}
		}
	}
	return found, found.ID != ""
}

func (r *Registry) policy(p Principal) (config.ClientConfig, bool) {
	c, ok := r.clients[p.ID]
	return c, ok && subtle.ConstantTimeCompare([]byte(c.TokenHash), []byte(p.hash)) == 1
}

// Policy returns a detached policy for a currently valid credential.
func (r *Registry) Policy(p Principal) (config.ClientConfig, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.policy(p)
	c.Protocols, c.Tools, c.Agents = slices.Clone(c.Protocols), slices.Clone(c.Tools), slices.Clone(c.Agents)
	return c, ok
}

// Allowed checks an exact tool grant against the current credential generation.
func (r *Registry) Allowed(p Principal, tool string) bool {
	c, ok := r.Policy(p)
	return ok && slices.Contains(c.Protocols, "mcp") && slices.Contains(c.Tools, tool)
}

// Admit keeps the policy locked through the side-effecting admission callback.
// Execution has no caller principal or request cancellation in its context.
func (r *Registry) Admit(p Principal, agent string, fn func(context.Context, func()) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.policy(p)
	if !ok || !slices.Contains(c.Protocols, "mcp") || !slices.Contains(c.Tools, "agent_run") || !slices.Contains(c.Agents, agent) {
		return ErrDenied
	}
	ctx, cancel := context.WithCancel(context.Background())
	work := &run{p.ID, agent, cancel}
	r.runs[work] = struct{}{}
	// Release may be invoked synchronously by a rejection; do cleanup after unlock.
	release := func() { cancel(); go func() { r.mu.Lock(); delete(r.runs, work); r.mu.Unlock() }() }
	if err := fn(ctx, release); err != nil {
		cancel()
		delete(r.runs, work)
		return err
	}
	return nil
}

// TrackStream binds a request/stream to the current credential generation.
func (r *Registry) TrackStream(ctx context.Context, p Principal) (context.Context, func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.policy(p); !ok {
		return nil, nil, ErrDenied
	}
	ctx, cancel := context.WithCancel(ctx)
	s := &stream{p, cancel}
	r.streams[s] = struct{}{}
	return ctx, func() { cancel(); r.mu.Lock(); delete(r.streams, s); r.mu.Unlock() }, nil
}

// Install revokes changed transport policy and cancels removed execution scopes.
// Rotation changes only transport lifetimes; accepted runs retain ownership.
func (r *Registry) Install(cfg *config.Config) error {
	if err := config.ValidateClients(cfg); err != nil {
		return err
	}
	next := map[string]config.ClientConfig{}
	for _, c := range cfg.Server.Clients {
		c.Protocols, c.Tools, c.Agents = slices.Clone(c.Protocols), slices.Clone(c.Tools), slices.Clone(c.Agents)
		next[c.ID] = c
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for s := range r.streams {
		old := r.clients[s.principal.ID]
		c, ok := next[s.principal.ID]
		if !ok || c.TokenHash != s.principal.hash || !slices.Equal(c.Tools, old.Tools) || !slices.Equal(c.Agents, old.Agents) || !slices.Equal(c.Protocols, old.Protocols) {
			s.cancel()
		}
	}
	for w := range r.runs {
		c, ok := next[w.clientID]
		if !ok || !slices.Contains(c.Protocols, "mcp") || !slices.Contains(c.Tools, "agent_run") || !slices.Contains(c.Agents, w.agent) {
			w.cancel()
		}
	}
	r.clients = next
	r.revision = config.ClientPolicyRevision(cfg.Server.Clients)
	return nil
}

// Revision identifies the installed client policy for local operator acknowledgment.
func (r *Registry) Revision() string { r.mu.Lock(); defer r.mu.Unlock(); return r.revision }

type principalKey struct{}

// WithPrincipal attaches a trusted inbound identity to transport handling only.
func WithPrincipal(ctx context.Context, p Principal, registry *Registry) context.Context {
	return context.WithValue(ctx, principalKey{}, callIdentity{p, registry})
}

type callIdentity struct {
	principal Principal
	registry  *Registry
}

// FromContext retrieves the server-established identity and its policy registry.
func FromContext(ctx context.Context) (Principal, *Registry, bool) {
	c, ok := ctx.Value(principalKey{}).(callIdentity)
	return c.principal, c.registry, ok
}
