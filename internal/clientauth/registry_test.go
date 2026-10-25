package clientauth

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/config"
)

func fixture() *config.Config {
	return &config.Config{Agents: []config.AgentConfig{{Name: "expert"}}, Server: config.ServerConfig{Clients: []config.ClientConfig{{ID: "client_00000000000000000000000000000001", Name: "peer", TokenHash: config.ClientTokenHash("fake-old"), Protocols: []string{"mcp"}, Tools: []string{"agent_run", "ping"}, Agents: []string{"expert"}}}}}
}

func TestCredentialLifecycleSeparatesRunsAndStreams(t *testing.T) {
	r := New()
	cfg := fixture()
	require.NoError(t, r.Install(cfg))
	p, ok := r.Authenticate("fake-old")
	require.True(t, ok)
	stream, releaseStream, err := r.TrackStream(context.Background(), p)
	require.NoError(t, err)
	defer releaseStream()
	var execution context.Context
	var releaseRun func()
	require.NoError(t, r.Admit(p, "expert", func(ctx context.Context, release func()) error { execution = ctx; releaseRun = release; return nil }))
	defer releaseRun()
	cfg.Server.Clients[0].TokenHash = config.ClientTokenHash("fake-new")
	require.NoError(t, r.Install(cfg))
	require.Eventually(t, func() bool { return stream.Err() != nil }, time.Second, time.Millisecond)
	require.NoError(t, execution.Err())
	_, ok = r.Authenticate("fake-old")
	require.False(t, ok)
	newP, ok := r.Authenticate("fake-new")
	require.True(t, ok)
	require.Equal(t, p.ID, newP.ID)
	called := false
	require.Error(t, r.Admit(p, "expert", func(context.Context, func()) error { called = true; return nil }))
	require.False(t, called)
	cfg.Server.Clients = nil
	require.NoError(t, r.Install(cfg))
	require.Eventually(t, func() bool { return execution.Err() != nil }, time.Second, time.Millisecond)
	recreated := fixture()
	recreated.Server.Clients[0].ID = "client_00000000000000000000000000000002"
	require.NoError(t, r.Install(recreated))
	again, ok := r.Authenticate("fake-old")
	require.True(t, ok)
	require.NotEqual(t, p.ID, again.ID)
}

func TestRevokedScopesCancelRunsAndInvalidReloadDoesNotInstall(t *testing.T) {
	for _, scope := range []string{"agent", "tool"} {
		t.Run(scope, func(t *testing.T) {
			r := New()
			cfg := fixture()
			require.NoError(t, r.Install(cfg))
			p, _ := r.Authenticate("fake-old")
			var execution context.Context
			require.NoError(t, r.Admit(p, "expert", func(ctx context.Context, release func()) error { execution = ctx; t.Cleanup(release); return nil }))
			bad := fixture()
			bad.Server.Clients[0].Tools = []string{"config_save"}
			revision := r.Revision()
			require.Error(t, r.Install(bad))
			require.Equal(t, revision, r.Revision())
			require.NoError(t, execution.Err())
			if scope == "agent" {
				cfg.Agents = append(cfg.Agents, config.AgentConfig{Name: "other"})
				cfg.Server.Clients[0].Agents = []string{"other"}
			} else {
				cfg.Server.Clients[0].Tools = []string{"ping"}
			}
			require.NoError(t, r.Install(cfg))
			require.Error(t, execution.Err())
		})
	}
}

func TestDeniedAdmissionAndIndependentExecutionPolicy(t *testing.T) {
	r := New()
	cfg := fixture()
	require.NoError(t, r.Install(cfg))
	p, _ := r.Authenticate("fake-old")
	called := false
	require.Error(t, r.Admit(p, "other", func(context.Context, func()) error { called = true; return nil }))
	require.False(t, called)
	require.NoError(t, r.Admit(p, "expert", func(ctx context.Context, release func()) error {
		_, _, present := FromContext(ctx)
		require.False(t, present)
		release()
		return nil
	}))
	require.True(t, EqualToken("fake-admin", "fake-admin"))
	require.False(t, EqualToken("fake-admin", "fake-peer"))
}
