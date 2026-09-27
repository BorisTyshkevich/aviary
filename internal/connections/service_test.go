package connections

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testScope(root string) Scope {
	return Scope{AgentID: "agent", InstallationID: "install", WorkspaceID: "workspace", ChannelID: "channel", RootThreadID: root}
}
func testPrincipal(user string) Principal {
	return Principal{InstallationID: "install", WorkspaceID: "workspace", UserID: user}
}

func TestTargetLeaseIsolationAndRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	require.NoError(t, err)
	a := testScope("a")
	b := testScope("b")
	emptyLease, err := s.Begin(a)
	require.NoError(t, err)
	require.Equal(t, a, emptyLease.Target().Scope)
	_, _, err = s.Select(a, "clickhouse", "https://a.example")
	require.ErrorIs(t, err, ErrBusy)
	emptyLease.End()
	ta, changed, err := s.Select(a, "clickhouse", "https://a.example")
	require.NoError(t, err)
	require.True(t, changed)
	tb, changed, err := s.Select(b, "clickhouse", "https://b.example")
	require.NoError(t, err)
	require.True(t, changed)
	lease, err := s.Begin(a)
	require.NoError(t, err)
	require.Equal(t, ta, lease.Target())
	_, _, err = s.Select(a, "clickhouse", "https://new.example")
	require.ErrorIs(t, err, ErrBusy)
	require.ErrorIs(t, s.Disconnect(a), ErrBusy)
	_, changed, err = s.Select(a, "clickhouse", "https://a.example")
	require.NoError(t, err)
	require.False(t, changed)
	got, ok := s.Current(b)
	require.True(t, ok)
	require.Equal(t, tb, got)
	lease.End()
	lease.End()
	next, changed, err := s.Select(a, "clickhouse", "https://new.example")
	require.NoError(t, err)
	require.True(t, changed)
	require.NotEqual(t, ta.Generation, next.Generation)
	s2, err := Open(dir)
	require.NoError(t, err)
	got, ok = s2.Current(a)
	require.True(t, ok)
	require.Equal(t, next, got)
	require.NoError(t, s2.Disconnect(a))
	_, ok = s2.Current(a)
	require.False(t, ok)
}

func TestPrivatePromptsAndCredentialOwnership(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	require.NoError(t, err)
	target, _, err := s.Select(testScope("thread"), "clickhouse", "https://cluster.example")
	require.NoError(t, err)
	alice := testPrincipal("alice")
	bob := testPrincipal("bob")
	base := Prompt{Principal: alice, DMChannelID: "dm-alice", DMRootID: "prompt-a", Target: target, Stage: "password", Username: " exact user ", ExpiresAt: time.Now().Add(time.Hour)}
	require.NoError(t, s.PutPrompt(base))
	require.True(t, s.ClassifyReply("install", "workspace", "dm-alice", "prompt-a"))
	_, ok := s.PromptFor(bob, "dm-alice", "prompt-a")
	require.False(t, ok)
	secret := " fake password with spaces ! "
	require.ErrorIs(t, s.CompletePassword(context.Background(), bob, "dm-alice", "prompt-a", secret, nil), ErrPrompt)
	require.NoError(t, s.CompletePassword(context.Background(), alice, "dm-alice", "prompt-a", secret, func(context.Context, Target, Credential) error { return nil }))
	require.ErrorIs(t, s.CompletePassword(context.Background(), alice, "dm-alice", "prompt-a", secret, nil), ErrPrompt)
	e := Execution{Kind: Interactive, Scope: target.Scope, Principal: alice}
	c, ok := s.CredentialFor(e, target)
	require.True(t, ok)
	require.Equal(t, secret, c.Password)
	require.Equal(t, " exact user ", c.Username)
	public, err := json.Marshal(c)
	require.NoError(t, err)
	require.NotContains(t, string(public), secret)
	require.NotContains(t, c.String(), secret)
	_, ok = s.CredentialFor(Execution{Kind: Scheduled, Scope: target.Scope, Principal: alice}, target)
	require.False(t, ok)
	_, ok = s.CredentialFor(Execution{Kind: Interactive, Scope: target.Scope, Principal: bob}, target)
	require.False(t, ok)
	require.False(t, s.HasCredential(bob, target))

	stateBytes, err := os.ReadFile(filepath.Join(dir, "state.json"))
	require.NoError(t, err)
	require.NotContains(t, string(stateBytes), secret)
	privateBytes, err := os.ReadFile(filepath.Join(dir, "private-credentials.json"))
	require.NoError(t, err)
	require.Contains(t, string(privateBytes), secret)
	fi, err := os.Stat(filepath.Join(dir, "private-credentials.json"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), fi.Mode().Perm())
	s2, err := Open(dir)
	require.NoError(t, err)
	require.True(t, s2.ClassifyReply("install", "workspace", "dm-alice", "prompt-a"))
	c, ok = s2.CredentialFor(e, target)
	require.True(t, ok)
	require.Equal(t, secret, c.Password)
}

func TestStaleCompletionAndExpiredTombstones(t *testing.T) {
	s, err := Open(t.TempDir())
	require.NoError(t, err)
	p := testPrincipal("alice")
	old, _, err := s.Select(testScope("thread"), "clickhouse", "https://old.example")
	require.NoError(t, err)
	pr := Prompt{Principal: p, DMChannelID: "dm", DMRootID: "prompt", Target: old, Stage: "password", Username: "alice", ExpiresAt: time.Now().Add(time.Hour)}
	require.NoError(t, s.PutPrompt(pr))
	started := make(chan struct{})
	resume := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- s.CompletePassword(context.Background(), p, "dm", "prompt", "fake-secret", func(_ context.Context, _ Target, _ Credential) error { close(started); <-resume; return nil })
	}()
	<-started
	next, _, err := s.Select(testScope("thread"), "clickhouse", "https://next.example")
	require.NoError(t, err)
	close(resume)
	require.ErrorIs(t, <-done, ErrStale)
	require.False(t, s.HasCredential(p, next))
	require.True(t, s.ClassifyReply("install", "workspace", "dm", "prompt"))
	require.ErrorIs(t, s.CompletePassword(context.Background(), p, "dm", "prompt", "fake-secret", nil), ErrPrompt)

	expired := Prompt{Principal: p, DMChannelID: "dm", DMRootID: "expired", Target: next, Stage: "username", ExpiresAt: time.Now().Add(-time.Second)}
	require.NoError(t, s.PutPrompt(expired))
	require.True(t, s.ClassifyReply("install", "workspace", "dm", "expired"))
	_, ok := s.PromptFor(p, "dm", "expired")
	require.False(t, ok)
	require.ErrorIs(t, s.FinishPrompt(p, "dm", "expired"), ErrPrompt)
}

func TestValidationFailureNeverLeaksSecret(t *testing.T) {
	s, err := Open(t.TempDir())
	require.NoError(t, err)
	p := testPrincipal("alice")
	target, _, err := s.Select(testScope("thread"), "clickhouse", "https://cluster.example")
	require.NoError(t, err)
	require.NoError(t, s.PutPrompt(Prompt{Principal: p, DMChannelID: "dm", DMRootID: "prompt", Target: target, Stage: "password", Username: "alice", ExpiresAt: time.Now().Add(time.Hour)}))
	secret := "fake-secret-cannot-appear"
	err = s.CompletePassword(context.Background(), p, "dm", "prompt", secret, func(context.Context, Target, Credential) error { return errors.New("driver said " + secret) })
	require.ErrorIs(t, err, ErrValidation)
	require.False(t, strings.Contains(err.Error(), secret))
	require.False(t, s.HasCredential(p, target))
}

func TestMissingValidatorConsumesPromptAndRotationChangesVersion(t *testing.T) {
	s, err := Open(t.TempDir())
	require.NoError(t, err)
	p := testPrincipal("alice")
	target, _, err := s.Select(testScope("thread"), "clickhouse", "https://cluster.example")
	require.NoError(t, err)
	prompt := func(root string) Prompt {
		return Prompt{Principal: p, DMChannelID: "dm", DMRootID: root, Target: target, Stage: "password", Username: "alice", ExpiresAt: time.Now().Add(time.Hour)}
	}
	require.NoError(t, s.PutPrompt(prompt("first")))
	require.ErrorIs(t, s.CompletePassword(context.Background(), p, "dm", "first", "fake-one", nil), ErrValidation)
	require.ErrorIs(t, s.CompletePassword(context.Background(), p, "dm", "first", "fake-one", nil), ErrPrompt)
	require.False(t, s.HasCredential(p, target))
	validate := func(context.Context, Target, Credential) error { return nil }
	require.NoError(t, s.PutPrompt(prompt("second")))
	require.NoError(t, s.CompletePassword(context.Background(), p, "dm", "second", "fake-two", validate))
	e := Execution{Kind: Interactive, Scope: target.Scope, Principal: p}
	first, ok := s.CredentialFor(e, target)
	require.True(t, ok)
	require.NoError(t, s.PutPrompt(prompt("third")))
	require.NoError(t, s.CompletePassword(context.Background(), p, "dm", "third", "fake-three", validate))
	second, ok := s.CredentialFor(e, target)
	require.True(t, ok)
	require.NotEqual(t, first.Version, second.Version)
	require.Equal(t, "fake-three", second.Password)
}

func TestReplacementDisconnectAndRestartScrubOldCredentials(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	require.NoError(t, err)
	p := testPrincipal("alice")
	scope := testScope("thread")
	privatePath := filepath.Join(dir, "private-credentials.json")
	var previous []byte
	for i, endpoint := range []string{"https://one.example", "https://two.example", "https://three.example"} {
		target, _, err := s.Select(scope, "clickhouse", endpoint)
		require.NoError(t, err)
		if i > 0 {
			b, err := os.ReadFile(privatePath)
			require.NoError(t, err)
			require.NotContains(t, string(b), "fake-old-secret")
		}
		root := fmt.Sprintf("prompt-%d", i)
		require.NoError(t, s.PutPrompt(Prompt{Principal: p, DMChannelID: "dm", DMRootID: root, Target: target, Stage: "password", Username: "alice", ExpiresAt: time.Now().Add(time.Hour)}))
		secret := fmt.Sprintf("fake-old-secret-%d", i)
		require.NoError(t, s.CompletePassword(context.Background(), p, "dm", root, secret, func(context.Context, Target, Credential) error { return nil }))
		if i == 0 {
			previous, err = os.ReadFile(privatePath)
			require.NoError(t, err)
		}
	}
	require.NoError(t, s.Disconnect(scope))
	b, err := os.ReadFile(privatePath)
	require.NoError(t, err)
	require.NotContains(t, string(b), "fake-old-secret")
	// Simulate a crash after state invalidation but before private-file cleanup.
	require.NoError(t, os.WriteFile(privatePath, previous, 0o600))
	_, err = Open(dir)
	require.NoError(t, err)
	b, err = os.ReadFile(privatePath)
	require.NoError(t, err)
	require.NotContains(t, string(b), "fake-old-secret")
}
