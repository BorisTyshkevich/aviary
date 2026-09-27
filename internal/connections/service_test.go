package connections

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

	expired := Prompt{Principal: p, DMChannelID: "dm", DMRootID: "expired", Target: next, Stage: "password", ExpiresAt: time.Now().Add(-time.Second)}
	require.NoError(t, s.PutPrompt(expired))
	require.True(t, s.ClassifyReply("install", "workspace", "dm", "expired"))
	_, ok := s.PromptFor(p, "dm", "expired")
	require.False(t, ok)
	require.ErrorIs(t, s.FinishPrompt(p, "dm", "expired"), ErrPrompt)
}

func TestActivePromptForExactCurrentPrincipalAndTarget(t *testing.T) {
	s, err := Open(t.TempDir())
	require.NoError(t, err)
	alice := testPrincipal("alice")
	bob := testPrincipal("bob")
	target, _, err := s.Select(testScope("thread"), "clickhouse", "https://cluster.example")
	require.NoError(t, err)
	other, _, err := s.Select(testScope("other-thread"), "clickhouse", "https://other.example")
	require.NoError(t, err)
	newPrompt := func(root string, principal Principal, target Target, expires time.Time) Prompt {
		return Prompt{Principal: principal, DMChannelID: "dm", DMRootID: root, Target: target, Stage: "password", Username: " exact username ", ExpiresAt: expires}
	}
	active := newPrompt("active", alice, target, time.Now().Add(time.Hour))
	require.NoError(t, s.PutPrompt(active))
	require.NoError(t, s.PutPrompt(newPrompt("expired", alice, target, time.Now().Add(-time.Second))))
	require.NoError(t, s.PutPrompt(newPrompt("bob", bob, target, time.Now().Add(time.Hour))))
	require.NoError(t, s.PutPrompt(newPrompt("other", alice, other, time.Now().Add(time.Hour))))

	got, ok := s.ActivePromptFor(alice, target)
	require.True(t, ok)
	require.Equal(t, active, got)
	require.True(t, s.HasActivePrompt(alice, target))
	_, ok = s.ActivePromptFor(alice, Target{})
	require.False(t, ok)
	_, ok = s.PromptFor(alice, "dm", "expired")
	require.False(t, ok)
	require.True(t, s.ClassifyReply("install", "workspace", "dm", "expired"))

	require.NoError(t, s.FinishPrompt(alice, "dm", "active"))
	_, ok = s.ActivePromptFor(alice, target)
	require.False(t, ok, "completed and expired prompts must not be reused")
	require.False(t, s.HasActivePrompt(alice, target))
	require.True(t, s.ClassifyReply("install", "workspace", "dm", "active"))
	got, ok = s.ActivePromptFor(bob, target)
	require.True(t, ok)
	require.Equal(t, bob, got.Principal)
	got, ok = s.ActivePromptFor(alice, other)
	require.True(t, ok)
	require.Equal(t, other, got.Target)

	replacement, _, err := s.Select(target.Scope, "clickhouse", "https://replacement.example")
	require.NoError(t, err)
	_, ok = s.ActivePromptFor(bob, target)
	require.False(t, ok, "a replaced generation invalidates old prompts")
	_, ok = s.ActivePromptFor(bob, replacement)
	require.False(t, ok, "an old prompt must not migrate to a new generation")
	require.True(t, s.ClassifyReply("install", "workspace", "dm", "bob"))
}

func TestActivePromptForWhilePasswordValidationRuns(t *testing.T) {
	for _, tc := range []struct {
		name       string
		validation error
	}{
		{name: "success"},
		{name: "failure", validation: errors.New("fake validation failure")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := Open(t.TempDir())
			require.NoError(t, err)
			p := testPrincipal("alice")
			target, _, err := s.Select(testScope("thread"), "clickhouse", "https://cluster.example")
			require.NoError(t, err)
			prompt := Prompt{Principal: p, DMChannelID: "dm", DMRootID: "password-prompt", Target: target, Stage: "password", Username: "alice", ExpiresAt: time.Now().Add(time.Hour)}
			require.NoError(t, s.PutPrompt(prompt))
			started := make(chan struct{})
			release := make(chan struct{})
			releaseValidation := sync.OnceFunc(func() { close(release) })
			t.Cleanup(releaseValidation)
			done := make(chan error, 1)
			go func() {
				done <- s.CompletePassword(context.Background(), p, "dm", "password-prompt", "fake-password", func(context.Context, Target, Credential) error {
					close(started)
					<-release
					return tc.validation
				})
			}()
			select {
			case <-started: // Prompt has already been durably marked Completed.
			case err := <-done:
				t.Fatalf("validation did not start: %v", err)
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for validation to start")
			}
			active, ok := s.ActivePromptFor(p, target)
			require.True(t, ok)
			require.Equal(t, prompt.DMRootID, active.DMRootID)
			require.True(t, active.Completed)
			require.True(t, s.HasActivePrompt(p, target))
			_, ok = s.ActivePromptFor(testPrincipal("bob"), target)
			require.False(t, ok)
			releaseValidation()
			select {
			case err = <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for validation to finish")
			}
			if tc.validation == nil {
				require.NoError(t, err)
				require.True(t, s.HasCredential(p, target))
			} else {
				require.ErrorIs(t, err, ErrValidation)
				require.False(t, s.HasCredential(p, target))
			}
			_, ok = s.ActivePromptFor(p, target)
			require.False(t, ok)
			require.False(t, s.HasActivePrompt(p, target))
		})
	}
}

func TestPutPromptRejectsUsernameStageButRetainsHistoricalClassification(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	require.NoError(t, err)
	p := testPrincipal("alice")
	target, _, err := s.Select(testScope("thread"), "clickhouse", "https://cluster.example")
	require.NoError(t, err)
	old := Prompt{Principal: p, DMChannelID: "dm", DMRootID: "old-username", Target: target, Stage: "username", ExpiresAt: time.Now().Add(time.Hour)}
	require.ErrorIs(t, s.PutPrompt(old), ErrInvalid)
	// A prompt written before the password-only change still classifies late
	// replies after restart, but cannot resume a username collection stage.
	s.state.Prompts[promptKey("install", "workspace", "dm", "old-username")] = old
	require.NoError(t, s.saveState())
	restarted, err := Open(dir)
	require.NoError(t, err)
	require.True(t, restarted.ClassifyReply("install", "workspace", "dm", "old-username"))
	_, ok := restarted.PromptFor(p, "dm", "old-username")
	require.False(t, ok)
	_, ok = restarted.ActivePromptFor(p, target)
	require.False(t, ok)
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
