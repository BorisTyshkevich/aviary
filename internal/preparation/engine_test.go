package preparation

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/connections"
)

func helperConfig(mode string) config.BeforeTurnHookConfig {
	return config.BeforeTurnHookConfig{Argv: []string{os.Args[0], "-test.run=TestPreparationHelper", "--", mode}, Timeout: "3s", AllowCredential: true}
}
func testInput(user, thread string) Input {
	s := connections.Scope{AgentID: "agent", InstallationID: "install", WorkspaceID: "workspace", ChannelID: "channel", RootThreadID: thread}
	p := connections.Principal{InstallationID: "install", WorkspaceID: "workspace", UserID: user}
	return Input{Execution: connections.Execution{Kind: connections.Interactive, Scope: s, Principal: p}, Target: connections.Target{Scope: s, Transport: "clickhouse", Endpoint: "https://fake.example", Generation: "generation"}, CredentialVersion: "credential-version"}
}

// TestPreparationHelper is a fake external executable, using the Go test binary
// so tests do not depend on a shell or another installed runtime.
func TestPreparationHelper(_ *testing.T) {
	mode := ""
	modeIndex := 0
	for i, arg := range os.Args {
		if arg == "--" && i+1 < len(os.Args) {
			mode = os.Args[i+1]
			modeIndex = i + 1
			break
		}
	}
	if mode == "" {
		return
	}
	if mode == "delayed-marker" {
		time.Sleep(1500 * time.Millisecond)
		_ = os.WriteFile(os.Args[modeIndex+1], []byte("child survived"), 0o600)
		os.Exit(0)
	}
	var req request
	if err := json.NewDecoder(os.Stdin).Decode(&req); err != nil {
		os.Exit(2)
	}
	if req.Version != ProtocolVersion {
		os.Exit(2)
	}
	switch mode {
	case "success":
		if req.Credential == nil || req.Credential.Password != "fake-secret" || os.Getenv("UNRELATED_DEPLOYMENT_SECRET") != "" {
			os.Exit(2)
		}
		if err := os.WriteFile(filepath.Join(req.ArtifactDir, "data.txt"), []byte("private evidence"), 0o600); err != nil {
			os.Exit(2)
		}
		_, _ = fmt.Fprint(os.Stdout, `{"status":"complete","summary":"evidence ready","artifacts":["data.txt"],"observed_at":"2026-09-27T00:00:00Z","producer_revision":"fake-v1"}`)
	case "no-credential":
		if req.Credential != nil {
			os.Exit(2)
		}
		_, _ = fmt.Fprint(os.Stdout, `{"status":"complete","summary":"no target","artifacts":[],"observed_at":"2026-09-27T00:00:00Z","producer_revision":"fake-v1"}`)
	case "malformed":
		_, _ = fmt.Fprint(os.Stdout, "this is not JSON")
	case "oversized":
		_, _ = fmt.Fprint(os.Stdout, strings.Repeat("x", maxOutput+1))
	case "traversal":
		_, _ = fmt.Fprint(os.Stdout, `{"status":"complete","summary":"bad","artifacts":["../secret"],"observed_at":"2026-09-27T00:00:00Z"}`)
	case "symlink":
		if err := os.Symlink("../outside", filepath.Join(req.ArtifactDir, "link")); err != nil {
			os.Exit(2)
		}
		_, _ = fmt.Fprint(os.Stdout, `{"status":"complete","summary":"bad","artifacts":["link"],"observed_at":"2026-09-27T00:00:00Z"}`)
	case "secret-error":
		_, _ = fmt.Fprint(os.Stderr, "fake-secret in stderr")
		os.Exit(2)
	case "secret-summary":
		_, _ = fmt.Fprint(os.Stdout, `{"status":"complete","summary":"fake-secret","artifacts":[],"observed_at":"2026-09-27T00:00:00Z"}`)
	case "secret-file":
		_ = os.WriteFile(filepath.Join(req.ArtifactDir, "secret.txt"), []byte("fake-secret"), 0o600)
		_, _ = fmt.Fprint(os.Stdout, `{"status":"complete","summary":"ready","artifacts":["secret.txt"],"observed_at":"2026-09-27T00:00:00Z"}`)
	case "sleep":
		time.Sleep(10 * time.Second)
	case "spawn-child":
		child := exec.Command(os.Args[0], "-test.run=TestPreparationHelper", "--", "delayed-marker", os.Args[modeIndex+1])
		if child.Start() != nil {
			os.Exit(2)
		}
		time.Sleep(10 * time.Second)
	case "spawn-child-success":
		child := exec.Command(os.Args[0], "-test.run=TestPreparationHelper", "--", "delayed-marker", os.Args[modeIndex+1])
		if child.Start() != nil {
			os.Exit(2)
		}
		_, _ = fmt.Fprint(os.Stdout, `{"status":"complete","summary":"ready","artifacts":[],"observed_at":"2026-09-27T00:00:00Z","producer_revision":"fake-v1"}`)
	default:
		os.Exit(2)
	}
	os.Exit(0)
}

func TestRunPublishesPrivateArtifactsAndConfinesReads(t *testing.T) {
	e, err := Open(t.TempDir())
	require.NoError(t, err)
	in := testInput("alice", "thread")
	secret := &connections.Credential{Username: "alice", Password: "fake-secret", Version: in.CredentialVersion}
	t.Setenv("UNRELATED_DEPLOYMENT_SECRET", "fake-environment-secret")
	r, err := e.Run(context.Background(), helperConfig("success"), in, secret)
	require.NoError(t, err)
	require.Equal(t, "complete", r.Status)
	require.Equal(t, []string{"data.txt"}, r.Artifacts)
	b, err := e.Read(context.Background(), in, r.RunID, "data.txt", 1024)
	require.NoError(t, err)
	require.Equal(t, "private evidence", string(b))
	other := testInput("bob", "thread")
	_, err = e.Read(context.Background(), other, r.RunID, "data.txt", 1024)
	require.ErrorIs(t, err, ErrForbidden)
	other = testInput("alice", "another-thread")
	_, err = e.Read(context.Background(), other, r.RunID, "data.txt", 1024)
	require.ErrorIs(t, err, ErrForbidden)
	other = in
	other.Target.Generation = "different"
	_, err = e.Read(context.Background(), other, r.RunID, "data.txt", 1024)
	require.ErrorIs(t, err, ErrForbidden)
	other = in
	other.CredentialVersion = "rotated"
	_, err = e.Read(context.Background(), other, r.RunID, "data.txt", 1024)
	require.ErrorIs(t, err, ErrForbidden)
	_, err = e.Read(context.Background(), in, r.RunID, "../data.txt", 1024)
	require.ErrorIs(t, err, ErrForbidden)
	_, err = e.Read(context.Background(), in, r.RunID, "data.txt", 1)
	require.ErrorIs(t, err, ErrForbidden)
}

func TestRunRejectsMalformedOversizedAndUnsafeArtifacts(t *testing.T) {
	e, err := Open(t.TempDir())
	require.NoError(t, err)
	in := testInput("alice", "thread")
	for _, mode := range []string{"malformed", "oversized", "traversal", "symlink", "secret-error"} {
		t.Run(mode, func(t *testing.T) {
			_, err := e.Run(context.Background(), helperConfig(mode), in, nil)
			require.ErrorIs(t, err, ErrUnavailable)
			require.NotContains(t, err.Error(), "fake-secret")
		})
	}
}

func TestRunRejectsCredentialEcho(t *testing.T) {
	e, err := Open(t.TempDir())
	require.NoError(t, err)
	in := testInput("alice", "thread")
	c := &connections.Credential{Username: "alice", Password: "fake-secret", Version: in.CredentialVersion}
	for _, mode := range []string{"secret-summary", "secret-file"} {
		_, err = e.Run(context.Background(), helperConfig(mode), in, c)
		require.ErrorIs(t, err, ErrUnavailable)
	}
}

func TestRunCredentialGrantAndCancellation(t *testing.T) {
	e, err := Open(t.TempDir())
	require.NoError(t, err)
	in := testInput("alice", "thread")
	secret := &connections.Credential{Username: "alice", Password: "fake-secret", Version: in.CredentialVersion}
	cfg := helperConfig("success")
	cfg.AllowCredential = false
	_, err = e.Run(context.Background(), cfg, in, secret)
	require.ErrorIs(t, err, ErrForbidden)
	scheduled := in
	scheduled.Execution.Kind = connections.Scheduled
	scheduled.Execution.Principal = connections.Principal{}
	scheduled.CredentialVersion = ""
	_, err = e.Run(context.Background(), helperConfig("success"), scheduled, secret)
	require.ErrorIs(t, err, ErrForbidden)
	scheduled.Target = connections.Target{}
	result, err := e.Run(context.Background(), helperConfig("no-credential"), scheduled, nil)
	require.NoError(t, err)
	require.Equal(t, "complete", result.Status)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = e.Run(ctx, helperConfig("sleep"), in, nil)
	require.ErrorIs(t, err, ErrUnavailable)
	require.Less(t, time.Since(start), time.Second)
}

func TestCancellationKillsDescendants(t *testing.T) {
	e, err := Open(t.TempDir())
	require.NoError(t, err)
	marker := filepath.Join(t.TempDir(), "child-survived")
	cfg := helperConfig("spawn-child")
	cfg.Argv = append(cfg.Argv, marker)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = e.Run(ctx, cfg, testInput("alice", "thread"), nil)
	require.ErrorIs(t, err, ErrUnavailable)
	time.Sleep(1700 * time.Millisecond)
	_, err = os.Stat(marker)
	require.True(t, os.IsNotExist(err), "child process survived cancellation")
}

func TestSuccessfulParentAlsoKillsDescendants(t *testing.T) {
	e, err := Open(t.TempDir())
	require.NoError(t, err)
	marker := filepath.Join(t.TempDir(), "child-survived")
	cfg := helperConfig("spawn-child-success")
	cfg.Argv = append(cfg.Argv, marker)
	_, err = e.Run(context.Background(), cfg, testInput("alice", "thread"), nil)
	require.NoError(t, err)
	time.Sleep(1700 * time.Millisecond)
	_, err = os.Stat(marker)
	require.True(t, os.IsNotExist(err), "child process survived leader exit")
}

func TestRestartRemovesInterruptedStaging(t *testing.T) {
	root := t.TempDir()
	stale := filepath.Join(root, ".staging", "interrupted", "partial.txt")
	require.NoError(t, os.MkdirAll(filepath.Dir(stale), 0o700))
	require.NoError(t, os.WriteFile(stale, []byte("partial"), 0o600))
	_, err := Open(root)
	require.NoError(t, err)
	_, err = os.Stat(stale)
	require.True(t, os.IsNotExist(err))
}

func TestExpiredArtifactDeniedBeforeNextRunAndPrunedOnOpen(t *testing.T) {
	root := t.TempDir()
	e, err := Open(root)
	require.NoError(t, err)
	in := testInput("alice", "thread")
	result, err := e.Run(context.Background(), helperConfig("success"), in, &connections.Credential{Username: "alice", Password: "fake-secret", Version: in.CredentialVersion})
	require.NoError(t, err)
	runDir := filepath.Join(root, identityKey(in), result.RunID)
	old := time.Now().Add(-retention - time.Hour)
	require.NoError(t, os.Chtimes(runDir, old, old))
	_, err = e.Read(context.Background(), in, result.RunID, "data.txt", 1024)
	require.ErrorIs(t, err, ErrForbidden)
	_, err = Open(root)
	require.NoError(t, err)
	_, err = os.Stat(runDir)
	require.True(t, os.IsNotExist(err))
}

func TestConcurrentPublicationEnforcesRetainedLimit(t *testing.T) {
	e, err := Open(t.TempDir())
	require.NoError(t, err)
	in := testInput("alice", "one")
	first, err := e.Run(context.Background(), helperConfig("success"), in, &connections.Credential{Username: "alice", Password: "fake-secret", Version: in.CredentialVersion})
	require.NoError(t, err)
	firstDir := filepath.Join(e.root, identityKey(in), first.RunID)
	e.retainedLimit = 2*stageBytes(firstDir) + 1
	old := time.Now().Add(-time.Hour)
	require.NoError(t, os.Chtimes(firstDir, old, old))
	result := make(chan error, 2)
	for _, thread := range []string{"two", "three"} {
		go func(thread string) {
			input := testInput("alice", thread)
			_, err := e.Run(context.Background(), helperConfig("success"), input, &connections.Credential{Username: "alice", Password: "fake-secret", Version: input.CredentialVersion})
			result <- err
		}(thread)
	}
	a, b := <-result, <-result
	require.NoError(t, a)
	require.NoError(t, b)
	require.LessOrEqual(t, stageBytes(e.root), e.retainedLimit)
	_, err = e.Read(context.Background(), in, first.RunID, "data.txt", 1024)
	require.ErrorIs(t, err, ErrForbidden)
}

func TestRunCountEvictsOldestAndOversizedRunFails(t *testing.T) {
	e, err := Open(t.TempDir())
	require.NoError(t, err)
	in := testInput("alice", "one")
	c := &connections.Credential{Username: "alice", Password: "fake-secret", Version: in.CredentialVersion}
	first, err := e.Run(context.Background(), helperConfig("success"), in, c)
	require.NoError(t, err)
	e.runLimit = 1
	next := testInput("alice", "two")
	_, err = e.Run(context.Background(), helperConfig("success"), next, c)
	require.NoError(t, err)
	_, err = e.Read(context.Background(), in, first.RunID, "data.txt", 1024)
	require.ErrorIs(t, err, ErrForbidden)
	e.retainedLimit = 1
	_, err = e.Run(context.Background(), helperConfig("success"), testInput("alice", "three"), c)
	require.ErrorIs(t, err, ErrUnavailable)
}
