// Package preparation runs trusted before-turn executables and confines their
// published, private artifacts to the caller's execution identity.
package preparation

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/connections"
)

// ProtocolVersion is the version of the stdin/stdout JSON protocol.
const ProtocolVersion = 1

const (
	maxOutput   = 64 << 10
	maxInput    = 64 << 10
	maxFiles    = 32
	maxFile     = 1 << 20
	maxTotal    = 8 << 20
	maxSummary  = 4096
	maxRevision = 256
	maxRetained = 64 << 20
	maxRuns     = 256
	retention   = 7 * 24 * time.Hour
)

var (
	// ErrUnavailable is a sanitized hook failure suitable for caller policy.
	ErrUnavailable = errors.New("preparation unavailable")
	// ErrForbidden means the artifact or credential request exceeds its scope.
	ErrForbidden = errors.New("preparation access denied")
)

// Input is trusted, non-secret turn identity and target metadata.
type Input struct {
	Execution         connections.Execution `json:"execution"`
	Target            connections.Target    `json:"target"`
	CredentialVersion string                `json:"credential_version,omitempty"`
}

// Result is bounded evidence metadata, never execution instructions.
type Result struct {
	Status           string    `json:"status"`
	Summary          string    `json:"summary"`
	Artifacts        []string  `json:"artifacts"`
	RunID            string    `json:"run_id,omitempty"`
	ObservedAt       time.Time `json:"observed_at"`
	ProducerRevision string    `json:"producer_revision"`
}

type wireResult struct {
	Status           string    `json:"status"`
	Summary          string    `json:"summary"`
	Artifacts        []string  `json:"artifacts"`
	ObservedAt       time.Time `json:"observed_at"`
	ProducerRevision string    `json:"producer_revision"`
}

type privateCredential struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Version  string `json:"version"`
}
type request struct {
	Version int `json:"version"`
	Input
	ArtifactDir string             `json:"artifact_dir"`
	Credential  *privateCredential `json:"credential,omitempty"`
}

// Engine holds private artifact storage and coordinates runs per exact scope.
type Engine struct {
	root            string
	mu              sync.Mutex
	running         map[string]bool
	retainedLimit   int64
	runLimit        int
	retentionPeriod time.Duration
}

// Open creates a private artifact root for one process.
func Open(root string) (*Engine, error) {
	if root == "" {
		return nil, ErrUnavailable
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, ErrUnavailable
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return nil, ErrUnavailable
	}
	// The root is single-writer. Previous staging trees are interrupted runs.
	if err := os.RemoveAll(filepath.Join(root, ".staging")); err != nil {
		return nil, ErrUnavailable
	}
	e := &Engine{root: root, running: map[string]bool{}, retainedLimit: maxRetained, runLimit: maxRuns, retentionPeriod: retention}
	if err := e.prune(0, 0); err != nil {
		return nil, ErrUnavailable
	}
	return e, nil
}

func identityKey(in Input) string {
	b, _ := json.Marshal(in)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func runLockKey(in Input) string { in.CredentialVersion = ""; return identityKey(in) }
func randomID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func validateInput(in Input) bool {
	e := in.Execution
	if e.Scope.AgentID == "" {
		return false
	}
	if e.Kind == connections.Interactive {
		if !e.Personal() {
			return false
		}
		if in.Target.Generation != "" && in.Target.Scope != e.Scope {
			return false
		}
		return true
	}
	return (e.Kind == connections.Scheduled || e.Kind == connections.Control) && e.Principal == (connections.Principal{}) && in.CredentialVersion == ""
}

// Run executes the administrator command once for this call. The caller owns
// optional/required failure policy and holds the target lease during Run.
func (e *Engine) Run(ctx context.Context, cfg config.BeforeTurnHookConfig, in Input, credential *connections.Credential) (Result, error) {
	if !validateInput(in) || len(cfg.Argv) == 0 || cfg.Argv[0] == "" {
		return Result{}, ErrUnavailable
	}
	if credential != nil && (!cfg.AllowCredential || !in.Execution.Personal() || in.Target.Generation == "" || credential.Version == "" || credential.Version != in.CredentialVersion) {
		return Result{}, ErrForbidden
	}
	if len(cfg.Argv) > 32 {
		return Result{}, ErrUnavailable
	}
	for _, arg := range cfg.Argv {
		if len(arg) > 4096 {
			return Result{}, ErrUnavailable
		}
	}
	duration := 15 * time.Second
	if cfg.Timeout != "" {
		parsed, err := time.ParseDuration(cfg.Timeout)
		if err != nil || parsed <= 0 || parsed > 2*time.Minute {
			return Result{}, ErrUnavailable
		}
		duration = parsed
	}
	if cfg.OnError != "" && cfg.OnError != "continue" && cfg.OnError != "stop" {
		return Result{}, ErrUnavailable
	}
	k := runLockKey(in)
	e.mu.Lock()
	if e.running[k] {
		e.mu.Unlock()
		return Result{}, ErrUnavailable
	}
	e.running[k] = true
	e.mu.Unlock()
	defer func() { e.mu.Lock(); delete(e.running, k); e.mu.Unlock() }()
	id, err := randomID()
	if err != nil {
		return Result{}, ErrUnavailable
	}
	stageRoot := filepath.Join(e.root, ".staging")
	if err := os.MkdirAll(stageRoot, 0o700); err != nil {
		return Result{}, ErrUnavailable
	}
	stage := filepath.Join(stageRoot, id)
	if err := os.Mkdir(stage, 0o700); err != nil {
		return Result{}, ErrUnavailable
	}
	defer func() { _ = os.RemoveAll(stage) }()
	req := request{Version: ProtocolVersion, Input: in, ArtifactDir: stage}
	if credential != nil {
		req.Credential = &privateCredential{Username: credential.Username, Password: credential.Password, Version: credential.Version}
	}
	payload, err := json.Marshal(req)
	if err != nil || len(payload) > maxInput {
		return Result{}, ErrUnavailable
	}
	runCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	cmd := exec.Command(cfg.Argv[0], cfg.Argv[1:]...)
	cmd.Stdin = bytes.NewReader(append(payload, '\n'))
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "LANG=C", "LC_ALL=C"}
	cmd.Dir = stage
	out := &limitWriter{limit: maxOutput}
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		return Result{}, ErrUnavailable
	}
	cmd.Stdout = stdoutWriter
	cmd.Stderr = nil
	if err := startProcess(cmd); err != nil {
		_ = stdoutReader.Close()
		_ = stdoutWriter.Close()
		return Result{}, ErrUnavailable
	}
	_ = stdoutWriter.Close()
	copied := make(chan struct{})
	go func() { _, _ = io.Copy(out, stdoutReader); close(copied) }()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err = <-done:
		// The leader can exit while descendants continue writing stage files.
		if killProcessTree(cmd) != nil {
			return Result{}, ErrUnavailable
		}
	case <-runCtx.Done():
		_ = killProcessTree(cmd)
		<-done
		_ = stdoutReader.Close()
		<-copied
		return Result{}, ErrUnavailable
	}
	select {
	case <-copied:
	case <-time.After(200 * time.Millisecond):
		_ = stdoutReader.Close()
		<-copied
	}
	_ = stdoutReader.Close()
	if err != nil || out.overflow {
		return Result{}, ErrUnavailable
	}
	if credential != nil && credential.Password != "" && bytes.Contains(out.Bytes(), []byte(credential.Password)) {
		return Result{}, ErrUnavailable
	}
	var wire wireResult
	decoder := json.NewDecoder(bytes.NewReader(out.Bytes()))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&wire) != nil || decoder.Decode(new(any)) != io.EOF {
		return Result{}, ErrUnavailable
	}
	if !validResult(wire) {
		return Result{}, ErrUnavailable
	}
	if err := validateFiles(stage, wire.Artifacts); err != nil {
		return Result{}, ErrUnavailable
	}
	if credential != nil && credential.Password != "" {
		for _, name := range wire.Artifacts {
			content, err := os.ReadFile(filepath.Join(stage, name))
			if err != nil || bytes.Contains(content, []byte(credential.Password)) {
				return Result{}, ErrUnavailable
			}
		}
	}
	manifest, err := json.Marshal(wire)
	if err != nil || len(manifest) > maxOutput {
		return Result{}, ErrUnavailable
	}
	if err := os.WriteFile(filepath.Join(stage, ".manifest.json"), manifest, 0o600); err != nil {
		return Result{}, ErrUnavailable
	}
	final := filepath.Join(e.root, identityKey(in), id)
	e.mu.Lock()
	defer e.mu.Unlock()
	incoming, err := dirBytes(stage)
	if err != nil {
		return Result{}, ErrUnavailable
	}
	if err := e.prune(incoming, 1); err != nil {
		return Result{}, ErrUnavailable
	}
	if err := os.MkdirAll(filepath.Dir(final), 0o700); err != nil {
		return Result{}, ErrUnavailable
	}
	if err := os.Rename(stage, final); err != nil {
		return Result{}, ErrUnavailable
	}
	return Result{Status: wire.Status, Summary: wire.Summary, Artifacts: wire.Artifacts, RunID: id, ObservedAt: wire.ObservedAt, ProducerRevision: wire.ProducerRevision}, nil
}

type limitWriter struct {
	bytes.Buffer
	limit    int
	overflow bool
}

func (w *limitWriter) Write(p []byte) (int, error) {
	if w.Len()+len(p) > w.limit {
		w.overflow = true
		if remaining := w.limit - w.Len(); remaining > 0 {
			_, _ = w.Buffer.Write(p[:remaining])
		}
		return len(p), nil
	}
	return w.Buffer.Write(p)
}

func validResult(r wireResult) bool {
	if r.Status != "complete" && r.Status != "partial" && r.Status != "unavailable" && r.Status != "denied" && r.Status != "timed_out" {
		return false
	}
	if len(r.Summary) > maxSummary || r.ProducerRevision == "" || len(r.ProducerRevision) > maxRevision || r.ObservedAt.IsZero() || len(r.Artifacts) > maxFiles {
		return false
	}
	return true
}

func cleanRelative(name string) bool {
	if name == "" || filepath.IsAbs(name) || filepath.Clean(name) != name || name == "." || name == ".." || strings.HasPrefix(name, ".."+string(filepath.Separator)) {
		return false
	}
	for _, part := range strings.Split(name, string(filepath.Separator)) {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}

func validateFiles(root string, names []string) error {
	listed := make(map[string]bool, len(names))
	var total int64
	for _, name := range names {
		if !cleanRelative(name) || name == ".manifest.json" || listed[name] {
			return ErrUnavailable
		}
		listed[name] = true
		path := root
		for _, part := range strings.Split(name, string(filepath.Separator)) {
			path = filepath.Join(path, part)
			info, err := os.Lstat(path)
			if err != nil || info.Mode()&os.ModeSymlink != 0 {
				return ErrUnavailable
			}
		}
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Size() > maxFile {
			return ErrUnavailable
		}
		total += info.Size()
		if total > maxTotal {
			return ErrUnavailable
		}
	}
	count := 0
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return ErrUnavailable
		}
		if path == root {
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			return ErrUnavailable
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return ErrUnavailable
		}
		rel, err := filepath.Rel(root, path)
		if err != nil || !listed[rel] {
			return ErrUnavailable
		}
		count++
		return nil
	})
	if err != nil || count != len(names) {
		return ErrUnavailable
	}
	return nil
}

// Read returns at most maxBytes from a published artifact in the exact private scope.
func (e *Engine) Read(_ context.Context, in Input, runID, relative string, maxBytes int64) ([]byte, error) {
	if !validateInput(in) || !cleanRelative(relative) || len(runID) != 32 || maxBytes <= 0 || maxBytes > maxFile {
		return nil, ErrForbidden
	}
	for _, c := range runID {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return nil, ErrForbidden
		}
	}
	root := filepath.Join(e.root, identityKey(in), runID)
	for _, ancestor := range []string{e.root, filepath.Join(e.root, identityKey(in)), root} {
		info, err := os.Lstat(ancestor)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, ErrForbidden
		}
	}
	runInfo, err := os.Stat(root)
	if err != nil || time.Since(runInfo.ModTime()) > e.retentionPeriod {
		return nil, ErrForbidden
	}
	rootHandle, err := os.OpenRoot(root)
	if err != nil {
		return nil, ErrForbidden
	}
	defer func() { _ = rootHandle.Close() }()
	manifestFile, err := rootHandle.Open(".manifest.json")
	if err != nil {
		return nil, ErrForbidden
	}
	manifestBytes, err := io.ReadAll(io.LimitReader(manifestFile, maxOutput+1))
	_ = manifestFile.Close()
	if err != nil || len(manifestBytes) > maxOutput {
		return nil, ErrForbidden
	}
	var manifest wireResult
	if json.Unmarshal(manifestBytes, &manifest) != nil {
		return nil, ErrForbidden
	}
	listed := false
	for _, name := range manifest.Artifacts {
		if name == relative {
			listed = true
			break
		}
	}
	if !listed {
		return nil, ErrForbidden
	}
	path := root
	for _, part := range strings.Split(relative, string(filepath.Separator)) {
		path = filepath.Join(path, part)
		info, err := os.Lstat(path)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return nil, ErrForbidden
		}
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxBytes {
		return nil, ErrForbidden
	}
	file, err := rootHandle.Open(relative)
	if err != nil {
		return nil, ErrForbidden
	}
	defer func() { _ = file.Close() }()
	b, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil || int64(len(b)) > maxBytes {
		return nil, ErrForbidden
	}
	return b, nil
}

type retainedRun struct {
	path     string
	modified time.Time
	bytes    int64
}

// prune evicts expired then oldest runs until a new validated publication fits.
// The caller holds e.mu when another Run may publish concurrently.
func (e *Engine) prune(incoming int64, incomingCount int) error {
	if incoming > e.retainedLimit || incomingCount > e.runLimit {
		return ErrUnavailable
	}
	entries, err := os.ReadDir(e.root)
	if err != nil {
		return err
	}
	var total int64
	var retained []retainedRun
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == ".staging" {
			continue
		}
		scope := filepath.Join(e.root, entry.Name())
		runs, err := os.ReadDir(scope)
		if err != nil {
			return err
		}
		for _, run := range runs {
			if !run.IsDir() {
				continue
			}
			path := filepath.Join(scope, run.Name())
			info, err := run.Info()
			if err != nil {
				return err
			}
			if time.Since(info.ModTime()) > e.retentionPeriod {
				if err := os.RemoveAll(path); err != nil {
					return err
				}
				continue
			}
			bytes, err := dirBytes(path)
			if err != nil {
				return err
			}
			total += bytes
			retained = append(retained, retainedRun{path: path, modified: info.ModTime(), bytes: bytes})
		}
	}
	sort.Slice(retained, func(i, j int) bool {
		if retained[i].modified.Equal(retained[j].modified) {
			return retained[i].path < retained[j].path
		}
		return retained[i].modified.Before(retained[j].modified)
	})
	for len(retained) > 0 && (total+incoming > e.retainedLimit || len(retained)+incomingCount > e.runLimit) {
		run := retained[0]
		if err := os.RemoveAll(run.path); err != nil {
			return err
		}
		total -= run.bytes
		retained = retained[1:]
	}
	if total+incoming > e.retainedLimit || len(retained)+incomingCount > e.runLimit {
		return ErrUnavailable
	}
	return nil
}

func dirBytes(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(_ string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !d.IsDir() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			total += info.Size()
		}
		return nil
	})
	return total, err
}

func stageBytes(root string) int64 {
	var total int64
	_ = filepath.WalkDir(root, func(_ string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if i, e := d.Info(); e == nil {
				total += i.Size()
			}
		}
		return nil
	})
	return total
}
