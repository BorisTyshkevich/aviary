// Package connections owns thread attachments, turn leases and private setup state.
package connections

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lsegal/aviary/internal/store"
)

// Connection lifecycle errors are safe to show without exposing credentials.
var (
	ErrInvalid    = errors.New("invalid connection identity")
	ErrBusy       = errors.New("thread has an active turn")
	ErrStale      = errors.New("connection setup is stale")
	ErrPrompt     = errors.New("private setup prompt is unavailable")
	ErrDuplicate  = errors.New("private setup reply was already processed")
	ErrValidation = errors.New("connection validation failed")
)

// Scope identifies one shared destination thread and agent.
type Scope struct {
	AgentID        string `json:"agent_id"`
	InstallationID string `json:"installation_id"`
	WorkspaceID    string `json:"workspace_id"`
	ChannelID      string `json:"channel_id"`
	RootThreadID   string `json:"root_thread_id"`
}

func (s Scope) valid() bool {
	return s.AgentID != "" && s.InstallationID != "" && s.WorkspaceID != "" && s.ChannelID != "" && s.RootThreadID != ""
}

// Principal identifies a trusted user in one Slack installation and workspace.
type Principal struct {
	InstallationID string `json:"installation_id"`
	WorkspaceID    string `json:"workspace_id"`
	UserID         string `json:"user_id"`
}

func (p Principal) valid() bool {
	return p.InstallationID != "" && p.WorkspaceID != "" && p.UserID != ""
}

// Target is the stable selected attachment for a thread generation.
type Target struct {
	Scope      Scope  `json:"scope"`
	Transport  string `json:"transport"`
	Endpoint   string `json:"endpoint"`
	Generation string `json:"generation"`
}

// Credential contains a personal login; JSON and string formatting redact its password.
type Credential struct {
	Username string `json:"username"`
	Password string `json:"-"`
	Version  string `json:"version"`
}

func (c Credential) String() string { return "credential version " + c.Version }

// GoString redacts the password in Go diagnostic formatting.
func (c Credential) GoString() string { return c.String() }

type privateCredential struct {
	Username   string            `json:"username"`
	Password   string            `json:"password"`
	Version    string            `json:"version"`
	Principal  Principal         `json:"principal"`
	Scope      Scope             `json:"scope"`
	Generation string            `json:"generation"`
	Evidence   *EvidenceSnapshot `json:"evidence,omitempty"`
}

// EvidenceSnapshot is immutable, private connection evidence collected after login.
// It lasts exactly as long as the owning credential and target generation.
type EvidenceSnapshot struct {
	Status           string    `json:"status"`
	Summary          string    `json:"summary"`
	RunID            string    `json:"run_id"`
	ObservedAt       time.Time `json:"observed_at"`
	ProducerRevision string    `json:"producer_revision"`
	Path             string    `json:"path"`
	Content          []byte    `json:"content"`
}

// Prompt contains only non-secret classification and correlation metadata.
type Prompt struct {
	Principal   Principal `json:"principal"`
	DMChannelID string    `json:"dm_channel_id"`
	DMRootID    string    `json:"dm_root_id"`
	Target      Target    `json:"target"`
	Stage       string    `json:"stage"`
	Username    string    `json:"username"`
	LastReplyTS string    `json:"last_reply_ts,omitempty"`
	ExpiresAt   time.Time `json:"expires_at"`
	Completed   bool      `json:"completed"`
}

type state struct {
	Targets map[string]Target `json:"targets"`
	Prompts map[string]Prompt `json:"prompts"`
}

// Service is safe for concurrent use within one Aviary process. Open one instance
// per data directory; storage is not designed for multiple writer processes.
type Service struct {
	mu              sync.Mutex
	dir             string
	state           state
	credentials     map[string]privateCredential
	busy            map[string]int
	completing      map[string]bool
	evidencePending map[string]pendingEvidence
}

type pendingEvidence struct {
	version string
	done    chan struct{}
}

// Open loads or creates one private connection store at dir.
func Open(dir string) (*Service, error) {
	if dir == "" {
		return nil, ErrInvalid
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, errors.New("opening connection storage failed")
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, errors.New("securing connection storage failed")
	}
	s := &Service{dir: dir, state: state{Targets: map[string]Target{}, Prompts: map[string]Prompt{}}, credentials: map[string]privateCredential{}, busy: map[string]int{}, completing: map[string]bool{}, evidencePending: map[string]pendingEvidence{}}
	if err := readOptional(filepath.Join(dir, "state.json"), &s.state); err != nil {
		return nil, err
	}
	if err := readOptional(filepath.Join(dir, "private-credentials.json"), &s.credentials); err != nil {
		return nil, err
	}
	if s.state.Targets == nil {
		s.state.Targets = map[string]Target{}
	}
	if s.state.Prompts == nil {
		s.state.Prompts = map[string]Prompt{}
	}
	if s.credentials == nil {
		s.credentials = map[string]privateCredential{}
	}
	if s.purgeOrphansLocked() {
		if err := s.saveCredentials(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func readOptional(path string, v any) error {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil || json.Unmarshal(b, v) != nil {
		return errors.New("reading connection storage failed")
	}
	return nil
}

func key(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func generation() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
func (s *Service) saveState() error {
	if err := store.WriteJSON(filepath.Join(s.dir, "state.json"), s.state); err != nil {
		return errors.New("saving connection state failed")
	}
	return nil
}
func (s *Service) saveCredentials() error {
	if err := store.WriteJSON(filepath.Join(s.dir, "private-credentials.json"), s.credentials); err != nil {
		return errors.New("saving private credentials failed")
	}
	return nil
}

func (s *Service) purgeOrphansLocked() bool {
	changed := false
	for k, v := range s.credentials {
		t := s.state.Targets[key(v.Scope)]
		if t.Generation != v.Generation || v.Generation == "" || !v.Principal.valid() || v.Principal.InstallationID != v.Scope.InstallationID || v.Principal.WorkspaceID != v.Scope.WorkspaceID || k != credentialKey(v.Principal, t) {
			delete(s.credentials, k)
			if pending, ok := s.evidencePending[k]; ok {
				close(pending.done)
				delete(s.evidencePending, k)
			}
			changed = true
		}
	}
	return changed
}

// Select attaches a new target or returns the existing target unchanged.
func (s *Service) Select(scope Scope, transport, endpoint string) (Target, bool, error) {
	if !scope.valid() || transport == "" || endpoint == "" {
		return Target{}, false, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(scope)
	old, exists := s.state.Targets[k]
	if exists && old.Transport == transport && old.Endpoint == endpoint {
		return old, false, nil
	}
	if s.busy[k] > 0 {
		return Target{}, false, ErrBusy
	}
	g, err := generation()
	if err != nil {
		return Target{}, false, errors.New("creating connection generation failed")
	}
	next := Target{Scope: scope, Transport: transport, Endpoint: endpoint, Generation: g}
	s.state.Targets[k] = next
	if err := s.saveState(); err != nil {
		if exists {
			s.state.Targets[k] = old
		} else {
			delete(s.state.Targets, k)
		}
		return Target{}, false, err
	}
	if s.purgeOrphansLocked() {
		if err := s.saveCredentials(); err != nil {
			return Target{}, false, err
		}
	}
	return next, true, nil
}

// Current returns the selected target for a thread.
func (s *Service) Current(scope Scope) (Target, bool) {
	if !scope.valid() {
		return Target{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.state.Targets[key(scope)]
	return v, ok
}

// Disconnect removes a target when its thread has no active turn.
func (s *Service) Disconnect(scope Scope) error {
	if !scope.valid() {
		return ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(scope)
	if s.busy[k] > 0 {
		return ErrBusy
	}
	old, ok := s.state.Targets[k]
	if !ok {
		return nil
	}
	delete(s.state.Targets, k)
	if err := s.saveState(); err != nil {
		s.state.Targets[k] = old
		return err
	}
	if s.purgeOrphansLocked() {
		if err := s.saveCredentials(); err != nil {
			return err
		}
	}
	return nil
}

// Lease reserves a thread and captures its selected target for a turn.
type Lease struct {
	service *Service
	target  Target
	once    sync.Once
}

// Target returns the leased attachment snapshot.
func (l *Lease) Target() Target {
	if l == nil {
		return Target{}
	}
	return l.target
}

// End releases the thread reservation once.
func (l *Lease) End() {
	if l == nil || l.service == nil {
		return
	}
	l.once.Do(func() { l.service.mu.Lock(); defer l.service.mu.Unlock(); l.service.busy[key(l.target.Scope)]-- })
}

// Begin atomically snapshots the attachment and reserves the thread against replacement.
func (s *Service) Begin(scope Scope) (*Lease, error) {
	if !scope.valid() {
		return nil, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.state.Targets[key(scope)]
	if !ok {
		t = Target{Scope: scope}
	}
	s.busy[key(scope)]++
	return &Lease{service: s, target: t}, nil
}

type leaseContextKey struct{}

// WithLease attaches the trusted turn lease to context.
func WithLease(ctx context.Context, lease *Lease) context.Context {
	if lease == nil {
		return ctx
	}
	return context.WithValue(ctx, leaseContextKey{}, lease)
}

// LeaseFromContext extracts a trusted turn lease from context.
func LeaseFromContext(ctx context.Context) (*Lease, bool) {
	l, ok := ctx.Value(leaseContextKey{}).(*Lease)
	return l, ok && l != nil
}

// ExecutionKind distinguishes personal interactive turns from other work.
type ExecutionKind string

// Execution kinds explicitly separate interactive, scheduled and control work.
const (
	Interactive ExecutionKind = "interactive"
	Scheduled   ExecutionKind = "scheduled"
	Control     ExecutionKind = "control"
)

// Execution carries trusted ingress identity for one turn.
type Execution struct {
	Kind      ExecutionKind `json:"kind"`
	Scope     Scope         `json:"scope"`
	Principal Principal     `json:"principal"`
}

// Personal reports whether a trusted interactive principal matches its scope.
func (e Execution) Personal() bool {
	return e.Kind == Interactive && e.Scope.valid() && e.Principal.valid() && e.Scope.InstallationID == e.Principal.InstallationID && e.Scope.WorkspaceID == e.Principal.WorkspaceID
}

type executionContextKey struct{}

// WithExecution attaches trusted execution identity to context.
func WithExecution(ctx context.Context, e Execution) context.Context {
	return context.WithValue(ctx, executionContextKey{}, e)
}

// ExecutionFromContext extracts execution identity from context.
func ExecutionFromContext(ctx context.Context) (Execution, bool) {
	e, ok := ctx.Value(executionContextKey{}).(Execution)
	return e, ok
}

func credentialKey(p Principal, t Target) string {
	return key(struct {
		Principal  Principal
		Scope      Scope
		Generation string
	}{p, t.Scope, t.Generation})
}

// HasCredential reports whether the current target has a personal login.
func (s *Service) HasCredential(p Principal, t Target) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.currentLocked(t) || !p.valid() || p.InstallationID != t.Scope.InstallationID || p.WorkspaceID != t.Scope.WorkspaceID {
		return false
	}
	_, ok := s.credentials[credentialKey(p, t)]
	return ok
}

// ActivePromptFor returns a pending or validating password prompt for the
// exact principal and current target. In-flight validation remains active even
// after the prompt is durably consumed or expires, preventing duplicate setup.
func (s *Service) ActivePromptFor(p Principal, t Target) (Prompt, bool) {
	if !p.valid() {
		return Prompt{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.currentLocked(t) {
		return Prompt{}, false
	}
	now := time.Now()
	var active Prompt
	found := false
	activeCompleting := false
	for _, prompt := range s.state.Prompts {
		if prompt.Principal != p || prompt.Target != t || prompt.Stage != "password" {
			continue
		}
		completing := s.completing[promptKey(p.InstallationID, p.WorkspaceID, prompt.DMChannelID, prompt.DMRootID)]
		if !completing && (prompt.Completed || !now.Before(prompt.ExpiresAt)) {
			continue
		}
		if !found || (completing && !activeCompleting) || (completing == activeCompleting && prompt.ExpiresAt.After(active.ExpiresAt)) {
			active = prompt
			found = true
			activeCompleting = completing
		}
	}
	return active, found
}

// HasActivePrompt reports whether a password prompt is pending or validating
// for the exact principal and current target.
func (s *Service) HasActivePrompt(p Principal, t Target) bool {
	_, ok := s.ActivePromptFor(p, t)
	return ok
}

// CredentialFor returns a login only for the matching interactive principal.
func (s *Service) CredentialFor(e Execution, t Target) (Credential, bool) {
	if !e.Personal() || e.Scope != t.Scope {
		return Credential{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.currentLocked(t) {
		return Credential{}, false
	}
	v, ok := s.credentials[credentialKey(e.Principal, t)]
	return Credential{Username: v.Username, Password: v.Password, Version: v.Version}, ok
}

// SaveEvidence attaches a bounded snapshot only to the current personal login.
func (s *Service) SaveEvidence(e Execution, t Target, version string, evidence EvidenceSnapshot) error {
	if !e.Personal() || e.Scope != t.Scope || version == "" || len(evidence.Content) == 0 || len(evidence.Content) > 1<<20 || evidence.Path != "evidence.json" || len(evidence.RunID) != 32 {
		return ErrInvalid
	}
	for _, c := range evidence.RunID {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return ErrInvalid
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.currentLocked(t) {
		return ErrStale
	}
	k := credentialKey(e.Principal, t)
	c, ok := s.credentials[k]
	escapedPassword, _ := json.Marshal(c.Password)
	if !ok || c.Version != version || (c.Password != "" && (strings.Contains(string(evidence.Content), c.Password) || strings.Contains(string(evidence.Content), string(escapedPassword[1:len(escapedPassword)-1])))) {
		return ErrStale
	}
	previous := c
	copyEvidence := evidence
	copyEvidence.Content = append([]byte(nil), evidence.Content...)
	c.Evidence = &copyEvidence
	s.credentials[k] = c
	if err := s.saveCredentials(); err != nil {
		s.credentials[k] = previous
		return err
	}
	return nil
}

// WaitForEvidence lets an immediately following turn wait for the one in-flight
// post-connect collection. No later turn starts or refreshes collection.
func (s *Service) WaitForEvidence(ctx context.Context, e Execution, t Target) {
	if !e.Personal() || e.Scope != t.Scope {
		return
	}
	s.mu.Lock()
	var done <-chan struct{}
	if s.currentLocked(t) {
		k := credentialKey(e.Principal, t)
		if c, ok := s.credentials[k]; ok {
			if pending, ok := s.evidencePending[k]; ok && pending.version == c.Version {
				done = pending.done
			}
		}
	}
	s.mu.Unlock()
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
		}
	}
}

// FinishEvidence releases turns waiting for this credential's post-connect hook.
func (s *Service) FinishEvidence(p Principal, t Target, version string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := credentialKey(p, t)
	if pending, ok := s.evidencePending[k]; ok && pending.version == version {
		close(pending.done)
		delete(s.evidencePending, k)
	}
}

// EvidenceFor returns a copy only to the matching current personal credential.
func (s *Service) EvidenceFor(e Execution, t Target) (EvidenceSnapshot, string, bool) {
	if !e.Personal() || e.Scope != t.Scope {
		return EvidenceSnapshot{}, "", false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.currentLocked(t) {
		return EvidenceSnapshot{}, "", false
	}
	c, ok := s.credentials[credentialKey(e.Principal, t)]
	if !ok || c.Evidence == nil {
		return EvidenceSnapshot{}, "", false
	}
	result := *c.Evidence
	result.Content = append([]byte(nil), result.Content...)
	return result, c.Version, true
}
func (s *Service) currentLocked(t Target) bool {
	v, ok := s.state.Targets[key(t.Scope)]
	return ok && v == t && t.Generation != ""
}

func promptKey(installation, workspace, channel, root string) string {
	return key(struct{ Installation, Workspace, Channel, Root string }{installation, workspace, channel, root})
}

func slackReplyTimestamp(ts string) (uint64, uint64, bool) {
	seconds, fraction, ok := strings.Cut(ts, ".")
	if !ok || seconds == "" || len(fraction) != 6 {
		return 0, 0, false
	}
	sec, secErr := strconv.ParseUint(seconds, 10, 64)
	micro, microErr := strconv.ParseUint(fraction, 10, 64)
	return sec, micro, secErr == nil && microErr == nil
}

// PutPrompt durably records non-secret private prompt correlation metadata.
func (s *Service) PutPrompt(p Prompt) error {
	if !p.Principal.valid() || p.DMChannelID == "" || p.DMRootID == "" || p.Stage != "password" || p.ExpiresAt.IsZero() || p.Target.Scope.InstallationID != p.Principal.InstallationID || p.Target.Scope.WorkspaceID != p.Principal.WorkspaceID {
		return ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.currentLocked(p.Target) {
		return ErrStale
	}
	k := promptKey(p.Principal.InstallationID, p.Principal.WorkspaceID, p.DMChannelID, p.DMRootID)
	if _, exists := s.state.Prompts[k]; exists {
		return ErrPrompt
	}
	s.state.Prompts[k] = p
	if err := s.saveState(); err != nil {
		delete(s.state.Prompts, k)
		return err
	}
	return nil
}

// TombstonePrompt classifies a posted DM prompt even if target selection raced
// its delivery. It must be called when PutPrompt fails after a message is posted.
func (s *Service) TombstonePrompt(installation, workspace, dmChannel, dmRoot string) error {
	if installation == "" || workspace == "" || dmChannel == "" || dmRoot == "" {
		return ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := promptKey(installation, workspace, dmChannel, dmRoot)
	if _, ok := s.state.Prompts[k]; ok {
		return nil
	}
	s.state.Prompts[k] = Prompt{Principal: Principal{InstallationID: installation, WorkspaceID: workspace}, DMChannelID: dmChannel, DMRootID: dmRoot, Stage: "tombstone", Completed: true}
	if err := s.saveState(); err != nil {
		delete(s.state.Prompts, k)
		return err
	}
	return nil
}

// ClassifyReply stays true after completion or expiry, so late private replies
// never enter ordinary history or model processing.
func (s *Service) ClassifyReply(installation, workspace, dmChannel, dmRoot string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.state.Prompts[promptKey(installation, workspace, dmChannel, dmRoot)]
	return ok
}

// PromptFor returns an active prompt to its intended principal.
func (s *Service) PromptFor(p Principal, dmChannel, dmRoot string) (Prompt, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.state.Prompts[promptKey(p.InstallationID, p.WorkspaceID, dmChannel, dmRoot)]
	return v, ok && v.Principal == p && v.Stage == "password" && !v.Completed && time.Now().Before(v.ExpiresAt) && s.currentLocked(v.Target)
}

// FinishPrompt durably tombstones a private prompt after a delivery failure.
func (s *Service) FinishPrompt(p Principal, dmChannel, dmRoot string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := promptKey(p.InstallationID, p.WorkspaceID, dmChannel, dmRoot)
	v, ok := s.state.Prompts[k]
	if !ok || v.Principal != p || v.Completed || !time.Now().Before(v.ExpiresAt) || !s.currentLocked(v.Target) {
		return ErrPrompt
	}
	v.Completed = true
	s.state.Prompts[k] = v
	if err := s.saveState(); err != nil {
		v.Completed = false
		s.state.Prompts[k] = v
		return err
	}
	return nil
}

// CompletePassword durably marks a prompt busy before validation, so a crash or
// concurrent reply cannot replay it. A failed authentication reopens the same
// prompt while its target and expiry remain valid. Validation runs outside the
// lifecycle lock.
func (s *Service) CompletePassword(ctx context.Context, p Principal, dmChannel, dmRoot, replyTS, password string, validate func(context.Context, Target, Credential) error) error {
	seconds, micros, valid := slackReplyTimestamp(replyTS)
	if !valid {
		return ErrInvalid
	}
	k := promptKey(p.InstallationID, p.WorkspaceID, dmChannel, dmRoot)
	s.mu.Lock()
	pr, ok := s.state.Prompts[k]
	if !ok || pr.Principal != p || pr.Stage != "password" || !time.Now().Before(pr.ExpiresAt) || !s.currentLocked(pr.Target) {
		s.mu.Unlock()
		return ErrPrompt
	}
	if pr.LastReplyTS != "" {
		previousSeconds, previousMicros, previousValid := slackReplyTimestamp(pr.LastReplyTS)
		if !previousValid {
			s.mu.Unlock()
			return ErrPrompt
		}
		if seconds < previousSeconds || (seconds == previousSeconds && micros <= previousMicros) {
			s.mu.Unlock()
			return ErrDuplicate
		}
	}
	if pr.Completed || s.completing[k] {
		s.mu.Unlock()
		return ErrPrompt
	}
	previous := pr
	pr.Completed = true
	pr.LastReplyTS = replyTS
	s.state.Prompts[k] = pr
	if err := s.saveState(); err != nil {
		s.state.Prompts[k] = previous
		s.mu.Unlock()
		return err
	}
	s.completing[k] = true
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.completing, k); s.mu.Unlock() }()
	version, err := generation()
	if err != nil {
		return errors.New("creating credential version failed")
	}
	c := Credential{Username: pr.Username, Password: password, Version: version}
	if validate == nil {
		return ErrValidation
	}
	if err := validate(ctx, pr.Target, c); err != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		latest := s.state.Prompts[k]
		if latest != pr || !s.currentLocked(latest.Target) {
			return ErrStale
		}
		if !time.Now().Before(latest.ExpiresAt) {
			return ErrPrompt
		}
		latest.Completed = false
		s.state.Prompts[k] = latest
		if saveErr := s.saveState(); saveErr != nil {
			s.state.Prompts[k] = pr
			return saveErr
		}
		return ErrValidation
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	latest := s.state.Prompts[k]
	if latest != pr || !s.currentLocked(latest.Target) {
		return ErrStale
	}
	ck := credentialKey(p, latest.Target)
	old, had := s.credentials[ck]
	s.credentials[ck] = privateCredential{Username: c.Username, Password: c.Password, Version: c.Version, Principal: p, Scope: latest.Target.Scope, Generation: latest.Target.Generation}
	if err := s.saveCredentials(); err != nil {
		if had {
			s.credentials[ck] = old
		} else {
			delete(s.credentials, ck)
		}
		return err
	}
	if pending, ok := s.evidencePending[ck]; ok {
		close(pending.done)
	}
	s.evidencePending[ck] = pendingEvidence{version: c.Version, done: make(chan struct{})}
	return nil
}
