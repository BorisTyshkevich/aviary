// Package chlab runs disposable ClickHouse containers behind a local root service.
package chlab

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Socket is the local Unix socket exposed by the root service.
const Socket = "/run/aviary-chlab.sock"

const (
	maxLabs     = 2
	idleTTL     = 15 * time.Minute
	totalTTL    = time.Hour
	minFreeDisk = 8 << 30
	maxOutput   = 64 << 10
)

var tagRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

type lockedListener struct {
	net.Listener
	lock *os.File
}

func (l *lockedListener) Close() error {
	err := l.Listener.Close()
	_ = syscall.Flock(int(l.lock.Fd()), syscall.LOCK_UN)
	_ = l.lock.Close()
	return err
}

// Request is a session-scoped lab operation sent to the root service.
type Request struct {
	Action  string `json:"action"`
	Agent   string `json:"agent"`
	Session string `json:"session"`
	Version string `json:"version,omitempty"`
	Shape   string `json:"shape,omitempty"`
	Node    string `json:"node,omitempty"`
	SQL     string `json:"sql,omitempty"`
}

// Status describes a lab and, for a query, its bounded output.
type Status struct {
	State       string            `json:"state"`
	Session     string            `json:"session,omitempty"`
	VersionTag  string            `json:"version_tag,omitempty"`
	Version     string            `json:"version,omitempty"`
	ImageDigest string            `json:"image_digest,omitempty"`
	Shape       string            `json:"shape,omitempty"`
	Nodes       map[string]string `json:"nodes,omitempty"`
	CreatedAt   time.Time         `json:"created_at,omitzero"`
	LastUsedAt  time.Time         `json:"last_used_at,omitzero"`
	Error       string            `json:"error,omitempty"`
	Output      string            `json:"output,omitempty"`
}

type lab struct {
	Status
	id     string
	mu     sync.Mutex
	cancel context.CancelFunc
}

// snapshotLocked returns a status that remains safe after l.mu is released.
func (l *lab) snapshotLocked() Status {
	status := l.Status
	if l.Nodes != nil {
		status.Nodes = make(map[string]string, len(l.Nodes))
		for name, state := range l.Nodes {
			status.Nodes[name] = state
		}
	}
	return status
}

// Service owns active labs and is the only component that invokes Docker.
type Service struct {
	mu   sync.Mutex
	labs map[string]*lab
	stop chan struct{}
}

// NewService creates an empty lab service.
func NewService() *Service { return &Service{labs: make(map[string]*lab), stop: make(chan struct{})} }

func key(r Request) string { return r.Agent + "\x00" + r.Session }

func randomID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

type limitWriter struct {
	buf   bytes.Buffer
	limit int
}

func (w *limitWriter) Write(p []byte) (int, error) {
	remaining := w.limit - w.buf.Len()
	if remaining <= 0 {
		return 0, errors.New("output limit exceeded")
	}
	if len(p) > remaining {
		n, _ := w.buf.Write(p[:remaining])
		return n, errors.New("output limit exceeded")
	}
	return w.buf.Write(p)
}

func (w *limitWriter) String() string { return w.buf.String() }

func docker(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=/root"}
	w := &limitWriter{limit: maxOutput}
	cmd.Stdout, cmd.Stderr = w, w
	err := cmd.Run()
	out := strings.TrimSpace(w.String())
	if err != nil {
		return out, fmt.Errorf("docker %s: %w: %s", args[0], err, out[:min(len(out), 1024)])
	}
	return out, nil
}

func shortDocker(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return docker(ctx, args...)
}

func freeDisk() (uint64, error) {
	root, err := shortDocker("info", "--format", "{{.DockerRootDir}}")
	if err != nil {
		return 0, err
	}
	var s syscall.Statfs_t
	if err := syscall.Statfs(root, &s); err != nil {
		return 0, err
	}
	return s.Bavail * uint64(s.Bsize), nil
}

// Serve cleans up stale labs and accepts local requests.
func (s *Service) Serve(listener net.Listener) error {
	// Only this service creates these labels. A restart discards every old lab.
	if err := s.cleanupOrphans(); err != nil {
		return err
	}
	go s.reap()
	mux := http.NewServeMux()
	mux.HandleFunc("/lab", s.handle)
	return http.Serve(listener, mux)
}

func (s *Service) cleanupOrphans() error {
	out, err := shortDocker("ps", "-aq", "--filter", "label=aviary.chlab=1")
	if err != nil {
		return err
	}
	for _, name := range strings.Fields(out) {
		_, _ = shortDocker("rm", "-fv", name)
	}
	out, err = shortDocker("network", "ls", "-q", "--filter", "label=aviary.chlab=1")
	if err != nil {
		return err
	}
	for _, name := range strings.Fields(out) {
		_, _ = shortDocker("network", "rm", name)
	}
	return nil
}

func (s *Service) reap() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			for _, id := range s.expire(time.Now()) {
				go s.destroy(id)
			}
		}
	}
}

func (s *Service) expire(now time.Time) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	for k, l := range s.labs {
		l.mu.Lock()
		if now.Sub(l.CreatedAt) > totalTTL || now.Sub(l.LastUsedAt) > idleTTL {
			delete(s.labs, k)
			if l.cancel != nil {
				l.cancel()
			}
			ids = append(ids, l.id)
		}
		l.mu.Unlock()
	}
	return ids
}

func (s *Service) handle(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var r Request
	if err := json.NewDecoder(io.LimitReader(req.Body, 1<<20)).Decode(&r); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if r.Agent == "" || r.Session == "" {
		http.Error(w, "agent and session required", 400)
		return
	}
	result, err := s.Call(req.Context(), r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

// Call executes a typed request for one agent session.
func (s *Service) Call(ctx context.Context, r Request) (Status, error) {
	s.mu.Lock()
	l := s.labs[key(r)]
	if r.Action == "start" && l != nil && (r.Version != l.VersionTag || r.Shape != l.Shape) {
		s.mu.Unlock()
		return Status{}, errors.New("this session already has a lab with a different version or shape; stop it before starting another")
	}
	if r.Action == "start" && l == nil {
		if !tagRE.MatchString(r.Version) {
			s.mu.Unlock()
			return Status{}, errors.New("version must be a published official image tag")
		}
		if r.Shape != "1x1" && r.Shape != "1x2" && r.Shape != "2x2" {
			s.mu.Unlock()
			return Status{}, errors.New("shape must be 1x1, 1x2, or 2x2")
		}
		active := 0
		for _, existing := range s.labs {
			existing.mu.Lock()
			if existing.State == "starting" || existing.State == "ready" {
				active++
			}
			existing.mu.Unlock()
		}
		if active >= maxLabs {
			s.mu.Unlock()
			return Status{}, errors.New("busy: two ClickHouse labs are already active")
		}
		free, err := freeDisk()
		if err != nil || free < minFreeDisk {
			s.mu.Unlock()
			return Status{}, errors.New("insufficient free Docker disk space (8 GiB required)")
		}
		id, err := randomID()
		if err != nil {
			s.mu.Unlock()
			return Status{}, err
		}
		startCtx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
		l = &lab{Status: Status{State: "starting", Session: r.Session, VersionTag: r.Version, Shape: r.Shape, Nodes: map[string]string{}, CreatedAt: time.Now(), LastUsedAt: time.Now()}, id: id, cancel: cancel}
		s.labs[key(r)] = l
		go s.start(startCtx, l)
	}
	s.mu.Unlock()
	if l == nil {
		return Status{}, errors.New("no lab for this session")
	}
	if r.Action == "stop" {
		s.mu.Lock()
		delete(s.labs, key(r))
		s.mu.Unlock()
		if l.cancel != nil {
			l.cancel()
		}
		go s.destroy(l.id)
		return Status{State: "stopped", Session: r.Session}, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if r.Action == "start" || r.Action == "status" {
		l.LastUsedAt = time.Now()
		if r.Action == "status" && l.State == "ready" {
			for node := range l.Nodes {
				out, err := docker(ctx, "inspect", "--format", "{{.State.Running}}", "aviary-chlab-"+l.id+"-"+node)
				if err != nil || out != "true" {
					l.Nodes[node] = "stopped"
				} else {
					l.Nodes[node] = "running"
				}
			}
		}
		return l.snapshotLocked(), nil
	}
	if l.State != "ready" {
		return l.snapshotLocked(), fmt.Errorf("lab is %s: %s", l.State, l.Error)
	}
	if r.Node == "" {
		r.Node = "ch1"
	}
	name := "aviary-chlab-" + l.id + "-" + r.Node
	if _, ok := l.Nodes[r.Node]; !ok {
		return Status{}, fmt.Errorf("unknown node %q", r.Node)
	}
	l.LastUsedAt = time.Now()
	result := l.snapshotLocked()
	switch r.Action {
	case "query":
		if r.SQL == "" || len(r.SQL) > 32<<10 {
			return Status{}, errors.New("SQL must be 1 to 32768 bytes")
		}
		qctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		out, err := docker(qctx, "exec", name, "clickhouse-client", "--connect_timeout=2", "--receive_timeout=12", "--send_timeout=12", "--max_execution_time=10", "--max_memory_usage=536870912", "--max_result_rows=1000", "--result_overflow_mode=break", "--format=TSVWithNames", "--query", r.SQL)
		result.Output = out
		if err != nil {
			result.Error = err.Error()
		}
		return result, nil
	case "node_start", "node_stop", "node_restart":
		verb := strings.TrimPrefix(r.Action, "node_")
		_, err := docker(ctx, verb, name)
		if err != nil {
			return Status{}, err
		}
		if verb == "stop" {
			l.Nodes[r.Node] = "stopped"
		} else {
			if err := waitNode(ctx, name); err != nil {
				return Status{}, err
			}
			l.Nodes[r.Node] = "running"
		}
		result = l.snapshotLocked()
		return result, nil
	default:
		return Status{}, fmt.Errorf("unknown action %q", r.Action)
	}
}

func waitNode(ctx context.Context, name string) error {
	for attempt := 0; attempt < 30; attempt++ {
		probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		probe, err := docker(probeCtx, "exec", name, "clickhouse-client", "--connect_timeout=1", "--query", "SELECT 1")
		cancel()
		if err == nil && strings.TrimSpace(probe) == "1" {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return errors.New("ClickHouse node did not become ready")
}

func (s *Service) start(ctx context.Context, l *lab) {
	defer l.cancel()
	err := s.provision(ctx, l)
	l.mu.Lock()
	if err != nil {
		l.State = "error"
		l.Error = err.Error()
		l.Nodes = map[string]string{}
	} else {
		l.State = "ready"
	}
	l.mu.Unlock()
	if err != nil {
		s.destroy(l.id)
	}
}

func (s *Service) provision(ctx context.Context, l *lab) error {
	image := "clickhouse/clickhouse-server:" + l.VersionTag
	if _, err := docker(ctx, "pull", image); err != nil {
		return fmt.Errorf("image tag %q unavailable: %w", l.VersionTag, err)
	}
	if free, err := freeDisk(); err != nil || free < minFreeDisk {
		return errors.New("insufficient free Docker disk space after image pull (8 GiB required)")
	}
	var inspect []struct {
		RepoDigests []string `json:"RepoDigests"`
	}
	out, err := docker(ctx, "image", "inspect", image)
	if err != nil {
		return err
	}
	if err := json.Unmarshal([]byte(out), &inspect); err != nil || len(inspect) == 0 || len(inspect[0].RepoDigests) == 0 {
		return errors.New("image digest unavailable")
	}
	resolvedImage := ""
	for _, digest := range inspect[0].RepoDigests {
		if strings.HasPrefix(digest, "clickhouse/clickhouse-server@sha256:") {
			resolvedImage = digest
			break
		}
	}
	if resolvedImage == "" {
		return errors.New("official image digest unavailable")
	}
	l.mu.Lock()
	l.ImageDigest = resolvedImage
	l.mu.Unlock()
	network := "aviary-chlab-" + l.id
	if _, err := docker(ctx, "network", "create", "--driver", "bridge", "--internal", "--ipv6=false", "--opt", "com.docker.network.bridge.gateway_mode_ipv4=isolated", "--label", "aviary.chlab=1", network); err != nil {
		return err
	}
	if l.Shape != "1x1" {
		if err := s.runKeeper(ctx, l.id, network); err != nil {
			return fmt.Errorf("cluster incompatible with requested image or Keeper: %w", err)
		}
	}
	nodes := 1
	switch l.Shape {
	case "1x2":
		nodes = 2
	case "2x2":
		nodes = 4
	}
	for i := 1; i <= nodes; i++ {
		name := fmt.Sprintf("ch%d", i)
		if err := s.runServer(ctx, l.id, network, resolvedImage, name, l.Shape); err != nil {
			if l.Shape != "1x1" {
				return fmt.Errorf("image %q may be incompatible with %s cluster: node %s failed: %w", l.VersionTag, l.Shape, name, err)
			}
			return fmt.Errorf("node %s failed on image %q: %w", name, l.VersionTag, err)
		}
		l.mu.Lock()
		l.Nodes[name] = "running"
		l.mu.Unlock()
	}
	if l.Shape != "1x1" {
		probeCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
		_, err := docker(probeCtx, "exec", "aviary-chlab-"+l.id+"-ch1", "clickhouse-client", "--query", "SELECT count() FROM system.zookeeper WHERE path='/'")
		cancel()
		if err != nil {
			return fmt.Errorf("cluster incompatible with requested image: keeper is unavailable: %w", err)
		}
	}
	version, err := docker(ctx, "exec", "aviary-chlab-"+l.id+"-ch1", "clickhouse-client", "--query", "SELECT version()")
	if err != nil {
		return err
	}
	l.mu.Lock()
	l.Version = strings.TrimSpace(version)
	l.mu.Unlock()
	return nil
}

func (s *Service) runKeeper(ctx context.Context, id, network string) error {
	name := "aviary-chlab-" + id + "-keeper"
	_, err := docker(ctx, "pull", "clickhouse/clickhouse-keeper:latest")
	if err != nil {
		return err
	}
	if free, err := freeDisk(); err != nil || free < minFreeDisk {
		return errors.New("insufficient free Docker disk space after Keeper pull (8 GiB required)")
	}
	_, err = docker(ctx, "create", "--name", name, "--hostname", "keeper", "--network", network, "--label", "aviary.chlab=1", "--cpus=1", "--memory=1g", "--pids-limit=512", "--read-only", "--log-driver=local", "--log-opt=max-size=10m", "--log-opt=max-file=2", "--security-opt=no-new-privileges", "--cap-drop=ALL", "--cap-add=CHOWN", "--cap-add=SETUID", "--cap-add=SETGID", "--cap-add=DAC_OVERRIDE", "--tmpfs", "/var/lib/clickhouse:rw,size=512m,uid=101,gid=101", "--tmpfs", "/var/lib/clickhouse-keeper:rw,size=64m,uid=101,gid=101", "--tmpfs", "/var/log/clickhouse-keeper:rw,size=64m,uid=101,gid=101", "--tmpfs", "/etc/clickhouse-keeper:rw,size=1m", "--env", "CHLAB_CONFIG="+keeperConfig, "--entrypoint", "/bin/sh", "clickhouse/clickhouse-keeper:latest", "-c", `printf '%s' "$CHLAB_CONFIG" > /etc/clickhouse-keeper/keeper_config.xml; exec /entrypoint.sh`)
	if err != nil {
		return err
	}
	_, err = docker(ctx, "start", name)
	return err
}

const keeperConfig = `<clickhouse><logger><level>warning</level><console>1</console></logger><listen_host>0.0.0.0</listen_host><keeper_server><tcp_port>9181</tcp_port><server_id>1</server_id><log_storage_path>/var/lib/clickhouse/coordination/logs</log_storage_path><snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path><data_storage_path>/var/lib/clickhouse/coordination/data</data_storage_path><coordination_settings><operation_timeout_ms>10000</operation_timeout_ms></coordination_settings><raft_configuration><server><id>1</id><hostname>keeper</hostname><port>9234</port></server></raft_configuration></keeper_server></clickhouse>`

const userConfig = `<clickhouse><profiles><default><max_execution_time>10</max_execution_time><max_memory_usage>536870912</max_memory_usage><max_result_rows>1000</max_result_rows><result_overflow_mode>break</result_overflow_mode><timeout_before_checking_execution_speed>0</timeout_before_checking_execution_speed><constraints><max_execution_time><readonly/></max_execution_time><max_memory_usage><readonly/></max_memory_usage><max_result_rows><readonly/></max_result_rows><result_overflow_mode><readonly/></result_overflow_mode></constraints></default></profiles></clickhouse>`

func (s *Service) runServer(ctx context.Context, id, network, image, node, shape string) error {
	name := "aviary-chlab-" + id + "-" + node
	args := []string{"create", "--name", name, "--hostname", node, "--network", network, "--label", "aviary.chlab=1", "--cpus=2", "--memory=2g", "--pids-limit=1024", "--read-only", "--log-driver=local", "--log-opt=max-size=10m", "--log-opt=max-file=2", "--security-opt=no-new-privileges", "--cap-drop=ALL", "--cap-add=CHOWN", "--cap-add=SETUID", "--cap-add=SETGID", "--env", "CLICKHOUSE_SKIP_USER_SETUP=1", "--env", "CHLAB_USER_CONFIG=" + userConfig, "--tmpfs", "/var/lib/clickhouse:rw,size=1g", "--tmpfs", "/var/log/clickhouse-server:rw,size=64m", "--tmpfs", "/tmp:rw,size=128m", "--tmpfs", "/etc/clickhouse-server/config.d:rw,size=1m", "--tmpfs", "/etc/clickhouse-server/users.d:rw,size=1m", "--entrypoint", "/bin/sh"}
	if shape != "1x1" {
		args = append(args, "--env", "CHLAB_CONFIG="+clusterConfig(node, shape))
	}
	args = append(args, image, "-c")
	if shape != "1x1" {
		args = append(args, `printf '%s' "$CHLAB_USER_CONFIG" > /etc/clickhouse-server/users.d/chlab.xml; printf '%s' "$CHLAB_CONFIG" > /etc/clickhouse-server/config.d/chlab.xml; exec /entrypoint.sh`)
	} else {
		args = append(args, `printf '%s' "$CHLAB_USER_CONFIG" > /etc/clickhouse-server/users.d/chlab.xml; exec /entrypoint.sh`)
	}
	if _, err := docker(ctx, args...); err != nil {
		return err
	}
	if _, err := docker(ctx, "start", name); err != nil {
		return err
	}
	return waitNode(ctx, name)
}

func clusterConfig(node, shape string) string {
	shard := "01"
	if node == "ch3" || node == "ch4" {
		shard = "02"
	}
	shards := `<shard><internal_replication>true</internal_replication><replica><host>ch1</host><port>9000</port></replica><replica><host>ch2</host><port>9000</port></replica></shard>`
	if shape == "2x2" {
		shards += `<shard><internal_replication>true</internal_replication><replica><host>ch3</host><port>9000</port></replica><replica><host>ch4</host><port>9000</port></replica></shard>`
	}
	return `<clickhouse><listen_host>0.0.0.0</listen_host><remote_servers><chlab>` + shards + `</chlab></remote_servers><zookeeper><node><host>keeper</host><port>9181</port></node></zookeeper><macros><shard>` + shard + `</shard><replica>` + node + `</replica></macros></clickhouse>`
}

func (s *Service) destroy(id string) {
	for _, n := range []string{"ch1", "ch2", "ch3", "ch4", "keeper"} {
		_, _ = shortDocker("rm", "-fv", "aviary-chlab-"+id+"-"+n)
	}
	_, _ = shortDocker("network", "rm", "aviary-chlab-"+id)
}

// Call sends a typed request to the root service over the Unix socket.
func Call(ctx context.Context, r Request) (Status, error) {
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", Socket)
	}}
	client := &http.Client{Transport: transport, Timeout: 20 * time.Second}
	defer transport.CloseIdleConnections()
	b, err := json.Marshal(r)
	if err != nil {
		return Status{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://unix/lab", bytes.NewReader(b))
	if err != nil {
		return Status{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return Status{}, fmt.Errorf("lab service unavailable: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return Status{}, errors.New(strings.TrimSpace(string(body)))
	}
	var status Status
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return Status{}, err
	}
	return status, nil
}

// Listen creates the root-owned local service socket.
func Listen() (net.Listener, error) {
	if os.Geteuid() != 0 {
		return nil, errors.New("chlab service must run as root")
	}
	lock, err := os.OpenFile("/run/aviary-chlab.lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, errors.New("chlab service is already running")
	}
	if err := os.Remove(Socket); err != nil && !os.IsNotExist(err) {
		_ = lock.Close()
		return nil, err
	}
	l, err := net.Listen("unix", Socket)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	if err := os.Chmod(Socket, 0660); err != nil {
		_ = l.Close()
		_ = lock.Close()
		return nil, err
	}
	if group := os.Getenv("SUDO_GID"); group != "" {
		gid, err := strconv.Atoi(group)
		if err != nil || gid < 0 {
			_ = l.Close()
			_ = lock.Close()
			return nil, errors.New("invalid SUDO_GID")
		}
		if err := os.Chown(Socket, 0, gid); err != nil {
			_ = l.Close()
			_ = lock.Close()
			return nil, err
		}
	}
	return &lockedListener{Listener: l, lock: lock}, nil
}
