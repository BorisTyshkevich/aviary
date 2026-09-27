// Package server implements the Aviary HTTPS server.
package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lsegal/aviary/internal/agent"
	"github.com/lsegal/aviary/internal/auth"
	"github.com/lsegal/aviary/internal/browser"
	"github.com/lsegal/aviary/internal/channels"
	"github.com/lsegal/aviary/internal/clickhouseconn"
	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/connections"
	"github.com/lsegal/aviary/internal/domain"
	"github.com/lsegal/aviary/internal/llm"
	"github.com/lsegal/aviary/internal/mcp"
	"github.com/lsegal/aviary/internal/preparation"
	"github.com/lsegal/aviary/internal/scheduler"
	"github.com/lsegal/aviary/internal/sessiontarget"
	"github.com/lsegal/aviary/internal/store"
	"github.com/lsegal/aviary/internal/update"
	"github.com/lsegal/aviary/skills"
)

// ErrRestartRequired is returned by ListenAndServe when an explicit process
// restart was requested (for example via the daemons API).
var ErrRestartRequired = errors.New("server restart required")

// Server wraps an HTTPS server with token auth, MCP routing, and agent management.
type Server struct {
	cfg               *config.Config
	token             string
	mux               *http.ServeMux
	httpSrv           *http.Server
	runCtx            context.Context
	agents            *agent.Manager
	llmFactory        *llm.Factory
	sched             *scheduler.Scheduler
	channels          *channels.Manager
	connections       *connections.Service
	preparation       *preparation.Engine
	startupErr        error
	connectionPolicy  atomic.Pointer[config.ConnectionPolicyConfig]
	brw               *browser.Manager
	sampler           *ProcSampler
	watcher           *config.Watcher
	skillsWatcher     *skills.Watcher
	listenerRestartCh chan struct{}
	hardRestartCh     chan struct{}
	upgradeCh         chan struct{}
	msgFn             func(agentName, channelType, configuredID string, ch channels.Channel, msg channels.IncomingMessage)
}

// New creates a new Server with the given config and auth token.
func New(cfg *config.Config, token string) *Server {
	s := &Server{
		cfg:               cfg,
		token:             token,
		mux:               http.NewServeMux(),
		listenerRestartCh: make(chan struct{}, 1),
		hardRestartCh:     make(chan struct{}, 1),
		upgradeCh:         make(chan struct{}, 1),
	}
	s.connectionPolicy.Store(cfg.Connections)
	// Create auth store first — needed for both MCP deps and LLM token refresh.
	authPath := filepath.Join(store.SubDir(store.DirAuth), "credentials.json")
	authStore, _ := auth.NewFileStore(authPath)

	authResolver := makeAuthResolver()
	factory := llm.NewFactory(authResolver).WithProviderOptionsResolver(func(provider string) (llm.ProviderOptions, bool) {
		if s.cfg == nil || s.cfg.Models.Providers == nil {
			return llm.ProviderOptions{}, false
		}
		pc, ok := s.cfg.Models.Providers[strings.TrimSpace(provider)]
		if !ok {
			return llm.ProviderOptions{}, false
		}
		return llm.ProviderOptions{
			Auth:    pc.Auth,
			BaseURI: pc.BaseURI,
		}, true
	})
	if authStore != nil {
		factory.WithTokenSetter(authStore.Set)
	}
	s.agents = agent.NewManager(factory)
	s.llmFactory = factory
	if service, err := connections.Open(store.SubDir("connections")); err == nil {
		s.connections = service
		s.agents.SetConnectionService(service)
	} else {
		s.startupErr = errors.New("connection storage is unavailable; refusing to start credential intake")
	}
	if engine, err := preparation.Open(store.SubDir("preparation")); err == nil {
		s.preparation = engine
		s.agents.SetPreparationEngine(engine)
	} else {
		slog.Error("server: preparation storage unavailable")
	}

	// Initial reconcile from loaded config.
	s.agents.Reconcile(cfg)

	// Create scheduler (non-fatal if it fails).
	if sched, err := scheduler.New(s.agents, 0); err == nil {
		s.sched = sched
		s.sched.Reconcile(cfg)
	} else {
		slog.Warn("server: scheduler initialization failed; scheduled tasks disabled", "err", err)
	}

	s.channels = channels.NewManager()
	s.channels.SetConnectionService(s.connections)
	s.channels.SetConnectionValidator(func(_ context.Context, transport, endpoint string) error {
		policy := s.connectionPolicy.Load()
		if transport != "clickhouse" || policy == nil {
			return errors.New("connection endpoint is not allowed")
		}
		_, err := policy.Network.ValidateURL(endpoint)
		return err
	})
	adapter := func() clickhouseconn.Adapter {
		policy := s.connectionPolicy.Load()
		if policy == nil {
			return clickhouseconn.Adapter{}
		}
		return clickhouseconn.Adapter{Policy: policy.Network}
	}
	s.channels.SetCredentialValidator(func(ctx context.Context, target connections.Target, credential connections.Credential) error {
		return adapter().ValidateReadOnly(ctx, clickhouseconn.Target{Endpoint: target.Endpoint, Username: credential.Username}, clickhouseconn.NewCredentials(credential.Password))
	})
	s.channels.SetPostConnectHook(s.collectAfterConnect)
	if s.sched != nil {
		s.sched.SetTaskOutputDelivery(s.deliverTaskOutput)
	}
	s.sampler = NewProcSampler()
	cdpPort := cfg.Browser.CDPPort
	if cdpPort == 0 {
		cdpPort = config.DefaultCDPPort
	}
	s.brw = browser.NewManager(
		cfg.Browser.Binary,
		cdpPort,
		cfg.Browser.ProfileDir,
		cfg.Browser.Headless,
		config.EffectiveBrowserReuseTabs(cfg.Browser),
	)

	// Inject deps into MCP tool handlers.
	mcp.SetDeps(&mcp.Deps{
		Connections: s.connections,
		ClickHouse:  adapter,
		Agents:      s.agents,
		Scheduler:   s.sched,
		Channels:    s.channels,
		Browser:     s.brw,
		Auth:        authStore,
		Upgrade:     s.triggerUpgrade,
	})
	agent.SetToolClientFactory(mcp.NewAgentToolClient)
	agent.SetSessionMessageObserver(func(agentID, sessionID, role string) {
		wsBroadcast(wsEvent{Type: "session_message", AgentID: agentID, SessionID: sessionID, Role: role})
	})
	agent.SetSessionProcessingObserver(func(agentID, sessionID string, processing bool) {
		v := processing
		wsBroadcast(wsEvent{Type: "session_processing", AgentID: agentID, SessionID: sessionID, IsProcessing: &v})
	})

	// Install the log hub as the global slog handler, delegating to the
	// preconfigured default handler (stderr + file, when logging.Init() ran).
	// Only do this once — on restart slog.Default() is already globalHub,
	// so setting it as its own delegate would cause infinite recursion.
	if slog.Default().Handler() != globalHub {
		globalHub.setDelegate(slog.Default().Handler())
		slog.SetDefault(slog.New(globalHub))
		slog.Info("server: logger initialized", "component", "server")
	}

	// Set up config watcher.
	s.watcher = config.NewWatcher("")
	s.watcher.OnChange(func(newCfg *config.Config) {
		s.applyConfigReload(newCfg)
	})
	s.skillsWatcher = skills.NewWatcher()
	s.skillsWatcher.OnChange(func() {
		mcp.SyncLiveServer(s.cfg)
	})

	s.registerRoutes()
	return s
}

func (s *Server) applyConfigReload(newCfg *config.Config) {
	oldCfg := s.cfg
	s.connectionPolicy.Store(newCfg.Connections)
	if err := store.UpdateChannelMetadataState(oldCfg, newCfg, time.Now().UTC()); err != nil {
		slog.Warn("server: failed to update channel metadata state", "err", err)
	}
	mcp.SyncLiveServer(newCfg)
	s.agents.Reconcile(newCfg)
	if s.sched != nil {
		s.sched.Reconcile(newCfg)
	}
	if s.runCtx != nil && s.msgFn != nil && s.channels != nil {
		s.channels.Reconcile(s.runCtx, newCfg, s.msgFn)
	}
	cdpPort := newCfg.Browser.CDPPort
	if cdpPort == 0 {
		cdpPort = config.DefaultCDPPort
	}
	s.brw = browser.NewManager(
		newCfg.Browser.Binary,
		cdpPort,
		newCfg.Browser.ProfileDir,
		newCfg.Browser.Headless,
		config.EffectiveBrowserReuseTabs(newCfg.Browser),
	)
	deps := mcp.GetDeps()
	deps.Browser = s.brw
	s.cfg = newCfg
	if serverSettingsChanged(oldCfg, newCfg) {
		slog.Info("server: settings changed, rotating listener")
		select {
		case s.listenerRestartCh <- struct{}{}:
		default:
		}
	}
}

func (s *Server) registerRoutes() {
	mcpSrv := mcp.NewServer()
	mcp.SetLiveServer(mcpSrv)
	mcpHandler := mcp.HTTPHandler(mcpSrv)

	// Login does not require auth.
	s.mux.HandleFunc("/api/login", LoginHandler(s.token))

	// Health check (public) and WebSocket keepalive (auth via session cookie / ?token=).
	s.mux.HandleFunc("/api/health", healthHandler)
	s.mux.HandleFunc("/api/ws", wsHandler(s.token))

	// MCP endpoint: wrapped in bearer middleware.
	s.mux.Handle("/mcp", BearerMiddleware(s.token, mcpHandler))
	s.mux.Handle("/mcp/", BearerMiddleware(s.token, mcpHandler))

	// Log stream SSE endpoint + history REST endpoint.
	s.mux.Handle("/api/logs", BearerMiddleware(s.token, http.HandlerFunc(logsHandler)))
	s.mux.Handle("/api/logs/history", BearerMiddleware(s.token, http.HandlerFunc(logsHistoryHandler)))
	s.mux.Handle("/api/version", BearerMiddleware(s.token, http.HandlerFunc(s.versionHandler)))
	s.mux.Handle("/api/version/upgrade", BearerMiddleware(s.token, http.HandlerFunc(s.versionUpgradeHandler)))

	// Daemons status + log-stream endpoints.
	s.mux.Handle("/api/daemons", BearerMiddleware(s.token, http.HandlerFunc(s.daemonsHandler)))
	s.mux.Handle("/api/daemons/logs", BearerMiddleware(s.token, http.HandlerFunc(s.daemonLogsHandler)))
	s.mux.Handle("/api/daemons/restart", BearerMiddleware(s.token, http.HandlerFunc(s.daemonRestartHandler)))

	// Web UI: SPA served from embedded web/dist.
	s.mux.Handle("/", webFileServer())
}

// ListenAndServe starts the server on the configured port.
// It returns only when the context is cancelled, an error occurs, or an
// explicit process restart is requested.
func (s *Server) ListenAndServe(ctx context.Context) error {
	if s.startupErr != nil {
		return s.startupErr
	}
	s.runCtx = ctx

	// Start config watcher in background.
	go func() {
		if err := s.watcher.Start(); err != nil {
			_ = err // Non-fatal; hot-reload just won't work.
		}
	}()
	go func() {
		if err := s.skillsWatcher.Start(); err != nil {
			_ = err // Non-fatal; skill hot-reload just won't work.
		}
	}()

	// Start scheduler.
	if s.sched != nil {
		s.sched.Start(ctx)
	}

	// Start process sampler — periodically collects CPU/RSS for all daemon PIDs.
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pids := []int{os.Getpid()}
				for _, cs := range s.channels.List() {
					if cs.Daemon != nil && cs.Daemon.PID > 0 {
						pids = append(pids, cs.Daemon.PID)
					}
				}
				s.sampler.Sample(pids)
			}
		}
	}()

	// Start channel integrations and route messages to agents.
	s.msgFn = func(agentName, channelType, configuredID string, ch channels.Channel, msg channels.IncomingMessage) {
		s.handleIncomingChannelMessage(ctx, agentName, channelType, configuredID, ch, msg)
	}
	s.channels.Reconcile(ctx, s.cfg, s.msgFn)
	s.loadSessionDeliveries()

	for {
		ln, err := s.listen()
		if err != nil {
			return err
		}
		s.httpSrv = &http.Server{Handler: s.mux}

		errCh := make(chan error, 1)
		go func(httpSrv *http.Server, ln net.Listener) {
			errCh <- httpSrv.Serve(ln)
		}(s.httpSrv, ln)

		var (
			listenerRestart bool
			hardRestart     bool
		)
		select {
		case <-ctx.Done():
		case <-s.listenerRestartCh:
			listenerRestart = true
		case <-s.hardRestartCh:
			hardRestart = true
		case <-s.upgradeCh:
		case err := <-errCh:
			if errors.Is(err, http.ErrServerClosed) {
				continue
			}
			return err
		}

		_ = s.httpSrv.Shutdown(context.Background())
		if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		if listenerRestart {
			continue
		}

		s.watcher.Stop()
		s.skillsWatcher.Stop()
		s.channels.Stop()
		if s.sched != nil {
			s.sched.Stop()
		}
		s.agents.Stop()

		if hardRestart {
			return ErrRestartRequired
		}
		return nil
	}
}

func (s *Server) handleIncomingChannelMessage(ctx context.Context, agentName, channelType, configuredID string, ch channels.Channel, msg channels.IncomingMessage) {
	runner, ok := s.agents.Get(agentName)
	if !ok {
		return
	}
	msgCtx := agent.WithChannelSession(ctx, channelType, configuredID, msg.Channel)
	msgCtx = agent.WithSessionSender(msgCtx, domain.NewMessageSender(msg.From, msg.SenderName, true))

	agentID := agentName
	if channelType == "slack" && msg.InstallationID != "" && msg.WorkspaceID != "" {
		msgCtx = connections.WithExecution(msgCtx, connections.Execution{
			Kind: connections.Interactive,
			Scope: connections.Scope{AgentID: agentID, InstallationID: msg.InstallationID,
				WorkspaceID: msg.WorkspaceID, ChannelID: msg.Channel, RootThreadID: msg.ThreadTS},
			Principal: connections.Principal{InstallationID: msg.InstallationID,
				WorkspaceID: msg.WorkspaceID, UserID: msg.From},
		})
	}
	channelCfg, _ := s.findChannelConfig(agentName, channelType, configuredID)
	sessionName := channelSessionNameForIncoming(agentID, channelCfg, msg)
	if sess, err := agent.NewSessionManager().GetOrCreateNamed(agentID, sessionName); err == nil && sess != nil {
		msgCtx = agent.WithSessionID(msgCtx, sess.ID)
		target := store.SessionChannel{
			Type:         msg.Type,
			ConfiguredID: configuredID,
			ID:           msg.Channel,
			ThreadTS:     strings.TrimSpace(msg.ThreadTS),
		}
		sessiontarget.Register(agentID, agentName, sess.ID, target, s.channels)
		if err := store.EnsureSessionChannelTarget(agentID, sess.ID, target); err != nil {
			slog.Warn("server: failed to update session channels config", "session", sess.ID, "err", err)
		}
	}

	var stopTyping context.CancelFunc
	if ts, ok := ch.(channels.TypingSender); ok && ts.ShowTyping() {
		_ = ts.SendTyping(msg.Channel, false)
		typingCtx, cancel := context.WithCancel(ctx)
		stopTyping = cancel
		go func() {
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			defer ts.SendTyping(msg.Channel, true) //nolint:errcheck
			for {
				select {
				case <-typingCtx.Done():
					return
				case <-ticker.C:
					_ = ts.SendTyping(msg.Channel, false)
				}
			}
		}()
	}

	var assistantStatus *slackRunStatus
	if channelType == "slack" && config.BoolOr(channelCfg.ShowTyping, true) && strings.TrimSpace(msg.ThreadTS) != "" {
		if as, ok := ch.(channels.AssistantStatusSender); ok {
			assistantStatus = newSlackRunStatus(as, msg.Channel, msg.ThreadTS)
			assistantStatus.Start()
		}
	}

	rOpts := agent.RunOverrides{
		Model:         msg.Model,
		Fallbacks:     msg.Fallbacks,
		RestrictTools: msg.RestrictTools,
		DisabledTools: msg.DisabledTools,
	}

	// If this message quotes another message, include the quoted author/text
	// in the message text so it becomes part of the prompt/session history.
	if strings.TrimSpace(msg.QuoteAuthor) != "" && strings.TrimSpace(msg.QuoteText) != "" {
		// Prefix the incoming user text with the quoted line and author.
		msg.Text = fmt.Sprintf("%s: %s\n\n%s", msg.QuoteAuthor, msg.QuoteText, msg.Text)
	}

	slackStreamer := newSlackThreadStreamer(channelType, ch, msg)
	if slackStreamer != nil {
		slackStreamer.summarize = func(ctx context.Context, model, answer string) (string, error) {
			return summarizeSlackAnswer(ctx, s.llmFactory, model, answer)
		}
	}
	if slackStreamer != nil {
		rOpts.SuppressDelivery = true
		execution, _ := connections.ExecutionFromContext(msgCtx)
		rOpts.DeferAnswerPersistence = execution.Personal()
	}

	runner.PromptMediaWithOverrides(msgCtx, msg.Text, msg.MediaURL, rOpts, func(e agent.StreamEvent) {
		switch e.Type {
		case agent.StreamEventError:
			if assistantStatus != nil {
				assistantStatus.BeforeTerminal()
			}
			delivered := slackStreamer != nil && slackStreamer.SendPlain("Unable to complete this request.")
			if stopTyping != nil {
				stopTyping()
			}
			if assistantStatus != nil {
				assistantStatus.Finish(delivered)
			}
		case agent.StreamEventStop:
			if assistantStatus != nil {
				assistantStatus.BeforeTerminal()
			}
			delivered := slackStreamer != nil && slackStreamer.SendPlain("Stopped.")
			if stopTyping != nil {
				stopTyping()
			}
			if assistantStatus != nil {
				assistantStatus.Finish(delivered)
			}
		case agent.StreamEventDone:
			if assistantStatus != nil {
				assistantStatus.BeforeTerminal()
			}
			delivered := false
			if slackStreamer != nil && agent.ShouldDeliverReply(e.Text) {
				delivered = slackStreamer.SendAnswer(msgCtx, e.Model, e.Text)
				if delivered && rOpts.DeferAnswerPersistence {
					if sessionID, ok := agent.SessionIDFromContext(msgCtx); ok {
						if err := agent.AppendMessageToSessionWithSender(agentID, sessionID, domain.MessageRoleAssistant, e.Text, nil); err != nil {
							slog.Warn("server: failed to record delivered answer")
						}
					}
				}
			}
			if stopTyping != nil {
				stopTyping()
			}
			if assistantStatus != nil {
				assistantStatus.Finish(delivered)
			}
		}
	})
}

func (s *Server) findChannelConfig(agentName, channelType, configuredID string) (config.ChannelConfig, bool) {
	if s.cfg == nil {
		return config.ChannelConfig{}, false
	}
	for _, ac := range s.cfg.Agents {
		if ac.Name != agentName {
			continue
		}
		for _, cc := range ac.Channels {
			if cc.Type == channelType && cc.ID == configuredID {
				return cc, true
			}
		}
	}
	return config.ChannelConfig{}, false
}

func channelSessionName(cc config.ChannelConfig, msg channels.IncomingMessage) string {
	base := msg.Type + ":" + msg.Channel
	if msg.Type != "slack" {
		return base
	}
	threadTS := strings.TrimSpace(msg.ThreadTS)
	if threadTS == "" {
		return base
	}
	if config.BoolOr(cc.SeparateTopLevelSessions, false) {
		return base + ":" + threadTS
	}
	return base
}

func channelSessionNameForIncoming(agentID string, cc config.ChannelConfig, msg channels.IncomingMessage) string {
	sessionName := channelSessionName(cc, msg)
	if msg.Type != "slack" || !msg.IsThreadReply || !config.BoolOr(cc.SeparateTopLevelSessions, false) {
		return sessionName
	}
	baseName := msg.Type + ":" + msg.Channel
	if sessionName == baseName {
		return sessionName
	}
	if store.FindSessionPath(agentID, sessionName) == "" && store.FindSessionPath(agentID, baseName) != "" {
		return baseName
	}
	return sessionName
}

type slackThreadStreamer struct {
	thread    channels.ThreadMessageSender
	answer    slackAnswerSender
	summarize func(context.Context, string, string) (string, error)
	channel   string
	threadTS  string
}

type slackAnswerSender interface {
	SendThreadPlainText(channel, threadTS, text string) error
	SendThreadMarkdownFile(ctx context.Context, channel, threadTS, introduction, answer string) error
}

func newSlackThreadStreamer(channelType string, ch channels.Channel, msg channels.IncomingMessage) *slackThreadStreamer {
	if channelType != "slack" || strings.TrimSpace(msg.ThreadTS) == "" {
		return nil
	}
	thread, ok := ch.(channels.ThreadMessageSender)
	if !ok {
		return nil
	}
	streamer := &slackThreadStreamer{
		thread:   thread,
		channel:  msg.Channel,
		threadTS: strings.TrimSpace(msg.ThreadTS),
	}
	if answer, ok := ch.(slackAnswerSender); ok {
		streamer.answer = answer
	}
	return streamer
}

func (s *slackThreadStreamer) SendAnswer(ctx context.Context, model, answer string) bool {
	if s == nil || strings.TrimSpace(answer) == "" {
		return false
	}
	if !shouldAttachSlackAnswer(answer) || s.answer == nil {
		return s.SendPlain(answer)
	}
	introduction := "Full answer attached."
	if s.summarize != nil {
		if summary, err := s.summarize(ctx, model, answer); err == nil && summary != "" {
			introduction = summary
		} else if err != nil {
			slog.Warn("server: failed to summarize Slack answer", "err", err)
		}
	}
	if err := s.answer.SendThreadMarkdownFile(ctx, s.channel, s.threadTS, introduction, answer); err != nil {
		slog.Warn("server: failed to upload Slack answer", "channel", s.channel, "thread", s.threadTS, "err", err)
		return s.SendPlain(answer)
	}
	return true
}

func (s *slackThreadStreamer) SendPlain(answer string) bool {
	if s == nil || answer == "" {
		return false
	}
	for _, part := range splitSlackPlainText(answer, 3900) {
		var err error
		if s.answer != nil {
			err = s.answer.SendThreadPlainText(s.channel, s.threadTS, part)
		} else {
			_, err = s.thread.SendThreadMessageAndGetID(s.channel, s.threadTS, part)
		}
		if err != nil {
			slog.Warn("server: failed to send Slack answer", "channel", s.channel, "thread", s.threadTS, "err", err)
			return false
		}
	}
	return true
}

func (s *Server) listen() (net.Listener, error) {
	port := s.cfg.Server.Port
	if port == 0 {
		port = 16677
	}

	host := "127.0.0.1"
	if config.EffectiveServerExternalAccess(s.cfg.Server) {
		host = "0.0.0.0"
	}
	addr := fmt.Sprintf("%s:%d", host, port)

	if s.cfg.Server.NoTLS {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("listening on %s: %w", addr, err)
		}
		return ln, nil
	}

	var tlsCert, tlsKey string
	if s.cfg.Server.TLS != nil {
		tlsCert = s.cfg.Server.TLS.Cert
		tlsKey = s.cfg.Server.TLS.Key
	}
	cert, err := LoadOrGenerateTLS(tlsCert, tlsKey)
	if err != nil {
		return nil, fmt.Errorf("loading TLS: %w", err)
	}
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	ln, err := tls.Listen("tcp", addr, tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("listening on %s: %w", addr, err)
	}
	return ln, nil
}

func (s *Server) triggerUpgrade(_ context.Context, version string) error {
	if update.EmulationActive() {
		return nil
	}
	exePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("locating executable: %w", err)
	}
	if err := update.StartHelper(update.HelperRequest{
		TargetPath:  exePath,
		WaitPID:     os.Getpid(),
		Version:     version,
		RestartArgs: append([]string{}, os.Args[1:]...),
		Repo:        update.DefaultRepo,
		APIBase:     update.DefaultAPIBase,
	}); err != nil {
		return err
	}
	select {
	case s.upgradeCh <- struct{}{}:
	default:
	}
	return nil
}

func tlsConfigChanged(a, b *config.TLSConfig) bool {
	if a == nil && b == nil {
		return false
	}
	if a == nil || b == nil {
		return true
	}
	return *a != *b
}

// serverSettingsChanged reports whether a config change affects server-level
// settings that require a restart (port, TLS mode, bind address).
func serverSettingsChanged(oldCfg, newCfg *config.Config) bool {
	return oldCfg.Server.Port != newCfg.Server.Port ||
		config.EffectiveServerExternalAccess(oldCfg.Server) != config.EffectiveServerExternalAccess(newCfg.Server) ||
		oldCfg.Server.NoTLS != newCfg.Server.NoTLS ||
		tlsConfigChanged(oldCfg.Server.TLS, newCfg.Server.TLS)
}

// Addr returns the server address string.
func (s *Server) Addr() string {
	port := s.cfg.Server.Port
	if port == 0 {
		port = 16677
	}
	scheme := "https"
	if s.cfg.Server.NoTLS {
		scheme = "http"
	}
	return fmt.Sprintf("%s://localhost:%d", scheme, port)
}

// Agents returns the agent manager.
func (s *Server) Agents() *agent.Manager { return s.agents }

func (s *Server) deliverTaskOutput(agentName, route, text string) error {
	route = strings.TrimSpace(route)
	if route == "" || strings.EqualFold(route, "silent") {
		return nil
	}
	if strings.HasPrefix(route, "session:") {
		sessionRef := strings.TrimSpace(strings.TrimPrefix(route, "session:"))
		if sessionRef == "" {
			return fmt.Errorf("task target session is required")
		}
		if store.FindSessionPath(agentName, sessionRef) != "" {
			return agent.AppendReplyToSession(agentName, sessionRef, text)
		}
		sess, err := agent.NewSessionManager().GetOrCreateNamed(agentName, sessionRef)
		if err != nil {
			return fmt.Errorf("resolving task target session %q: %w", sessionRef, err)
		}
		return agent.AppendReplyToSession(agentName, sess.ID, text)
	}
	parts := strings.SplitN(route, ":", 3)
	if len(parts) != 3 {
		return fmt.Errorf("invalid task target %q", route)
	}
	channelType := strings.TrimSpace(parts[0])
	configuredID := strings.TrimSpace(parts[1])
	targetID := strings.TrimSpace(parts[2])
	if channelType == "" {
		return fmt.Errorf("task target channel type is required")
	}
	if configuredID == "" {
		return fmt.Errorf("task target configured channel id is required")
	}
	if targetID == "" {
		return fmt.Errorf("task target delivery id is required")
	}
	return s.channels.SendOnConfiguredChannel(agentName, channelType, configuredID, targetID, text)
}

func stageOutgoingMedia(channelType, sourcePath string) (string, error) {
	return channels.StageOutgoingMedia(channelType, sourcePath)
}

// loadSessionDeliveries reads all persisted session channel configs and
// registers delivery functions so that sessions started from channels continue
// to route responses back to those channels after a server restart.
// Per-message registrations (Reconcile closure) will overwrite these with a
// more direct closure on the next inbound message.
func (s *Server) loadSessionDeliveries() {
	cfgs, err := store.FindAllSessionChannelsConfigs()
	if err != nil {
		slog.Warn("server: failed to load session channel configs", "err", err)
		return
	}
	for _, cfg := range cfgs {
		for _, ch := range cfg.Channels {
			sessiontarget.Register(cfg.AgentID, cfg.AgentID, cfg.SessionID, ch, s.channels)
		}
	}
	if len(cfgs) > 0 {
		slog.Info("server: loaded session channel deliveries", "sessions", len(cfgs))
	}
}
