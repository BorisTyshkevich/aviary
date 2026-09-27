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
	"sync"
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
	cfg                   *config.Config
	token                 string
	mux                   *http.ServeMux
	httpSrv               *http.Server
	runCtx                context.Context
	executionCancel       context.CancelFunc
	nonSlackIngressClosed atomic.Bool
	terminalDrainUntil    atomic.Int64
	agents                *agent.Manager
	llmFactory            *llm.Factory
	sched                 *scheduler.Scheduler
	channels              *channels.Manager
	connections           *connections.Service
	preparation           *preparation.Engine
	startupErr            error
	connectionPolicy      atomic.Pointer[config.ConnectionPolicyConfig]
	brw                   *browser.Manager
	sampler               *ProcSampler
	watcher               *config.Watcher
	skillsWatcher         *skills.Watcher
	listenerRestartCh     chan struct{}
	hardRestartCh         chan struct{}
	upgradeCh             chan struct{}
	msgFn                 func(agentName, channelType, configuredID string, ch channels.Channel, msg channels.IncomingMessage)
	routerReady           chan struct{}
	routerReadyOnce       sync.Once
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
		routerReady:       make(chan struct{}),
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
	s.channels.SetSlackAuthenticatedHook(func(route channels.SlackAuthenticatedRoute) {
		go s.recoverSlackForRoute(context.Background(), route)
	})
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
	// Keep channel transports and their outgoing dependencies alive through
	// terminal drain even when the process signal cancels ctx.
	channelCtx, channelCancel := context.WithCancel(context.WithoutCancel(ctx))
	defer channelCancel()
	s.runCtx = channelCtx
	// Channel runs already admitted by Socket Mode must outlive socket/root
	// cancellation long enough to select and deliver a terminal outcome.
	executionCtx, executionCancel := context.WithCancel(context.WithoutCancel(ctx))
	s.executionCancel = executionCancel
	defer executionCancel()

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
		s.sched.Start(executionCtx)
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
		s.handleIncomingChannelMessage(executionCtx, agentName, channelType, configuredID, ch, msg)
	}
	s.routerReadyOnce.Do(func() { close(s.routerReady) })
	s.channels.Reconcile(channelCtx, s.cfg, s.msgFn)
	go s.RecoverSlackCheckpoints(context.Background())
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

		if listenerRestart {
			shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 45*time.Second)
			_ = s.httpSrv.Shutdown(shutdownCtx)
			cancel()
			if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
				return err
			}
			continue
		}

		drainCtx, drainCancel := context.WithTimeout(context.WithoutCancel(ctx), 45*time.Second)
		if deadline, ok := drainCtx.Deadline(); ok {
			s.terminalDrainUntil.Store(deadline.UnixNano())
		}
		s.watcher.Stop()
		s.skillsWatcher.Stop()
		if s.sched != nil {
			s.sched.StopClaims()
		}
		// Shutdown closes the HTTP listener immediately, but waiting for active
		// requests must not postpone Socket Mode ingress quiescence.
		httpStopped := make(chan error, 1)
		go func() { httpStopped <- s.httpSrv.Shutdown(drainCtx) }()
		var socketCloseErr error
		if err := s.channels.QuiesceSlack(drainCtx); err != nil {
			slog.Warn("server: Slack ingress handoff incomplete", "err", err)
			if errors.Is(err, channels.ErrSlackSocketOpen) {
				socketCloseErr = err
			}
		}
		if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Warn("server: HTTP listener stopped with error", "err", err)
		}
		s.nonSlackIngressClosed.Store(true)
		s.agents.Stop()
		if s.sched != nil {
			stopped := make(chan struct{})
			go func() { s.sched.Stop(); close(stopped) }()
			select {
			case <-stopped:
			case <-drainCtx.Done():
				slog.Warn("server: scheduler stop incomplete", "err", drainCtx.Err())
			}
		}
		if err := s.agents.Drain(drainCtx); err != nil {
			slog.Warn("server: agent terminal drain incomplete", "err", err)
		}
		s.channels.Stop()
		channelCancel()
		select {
		case err := <-httpStopped:
			if err != nil {
				slog.Warn("server: HTTP shutdown incomplete", "err", err)
			}
		case <-drainCtx.Done():
			_ = s.httpSrv.Close()
		}
		drainCancel()
		if hardRestart && socketCloseErr != nil {
			return fmt.Errorf("cannot restart while old Slack socket may remain open: %w", socketCloseErr)
		}

		if hardRestart {
			return ErrRestartRequired
		}
		return nil
	}
}

// terminalContext preserves run values but gives terminal callbacks a bounded
// lifetime independent of user or process cancellation. During shutdown, the
// shared drain deadline caps each operation's normal budget.
func (s *Server) terminalContext(runCtx context.Context, budget time.Duration) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(budget)
	if drain := s.terminalDrainUntil.Load(); drain > 0 {
		if end := time.Unix(0, drain); end.Before(deadline) {
			deadline = end
		}
	}
	return context.WithDeadline(context.WithoutCancel(runCtx), deadline)
}

func (s *Server) handleIncomingChannelMessage(ctx context.Context, agentName, channelType, configuredID string, ch channels.Channel, msg channels.IncomingMessage) {
	if channelType != "slack" && s.nonSlackIngressClosed.Load() {
		s.sendAdmissionResendNotice(ch, msg)
		return
	}
	originalChannel, originalMessage := ch, msg
	runner, ok := s.agents.Get(agentName)
	if !ok {
		if _, _, valid := s.channels.RevalidateRoutedMessage(agentName, channelType, configuredID, msg); valid {
			s.sendAdmissionResendNotice(originalChannel, originalMessage)
		}
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
	baseMsgCtx := msgCtx
	attachSession := func(cc config.ChannelConfig, incoming channels.IncomingMessage) context.Context {
		turnCtx := baseMsgCtx
		sessionName := channelSessionNameForIncoming(agentID, cc, incoming)
		if sess, err := agent.NewSessionManager().GetOrCreateNamed(agentID, sessionName); err == nil && sess != nil {
			turnCtx = agent.WithSessionID(turnCtx, sess.ID)
			target := store.SessionChannel{Type: incoming.Type, ConfiguredID: configuredID,
				ID: incoming.Channel, ThreadTS: strings.TrimSpace(incoming.ThreadTS)}
			sessiontarget.Register(agentID, agentName, sess.ID, target, s.channels)
			if err := store.EnsureSessionChannelTarget(agentID, sess.ID, target); err != nil {
				slog.Warn("server: failed to update session channels config", "session", sess.ID, "err", err)
			}
		}
		return turnCtx
	}
	channelCfg, _ := s.findChannelConfig(agentName, channelType, configuredID)
	msgCtx = attachSession(channelCfg, msg)

	var stateMu sync.Mutex
	var terminalSelected bool
	var statusStarted bool
	var stopTyping context.CancelFunc
	var startTyping func()
	var assistantStatus *slackRunStatus
	var presenter *slackPresenter
	var checkpoint *agent.CheckpointHandle

	rOpts := agent.RunOverrides{
		Model:         msg.Model,
		Fallbacks:     msg.Fallbacks,
		RestrictTools: msg.RestrictTools,
		DisabledTools: msg.DisabledTools,
	}

	// Quoted context is part of the admitted prompt on either accepted attempt.
	if strings.TrimSpace(msg.QuoteAuthor) != "" && strings.TrimSpace(msg.QuoteText) != "" {
		msg.Text = fmt.Sprintf("%s: %s\n\n%s", msg.QuoteAuthor, msg.QuoteText, msg.Text)
	}

	configureRun := func(candidate channels.Channel, cc config.ChannelConfig, incoming channels.IncomingMessage) {
		startTyping = nil
		stopTyping = nil
		assistantStatus = nil
		statusStarted = false
		presenter = nil
		checkpoint = nil
		if ts, ok := candidate.(channels.TypingSender); ok {
			enabled := ts.ShowTyping()
			if channelType == "slack" {
				enabled = config.BoolOr(cc.ShowTyping, true)
			}
			if enabled {
				startTyping = func() {
					_ = ts.SendTyping(incoming.Channel, false)
					typingCtx, cancel := context.WithCancel(ctx)
					stopTyping = cancel
					go func() {
						ticker := time.NewTicker(10 * time.Second)
						defer ticker.Stop()
						defer ts.SendTyping(incoming.Channel, true) //nolint:errcheck
						for {
							select {
							case <-typingCtx.Done():
								return
							case <-ticker.C:
								_ = ts.SendTyping(incoming.Channel, false)
							}
						}
					}()
				}
			}
		}
		if channelType == "slack" && config.BoolOr(cc.ShowTyping, true) && strings.TrimSpace(incoming.ThreadTS) != "" {
			if sender, ok := candidate.(channels.AssistantStatusSender); ok {
				assistantStatus = newSlackRunStatus(sender, incoming.Channel, incoming.ThreadTS)
				statusCtx := msgCtx
				assistantStatus.contextFactory = func(budget time.Duration) (context.Context, context.CancelFunc) {
					return s.terminalContext(statusCtx, budget)
				}
			}
		}
		if channelType == "slack" && strings.TrimSpace(incoming.ThreadTS) != "" {
			if sender, ok := candidate.(slackPresenterSender); ok {
				presenter = newSlackPresenter(sender, incoming.Channel, incoming.ThreadTS, config.BoolOr(cc.ToolProgress, false))
				if cc.ToolProgressMaxCalls != nil && *cc.ToolProgressMaxCalls >= config.MinToolProgressMaxCalls && *cc.ToolProgressMaxCalls <= config.MaxToolProgressMaxCalls {
					presenter.maxCalls = *cc.ToolProgressMaxCalls
				}
				if cc.ToolProgressMaxChars != nil && *cc.ToolProgressMaxChars >= config.MinToolProgressMaxChars && *cc.ToolProgressMaxChars <= config.MaxToolProgressMaxChars {
					presenter.maxChars = *cc.ToolProgressMaxChars
				}
				presenter.summarize = func(ctx context.Context, model, answer string) (string, error) {
					return summarizeSlackAnswer(ctx, s.llmFactory, model, answer)
				}
				checkpoint = agent.NewSlackCheckpointHandle(agent.SlackCheckpoint{
					InstallationID: incoming.InstallationID, WorkspaceID: incoming.WorkspaceID,
					ConfiguredID: configuredID, ChannelID: incoming.Channel, RootThreadTS: incoming.ThreadTS,
				})
				handle := checkpoint
				presenter.hooks.ProgressCreated = handle.RecordProgressTimestamp
				presenter.hooks.NoticeAttempting = handle.RecordNoticeAttempt
				presenter.hooks.TerminalAccepted = func(result slackTerminalResult) error {
					return handle.RecordTerminal(agent.SlackDispositionHandled, result.CleanupPending, result.ProgressTimestamps, result.PromotedProgressTS, result.NoticeAttempted)
				}
				presenter.hooks.TerminalFinalized = func(result slackTerminalResult) {
					if err := handle.RecordTerminal(agent.SlackDisposition(result.Disposition), result.CleanupPending, result.ProgressTimestamps, result.PromotedProgressTS, result.NoticeAttempted); err != nil {
						slog.Warn("server: Slack terminal checkpoint update failed")
					}
				}
			}
		}
		rOpts.SuppressDelivery = presenter != nil
		rOpts.Checkpoint = checkpoint
		rOpts.DeferAnswerPersistence = false
		if presenter != nil {
			execution, _ := connections.ExecutionFromContext(msgCtx)
			rOpts.DeferAnswerPersistence = execution.Personal()
		}
	}
	configureRun(ch, channelCfg, msg)

	consumer := func(e agent.StreamEvent) {
		if e.Type == agent.StreamEventToolProgress {
			if presenter != nil && !e.Private && e.PublicTool != nil {
				presenter.Tool(*e.PublicTool)
			}
			return
		}
		if e.Type != agent.StreamEventDone && e.Type != agent.StreamEventStop && e.Type != agent.StreamEventError {
			return
		}
		stateMu.Lock()
		if terminalSelected {
			stateMu.Unlock()
			return
		}
		terminalSelected = true
		activeStatus := (*slackRunStatus)(nil)
		if statusStarted {
			activeStatus = assistantStatus
		}
		stopActivity := stopTyping
		stopTyping = nil
		selectedPresenter := presenter
		selectedCheckpoint := checkpoint
		selectedCtx := msgCtx
		deferPersistence := rOpts.DeferAnswerPersistence
		stateMu.Unlock()
		if stopActivity != nil {
			stopActivity()
		}
		if selectedPresenter == nil {
			if activeStatus != nil {
				activeStatus.Finish(false)
			}
			return
		}
		if e.Type == agent.StreamEventError && selectedCheckpoint != nil && !selectedCheckpoint.Initialized() {
			// The run never executed or posted progress. There is no durable
			// marker yet, so make one bounded fixed reply to its original target.
			noticeCtx, noticeCancel := s.terminalContext(selectedCtx, slackAnswerCallTimeout)
			_, noticeErr := selectedPresenter.sender.PostThreadTextContext(noticeCtx, selectedPresenter.channel,
				selectedPresenter.threadTS, "Unable to complete this request.")
			noticeCancel()
			if noticeErr != nil {
				slog.Warn("server: initial Slack checkpoint failure notice unavailable")
			}
			if activeStatus != nil {
				activeStatus.Finish(noticeErr == nil)
			}
			return
		}
		selectedPresenter.terminalContextFactory = func(budget time.Duration) (context.Context, context.CancelFunc) {
			return s.terminalContext(selectedCtx, budget)
		}
		kind := "done"
		switch e.Type {
		case agent.StreamEventStop:
			kind = "stop"
			if e.StopCause == agent.StopCauseRunner {
				kind = "interrupted"
			}
		case agent.StreamEventError:
			kind = "error"
		}
		result, first := selectedPresenter.Terminal(activeStatus, kind, e.Model, e.Text, e.AlreadyAnswered)
		if first && result.Outcome == slackOutcomeAnswer && deferPersistence {
			if sessionID, ok := agent.SessionIDFromContext(selectedCtx); ok {
				if err := agent.AppendMessageToSessionWithSender(agentID, sessionID, domain.MessageRoleAssistant, e.Text, nil); err != nil {
					slog.Warn("server: failed to record delivered answer")
				}
			}
		}
	}
	submit := func(candidate *agent.AgentRunner) agent.RunAdmission {
		return candidate.PromptMediaWithOverrides(msgCtx, msg.Text, msg.MediaURL, rOpts, consumer)
	}
	admission := submit(runner)
	if admission.Status == agent.AdmissionRejectedStopping {
		// Only rejected handoff is retried. An accepted run owns its outcome.
		if currentCh, routed, valid := s.channels.RevalidateRoutedMessage(agentName, channelType, configuredID, originalMessage); valid {
			if replacement, exists := s.agents.Get(agentName); exists {
				ch = currentCh
				msg.Model, msg.Fallbacks = routed.Model, routed.Fallbacks
				msg.RestrictTools, msg.DisabledTools = routed.RestrictTools, routed.DisabledTools
				rOpts.Model, rOpts.Fallbacks = routed.Model, routed.Fallbacks
				rOpts.RestrictTools, rOpts.DisabledTools = routed.RestrictTools, routed.DisabledTools
				channelCfg, _ = s.findChannelConfig(agentName, channelType, configuredID)
				msgCtx = attachSession(channelCfg, routed)
				configureRun(ch, channelCfg, routed)
				admission = submit(replacement)
			}
		}
		if admission.Status == agent.AdmissionRejectedStopping {
			s.sendAdmissionResendNotice(originalChannel, originalMessage)
			return
		}
	}
	// The accepted callback can arrive before Prompt returns. Serialize optional
	// activity startup with terminal selection so it never starts afterward.
	stateMu.Lock()
	if !terminalSelected {
		if startTyping != nil {
			startTyping()
		}
		if assistantStatus != nil {
			statusStarted = true
			assistantStatus.Start()
		}
	}
	stateMu.Unlock()
}

func (s *Server) sendAdmissionResendNotice(ch channels.Channel, msg channels.IncomingMessage) {
	ctx, cancel := s.terminalContext(context.Background(), 5*time.Second)
	defer cancel()
	const notice = "Restarting; please resend your request."
	if sender, ok := ch.(channels.ContextThreadMessageSender); ok {
		if err := sender.SendThreadPlainTextContext(ctx, msg.Channel, msg.ThreadTS, notice); err != nil {
			slog.Warn("server: could not send rejected-admission notice", "err", err)
		}
		return
	}
	done := make(chan error, 1)
	go func() {
		if sender, ok := ch.(channels.ThreadMessageSender); ok && msg.ThreadTS != "" {
			_, err := sender.SendThreadMessageAndGetID(msg.Channel, msg.ThreadTS, notice)
			done <- err
			return
		}
		done <- ch.Send(msg.Channel, notice)
	}()
	select {
	case err := <-done:
		if err != nil {
			slog.Warn("server: could not send rejected-admission notice", "err", err)
		}
	case <-ctx.Done():
		slog.Warn("server: rejected-admission notice timed out", "err", ctx.Err())
	}
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
