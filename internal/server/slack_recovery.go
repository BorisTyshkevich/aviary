package server

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lsegal/aviary/internal/agent"
	"github.com/lsegal/aviary/internal/channels"
	"github.com/lsegal/aviary/internal/store"
)

func slackCheckpointFiles() []string {
	agents, err := os.ReadDir(store.SubDir(store.DirAgents))
	if err != nil {
		return nil
	}
	var paths []string
	for _, directory := range agents {
		if !directory.IsDir() {
			continue
		}
		entries, err := os.ReadDir(filepath.Join(store.SubDir(store.DirAgents), directory.Name(), "checkpoints"))
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
				paths = append(paths, filepath.Join(store.SubDir(store.DirAgents), directory.Name(), "checkpoints", entry.Name()))
			}
		}
	}
	return paths
}

func slackRouteMatchesCheckpoint(route channels.SlackAuthenticatedRoute, cp agent.RunCheckpoint) bool {
	meta := cp.Slack
	return slackRecoveryMetadataValid(meta) && cp.AgentName == route.AgentName && meta.ConfiguredID == route.ConfiguredID &&
		meta.InstallationID == route.InstallationID && meta.WorkspaceID == route.WorkspaceID &&
		meta.ChannelID != "" && meta.RootThreadTS != ""
}

func slackRecoveryMetadataValid(meta *agent.SlackCheckpoint) bool {
	if meta == nil {
		return false
	}
	switch meta.Disposition {
	case agent.SlackDispositionPending, agent.SlackDispositionHandled, agent.SlackDispositionUnconfirmed:
	default:
		return false
	}
	if meta.CleanupPending && meta.ProgressTS == "" {
		return false
	}
	if meta.ProgressTS == "" && len(meta.ProgressExtraTS) != 0 {
		return false
	}
	seen := make(map[string]bool, 1+len(meta.ProgressExtraTS))
	for _, ts := range meta.ProgressTimestamps() {
		if ts == "" || seen[ts] {
			return false
		}
		seen[ts] = true
	}
	return true
}

func setSlackRecoveryProgress(meta *agent.SlackCheckpoint, timestamps []string) {
	meta.ProgressTS, meta.ProgressExtraTS = "", nil
	if len(timestamps) != 0 {
		meta.ProgressTS = timestamps[0]
		meta.ProgressExtraTS = append([]string(nil), timestamps[1:]...)
	}
}

// RecoverSlackCheckpoints makes one bounded recovery pass over durable Slack
// work. Call it on startup, after authentication/reconnect, or explicitly.
func (s *Server) RecoverSlackCheckpoints(ctx context.Context) {
	if s.channels == nil {
		return
	}
	routes := s.channels.AuthenticatedSlackRoutes()
	for _, path := range slackCheckpointFiles() {
		if ctx.Err() != nil {
			return
		}
		cp, err := store.ReadJSON[agent.RunCheckpoint](path)
		if err != nil {
			if !os.IsNotExist(err) {
				slog.Warn("server: retaining unreadable checkpoint", "checkpoint", filepath.Base(path))
			}
			continue
		}
		if cp.Slack == nil {
			if cp.Overrides != nil && (cp.Overrides.SuppressDelivery || cp.Overrides.DeferAnswerPersistence) {
				s.dropTargetlessSlackCheckpoint(path)
			}
			continue
		}
		if cp.AgentName == "" || cp.Slack.InstallationID == "" ||
			cp.Slack.WorkspaceID == "" || cp.Slack.ChannelID == "" || cp.Slack.RootThreadTS == "" {
			slog.Warn("server: retaining Slack checkpoint with incomplete original target", "checkpoint", filepath.Base(path))
			continue
		}
		if !slackRecoveryMetadataValid(cp.Slack) {
			slog.Warn("server: retaining Slack checkpoint with invalid delivery state", "checkpoint", filepath.Base(path))
			continue
		}
		matched := false
		for _, route := range routes {
			if slackRouteMatchesCheckpoint(route, cp) && route.Current() {
				matched = true
				s.recoverSlackCheckpoint(ctx, route, path)
				break
			}
		}
		if !matched {
			slog.Warn("server: Slack checkpoint awaits authenticated matching route", "checkpoint", filepath.Base(path),
				"agent", cp.AgentName, "configured_id", cp.Slack.ConfiguredID)
		}
	}
}

func (s *Server) dropTargetlessSlackCheckpoint(path string) {
	release, claimed := agent.ClaimCheckpointRecovery(path, nil)
	if !claimed {
		return
	}
	defer release()
	cp, err := store.ReadJSON[agent.RunCheckpoint](path)
	if err != nil || cp.Slack != nil || cp.Overrides == nil ||
		(!cp.Overrides.SuppressDelivery && !cp.Overrides.DeferAnswerPersistence) {
		return
	}
	slog.Warn("server: dropping old Slack checkpoint without trusted target", "checkpoint", filepath.Base(path))
	if err := store.DeleteJSON(path); err != nil {
		slog.Warn("server: could not retire targetless Slack checkpoint", "checkpoint", filepath.Base(path))
	}
}

func (s *Server) recoverSlackForRoute(ctx context.Context, route channels.SlackAuthenticatedRoute) {
	if !route.Current() {
		return
	}
	for _, path := range slackCheckpointFiles() {
		if ctx.Err() != nil || !route.Current() {
			return
		}
		cp, err := store.ReadJSON[agent.RunCheckpoint](path)
		if err == nil && slackRouteMatchesCheckpoint(route, cp) {
			s.recoverSlackCheckpoint(ctx, route, path)
		}
	}
}

func (s *Server) recoverSlackCheckpoint(ctx context.Context, route channels.SlackAuthenticatedRoute, path string) {
	if !route.Current() {
		return
	}
	release, claimed := agent.ClaimCheckpointRecovery(path, func() {
		if route.Current() {
			go s.recoverSlackCheckpoint(context.Background(), route, path)
		}
	})
	if !claimed {
		return
	}
	defer release()
	cp, err := store.ReadJSON[agent.RunCheckpoint](path)
	if err != nil || !slackRouteMatchesCheckpoint(route, cp) || !route.Current() {
		return
	}
	s.recoverClaimedSlackCheckpoint(ctx, route, path, cp)
}

func slackRecoveryContext(ctx context.Context, route channels.SlackAuthenticatedRoute, budget time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(channels.WithSlackPreDispatchStop(ctx, route.PreDispatchStop()), budget)
}

func slackRecoveryNotice(meta *agent.SlackCheckpoint) string {
	if meta.Disposition == agent.SlackDispositionUnconfirmed {
		return "Delivery could not be confirmed. Please check this thread before retrying."
	}
	return "Interrupted; please resend your request."
}

func slackRecoveryRejected(err error) bool {
	var delivery *channels.SlackDeliveryError
	return errors.As(err, &delivery) && delivery.Rejected
}

func persistSlackRecovery(path string, cp *agent.RunCheckpoint) bool {
	if err := store.WriteJSON(path, cp); err != nil {
		slog.Warn("server: retaining Slack checkpoint after storage failure", "checkpoint", filepath.Base(path))
		return false
	}
	return true
}

func retireSlackRecovery(path string) {
	if err := store.DeleteJSON(path); err != nil {
		slog.Warn("server: could not retire handled Slack checkpoint", "checkpoint", filepath.Base(path))
	}
}

func (s *Server) recoverClaimedSlackCheckpoint(ctx context.Context, route channels.SlackAuthenticatedRoute, path string, cp agent.RunCheckpoint) {
	meta := cp.Slack
	if meta.Disposition == agent.SlackDispositionHandled {
		if meta.CleanupPending {
			s.recoverSlackCleanup(ctx, route, path, &cp)
		} else {
			retireSlackRecovery(path)
		}
		return
	}
	if !route.Current() {
		return
	}
	if timestamps := meta.ProgressTimestamps(); len(timestamps) != 0 {
		last := timestamps[len(timestamps)-1]
		callCtx, cancel := slackRecoveryContext(ctx, route, slackAnswerCallTimeout)
		err := route.Channel.EditThreadTextContext(callCtx, meta.ChannelID, last, channels.SlackReplyText(meta.ReplyPrefix, slackRecoveryNotice(meta)))
		cancel()
		if err == nil {
			// The edited final page is now the notice. Earlier pages still need
			// deletion; never delete the notice itself as progress cleanup.
			setSlackRecoveryProgress(meta, timestamps[:len(timestamps)-1])
			meta.Disposition, meta.CleanupPending = agent.SlackDispositionHandled, len(timestamps) > 1
			if persistSlackRecovery(path, &cp) {
				if meta.CleanupPending {
					s.recoverSlackCleanup(ctx, route, path, &cp)
				} else {
					retireSlackRecovery(path)
				}
			}
			return
		}
		if !slackRecoveryRejected(err) {
			meta.Disposition = agent.SlackDispositionUnconfirmed
			_ = persistSlackRecovery(path, &cp)
			slog.Warn("server: retaining Slack checkpoint after uncertain notice edit", "checkpoint", filepath.Base(path))
			return
		}
	}
	if meta.NoticeAttempted {
		slog.Warn("server: retaining Slack checkpoint after unconfirmed notice attempt", "checkpoint", filepath.Base(path))
		return
	}
	if !route.Current() {
		return
	}
	// Persist uncertainty before the first byte of a standalone notice can go
	// out. A crash or unknown transport result must never trigger a fresh post.
	body := channels.SlackReplyText(meta.ReplyPrefix, slackRecoveryNotice(meta))
	priorDisposition := meta.Disposition
	meta.Disposition, meta.NoticeAttempted = agent.SlackDispositionUnconfirmed, true
	if !persistSlackRecovery(path, &cp) {
		return
	}
	callCtx, cancel := slackRecoveryContext(ctx, route, slackAnswerCallTimeout)
	_, err := route.Channel.PostThreadTextContext(callCtx, meta.ChannelID, meta.RootThreadTS, body)
	cancel()
	if err != nil {
		if slackRecoveryRejected(err) {
			meta.Disposition, meta.NoticeAttempted = priorDisposition, false
			_ = persistSlackRecovery(path, &cp)
			slog.Warn("server: Slack recovery notice rejected; check route and posting permissions", "checkpoint", filepath.Base(path))
		} else {
			slog.Warn("server: retaining Slack checkpoint after unconfirmed notice post", "checkpoint", filepath.Base(path))
		}
		return
	}
	meta.Disposition, meta.CleanupPending, meta.NoticeAttempted = agent.SlackDispositionHandled, meta.ProgressTS != "", false
	if !persistSlackRecovery(path, &cp) {
		return
	}
	if meta.CleanupPending {
		s.recoverSlackCleanup(ctx, route, path, &cp)
	} else {
		retireSlackRecovery(path)
	}
}

func (s *Server) recoverSlackCleanup(ctx context.Context, route channels.SlackAuthenticatedRoute, path string, cp *agent.RunCheckpoint) {
	if cp.Slack.ProgressTS == "" || !route.Current() {
		slog.Warn("server: Slack progress cleanup awaits original message and authenticated route", "checkpoint", filepath.Base(path))
		return
	}
	for len(cp.Slack.ProgressTimestamps()) != 0 {
		if !route.Current() {
			return
		}
		remaining := cp.Slack.ProgressTimestamps()
		callCtx, cancel := slackRecoveryContext(ctx, route, slackProgressTimeout)
		err := route.Channel.DeleteThreadMessageContext(callCtx, cp.Slack.ChannelID, remaining[0])
		cancel()
		if err != nil {
			slog.Warn("server: retaining Slack checkpoint with pending progress cleanup", "checkpoint", filepath.Base(path))
			return
		}
		setSlackRecoveryProgress(cp.Slack, remaining[1:])
		cp.Slack.CleanupPending = cp.Slack.ProgressTS != ""
		if !persistSlackRecovery(path, cp) {
			return
		}
	}
	retireSlackRecovery(path)
}
