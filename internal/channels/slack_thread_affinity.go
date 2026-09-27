package channels

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/lsegal/aviary/internal/config"
	"github.com/lsegal/aviary/internal/store"
)

// Slack thread claims expire with their root timestamp. Expired roots cannot
// acquire a new claim, so pruning cannot transfer an old thread to a new bot.
const slackAffinityRetention = 90 * 24 * time.Hour

type slackThreadOwner struct {
	TeamID       string    `json:"team_id"`
	ChannelID    string    `json:"channel_id"`
	RootTS       string    `json:"root_ts"`
	BotUserID    string    `json:"bot_user_id"`
	AgentName    string    `json:"agent_name"`
	ConfiguredID string    `json:"configured_id"`
	EnabledAt    time.Time `json:"enabled_at,omitempty"`
}

type slackThreadAffinity struct {
	mu sync.Mutex
}

func (a *slackThreadAffinity) path(team, channel, root string) string {
	key := sha256.Sum256([]byte(team + "\x00" + channel + "\x00" + root))
	return filepath.Join(store.SubDir("channels"), "slack-thread-affinity", hex.EncodeToString(key[:])+".json")
}

func validSlackAffinityRoot(root string, now time.Time) bool {
	ts, ok := parseSlackTimestamp(root)
	return ok && !ts.After(now.Add(time.Minute)) && now.Sub(ts) <= slackAffinityRetention
}

// lookup returns nil for an unclaimed thread. A corrupt or unavailable record
// is an error and must never be treated as an unclaimed thread.
func (a *slackThreadAffinity) lookup(team, channel, root string) (*slackThreadOwner, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lookupLocked(team, channel, root)
}

func (a *slackThreadAffinity) lookupLocked(team, channel, root string) (*slackThreadOwner, error) {
	if team == "" || channel == "" || !validSlackAffinityRoot(root, time.Now()) {
		return nil, nil
	}
	data, err := os.ReadFile(a.path(team, channel, root))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var owner slackThreadOwner
	if err := json.Unmarshal(data, &owner); err != nil {
		return nil, err
	}
	if owner.TeamID != team || owner.ChannelID != channel || owner.RootTS != root || owner.BotUserID == "" || owner.AgentName == "" || owner.ConfiguredID == "" {
		return nil, fmt.Errorf("invalid Slack thread affinity record")
	}
	return &owner, nil
}

// claim persists the first owner before a message is dispatched. The manager
// shares this coordinator across physical Slack sockets in the process.
func (a *slackThreadAffinity) claim(want slackThreadOwner) (*slackThreadOwner, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !validSlackAffinityRoot(want.RootTS, time.Now()) {
		return nil, nil
	}
	if existing, err := a.lookupLocked(want.TeamID, want.ChannelID, want.RootTS); err != nil || existing != nil {
		return existing, err
	}
	path := a.path(want.TeamID, want.ChannelID, want.RootTS)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	data, err := json.Marshal(want)
	if err != nil {
		return nil, err
	}
	// A complete temporary file is linked into place without replacing a claim.
	tmp, err := os.CreateTemp(filepath.Dir(path), ".claim-*")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err = tmp.Write(data); err != nil {
		_ = tmp.Close()
		return nil, err
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return nil, err
	}
	if err = tmp.Close(); err != nil {
		return nil, err
	}
	if err = os.Link(tmp.Name(), path); errors.Is(err, os.ErrExist) {
		return a.lookupLocked(want.TeamID, want.ChannelID, want.RootTS)
	} else if err != nil {
		return nil, err
	}
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return &want, nil
}

func (a *slackThreadAffinity) prune() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	dir := filepath.Join(store.SubDir("channels"), "slack-thread-affinity")
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var owner slackThreadOwner
		if json.Unmarshal(data, &owner) != nil {
			continue // corrupt records stay fail-closed
		}
		if !validSlackAffinityRoot(owner.RootTS, time.Now()) {
			if err := os.Remove(path); err != nil {
				return err
			}
		}
	}
	return nil
}

type slackRouteCandidate struct {
	ch   *SlackChannel
	spec channelSpec
}

func (c *SlackChannel) affinityIdentity() (string, string) {
	c.identityMu.RLock()
	defer c.identityMu.RUnlock()
	return c.botUserID, c.teamID
}

func (m *Manager) slackCandidates() []slackRouteCandidate {
	m.mu.Lock()
	defer m.mu.Unlock()
	var candidates []slackRouteCandidate
	for _, shared := range m.slack {
		for _, spec := range shared.specs {
			candidates = append(candidates, slackRouteCandidate{shared.ch, spec})
		}
	}
	return candidates
}

func slackBotMention(text, botID string) bool {
	if botID == "" {
		return false
	}
	return isDirectMention(text, botID)
}

func addressedSlackCandidate(candidate slackRouteCandidate, msg IncomingMessage) bool {
	entries := candidate.ch.resolvedEntriesForRouting(candidate.spec.channelConfig.AllowFrom)
	botID, _ := candidate.ch.affinityIdentity()
	for _, entry := range entries {
		if !config.BoolOr(entry.Enabled, true) || !matchesAllowedGroup(entry.AllowedGroups, msg.Channel) {
			continue
		}
		if ((entry.RespondToMentions || len(entry.MentionPrefixes) == 0) && slackBotMention(msg.OriginalText, botID)) ||
			matchesSlackAffinityPrefix(msg.OriginalText, entry.MentionPrefixes) {
			return true
		}
	}
	return false
}

func matchesSlackAffinityPrefix(text string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.TrimSpace(prefix) == "" || strings.ContainsAny(prefix, "*?[") {
			continue
		}
		if matchesMentionPrefixes(text, []string{prefix}) {
			return true
		}
	}
	return false
}

func (m *Manager) addressedSlackTargets(team string, msg IncomingMessage) []slackRouteCandidate {
	var addressed []slackRouteCandidate
	for _, candidate := range m.slackCandidates() {
		_, candidateTeam := candidate.ch.affinityIdentity()
		if candidateTeam == team && shouldProcessIncomingMessage(candidate.spec.metadata, msg) && addressedSlackCandidate(candidate, msg) {
			addressed = append(addressed, candidate)
		}
	}
	return addressed
}

func ownerMatchesSlackCandidate(owner *slackThreadOwner, candidate slackRouteCandidate) bool {
	botID, _ := candidate.ch.affinityIdentity()
	return owner != nil && owner.BotUserID == botID &&
		owner.AgentName == candidate.spec.agentName && owner.ConfiguredID == candidate.spec.channelConfig.ID &&
		owner.EnabledAt.Equal(candidate.spec.metadata.EnabledAt)
}

func (m *Manager) routeSlackMessage(ch *SlackChannel, msg IncomingMessage, intake *slackConnectionIntake,
	msgFn func(agentName, channelType, configuredID string, ch Channel, msg IncomingMessage)) {
	candidates := m.slackCandidates()
	if strings.HasPrefix(msg.Channel, "D") {
		for _, candidate := range candidates {
			if candidate.ch != ch || !shouldProcessIncomingMessage(candidate.spec.metadata, msg) {
				continue
			}
			if routed, ok := routedSlackMessageOriginal(ch, candidate.spec, msg, false); ok {
				intake.ensureSetup(candidate.spec.agentName, routed)
				msgFn(candidate.spec.agentName, "slack", candidate.spec.channelConfig.ID, ch, routed)
			}
		}
		return
	}
	if ch.teamID == "" || msg.ThreadTS == "" {
		return
	}
	team := ch.teamID
	filtered := candidates[:0]
	for _, candidate := range candidates {
		_, candidateTeam := candidate.ch.affinityIdentity()
		if candidateTeam == team {
			filtered = append(filtered, candidate)
		}
	}
	candidates = filtered
	owner, err := m.affinity.lookup(ch.teamID, msg.Channel, msg.ThreadTS)
	if err != nil {
		ch.logf("slack: thread affinity unavailable: %v", err)
		return
	}
	var addressed []slackRouteCandidate
	var selected slackRouteCandidate
	var routed IncomingMessage
	authorized := 0
	for _, candidate := range candidates {
		if !shouldProcessIncomingMessage(candidate.spec.metadata, msg) || !addressedSlackCandidate(candidate, msg) {
			continue
		}
		addressed = append(addressed, candidate)
		if candidateRouted, ok := routedSlackMessageOriginal(candidate.ch, candidate.spec, msg, false); ok {
			selected = candidate
			routed = candidateRouted
			authorized++
		}
	}
	if authorized > 1 {
		ch.logf("slack: ambiguous explicit target channel=%s thread=%s", msg.Channel, msg.ThreadTS)
		return
	}
	if authorized == 1 {
		if selected.ch != ch {
			return
		}
		if !msg.IsEdited && owner == nil && config.BoolOr(selected.spec.channelConfig.ReplyToReplies, true) {
			_, err = m.affinity.claim(slackThreadOwner{TeamID: ch.teamID, ChannelID: msg.Channel,
				RootTS: msg.ThreadTS, BotUserID: ch.botUserID, AgentName: selected.spec.agentName,
				ConfiguredID: selected.spec.channelConfig.ID, EnabledAt: selected.spec.metadata.EnabledAt})
			if err != nil {
				ch.logf("slack: could not claim thread: %v", err)
				return
			}
		}
		intake.ensureSetup(selected.spec.agentName, routed)
		msgFn(selected.spec.agentName, "slack", selected.spec.channelConfig.ID, ch, routed)
		return
	}
	if len(addressed) > 0 {
		ch.logf("slack: denied explicit target channel=%s thread=%s", msg.Channel, msg.ThreadTS)
		return
	}
	if owner != nil {
		if !msg.IsThreadReply {
			return
		}
		for _, candidate := range candidates {
			if candidate.ch != ch || !ownerMatchesSlackCandidate(owner, candidate) ||
				!config.BoolOr(candidate.spec.channelConfig.ReplyToReplies, true) ||
				!shouldProcessIncomingMessage(candidate.spec.metadata, msg) {
				continue
			}
			if routed, ok := routedSlackMessageOriginal(ch, candidate.spec, msg, true); ok {
				intake.ensureSetup(candidate.spec.agentName, routed)
				msgFn(candidate.spec.agentName, "slack", candidate.spec.channelConfig.ID, ch, routed)
			}
		}
		return
	}
	// No claim exists. Catch-all policies retain their ordinary behavior.
	for _, candidate := range candidates {
		if candidate.ch != ch || !shouldProcessIncomingMessage(candidate.spec.metadata, msg) {
			continue
		}
		botID, _ := candidate.ch.affinityIdentity()
		if msg.IsThreadReply && !config.BoolOr(candidate.spec.channelConfig.ReplyToReplies, true) && !slackBotMention(msg.OriginalText, botID) {
			continue
		}
		if routed, ok := routedSlackMessageOriginal(ch, candidate.spec, msg, false); ok {
			intake.ensureSetup(candidate.spec.agentName, routed)
			msgFn(candidate.spec.agentName, "slack", candidate.spec.channelConfig.ID, ch, routed)
		}
	}
}

func routedSlackMessageOriginal(ch *SlackChannel, spec channelSpec, msg IncomingMessage, continuation bool) (IncomingMessage, bool) {
	return routedSlackMessageWithPolicy(ch, spec, msg, msg.OriginalText, continuation)
}

func routedSlackMessageWithPolicy(ch *SlackChannel, spec channelSpec, msg IncomingMessage, gateText string, continuation bool) (IncomingMessage, bool) {
	entries := ch.resolvedEntriesForRouting(spec.channelConfig.AllowFrom)
	var result allowResult
	if continuation {
		result = checkAllowedReplyContinuationText(entries, msg.From, msg.Channel, gateText, !strings.HasPrefix(msg.Channel, "D"))
	} else {
		botID, _ := ch.affinityIdentity()
		if botID == "" {
			botID = spec.channelConfig.ID
		}
		result = checkAllowed(entries, msg.From, msg.Channel, gateText, !strings.HasPrefix(msg.Channel, "D"), botID, false)
	}
	if !result.allowed {
		return IncomingMessage{}, false
	}
	routed := msg
	routed.RestrictTools = result.restrictTools
	routed.DisabledTools = spec.channelConfig.DisabledTools
	routed.Model = result.model
	if routed.Model == "" {
		routed.Model = firstNonEmpty(spec.channelConfig.Model, spec.agentModel)
	}
	routed.Fallbacks = result.fallbacks
	if len(routed.Fallbacks) == 0 {
		routed.Fallbacks = spec.channelConfig.Fallbacks
	}
	if len(routed.Fallbacks) == 0 {
		routed.Fallbacks = spec.agentFallbacks
	}
	return routed, true
}

func (m *Manager) claimSlackCommand(ch *SlackChannel, in slackIngress, selected channelSpec) bool {
	if in.IsDM || in.IsEdited || !config.BoolOr(selected.channelConfig.ReplyToReplies, true) {
		return true
	}
	msg := IncomingMessage{Type: "slack", From: in.UserID, Channel: in.ChannelID, ThreadTS: in.RootTS,
		OriginalText: in.CommandText}
	if ts, ok := parseSlackTimestamp(in.MessageTS); ok {
		msg.ReceivedAt = ts
	}
	addressed := m.addressedSlackTargets(ch.teamID, msg)
	authorized := make([]slackRouteCandidate, 0, len(addressed))
	for _, candidate := range addressed {
		if _, ok := routedSlackMessageOriginal(candidate.ch, candidate.spec, msg, false); ok {
			authorized = append(authorized, candidate)
		}
	}
	if len(authorized) != 1 || authorized[0].ch != ch || authorized[0].spec.agentName != selected.agentName ||
		authorized[0].spec.channelConfig.ID != selected.channelConfig.ID {
		if len(authorized) > 1 {
			ch.logf("slack: ambiguous connection command channel=%s thread=%s", in.ChannelID, in.RootTS)
		} else {
			ch.logf("slack: denied connection command target channel=%s thread=%s", in.ChannelID, in.RootTS)
		}
		return false
	}
	_, err := m.affinity.claim(slackThreadOwner{TeamID: ch.teamID, ChannelID: in.ChannelID, RootTS: in.RootTS,
		BotUserID: ch.botUserID, AgentName: selected.agentName, ConfiguredID: selected.channelConfig.ID,
		EnabledAt: selected.metadata.EnabledAt})
	if err != nil {
		ch.logf("slack: could not claim command thread: %v", err)
	}
	return err == nil
}
