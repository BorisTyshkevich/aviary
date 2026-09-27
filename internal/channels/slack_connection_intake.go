package channels

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"

	"github.com/slack-go/slack"

	"github.com/lsegal/aviary/internal/connections"
)

// slackConnectionIntake owns the channel-specific setup conversation. The
// connection service owns target generations and durable prompt classification.
type slackConnectionIntake struct {
	channel          *SlackChannel
	service          *connections.Service
	validateEndpoint func(context.Context, string, string) error
	validatePassword func(context.Context, connections.Target, connections.Credential) error
	specs            []channelSpec
	promptMu         sync.Mutex
	startMu          sync.Mutex
	stopped          bool
	startOnce        sync.Once
	workers          sync.WaitGroup
	workerCtx        context.Context
	queues           [4]chan func()
}

func (i *slackConnectionIntake) start(ctx context.Context) {
	i.startMu.Lock()
	defer i.startMu.Unlock()
	if i.stopped {
		return
	}
	i.startOnce.Do(func() {
		i.workerCtx = ctx
		for n := range i.queues {
			queue := make(chan func(), 16)
			i.queues[n] = queue
			i.workers.Add(1)
			go func() {
				defer i.workers.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case work := <-queue:
						if ctx.Err() == nil {
							work()
						}
					}
				}
			}()
		}
	})
}

func (i *slackConnectionIntake) wait() {
	i.startMu.Lock()
	i.stopped = true
	i.startMu.Unlock()
	i.workers.Wait()
}

func (i *slackConnectionIntake) baseContext() context.Context {
	if i.workerCtx != nil {
		return i.workerCtx
	}
	return context.Background()
}

func (i *slackConnectionIntake) enqueue(key string, work func()) bool {
	i.startMu.Lock()
	if i.stopped || (i.workerCtx != nil && i.workerCtx.Err() != nil) {
		i.startMu.Unlock()
		return false
	}
	if i.queues[0] == nil {
		i.startMu.Unlock()
		work()
		return true
	} // direct adapter tests
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	queue := i.queues[h.Sum32()%uint32(len(i.queues))]
	i.startMu.Unlock()
	select {
	case queue <- work:
		return true
	default:
		i.channel.logf("slack: connection task queue full")
		return false
	}
}

// ensureSetup prompts the current sender when an ordinary turn has an attached
// target but no credential. It leaves the normal message route available.
func (i *slackConnectionIntake) ensureSetup(agentName string, msg IncomingMessage) {
	i.enqueue(msg.Channel+"\x00"+msg.ThreadTS, func() { i.ensureSetupNow(agentName, msg) })
}

func (i *slackConnectionIntake) ensureSetupNow(agentName string, msg IncomingMessage) {
	if i.service == nil || i.channel.botUserID == "" || i.channel.teamID == "" || msg.ThreadTS == "" {
		return
	}
	scope := connections.Scope{AgentID: agentName, InstallationID: i.channel.botUserID,
		WorkspaceID: i.channel.teamID, ChannelID: msg.Channel, RootThreadID: msg.ThreadTS}
	target, ok := i.service.Current(scope)
	if !ok || target.Transport != "clickhouse" {
		return
	}
	principal := connections.Principal{InstallationID: scope.InstallationID, WorkspaceID: scope.WorkspaceID, UserID: msg.From}
	i.promptMu.Lock()
	defer i.promptMu.Unlock()
	if i.service.HasCredential(principal, target) || i.service.HasActivePrompt(principal, target) {
		return
	}
	_ = i.startSetup(principal, target)
}

func (i *slackConnectionIntake) redactReference(channelID, rootTS string) bool {
	return i.service != nil && i.service.ClassifyReply(i.channel.botUserID, i.channel.teamID, channelID, rootTS)
}

func (i *slackConnectionIntake) handle(in slackIngress) bool {
	if in.IsDM && i.redactReference(in.ChannelID, in.RootTS) {
		// Even replies from the wrong user, edited replies and late replies to a
		// completed prompt remain outside the ordinary message path.
		// A direct reference fetch can omit thread_ts. Classify the reply's own
		// timestamp too, so that read remains redacted after restart.
		if i.service != nil && in.MessageTS != "" && in.MessageTS != in.RootTS {
			_ = i.service.TombstonePrompt(i.channel.botUserID, i.channel.teamID, in.ChannelID, in.MessageTS)
		}
		if in.IsEdited || in.MessageTS == in.RootTS {
			return true
		}
		i.enqueue(in.ChannelID+"\x00"+in.RootTS, func() { i.handleSetupReply(in) })
		return true
	}
	cmd, recognized, parseErr := parseSlackConnectionCommand(in.Text, i.channel.botUserID, in.IsDM)
	if !recognized {
		return false
	}
	if in.IsEdited {
		return true
	}
	var selected *channelSpec
	for n := range i.specs {
		spec := &i.specs[n]
		msg := IncomingMessage{Type: "slack", From: in.UserID, Channel: in.ChannelID,
			ThreadTS: in.RootTS, IsThreadReply: in.RootTS != in.MessageTS, Text: canonicalBotMention(in.Text, i.channel.botUserID)}
		if ts, ok := parseSlackTimestamp(in.MessageTS); ok {
			msg.ReceivedAt = ts
		}
		if !shouldProcessIncomingMessage(spec.metadata, msg) {
			continue
		}
		if _, ok := routedSlackMessage(i.channel, *spec, msg); ok {
			if selected != nil {
				i.enqueue(in.ChannelID+"\x00"+in.RootTS, func() {
					i.reply(in.ChannelID, in.RootTS, "Connection command matches multiple agents. Ask an administrator to make channel routing unambiguous.")
				})
				return true
			}
			selected = spec
		}
	}
	if selected == nil {
		// The ordinary path enforces the same sender/channel policy. Consuming
		// here avoids presenting a rejected command to another agent.
		return true
	}
	if parseErr != nil {
		i.enqueue(in.ChannelID+"\x00"+in.RootTS, func() { i.reply(in.ChannelID, in.RootTS, parseErr.Error()) })
		return true
	}
	if i.service == nil || i.channel.botUserID == "" || i.channel.teamID == "" || in.RootTS == "" {
		i.enqueue(in.ChannelID+"\x00"+in.RootTS, func() { i.reply(in.ChannelID, in.RootTS, "Connection setup is unavailable.") })
		return true
	}
	i.enqueue(in.ChannelID+"\x00"+in.RootTS, func() { i.executeCommand(in, *selected, cmd) })
	return true
}

func canonicalBotMention(text, botID string) string {
	prefix := "<@" + botID + "|"
	if botID == "" || !strings.HasPrefix(text, prefix) {
		return text
	}
	end := strings.IndexByte(text, '>')
	if end < 0 {
		return text
	}
	return "<@" + botID + ">" + text[end+1:]
}

func (i *slackConnectionIntake) executeCommand(in slackIngress, selected channelSpec, cmd slackConnectionCommand) {
	scope := connections.Scope{AgentID: selected.agentName, InstallationID: i.channel.botUserID,
		WorkspaceID: i.channel.teamID, ChannelID: in.ChannelID, RootThreadID: in.RootTS}
	principal := connections.Principal{InstallationID: scope.InstallationID, WorkspaceID: scope.WorkspaceID, UserID: in.UserID}
	switch cmd.verb {
	case "status":
		target, ok := i.service.Current(scope)
		if !ok {
			i.reply(in.ChannelID, in.RootTS, "No connection is attached to this thread.")
		} else if i.service.HasCredential(principal, target) {
			i.reply(in.ChannelID, in.RootTS, fmt.Sprintf("%s is attached. Your credentials are ready.", target.Endpoint))
		} else {
			i.reply(in.ChannelID, in.RootTS, fmt.Sprintf("%s is attached. You need private setup to use it.", target.Endpoint))
		}
	case "disconnect":
		if err := i.service.Disconnect(scope); err != nil {
			i.reply(in.ChannelID, in.RootTS, connectionActionError(err))
		} else {
			i.reply(in.ChannelID, in.RootTS, "Connection detached from this thread.")
		}
	case "connect":
		if cmd.transport == "mcp" {
			i.reply(in.ChannelID, in.RootTS, "MCP transport is not available yet.")
			return
		}
		ctx, cancel := context.WithTimeout(i.baseContext(), 5*time.Second)
		defer cancel()
		if i.validateEndpoint == nil || i.validateEndpoint(ctx, cmd.transport, cmd.endpoint) != nil {
			i.reply(in.ChannelID, in.RootTS, "The endpoint is not allowed by connection policy.")
			return
		}
		target, _, err := i.service.Select(scope, cmd.transport, cmd.endpoint)
		if err != nil {
			i.reply(in.ChannelID, in.RootTS, connectionActionError(err))
			return
		}
		if i.service.HasCredential(principal, target) {
			i.reply(in.ChannelID, in.RootTS, "Connection attached. Your credentials are ready.")
			return
		}
		i.promptMu.Lock()
		if i.service.HasActivePrompt(principal, target) {
			i.promptMu.Unlock()
			i.reply(in.ChannelID, in.RootTS, "Connection attached. Complete the existing private setup prompt.")
			return
		}
		setupErr := i.startSetup(principal, target)
		i.promptMu.Unlock()
		if setupErr != nil {
			i.reply(in.ChannelID, in.RootTS, "Connection attached, but private setup could not start. Retry connect in this thread.")
		} else {
			i.reply(in.ChannelID, in.RootTS, "Connection attached. Complete private setup in the bot DM.")
		}
	}
}

func connectionActionError(err error) string {
	if err == connections.ErrBusy {
		return "A turn is active in this thread. Wait for it to finish before changing the connection."
	}
	return "Connection action failed."
}

func (i *slackConnectionIntake) reply(channel, thread, text string) {
	ctx, cancel := context.WithTimeout(i.baseContext(), 5*time.Second)
	defer cancel()
	resolved, err := i.channel.resolveDeliveryTarget(ctx, channel)
	if err != nil {
		return
	}
	if thread != "" {
		_, _, _ = i.channel.client.PostMessageContext(ctx, resolved, slack.MsgOptionText(text, false), slack.MsgOptionTS(thread))
	} else {
		_, _, _ = i.channel.client.PostMessageContext(ctx, resolved, slack.MsgOptionText(text, false))
	}
}

func (i *slackConnectionIntake) startSetup(principal connections.Principal, target connections.Target) error {
	ctx, cancel := context.WithTimeout(i.baseContext(), 5*time.Second)
	defer cancel()
	dm, err := i.channel.openDirectConversation(ctx, principal.UserID)
	if err != nil {
		return err
	}
	username := ""
	if user, err := i.channel.client.GetUserInfoContext(ctx, principal.UserID); err == nil && user != nil {
		username = strings.TrimSpace(user.Profile.Email)
	}
	label := fmt.Sprintf("Database setup for %s in channel %s, thread %s. ", target.Endpoint, target.Scope.ChannelID, target.Scope.RootThreadID)
	if username != "" {
		label += fmt.Sprintf("Proposed username: %s. Reply `use proposed` in this thread to confirm, or reply with another database username.", username)
	} else {
		label += "Reply in this thread with your database username."
	}
	return i.postClassifiedPrompt(dm, label, connections.Prompt{Principal: principal, DMChannelID: dm,
		Target: target, Stage: "username", Username: username, ExpiresAt: time.Now().Add(15 * time.Minute)})
}

// Slack assigns the thread timestamp when posting. First post an inert message,
// persist its classification, and only then edit it into an actionable prompt.
func (i *slackConnectionIntake) postClassifiedPrompt(dm, text string, prompt connections.Prompt) error {
	ctx, cancel := context.WithTimeout(i.baseContext(), 5*time.Second)
	defer cancel()
	_, root, err := i.channel.client.PostMessageContext(ctx, dm, slack.MsgOptionText("Preparing private setup...", false))
	if err != nil {
		return err
	}
	prompt.DMRootID = root
	if err := i.service.PutPrompt(prompt); err != nil {
		_ = i.service.TombstonePrompt(prompt.Principal.InstallationID, prompt.Principal.WorkspaceID, dm, root)
		_, _, _ = i.channel.client.DeleteMessageContext(ctx, dm, root)
		return err
	}
	_, _, _, err = i.channel.client.UpdateMessageContext(ctx, dm, root, slack.MsgOptionText(text, false))
	if err != nil {
		_ = i.service.FinishPrompt(prompt.Principal, dm, root)
		_, _, _ = i.channel.client.DeleteMessageContext(ctx, dm, root)
		return err
	}
	return nil
}

func (i *slackConnectionIntake) handleSetupReply(in slackIngress) {
	principal := connections.Principal{InstallationID: i.channel.botUserID, WorkspaceID: i.channel.teamID, UserID: in.UserID}
	prompt, ok := i.service.PromptFor(principal, in.ChannelID, in.RootTS)
	if !ok {
		return
	}
	if prompt.Stage == "password" {
		if i.validatePassword == nil {
			i.reply(in.ChannelID, in.RootTS, "Database authentication is unavailable. Retry after it is configured.")
			return
		}
		ctx, cancel := context.WithTimeout(i.baseContext(), 15*time.Second)
		defer cancel()
		err := i.service.CompletePassword(ctx, principal, in.ChannelID, in.RootTS, in.Text, i.validatePassword)
		if err == nil {
			i.reply(in.ChannelID, in.RootTS, "Credentials saved. Return to the original thread to continue.")
		} else {
			i.reply(in.ChannelID, in.RootTS, "Credentials could not be saved. Check the connection and start setup again.")
		}
		return
	}
	if prompt.Stage != "username" {
		return
	}
	username := strings.TrimSpace(in.Text)
	if username == "use proposed" {
		username = prompt.Username
	}
	if username == "" || len(username) > 256 {
		i.reply(in.ChannelID, in.RootTS, "Reply with a database username, or `use proposed` when one is shown.")
		return
	}
	if err := i.service.FinishPrompt(principal, in.ChannelID, in.RootTS); err != nil {
		return
	}
	_ = i.postClassifiedPrompt(in.ChannelID, "Reply to this password prompt thread with only your database password. Your entire reply will be used exactly as sent.", connections.Prompt{
		Principal: principal, DMChannelID: in.ChannelID, Target: prompt.Target,
		Stage: "password", Username: username, ExpiresAt: time.Now().Add(15 * time.Minute),
	})
}
