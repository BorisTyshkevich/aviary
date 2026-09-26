# ADR 0003: Remote MCP agent runtime and Slack interaction

- Status: Proposed
- Date: 2026-09-26
- Related: #6

## Context

Aviary's Slack channel currently uses Slack Socket Mode. Slack messages start the normal Go agent/LLM loop. The current agent tool client exposes Aviary's local in-process MCP tools.

Remote MCP adds an outbound Go MCP client to the agent path. It must be scoped to the Slack session/thread and must not couple the Slack runtime to the AdminUI/control-plane path.

## Decision

### 1. Keep Slack Socket Mode

Remote MCP does not require Slack HTTP push mode.

Aviary continues receiving Slack messages over the existing Socket Mode WebSocket. OAuth browser redirects use Aviary's public HTTP server, but Slack event delivery remains independent.

### 2. Handle connect as a deterministic thread configuration command

Aviary recognizes this command directly in the incoming Slack message before invoking the agent/LLM:

```text
@bot connect URL
```

For example:

```text
@bot connect https://mcp.cluster.environment.altinity.cloud
```

Parse the command from the original message and trusted Slack sender identity, not from enriched channel history, quoted tool results, or model output. Normal sender/channel access checks still apply. Validate the exact endpoint through remote MCP network policy before dialing.

The command manages one dynamic connection descriptor per thread and attempts MCP initialization/tool discovery. Connection management is not exposed as an agent/script tool. Status and disconnect are also deterministic configuration operations. Configured static MCP servers may coexist with the single dynamic connection.

### 3. Remote tools are attached to the current session/thread

The agent tool client composes local and remote tools using the incoming thread identity independently of the session used for conversation history. The agent may explore the full channel history permitted by Slack, but the current thread has only one authoritative dynamic MCP target.

For Slack, the connection context includes:

- Slack workspace/team;
- channel;
- root thread timestamp and originating Aviary agent/installation;
- Slack user for personal credentials.

A dynamic connection in one thread does not automatically appear in another thread.

Historical connection names and results are context only. Validate every call against the current thread attachment and its connection identity; never map a stale connection name to the current cluster merely because it occupies the same slot.

Thread participants share connection descriptors and visible conversation results under Slack's access rules. Each interactive turn carries its own trusted sender principal. Resolve personal credentials and authenticated tool discovery for that principal on every turn; never borrow the attachment creator's credentials or authenticated client. Bob's missing authorization must produce an ephemeral handoff to Bob, even if Alice already authorized the same thread attachment.

### 4. Explicit connect and tool-call authorization handoffs

During explicit connect:

```text
@bot connect URL (no agent/LLM run)
  -> MCP initialize / tools/list
  -> authorization required
  -> create OAuth transaction
  -> send ephemeral authorization link
  -> finish command; browser completes independently
```

During later use:

```text
remote tools/call
  -> authorization required
  -> create OAuth transaction
  -> send ephemeral authorization link
  -> stop current agent run
```

There is no automatic retry in the same turn.

After OAuth completes, Aviary posts a thread message asking the user to send a new message such as `continue`.

### 5. A new turn performs fresh remote discovery as needed

When the user sends the next message, Aviary constructs the session-aware tool client.

For each attached remote connection with a valid credential, Aviary initializes/reuses a transient MCP client and obtains the current tool list.

Remote tools are namespaced before being exposed to the LLM.

### 6. Remote client objects are transient and isolated

Do not share an authenticated MCP client/session across principals.

Client/session caching keys must include enough identity to prevent token/session crossover, including the remote resource and credential owner.

Clients are closed on session expiry, idle timeout, connection removal, or process shutdown.

### 7. Permissions apply to remote tools at list and call time

A remote tool must pass the same effective agent/per-message permission checks when:

- it is exposed to the model;
- it is invoked by name;
- it is invoked indirectly by scripts.

Directly supplying a namespaced tool name must not bypass the current session's connection or permissions.

Scheduled prompt and script jobs have no authority to use personal credentials. Only eligible static no-auth or shared-OAuth connections may contribute tools to a scheduled run. Enforce this again at invocation so direct names and scripts cannot bypass it. A scheduled run's reply thread or creator must not be used to impersonate an interactive Slack principal.

### 8. Channel-specific UX stays outside OAuth protocol logic

The OAuth/core layer reports structured authorization-required/completed state.

The Slack adapter is responsible for:

- sending the ephemeral authorization link to the correct user;
- posting the completion notification to the original thread.

OAuth state and token exchange logic do not depend on Slack message rendering.

### 9. Missing authorization during discovery does not block normal turns

When an ordinary message starts a turn, missing or expired authorization on an attached dynamic or static MCP connection does not block available local or other eligible tools. Show that the connection needs login and exclude its unavailable tools. The current descriptor remains authoritative; do not fall back to a previous cluster, another thread's connection, or another user's credential.

If authorization instead fails during an actual remote tool call, follow the stop-and-authorize behavior above. Do not automatically replay the failed call.

### 10. Reject replacement while a thread has an active turn

If `@bot connect NEW_URL` would replace a connection while any agent turn is active in the same thread, reject the replacement with an instruction to wait or stop that turn first. Keep the current attachment unchanged. Check turn activity and update the attachment atomically relative to starting a new turn; a check followed by an uncoordinated update is insufficient.

Activity is tracked by the originating Slack thread, independently of shared channel-history session IDs. Never change an active turn's connection target underneath it.

## Consequences

### Positive

- No new Slack webhook transport is required.
- No suspended agent continuation/checkpoint is required.
- Dynamic remote tool availability naturally follows thread/session state.
- Slack UX remains a channel adapter around generic remote-MCP/OAuth state.

### Negative

- `IncomingMessage` / agent context must carry Slack workspace/team identity.
- The tool client becomes more stateful and session-aware.
- Each new turn may require remote tool discovery or client reconstruction.
- Completion notifications are asynchronous relative to the original agent run.

## Rejected alternatives

### Make Slack OAuth an admin flow

Rejected because dynamic remote MCP authorization belongs to the user who requested the connection.

### Automatically resume the old agent goroutine after browser callback

Rejected because the run has already ended and replay can duplicate previous side effects.

### Agent-global remote MCP connection state

Rejected because concurrent Slack threads/users can connect to different resources.
