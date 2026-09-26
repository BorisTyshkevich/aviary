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

### 2. Add an explicit local connect operation

Aviary exposes a local tool conceptually equivalent to:

```text
mcp_connect(endpoint)
```

Skills/prompts may guide the model to use this tool when the user explicitly requests a connection, for example:

```text
please connect to https://mcp.cluster.environment.altinity.cloud
```

The connection URL must come from explicit user intent and must pass remote MCP network policy.

The connect operation creates/updates the thread-scoped remote connection descriptor and attempts MCP initialization/tool discovery.

### 3. Remote tools are attached to the current session/thread

The agent tool client composes local and remote tools using the current Aviary session identity.

For Slack, the connection context includes:

- Slack workspace/team;
- channel;
- thread timestamp/session identity;
- Slack user for personal credentials.

A dynamic connection in one thread does not automatically appear in another thread.

### 4. The first authorization-required response ends the current turn

During explicit connect:

```text
mcp_connect
  -> MCP initialize / tools/list
  -> authorization required
  -> create OAuth transaction
  -> send ephemeral authorization link
  -> stop current agent run
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

### 8. Channel-specific UX stays outside OAuth protocol logic

The OAuth/core layer reports structured authorization-required/completed state.

The Slack adapter is responsible for:

- sending the ephemeral authorization link to the correct user;
- posting the completion notification to the original thread.

OAuth state and token exchange logic do not depend on Slack message rendering.

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
