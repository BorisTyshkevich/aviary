# ADR 0001: Remote MCP connection model

- Status: Proposed
- Date: 2026-09-26
- Related: #6

## Context

Aviary already has two distinct MCP roles:

1. The TypeScript AdminUI is an MCP client of Aviary's Go MCP server.
2. The Go agent loop uses Aviary's local tools, but does not yet act as a generic outbound MCP client.

Remote MCP support must not merge those paths. The AdminUI/control-plane path manages Aviary configuration. The Slack/agent path creates outbound MCP clients and uses remote tools.

Remote servers need to support two sources:

- **Static** servers configured by an administrator, such as documentation or KB MCP servers.
- **Dynamic** servers supplied explicitly by a user in a Slack thread, such as `https://mcp.cluster.environment.altinity.cloud`.

Authentication ownership is independent of how the server was discovered. A static server may be unauthenticated, use a shared credential, or use a personal user credential. Dynamic servers use personal credentials.

## Decision

### 1. Static and dynamic remote connections are both first-class

A static server is persisted in Aviary configuration. A dynamic server is attached to the current session/thread after an explicit user request.

Conceptually:

```yaml
mcp:
  remote:
    servers:
      docs:
        endpoint: https://docs.example.com/mcp
        availability: global
        auto_attach: true
        auth:
          mode: none

      kb:
        endpoint: https://kb.example.com/mcp
        availability: global
        auto_attach: false
        auth:
          mode: oauth
          credential_scope: user

      shared-kb:
        endpoint: https://shared-kb.example.com/mcp
        availability: global
        auto_attach: true
        auth:
          mode: oauth
          credential_scope: shared
```

Dynamic connections are not written into global configuration merely because a user connected to them.

### 2. Availability and credential ownership are separate concepts

Supported combinations:

| Connection | Availability | Authentication |
| --- | --- | --- |
| Static | Global, auto-attached | None |
| Static | Global, auto-attached or opt-in | Shared OAuth |
| Static | Global, auto-attached or opt-in | Per-user OAuth |
| Dynamic | Current session/thread | Per-user OAuth |

Dynamic + shared OAuth is out of scope initially because a Slack user must not implicitly create a deployment-wide credential.

### 3. The agent tool client is session-aware

The Go agent tool client composes:

- Aviary local tools;
- eligible static remote tools;
- dynamic remote tools attached to the current session/thread.

Remote connection state must follow the same session/thread identity used by the agent run. A connection created in thread A must not affect thread B, even when both use the same Aviary agent.

Within a Slack thread, connection descriptors and conversation history are shared. Slack permissions are the security boundary for reading that thread, including previously posted remote tool results. Aviary does not impose an additional per-user visibility boundary on those results.

Sharing an attachment does not share personal authorization. If Alice connects a resource and Bob later requests a tool call in the same thread, discovery and calls use Bob's credentials. If Bob has not authorized the resource, he receives his own authorization handoff. Alice's token, authenticated client, or user-specific tool catalog must never be reused for Bob.

### 4. Persist connection descriptors, not live MCP clients

Persist enough state to reconstruct a connection:

- configured/static server ID when applicable;
- requested endpoint;
- canonical remote resource identity after discovery;
- session/thread attachment;
- Aviary-generated alias/namespace.

Go MCP client objects are transient. They may be lazily cached for active sessions and closed after inactivity. They are recreated after Aviary restarts.

### 5. Remote tool names are namespaced by Aviary

Remote servers may expose identical tool names. Aviary assigns a stable connection alias and exposes tools as a collision-free composite name such as:

```text
<connection-alias>__<remote-tool-name>
```

The prefix is controlled by Aviary, not by the remote server.

Routing records the mapping from the model-visible name to:

- remote connection;
- original remote tool name.

Permission filtering is applied both while listing and while calling remote tools.

### 6. Admin control plane and agent data plane remain separate

AdminUI continues to use Aviary's inbound MCP server for configuration.

The Go agent runtime uses a generic outbound MCP client directly.

They may share configuration types, credential storage, and helper libraries, but there is no requirement for the agent runtime to route through the admin MCP API.

### 7. Scheduled jobs cannot use personal credentials

Scheduled prompt and script jobs may use eligible administrator-configured static servers with no authentication or shared OAuth. They cannot discover or call tools through personal OAuth connections, even if a job was created by an authenticated Slack user or targets that user's thread.

A job's creator, reply target, and conversation history do not grant a personal credential identity. Enforce this restriction during discovery and invocation, including indirect script calls. Missing or expired shared authorization requires an administrator action; a scheduled job must not start a personal OAuth flow.

## Consequences

### Positive

- Users can connect to new clusters without administrator provisioning.
- Static documentation/KB servers remain easy to make globally available.
- Per-thread dynamic connections do not mutate agent-global state.
- Personal and shared authentication are supported without duplicating the connection model.
- Live network sessions are not durable application state.

### Negative

- The agent tool set becomes session-dependent.
- Tool discovery can vary between runs and users.
- Connection aliases and canonical resource identity require careful persistence.
- Static auto-attached servers can increase prompt/tool-schema size.

## Rejected alternatives

### Only administrator-configured remote servers

Rejected because cluster MCP endpoints are dynamic and the user should be able to connect directly to an allowed MCP URL.

### One remote MCP client per Aviary agent

Rejected because users and Slack threads may target different clusters concurrently. Agent-global remote state would leak tools or credentials between sessions.

### Persist live MCP client/session objects

Rejected because network sessions are ephemeral and cannot survive process restart reliably.
