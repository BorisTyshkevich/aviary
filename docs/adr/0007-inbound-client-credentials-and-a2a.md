# ADR 0007: Inbound client credentials and A2A conversations

- Status: Accepted (design; implementation tracked in #54 and #55)
- Date: 2026-10-06
- Related: [#54](https://github.com/BorisTyshkevich/aviary/issues/54), [#55](https://github.com/BorisTyshkevich/aviary/issues/55)

> Execution-authority follow-up: accepted [ADR 0008](0008-execution-authority.md)
> defines ownership through internal tools and delegation. PR #62 does not yet
> satisfy that boundary. #54 and #55 require the authority/resource enforcement
> release gate before their isolation claims are considered implemented.

## Context

Aviary's inbound MCP endpoint accepts the administrator bearer token. External
callers have no lesser credential or authenticated principal. The existing
`agent_run` tool already sends text, media and optional tool progress through MCP
progress notifications, but has no client ownership boundary. MCP transport
sessions and Aviary conversation sessions are distinct objects.

Peer conversations additionally need discoverable agent interfaces, durable task
status, polling, reconnection and cancellation. A2A supplies those semantics.
This decision concerns inbound callers; it does not extend the outbound MCP
roadmap or Milestone A2 in ADRs 0001–0006.

## Decision

### 1. Shared authentication, separate surface grants

`server.clients` declares clients with an immutable, generated `id`, unique
operator-facing `name`, `token_hash`, explicit `protocols`, MCP `tools` and
allowed `agents`. Names are labels; ownership uses the immutable ID. Tokens are
generated from at least 32 random bytes, stored as versioned SHA-256 hashes, and
compared in constant time. High-entropy generated tokens do not require a
password hashing dependency. Administrator comparison, including login, is also
constant time.

Clients authenticate only through `Authorization: Bearer`. Cookies, query
parameters and `/api/login` accept only the administrator credential. Clients
cannot access any administrator API, including config, logs, daemons and
upgrades. `X-Aviary-Agent-ID` has no effect for clients. Shared authentication
returns a trusted principal; the MCP and A2A adapters enforce their own grants.
The administrator retains existing MCP/API behavior; A2A ingress requires a
configured client principal so every task has a peer owner.

#54 implements the `mcp` protocol grant and accepts only exact MCP tool names
`agent_run` and `ping`. Every other grant is invalid, including agent mutation,
scripts, scheduling, session tools and wildcard grants. This exact allow-list
deliberately bypasses the existing classification of `ping` as a `server` tool.
#55 adds the `a2a` protocol grant; it does not require an MCP grant. Granting
`a2a` authorizes only configured A2A routes whose agent is also in `agents`.
Channel `allow_from` further restricts that authorization.

For example, after both issues are implemented:

```yaml
server:
  clients:
    - id: client_0123456789abcdef0123456789abcdef
      name: investigation-peer
      token_hash: "sha256:<64 lowercase hexadecimal characters>"
      protocols: [mcp, a2a]
      tools: [agent_run, ping]
      agents: [clickhouse-expert]
```

The placeholder hash is explanatory, not a valid configuration. Validation
rejects malformed/duplicate IDs, names or hashes, unknown protocols, agents or
tools, empty protocol grants, and MCP tool grants without `mcp`. An MCP grant
requires at least one tool; `agent_run` and `a2a` require at least one agent.
The CLI generates IDs and tokens and persists config atomically, preserving
unrelated configuration. `aviary client add`, `rotate` and `remove` are local
operator commands, never client-callable MCP tools. Add/rotate print the new raw
token once after successful persistence; that CLI output is the sole exception
to the ban on tokens in config, logs and tool responses. Tests use fake tokens.

### 2. Ownership and credential lifecycle

MCP transport sessions bind to the principal ID at initialization, including
the administrator identity. Use the installed MCP SDK's `TokenInfo.UserID`
ownership support for subsequent POST, GET, DELETE and reconnect requests.
Transport ownership does not grant conversation access.

Client MCP conversations use an Aviary-owned namespace based on client ID and
agent identity. Omitted `session` selects that client's default conversation,
never the agent's shared `main` session. A caller's logical session name is
mapped to a safe internal identifier; separators, traversal and normalization
collisions cannot select another namespace. An explicit `session_id` must
resolve to an existing conversation owned by that client and permitted agent.
Validate canonical agent and persisted conversation ownership before creating,
reading or stopping anything, including an `agent_run` stop command. MCP and
A2A conversations have separate namespaces even for the same client.

The MCP tool grant authorizes the external operation. It is not the tool policy
for the resulting model run. The agent uses its configured execution permissions,
narrowed by inherited tool policy and resource authority from ADR 0008; clients
cannot supply tool/model overrides or trusted channel identity. Internal calls
retain caller ownership, and model runs cannot administer Aviary. Generic exec
is unavailable to scoped model execution pending a separate containment decision;
scoped scheduling/replay is deferred to durable authority issue #66. This
boundary does not make agent-global memory or workspace state private to each
peer. Operators must use separate agents where that isolation is required.

Rotation preserves client ID, conversation ownership and accepted runs. The old
token immediately loses admission after the new credential is successfully
installed in the running server, and its open streams/transport sessions close.
Runs use an independent execution context and may finish under their admitted
policy. The new token can reconnect to A2A tasks. #54 does not add MCP task
retrieval or promise recovery of a closed synchronous response.

Removal immediately denies new requests, closes client streams and cancels its
accepted runs. A2A tasks become canceled once execution stops. Recreating the
same name generates a fresh ID and inherits no conversations. Removing protocol
or agent grants cancels affected runs and closes affected streams; credential
rotation alone does not. Validated config reloads publish a coherent policy
snapshot. CLI commands report success only after the running server acknowledges
the change; when offline, they explicitly report persistence for next startup.

### 3. A2A protocol, dependency and routes

Use the [A2A 1.0.0 specification](https://a2a-protocol.org/v1.0.0/specification/),
HTTP JSON-RPC with SSE, and
[`github.com/a2aproject/a2a-go/v2` v2.6.0](https://github.com/a2aproject/a2a-go/releases/tag/v2.6.0).
The SDK's wire protocol version is `1.0`, distinct from its module version.
This release was verified on 2026-10-06 and requires Go 1.26, matching Aviary.

The standard library supplies HTTP, JSON, crypto and persistence primitives,
but no A2A types, protocol errors, task operations or streaming binding. Aviary's
MCP SDK implements a different protocol. The maintained official A2A SDK supplies
these components and a task-store interface; Aviary owns its durable store,
authorization, admission and runner adapter. Add the dependency and update
`go.mod`/`go.sum` together in #55; this design change adds no dependency.
The module includes other transport dependencies even though Aviary enables only
JSON-RPC. Reassess the pin before implementation if availability/security changes.

Each enabled `a2a` channel has a globally unique, stable `id` restricted to
`[a-z0-9][a-z0-9-]{0,63}` and binds to its containing agent. Routes are
`/a2a/<id>` for JSON-RPC and `/a2a/<id>/.well-known/agent-card.json` for its card.
Cards and task operations require bearer authentication, an A2A grant, the
allowed agent and a matching enabled `allow_from` entry. There is no public
agent directory or public discovery card. Authorized peers receive only that
route's explicit card name, description, text modalities, protocol URL and
capabilities; cards do not enumerate internal tools, config or other agents.
The advertised URL uses an administrator-configured HTTPS base URL, never an
untrusted request Host header.

```yaml
server:
  a2a:
    public_base_url: https://aviary.example.com
    max_request_bytes: 1048576
    max_runs_per_client: 2
    run_timeout: 15m
    stream_timeout: 15m
    task_retention: 168h
agents:
  - name: clickhouse-expert
    channels:
      - type: a2a
        id: expert-peer
        a2a:
          name: ClickHouse investigation
          description: Answer ClickHouse investigation questions.
        allow_from:
          - from: investigation-peer
            restrict_tools: [web_search]
```

Client names in `from` resolve to current immutable IDs; `*` matches only otherwise
authorized clients. First matching enabled entry supplies policy overrides,
as in existing channel matching. Empty `allow_from` denies access. A2A entries
support `enabled`, `from`, `restrict_tools`, `model` and `fallbacks`; chat-platform
matching options (including group, mention and text-prefix filters) are invalid.
Channel installation and thread-affinity options are also invalid on A2A.
Use existing entry model/fallback and channel disabled-tool fields. Revalidate
credentials and routing on every operation and immediately before run admission.

### 4. Task and conversation lifecycle

Support text-only `SendMessage`, `SendStreamingMessage`, `GetTask`, `ListTasks`,
`SubscribeToTask` and `CancelTask`. Reject unsupported parts before admission.
Advertise streaming; advertise no push notifications or extended-card capability
and return the specification's unsupported-feature errors for their operations.

An Aviary-generated opaque context ID maps to exactly one conversation owned by
`(client ID, channel ID, agent ID)`. An absent context creates one; an unknown or
foreign supplied context is rejected. A task has a separate server-generated ID
and durable owner, context, state, timestamps and bounded public output. Never
reuse scheduler task IDs or treat protocol IDs as filesystem paths. All lookup,
list, subscription, history and cancellation paths enforce the full owner tuple.
Unknown and inaccessible resources produce the same protocol error.

Each accepted message starts one task. Only one task may run per conversation;
additional messages while active are rejected without enqueueing work. A
completed/failed/canceled task cannot receive further messages; a follow-up uses
the same context and creates a new task. Supplied task references must resolve
within the owner scope and match the context; first-version active-task mutation
and input-required/auth-required flows are unsupported.

Admission reserves per-client and per-conversation capacity atomically and
persists the task before starting work. `SendMessage` uses the specification's
blocking/nonblocking configuration; task execution has an independent context.
Streams begin with the task, publish status/artifact updates, and close at a
terminal state. Subscribe sends the current durable snapshot plus future updates;
there is no promise of replaying every transient progress event.

Disconnect and stream timeout detach the subscriber while execution continues
within its original run deadline. Polling or reconnection retrieves the task.
Run timeout stops execution and ends the task as failed with a fixed timeout
notice. Cancellation targets only that task's run, waits for execution to stop,
and records canceled. Completion and cancellation coordinate one durable terminal
transition; if completion already won, cancel reports the protocol's
non-cancelable outcome and preserves the completed result.

On process or owning-runner restart, unfinished A2A tasks become failed with a
fixed interruption notice. Conversation history remains; generic checkpoint
recovery must not replay these tasks. Persist terminal state before publishing
the terminal event. Startup reconciles unfinished records before accepting A2A
traffic. Disabling/removing a channel cancels its runs and closes streams.

### 5. Effective execution policy and public events

Peer runs intersect the agent preset and configured tool allow-list with the
matched entry's restrictions, then apply both agent and channel deny-lists.
Restrictions cannot widen agent permissions. An explicit empty restriction, or
one reduced to zero eligible tools, means no tools rather than agent defaults.
Preserve that distinction during config decoding and listing/calling, including
indirect calls. Unknown tool restriction names are invalid. Model/fallback
overrides come only from administrator config.
External messages remain user content; payload metadata cannot supply trusted
installation, mention, sender, permission or session ownership fields.

Use the existing `PublicToolEvent` projection for tool progress. Never serialize
raw tool arguments, results, provider errors or private evidence into task
updates. Tool progress maps to bounded task-status messages; text replies map
to text artifacts with incremental updates and a final complete answer. Generic
`session_send` delivery goes only to the owning task/conversation subscriptions
and cannot bypass the admitted tool policy. Failures at any stage publish a fixed
notice, including failures before progress; private logs record agent, session,
client ID/name, task and elapsed time with the existing redaction policy.

Extract shared run-event/terminal coordination only when the A2A adapter becomes
its second consumer. Keep Slack formatting, editing, message timestamps, delivery
acknowledgments and cleanup in Slack. Preserve its existing lifecycle behavior.

### 6. Limits and retention

The configuration above gives defaults: 1 MiB request bodies, two active runs
per client across A2A channels, one active task per conversation, 15-minute run
and stream deadlines, and seven-day terminal-task retention. The conversation
limit is fixed in this version; the other values are configurable and must be
positive. Admission violations return protocol errors with no new conversation,
run or task.
Oversized requests are bounded before decoding; streams use bounded buffers and
disconnect slow subscribers without blocking the run.

Task retention is separate from conversation history. Public output storage is
bounded to 1 MiB per task; exceeding it fails the task with a fixed notice.
Each client retains at most 1,024 terminal tasks, evicting the oldest terminal
records first or records past the retention duration. Active tasks are never
evicted. Allow one stream per task; reconnection replaces its previous subscriber.
Expired tasks return the usual not-found error; retained contexts remain usable.

## Consequences and alternatives

#54 can ship independently for scoped MCP invocation. #55 reuses its principal
and lifecycle primitives, then adds the SDK adapter, durable tasks and channel
policy. Simple MCP callers keep their existing progress flow; A2A callers gain
polling, reconnectable streams and protocol task cancellation.

Authenticated discovery requires peers to know the configured route and obtain
credentials out of band. Durable records and independent execution contexts add
storage and lifecycle coordination. Peers share agent-global state unless
operators provision separate agents.

Broad MCP grants are deferred until each tool has reviewed ownership semantics.
Public discovery, files, push notifications, other bindings, input-required
flows and automatic replay are deferred. ACP serves editor integration and is
outside this decision. A custom protocol would duplicate A2A interoperability
work; treating an entire conversation as one task would break follow-up and
terminal-state semantics.

## Verification and delivery

#54 covers allowed calls, all rejected grants, canonical agent/conversation
ownership, transport ownership across POST/GET/DELETE, forged headers, cookie/
query/login and administrator API denial, CLI atomic updates, config validation,
rotation/removal and logging. Rejected calls must cause no persistence or run.

#55 covers streamed/nonstreamed execution, discovery isolation, ownership on all
task/context/reference operations, fail-closed effective tools including indirect
calls, safe public progress, session delivery, failure notices/logs, cancellation
races, disconnect/reconnect, admission races, deadlines, retention, restart
reconciliation without replay, and config validation. Use fake credentials and
a minimal client built with the pinned A2A SDK against the running HTTP endpoint;
use Aviary MCP/CLI for setup/inspection. Retest Slack lifecycle after extraction.

Implementation updates config structs/schema, affected web config handling,
CLI reference, MCP credential reference, security guide and A2A channel guide
together. Go changes require `pnpm test:go`; web changes require
`pnpm test:e2e`; code changes require `pnpm lint`. This ADR alone does not change
runtime behavior or require a server restart.
