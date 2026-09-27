# ADR 0005: Direct ClickHouse before remote MCP

- Status: Accepted (Milestone A2; 2026-09-27)
- Date: 2026-09-26
- Related: #15, #6; extends ADRs 0001–0004; complemented by ADR 0006

## Context

The first external-cluster integration uses the compiled-in Go ClickHouse driver
over HTTPS. It precedes the full outbound MCP client and OAuth/CIMD work.
Connection management remains deterministic and never invokes the agent or LLM.

The initial delivery stops at roadmap #6 Milestone A2: direct ClickHouse,
generic preparation hooks/artifacts (ADR 0006), and an external deployment
collector. Outbound MCP, OAuth/CIMD and static remote servers remain later work.

## Decision

### One shared target per thread

A thread has one dynamic target: either direct ClickHouse or remote MCP. Static
MCP servers remain separate. The descriptor is scoped by agent, Slack
installation/workspace, channel and root thread, independently of conversation
history. Replacing the transport also replaces the target. Reject replacement
while any turn is active in that thread, coordinated atomically with turn startup.
Historical URLs and tool names cannot retarget calls.

Selecting a different allowed target commits a new generation in a needs-login
state before private setup. Failed or abandoned authentication does not restore
the previous target. Unsupported transports and rejected endpoints leave the
existing attachment unchanged. Disconnect follows the same busy-thread rule,
invalidates pending setup and removes the attachment. A connect to the current
target can authorize the sender without replacing its generation. Credential
updates invalidate that owner's cached connections and prepared evidence.

### Short connect command and transport selection

The user sends this command in the destination Slack thread:

```text
@bot connect https://cluster.example.com:8443
```

Parse the URL before selecting transport. A hostname starting with `mcp.` or an
exact path `/mcp` (optionally with a trailing slash) selects MCP; otherwise select
direct ClickHouse. A substring in another hostname, query or fragment is not a
transport selector. Explicit `connect mcp URL` and `connect clickhouse URL`
override inference for other endpoint layouts. Network policy still applies.
Never probe another protocol or switch transport after an authentication failure.
Before outbound MCP is implemented, inferred/explicit MCP requests report that
transport as unavailable; they do not fall through to the database driver.

### Password entry through a bot DM prompt

The original command's trusted Slack metadata supplies the sender and destination
thread; no thread-link argument is needed. Apply sender/channel authorization.
The bot opens a one-to-one DM and identifies the target, destination thread and
proposed database username. Default the username to the sender's Slack profile
email, requiring `users:read` and `users:read.email`. If unavailable or the user
requests another username, collect it privately through deterministic
setup, then issue the password prompt. Email is a database login default, not the
credential ownership key; ownership remains the trusted Slack principal.

The user replies in the DM password prompt's thread with only the password.
Treat the entire reply as the password, preserving spaces and punctuation.
Bind each prompt to the sender, installation/workspace, DM channel, prompt root,
destination thread and exact target identity. Independent prompts may coexist.
Prompts expire; stale, duplicate or wrong-principal replies cannot authorize or
mutate a connection. Do not interpret arbitrary unthreaded DMs as passwords.

Consume credential replies before history enrichment, observers, persistence or
LLM processing. Store credentials privately for that sender and exact connection
identity. Exclude password-prompt replies from later Slack history/reference
reads and diagnostic output, including replies to expired/completed prompts.
Retain non-secret prompt classification across restarts so expiration or restart
cannot send late passwords to the agent. Password input is not accepted through
connect arguments, channel messages or group DMs. Never echo credentials or post
them to the destination thread. The original DM remains subject to Slack's
storage and access controls.

Bob authorizes the current attachment with Bob's credentials; Alice's credentials
and authenticated driver connections are never reused for him. Missing credentials
produce a private setup action while unrelated tools remain available. Scheduled
prompt and script jobs cannot use personal credentials.

Bind connection validation and credential completion to the intended thread and
attachment identity. A delayed completion cannot replace a newer target or bypass
the busy-thread check at commit. Adding credentials for the existing target does
not itself replace that target.

### Read-only database tools

Use `github.com/ClickHouse/clickhouse-go/v2` for HTTPS query execution and typed
results. The standard library HTTP client would require custom result decoding
and connection/query management. Pin the dependency and document its HTTP
transport behavior during implementation; update both Go manifests.

Expose schema inspection and bounded SQL queries. ClickHouse enforces read-only
access; prompt instructions and SQL-prefix checks are insufficient. Include query
IDs, cancellation, timeouts and bounded output. Tool arguments cannot override
the selected endpoint. Effective agent/message/script permissions apply during
both discovery and invocation.

The supported database account has least-privilege read/inspection grants and
server-enforced read-only restrictions. A client-side `readonly` flag alone is
not a promise that an administrator credential becomes safe: ClickHouse has
privileged operations outside that setting's boundary. Deployment documentation
must specify the account/profile requirements, and live verification must cover
write, administration and settings-override rejection with the supported account.
The database administrator remains responsible for its grants, source access,
and server-side resource limits. Do not silently weaken query safeguards when a
profile rejects requested settings; report the limitation explicitly.

Apply endpoint and network policy while preserving logical Host/SNI and TLS
verification. Report failures without fallback to another cluster or user, and
without blind query replay. Extract shared connection primitives when the MCP
consumer needs them, keeping transport-specific behavior in its adapter.

For the initial direct adapter, reject URL userinfo, fragments, and query
parameters rather than accepting credentials or driver settings in the endpoint.
Reject HTTP redirects. Logical HTTPS host/port and resolved or rewritten dial
destinations require explicit deployment authorization; TLS verification remains
enabled. Aviary's dial policy does not govern database-server-side network access
through tables or functions; that boundary belongs to database grants and policy.

### Private evidence and trusted execution

Posted Slack answers follow Slack visibility. Unposted preparation evidence and
personal tool results remain scoped to their trusted principal, originating
thread and target generation. This applies to tool-history reads, summaries,
memory and provider-side conversation continuation, not only artifact files.
Do not resume a channel-wide provider conversation containing another principal's
private evidence. A result becomes shared through successful posting, not merely
through insertion in Aviary's internal conversation log. Verbose channel output
also omits private tool details and progress. Built-in shared browser, search and
lab state is unavailable during private turns.

Checkpoint recovery, scheduled jobs and ordinary control-plane prompts cannot
manufacture a personal identity from session IDs or supplied sender fields.
Personal tool use requires an authenticated ingress to establish that identity.
Interrupted channel turns that depend on their original delivery consumer are
not replayed; recovery records an interruption notice requesting a fresh message.
Tests may inject identities through test-only seams; production model-callable
tools cannot impersonate Slack users.

### Repository boundary

Generic connection lifecycle, the direct ClickHouse adapter, Slack intake,
network policy and preparation/artifact facilities belong in Aviary and must
remain suitable for upstream submission. Altinity-specific collection SQL,
freshness policy, executable, skills and provisioning belong in
`BorisTyshkevich/aviary-deployment`. The `altinity-expert` checkout is a source
reference; this delivery does not modify it or require its interactive runtime.

## Verification

Issue #15 tracks implementation and tests: credential redaction at ingress and
history reads (including late replies and restarts), email fallback, transport
selection/overrides, prompt correlation, principal isolation, stale completion,
busy replacement, read-only enforcement, bounded queries, and scheduled-job
restrictions. Verify the implementation through Aviary's MCP control plane.

## Consequences

Users connect in the destination thread and reply to a private password prompt.
Slack email access requires the additional scope. A private username confirmation
accepts `use proposed` or a different username before the separate password prompt.
The thread retains one unambiguous cluster and transport while participants use
their own credentials. Direct ClickHouse is useful before MCP OAuth is available.
