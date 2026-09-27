# ADR 0006: Generic preparation hooks and private artifacts

- Status: Accepted (Milestone A2; 2026-09-27)
- Date: 2026-09-27
- Related: #16, #6 Milestone A2; complements ADR 0005

## Decision

An optional administrator-configured `before_turn` executable runs once after
trusted execution identity and the stable target generation are resolved, before
the first model request. Provider fallback and model tool rounds do not rerun it.
The target remains reserved throughout preparation and the turn. Hook arguments
come from configuration and are executed directly, without shell interpolation.
Connect/status/disconnect commands do not invoke `before_turn` preparation.
Collector context excludes raw conversation text by default.

An agent may instead configure `post_connect` for a direct ClickHouse
connection. After private credential validation, Aviary invokes that hook once,
stores its bounded `evidence.json` with the personal credential, and posts a
deterministic confirmation in the original thread. A validated optional public
summary may state version and uptime; the confirmation makes no LLM call.
Subsequent turns reuse the exact private snapshot for the credential and target
generation, without implicit refresh. An immediate turn waits for an in-flight
post-connect collection. Credential rotation, target replacement, and
disconnect discard the snapshot. Collection failure does not undo login; the
confirmation and later turns mark baseline evidence unavailable.

A versioned, bounded process protocol carries non-secret context, an assigned
output directory and a structured result. Explicitly authorized credentials use
a parent-created private pipe, never argv, ordinary environment variables or
persisted protocol files. The implementation defines portable framing and
platform-specific process-tree cancellation; Unix-only inherited descriptors are
not an assumed cross-platform API. Environment inheritance is allowlisted.
Scheduled runs never receive personal credentials. Raw stdout/stderr are not
automatically exposed as logs or model context.
Validated summaries, indexes and artifact contents are untrusted evidence, never
system instructions, target selection or permission changes. As with direct
query results, evidence used by the agent is sent to its configured LLM provider.

Timeouts, cancellation and per-scope concurrency are bounded. Required hook
failure stops the turn; optional failure leaves otherwise eligible tools usable
with explicit unavailable/partial evidence status. Initial Altinity preparation
uses optional failure. There is no automatic retry within the same turn, but
producers must tolerate new invocations after restart or interruption.

Aviary assigns a private staging directory for each invocation, validates bounded
structured output and manifest-listed relative files, and publishes accepted
artifacts atomically. Reject path traversal, absolute paths, symlink escapes and
non-regular files. Enforce file count, per-file/total size, retained storage and
retention limits. A `post_connect` snapshot is copied into the owner-only
credential store with a 1 MiB bound and lasts until that credential or target
is removed. Its temporary generic run is discarded after the copy. `before_turn`
runs keep their seven-day/cap-based retention. The executable is trusted
deployment code, not OS-sandboxed;
directory assignment alone does not restrict its filesystem or network access.

Artifact identity includes the agent, trusted principal, originating thread,
target generation and producing run. Provenance records observation time and
producer revision. Generic bounded artifact reads enforce that scope at call
time. Private indexes and read results remain private through history, summaries,
memory and provider continuation as specified by ADR 0005. Shared publication is
limited initially to posted Slack answers/files; no implicit cross-principal
artifact sharing is introduced. Non-personal control and scheduled runs receive
no personal credentials; their answers and tool history retain the ordinary
session/job visibility rules. The optional hook does not suppress those answers.

The producer owns domain-specific collection, cache and freshness policy. Cached
evidence is reusable only for the same principal, exact target, generation and
credential identity/version, with compatible producer revision. Each invocation
materializes its accepted files in the current Aviary scope. Old evidence may be
reported as historical, never silently treated as a new target's live state.
Missing or partial evidence cannot imply a healthy cluster.

An executable opening network connections enforces approved endpoint, DNS/dial,
redirect, routing and TLS policy itself. Aviary's validation of its initial input
does not sandbox subprocess networking. It must also honor the database account
and query restrictions in ADR 0005.

## Delivery boundary

Aviary contains protocol/lifecycle configuration, the process runner, artifact
storage/read tools, fake-executable tests and generic operator documentation.
The Altinity collector, SQL, cache policy, skills and deployment configuration
live in `BorisTyshkevich/aviary-deployment`, using `altinity-expert` as a reference.
This is neither external-tool RPC nor an outbound MCP client. Additional check
packs and monitoring parity remain separate work.

## Verification

Tests cover once-per-turn behavior, fallback, optional/required failures,
malformed/oversized output, constrained environment and private secret transport,
process-tree cleanup, concurrent principals/threads, stale generations,
credential changes, restart/interrupted publication, path confinement, retention
and private history/provider reuse. Exercise the feature through Aviary's inbound
MCP control plane; test trusted Slack ingress separately without introducing a
production impersonation API solely for smoke testing.
