# ADR 0008: Execution authority for a trusted company team

- Status: Accepted (design; phased implementation required)
- Date: 2026-10-08
- Related: [ADR 0007](0007-inbound-client-credentials-and-a2a.md),
  [#54](https://github.com/BorisTyshkevich/aviary/issues/54),
  [#55](https://github.com/BorisTyshkevich/aviary/issues/55),
  [PR #62](https://github.com/BorisTyshkevich/aviary/pull/62)
- Implementation: Not started by this ADR

## Context and goal

Aviary is intended for a closed company team. The goal is a small, consistent
permission model that simplifies the code and prevents accidental or
model-induced access across conversation boundaries. Shared agent memory and
workspace remain deliberate collaboration features. This is not a hostile
multi-tenant platform or a promise of operating-system containment.

Main at `1475db9` has useful but separate mechanisms: agent tool presets,
channel admission rules, intersecting turn tool policies, personal connection
identity, private artifact scopes, filesystem allowlists, and command rules.
Session, job and agent IDs also route work without universally authorizing it.
PR #62 at `7af9565` adds client authentication and direct conversation ownership,
but drops caller authority before normal model execution. A permitted internal
tool can therefore become a confused deputy.

Evidence reviewed:

- `internal/agent/tool_policy.go`: trusted turn policies compose by intersection,
  but decide only tool names; absent policy is not a denial.
- `internal/agent/runner.go`, `filterTools`: channel restrictions replace the
  agent tool list; empty restrictions fall back. This differs from ADR 0007's
  intended A2A intersection and explicit-empty semantics.
- `internal/mcp/tools.go`, `registerSessionTools`: session handlers accept target
  IDs without a general caller ownership check.
- `internal/store/store.go`, `ListAgentFiles`/`ReadAgentFile`/`WriteAgentFile`:
  generic agent file operations reach session JSONL and other runtime files.
- `internal/mcp/deps.go`: tools share process-wide managers and stores.
- `internal/connections/service.go`, `CredentialFor`, and
  `internal/preparation/engine.go`, `Read`: existing examples of resource access
  bound to trusted execution identity and a current credential generation.
- `internal/scheduler/worker.go` and `internal/agent/manager.go`: jobs and recovery
  construct execution contexts from durable records, not a universal authority.
- `internal/mcp/exec_tools.go` and `command_env.go`: commands run with the server's
  OS access and inherited environment. Tool authorization cannot contain them.

The design must replace overlapping checks with shared decisions. Merely adding
another context flag or filtering a model's tool list is insufficient.

## Decision

### Authority and identity

Every tool execution has explicit runtime-created authority. Distinguish:

1. Actor: administrator, integration client, platform user, or scheduled service.
2. Execution: direct operator operation or model/script execution, current agent,
   conversation, and run identity.
3. Resource scope: immutable owner and namespace, admitted agent set, and any
   configured sharing boundary.
4. Effective operation permissions and provenance needed for delegation,
   attribution and revocation.

These are conceptual fields, not a requirement for a universal policy language
or a publicly constructible struct. Prefer a small package with typed immutable
values, trusted constructors and narrowly defined derivation methods. Tool
arguments, prompts and request metadata never create or replace authority.

An administrator starting a model run does not give generated tool calls
direct-operator authority. Agent/config/credential administration is reserved
for direct operator operations, not model runs.
Scheduled execution is explicit; a missing principal never becomes a scheduler
or administrator identity. Missing/malformed authority is denied at callable
boundaries. No compatibility fallback treats unscoped model work as privileged.

Keep credentials and authority distinct. Bearer authentication establishes the
actor; admitted execution retains ownership without retaining raw credentials.
Token rotation may detach transport while the admitted execution continues.
Client removal or removal of its admitted execution scope cancels the run and
its descendants. Cancellation does not undo completed effects; effectful
operations recheck current execution validity before admission. Grant expansion
does not widen an already admitted run's authority.

### Two authorization decisions

An operation succeeds only when both decisions permit it:

- The effective tool policy permits the operation.
- Resource authorization permits that action on the canonical target.

The external MCP `agent_run` grant remains distinct from the model's internal
tool catalog. Resource ownership accompanies the run regardless of that catalog.

Use shared authorized resource services/resolvers for canonical lookup, ownership
validation, listing and mutation. Do not repeat ad hoc owner checks in each tool
handler. Checks occur before creation, reading, stopping, deletion, routing or
other effects. Validate ownership and mutation together where a concurrent
change could invalidate the decision. Unknown and inaccessible resources have
indistinguishable public errors where appropriate.

Every registered model-callable tool declares its authorization category:
operator administration, scoped resource operation, or an explicitly reviewed
shared/pure operation. Scoped operations identify their resource resolver.
Unknown/unclassified tools are unavailable to model execution. Listing and
calling use the same policy; internal calls cannot bypass it.

Tool-policy composition uses the preset as a ceiling, intersects each specified
allowlist and then applies denies. Absent restrictions inherit; explicit empty restrictions mean
zero tools. Preserve that distinction through YAML/JSON, schemas, web editing,
normalization, reload and execution. Reject unknown names rather than silently
turning a restriction into a default.

### Ownership and shared resources

Persist conversation ownership independently of logical display names. MCP and
A2A keep their protocol-specific namespaces; channel ownership includes trusted
installation/workspace/channel/thread identity. Platform users are not unified
across providers by matching display names. Integrations access conversations
they own within their admitted agents. Channel users share only their admitted
channel/thread. Direct operators retain explicit broad access.

Lists expose only authorized records. History, messages, creation, stop, delete,
metadata, delivery targets and sending use the same ownership policy. Broad
agent/job cancellation is not a substitute for scoped run cancellation.

Runtime state is not generic agent workspace. Conversation headers/history,
sidecars, checkpoints, job records, credentials and private artifacts are accessed
through their owning services, not generic read/write/delete/copy/send-file tools.
Audit alternate path forms, symlinks, media and downloadable artifacts as well as
`agent_file_*`. An owner field is authoritative only if ordinary tools cannot
rewrite it. Prefer separating runtime storage from workspace to multiplying path
exceptions; implementation may choose the smallest consistent layout.

Shared memory and workspace do not become private simply because conversations
have owners. Information intentionally copied into shared resources is shared.
Browser profiles/tabs and other shared state need an explicit tool classification;
no per-client browser or memory isolation is promised by this ADR.

Generic host exec is unavailable to scoped model/script execution, even when its
agent configuration permits that tool. Model runs started by an operator remain
scoped model runs. Lua may call scoped tools; it does not authorize otherwise
forbidden resources. Operator-managed, reviewed purpose-specific helpers may
perform approved work. Their implementations are trusted deployment code, not
model-supplied programs or shell commands. A helper declaration alone is not an
authorization bypass: its inputs, outputs and effects must enforce the admitted
resource scope and must not expose generic filesystem/command access. Do not
introduce a helper framework in this change; classify existing approved helpers
and reject unreviewed execution paths. Direct operator host access remains trusted.
OS containment for generic model-driven exec is deferred to a separate decision.

### Delegation, jobs and recovery

Derived execution cannot broaden agent, resource or operation authority. Context
propagation is tested across in-process MCP, Lua, nested agent calls and callbacks.
The permitted scope is independent of model-selected IDs. The first release
supports synchronous scoped delegation and denies integration-originated
scheduling/replay. Unsupported paths reject before creating sessions, jobs,
checkpoints or other work.

Durable work must persist an attributable owner and a serializable permission
ceiling, revalidate current grants on admission/resume, and preserve descendant
revocation. It must not serialize bearer tokens or executable policy closures.
Recurring tasks require an explicit durable owner; they cannot silently outlive
a revoked creator by becoming service-owned. Administrator-created tasks use an
explicit service authority. A2A's no-replay restart policy remains in force.

Unattributed existing records must not be inferred to belong to an integration.
Define a deterministic migration/classification for existing operator/channel
records before enabling scoped access. Unresolvable records are excluded from
scoped access. No legacy authorization fallback is retained.

### Existing connection and artifact scopes

Use the common identity representation as the source for personal connection and
artifact authority, retaining specialized leases, target generations, credential
versions and execution-kind checks. Do not replace these checks with a generic
"tool allowed" decision. Adapt these consumers incrementally after the shared
identity is stable; avoid a second independent actor or sharing model.

## Product decisions

All five decisions were accepted by the operator on 2026-10-08.

| ID | Decision | Accepted choice |
| --- | --- | --- |
| D1 | Conversation sharing | Integrations access owned conversations within admitted agents; channel users share only their admitted channel/thread; explicit operator access remains broad. |
| D2 | Model administration | Reserve agent/config/credential administration for direct operator calls, including when an operator started the model run. |
| D3 | First-release delegation | Support authority-preserving synchronous delegation; deny scoped scheduling/replay until durable authority is implemented. |
| D4 | Permission composition | Intersection everywhere; explicit empty means no tools; no legacy channel override semantics. |
| D5 | Host execution | Reviewed purpose-specific helpers only for scoped runs; generic exec requires a later enforceable storage boundary. |

Implementation issues must preserve these decisions. Acceptance of this ADR is
not acceptance of PR #62 or evidence that the boundary is implemented.

## Phased development and release gates

1. **[Authority foundation and tool policy](https://github.com/BorisTyshkevich/aviary/issues/63).** Typed authority, explicit ingress
   construction, uniform policy composition, tool classification and a common
   handler guard. Operator/model distinction is explicit. Existing special
   connection scopes retain their protections. This phase alone does not enable
   or claim isolated external agent execution.
2. **[Resource enforcement](https://github.com/BorisTyshkevich/aviary/issues/64).** Authorized conversation operations, protected runtime
   storage, scoped delivery/cancellation, and direct/internal/script parity.
   Replace duplicate authorization paths. Existing record classification is
   deterministic. Scope cannot be changed by writing owner metadata.
3. **[Delegation and #54 integration](https://github.com/BorisTyshkevich/aviary/issues/65).** Propagate authority through permitted nested
   runs, cancel descendants, deny unsupported background/host execution, and
   integrate the client registry. Complete adversarial two-client tests and HTTP
   MCP lifecycle verification. #54 and PR #62 cannot ship before this gate.
4. **[Durable scoped work](https://github.com/BorisTyshkevich/aviary/issues/66).** In a later release,
   persist and revalidate job authority, enforce owner-scoped jobs/output/delivery, and test
   rotation, removal, restart and recurring-task behavior. Until complete, scoped
   scheduling/replay stays denied; existing operator scheduling remains explicit.
5. **A2A integration (#55).** Consume the same authority/resource services; add
   protocol tasks and subscription ownership. #55 requires phases 1–3 and only
   requires phase 4 if it exposes scheduler/replay capabilities. A2A durable
   protocol status does not itself authorize a scheduler job or restart replay.

Keep #54 as inbound credentials/transport integration and #55 as the A2A adapter.
Track the common authority work in separate issues so neither protocol becomes
the owner of the generic permission model. ADR 0007 remains the protocol contract;
ADR 0008 supersedes its implication that internal agent permissions alone suffice
for resource ownership.

## Proof required

Use deterministic fake-model calls, not prompt compliance, with two client IDs
sharing an agent and additional operator/channel conversations. Test foreign
listing, history/messages, stopping a live run, deletion, routing and file access;
assert both fixed denial and unchanged foreign state. Exercise known foreign IDs
without relying on enumeration, and verify permitted own/shared operations.

Run equivalent requests through direct tool dispatch, in-process MCP, Lua and
nested execution. Test missing authority, attempted widening, unclassified tools,
explicit-empty policy round trips, storage aliases, descendant cancellation,
credential rotation and scope removal. Where background work is deferred, prove
rejection leaves no durable or running work. Later job tests cover restart and
revocation without privilege expansion. Preserve administrator/channel workflows
and personal-connection/artifact separation.

Use live HTTP MCP with fake credentials for #54 and A2A interop tests for #55.
Run the repository's required Go, web and lint checks for implementation changes.
A passing tool catalog test alone does not establish resource isolation.

## Consequences

The expected simplification is one explicit execution identity, one tool-policy
composition rule and shared authorized resource entry points. Handlers stop
reimplementing identity discovery and ownership checks. Breaking permission
semantics are documented rather than supported through parallel legacy paths.

This introduces no dependency, generic RBAC engine, organization directory or OS
sandbox. Trusted administrators and deployment code remain trusted. Stronger
containment for mutually untrusted code would be a separate decision.
