# Before-turn preparation hooks

An agent can run an administrator-configured executable once before its first model request in a turn. It receives trusted execution and target metadata and can publish bounded evidence files. The executable is deployment code: Aviary does not sandbox its filesystem or network access. A collector that connects to a database must enforce its own endpoint and read-only policies.

```yaml
agents:
  - name: research
    hooks:
      before_turn:
        argv: ["/opt/collector", "prepare"]
        timeout: 15s
        on_error: continue
        allow_credential: false
```

`argv` is executed directly without a shell. `timeout` defaults to `15s` and may be at most `2m`. `on_error` is `continue` or `stop`; the caller applies that policy to a sanitized failure. `allow_credential` defaults to false. When enabled, only a matching interactive principal's current personal credential can be sent. Scheduled and control runs never receive a personal credential.

The process reads one JSON object from stdin. Protocol version 1 has this shape:

```json
{
  "version": 1,
  "execution": {"kind": "interactive", "scope": {"agent_id": "research", "installation_id": "i", "workspace_id": "w", "channel_id": "c", "root_thread_id": "t"}, "principal": {"installation_id": "i", "workspace_id": "w", "user_id": "u"}},
  "target": {"scope": {"agent_id": "research", "installation_id": "i", "workspace_id": "w", "channel_id": "c", "root_thread_id": "t"}, "transport": "clickhouse", "endpoint": "https://cluster.example", "generation": "opaque-id"},
  "credential_version": "opaque-version",
  "artifact_dir": "/private/assigned/staging/path"
}
```

When explicitly granted, the same private stdin pipe includes `credential: {"username":"...","password":"...","version":"..."}`. The executable must avoid printing or saving that value. Aviary constrains the child environment to `PATH`, `LANG=C`, and `LC_ALL=C`; it does not pass conversation text, inherited deployment secrets, or credentials through arguments or environment variables. An absent target has empty target fields. Scope and generation identify the current attachment; the hook's output cannot change them.

The process writes one JSON object to stdout:

```json
{"status":"complete","summary":"Schema observed","artifacts":["schema.json"],"observed_at":"2026-09-27T00:00:00Z","producer_revision":"collector-v1"}
```

Status is `complete`, `partial`, `unavailable`, `denied`, or `timed_out`. The summary is at most 4096 bytes; the full response is at most 64 KiB. At most 32 listed relative regular files may be published, each at most 1 MiB and 8 MiB total. Symlinks, traversal, extra files, and credential echoes are rejected. The accepted directory is renamed atomically into private storage, keyed by agent, principal, originating thread, target generation, credential version, and run. A run may be repeated after interruption; producers must be idempotent. Authorized historical runs in the same exact private scope remain readable with an explicit byte bound while retained. Runs expire after seven days; startup and new preparation remove expired runs. A 64 MiB store cap and 256-run cap evict the oldest runs sooner when needed. Evicted references become unavailable.

On Unix, timeout or cancellation kills the process group, including descendants. On Windows, Aviary starts the process suspended, assigns it to a Job Object, then resumes it. Completion and cancellation terminate the job and its descendants before evidence is inspected. If job assignment or cleanup fails, preparation fails closed. The hook and its evidence are untrusted model data, never instructions or permission changes. Evidence sent to the agent is also sent to its configured model provider.

Only personal interactive evidence disables shared persistence and provider
continuation. Non-personal control and scheduled runs retain their usual
session/job answer history and receive no personal database credentials.
