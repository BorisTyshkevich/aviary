# Direct connections and preparation verification

The normal Go suite covers deterministic Slack ingress with fake credentials,
thread/principal/generation isolation, endpoint rejection, preparation lifecycle,
artifact confinement, and a complete MCP `agent_run` → preparation → model →
`artifact_read` path using a local model stub. No test-only impersonation tool is
registered in the production MCP server.

## Live connection smoke

Use two test accounts that can inspect `system.tables`. The adapter sends
`readonly=2` on every HTTPS connection and verifies its effective value; it
does not inspect or change grants. Test with an account that has broader grants
as well as a restricted account when possible. Operators can separately apply
server-side resource constraints, for example:

```sql
ALTER USER example_test_reader SETTINGS
    max_execution_time = 30 MIN 1 MAX 30,
    max_memory_usage = 268435456 MIN 1 MAX 268435456,
    max_result_rows = 10000 MIN 1 MAX 10000,
    max_result_bytes = 1048576 MIN 1 MAX 1048576,
    result_overflow_mode = 'break',
    cancel_http_readonly_queries_on_client_close = 1 MIN 1 MAX 1;
```

Mode 2 allows setting changes; positive resource minimums prevent setting a
limit to zero (unlimited), while maximums cap it. Readonly users can still use
`KILL QUERY` on their own queries. On the tested cluster,
`INSERT INTO FUNCTION null(...)` succeeds with a broad-grant account despite
`readonly=2`; mode 2 is not a universal SQL write filter.

Apply the complete settings list together: on the tested server,
`ALTER USER ... SETTINGS` replaces the previous list. Adding just the disconnect
setting otherwise removes the existing resource profile. Re-run the connection
smoke after any account/profile change.

Keep credentials in an owner-only JSON file outside Git:

```json
{"endpoint":"https://cluster.example:8443","accounts":[{"username":"test_alice","password":"FAKE-REPLACE-LOCALLY"},{"username":"test_bob","password":"FAKE-REPLACE-LOCALLY","expect_readonly_system_denial":true}]}
```

```sh
AVIARY_CLICKHOUSE_SMOKE_FILE=/private/accounts.json go test ./internal/clickhouseconn ./internal/mcp -run TestLive -count=1
```

The adapter smoke checks the effective `readonly=2` setting, typed/bounded query
results, rejected changes to `readonly`, and cancellation. For an account with
SYSTEM privilege, set `expect_readonly_system_denial` to check that the server
rejects `SYSTEM FLUSH LOGS` with READONLY code 164. The MCP smoke verifies live
identities
and schema inspection for both owners, including concurrent calls, stale
namespaces, permission denial, and non-personal execution restrictions. Fixtures
and returned identities are not printed on test failure. The smoke derives a
narrow temporary endpoint policy from current DNS; production policy still
requires an operator-approved host/port/address configuration.

For the joined collector → model → MCP smoke, add an optional
`preparation_argv` string array to the fixture, pointing to the deployment
collector executable and its operator-approved policy/cache arguments. The
`TestLivePreparedConnectionThroughMCP` test invokes that executable, reads its
artifact through MCP, and then queries the live account through the scoped MCP
tool. Without this field only the joined collector test skips.

The deployment repository has its own collector smoke and cache concurrency
suite. Preparation invokes trusted executables and is not an OS sandbox.
Windows Job Object tests require a Windows runner; crosscompilation only checks
build compatibility.

## Broader checks

Run `pnpm lint` for code changes, `pnpm test:go`, and `pnpm test:e2e` when web code
changes. Chromium tests may pass despite Playwright warning about libraries for
other installed browser engines. The e2e command reuses the dev server when it is
available and starts its configured dev command otherwise.

The new feature race tests can run independently of older lifecycle races:

```sh
go test -race ./internal/agent ./internal/mcp ./internal/server ./internal/channels -run 'TestConnection|TestPrivate|TestOrdinary|TestNested|TestPreparation|TestAgentPreparation|TestAgentRunPreparation|TestTurnPolicy|TestSlackConnection|TestParseSlackConnection|TestSlackVisible'
go test -race ./internal/connections ./internal/preparation ./internal/endpointpolicy
```

Broader reload/recovery and Signal lifecycle races are tracked in issues #17 and
#18. They are separate from the focused connection/preparation race checks.
