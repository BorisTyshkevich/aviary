# Inbound MCP client verification

Issue #54 adds bearer-only inbound clients with exact `ping`/`agent_run` grants.
The control-plane bearer, cookies, login, and query authentication remain
administrator-only. ADR 0007 describes the shared identity foundation for #55.

Run the normal checks:

```sh
pnpm test:go
pnpm test:e2e
pnpm lint
```

Focused regression tests exercise real HTTP MCP transports and a fake streaming
model, with fake credentials only:

```sh
go test ./internal/mcp -run TestClientHTTP -count=1
go test ./internal/server ./cmd/aviary/cmd -run TestClient -count=1
go test -race ./internal/clientauth ./internal/mcp -run 'TestCredential|TestRevoked|TestDenied|TestClientHTTP' -count=1
```

The installed MCP SDK binds transport ownership through `TokenInfo.UserID`.
HTTP request cancellation alone does not stop every SDK method handler, and
`ServerSession.Close` waits for pending handlers. Register both HTTP streams
and method contexts for credential revocation. Keep agent execution contexts
separate: rotation detaches the caller while its admitted run continues.
Removing the client, its `agent_run` grant, or its agent grant cancels that run.

Client conversations persist `client_id`, `owner_agent_id`, and `protocol` in
the session header. Logical names are hashed from a JSON tuple containing the
protocol, immutable client ID, canonical agent identity, and exact name.
Authorization reads the persisted header; an ID prefix is insufficient.
Generic checkpoint recovery never replays synchronous client runs, preventing
restart recovery from admitting work outside the current credential policy.

For a live smoke test, use a separate temporary data/config directory and
`go run ./cmd/aviary serve --config <temporary-config> --data-dir <temporary-data>`
on an unused loopback port. Supply a fake administrator token in the temporary
`token` file and a local fake model endpoint. Keep the existing development
server running. Initialize MCP, retain `Mcp-Session-Id`, send
`notifications/initialized`, then inspect `tools/list` and call `ping` and
`agent_run` with fake peer tokens. Exercise client CLI add/rotate/remove against
that server; capture its generated test credentials privately rather than
printing them into shared logs. Confirm revoked credentials fail, new tokens
retain their owned conversation, and client tokens fail on administrator APIs.

Focused server tests that construct `New` use `setupServerDataDir` and
`resetSlogForTest` first. The Go default slog handler delegates through the
standard logger, which `slog.SetDefault` redirects; retaining it as a hub delegate
creates a recursive logging loop. Production CLI startup initializes an explicit
logging handler before server construction.

Local CLI mutations update only the `server.clients` YAML node and publish
with a same-directory temporary-file rename. The `.clients.lock` file excludes
other client commands. If a process crashes leaving that file behind, verify
that no client command is still running before removing it. Full-config editor
concurrency is tracked separately by #34. A custom `--config` path must match
the running server's configuration path for an installation acknowledgment.
