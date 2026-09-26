# ADR 0002: Remote MCP OAuth and credential ownership

- Status: Proposed
- Date: 2026-09-26
- Related: #6

## Context

Remote MCP servers may require OAuth. Altinity MCP is the first interoperability target and advertises Client ID Metadata Documents (CIMD) rather than Dynamic Client Registration.

For dynamic connections, the Slack user is the source of authorization: the user explicitly asks Aviary to connect to an MCP URL, authenticates in a browser, and receives access to that resource for the lifetime of the issued credential.

Interactive OAuth must not block an agent goroutine and must not automatically replay an interrupted agent turn.

## Decision

### 1. Support MCP OAuth authorization-code flow with CIMD and S256 PKCE

For an OAuth-protected remote MCP resource Aviary performs:

1. protected-resource metadata discovery;
2. authorization-server metadata discovery;
3. CIMD client identity;
4. authorization-code flow;
5. S256 PKCE;
6. state validation;
7. authorization-code exchange;
8. bearer access-token use on subsequent MCP requests.

Aviary publishes a stable public HTTPS CIMD document and a public browser callback URL.

Dynamic Client Registration is not required for the initial Altinity interoperability target.

### 2. Interactive authorization is initiated by a remote connection attempt

A dynamic connection is explicit, for example:

```text
@aexp connect to https://mcp.cluster.environment.altinity.cloud
```

The connection attempt creates an outbound MCP client and starts the MCP handshake/tool discovery.

If the remote server returns an authorization-required response during initialization or discovery, Aviary creates an OAuth transaction and terminates the current agent run.

A later authorization-required response during a remote tool call behaves the same way.

No agent run waits for a browser.

### 3. Slack delivers a user-private authorization link

Aviary sends an ephemeral Slack message to the initiating user containing a short-lived link such as:

```text
https://aviary.example.com/oauth/mcp/start/<opaque-transaction-id>
```

The URL contains only an unpredictable transaction handle. OAuth state, PKCE verifier, resource identity, and Slack principal are stored server-side.

The start endpoint reconstructs the authorization request and redirects the browser through the MCP authorization server / upstream IdP chain.

The Aviary callback normally receives an authorization `code` and `state`. Aviary validates the transaction and exchanges the code at the authorization server's token endpoint.

### 4. OAuth transactions are short-lived and single-use

A transaction contains at least:

- random transaction ID;
- OAuth state;
- PKCE verifier;
- requested MCP endpoint;
- canonical resource identity when known;
- authorization-server identity;
- Slack workspace/team ID;
- Slack user ID;
- Slack channel/thread origin;
- requested scopes;
- creation/expiry time;
- consumed state.

Transactions are atomically consumed and cannot be reused.

### 5. Personal credentials are keyed by user and remote resource

For Slack-originated personal credentials, the logical key includes:

```text
Slack workspace/team
+ Slack user
+ canonical MCP resource/cluster
```

The endpoint string typed by the user is not by itself a sufficient long-term credential identity. After MCP protected-resource discovery, Aviary stores the canonical resource identity and binds credentials to it.

A user may reuse a still-valid personal credential for the same resource from another thread.

The personal credential owner is the authenticated sender of the current interactive turn. It must not be inferred from the thread creator, connection creator, most recent author, model arguments, or conversation text. Bob may read Alice's prior thread results when Slack permits that access, but any new personal-auth MCP discovery or call initiated by Bob must use Bob's credentials. If Bob has none, require Bob's authorization; never fall back to Alice's credentials.

Scheduled jobs cannot use personal credentials. This applies even when the job was created from a personal-authenticated conversation. Scheduled jobs may use configured static no-auth or shared-OAuth connections, subject to their permissions.

### 6. Shared credentials are allowed only for configured static servers

A statically configured server may specify shared OAuth credentials. Those are created through Aviary's control plane and are not owned by a Slack user.

If a shared credential requires interactive reauthorization, the Slack data plane must not silently replace it with the credential of whichever user encountered the error.

### 7. Token lifecycle follows server capabilities

Persist:

- access token;
- expiry;
- granted scopes;
- refresh token when issued;
- token metadata required for safe refresh.

If the server issues a refresh token, Aviary may refresh non-interactively.

If interactive authorization is required, Aviary terminates the current agent run and initiates a new authorization flow.

The current Altinity broker limitation of no downstream refresh token is accepted; reauthorization is required after expiry until that server behavior changes.

### 8. No automatic replay after OAuth

After successful callback Aviary may post a normal message to the originating Slack thread such as:

```text
Connected to <resource>. Send "continue" to continue.
```

The user sends a new message/turn.

Aviary does not automatically retry the failed tool call or restart the prior LLM turn. This avoids duplicate side effects from already-executed tools.

## Consequences

### Positive

- Credentials follow the human user who authorized the remote resource.
- Interactive browser work is decoupled from agent goroutine lifetime.
- The design does not require durable agent checkpoints or automatic replay.
- Static shared credentials and dynamic personal credentials coexist.

### Negative

- Slack principal identity must be propagated into the agent/tool context.
- The OAuth transaction store must survive long enough for browser completion.
- A user may need to reauthorize after token expiry when no refresh token exists.
- Associating a Slack user with the IdP identity they choose is intentional; strict identity-claim matching would require an additional policy.

## Rejected alternatives

### Deployment-wide credential for dynamic cluster connections

Rejected because different Slack users may have different Altinity/ClickHouse identities and permissions.

### Keep the original Go agent run blocked until OAuth completes

Rejected because human authorization is unbounded and should not hold an agent goroutine/session open.

### Automatically retry the interrupted agent run

Rejected because earlier tool calls may already have produced side effects. The user explicitly starts a new turn instead.
