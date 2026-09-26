# ADR 0004: Remote MCP URL policy and transport rewrites

- Status: Proposed
- Date: 2026-09-26
- Related: #6

## Context

Dynamic remote MCP means a Slack user can ask Aviary to connect to a URL supplied in the conversation.

That is intentionally an outbound-network capability from the Aviary host. It must therefore be controlled by deployment configuration.

The deployment may also need routing overrides. For example, a logical MCP URL under `*.altinity.cloud` may need to connect through an internal SNI proxy while preserving the logical HTTPS resource identity.

A universal rule such as "reject all private addresses" is not sufficient because private routing may be intentional.

## Decision

### 1. Dynamic endpoints must match configured URL/host policy

Aviary config defines which remote MCP URLs are permitted.

Conceptually:

```yaml
mcp:
  remote:
    network:
      allow:
        - scheme: https
          host: "*.altinity.cloud"
        - scheme: https
          host: "mcp.internal.example.com"
```

Patterns are matched against parsed URL components, not by substring matching.

For a host pattern:

```text
*.altinity.cloud
```

`foo.altinity.cloud` matches, while these do not:

```text
altinity.cloud.evil.example
foo.altinity.cloud.evil.example
```

The apex `altinity.cloud` is a separate match and must be explicitly allowed if desired.

### 2. Exact MCP endpoint paths are preserved

Aviary does not append `/mcp` automatically.

The user/configuration supplies the logical Streamable HTTP endpoint. Path and query canonicalization must not silently change the protected resource.

### 3. Network policy may include transport rewrite rules

Deployments may configure a logical-to-transport mapping, for example routing matching MCP hosts through an SNI proxy.

Conceptually:

```yaml
mcp:
  remote:
    network:
      rewrite:
        - match:
            host: "*.altinity.cloud"
          connect_via: "mcp-sni-proxy.internal:443"
          preserve_host: true
          tls_server_name: "$ORIGINAL_HOST"
```

The exact schema is implementation-defined, but the design separates:

- **logical resource URL** used for MCP/OAuth identity;
- **transport destination** used to establish the network connection.

Credentials, resource comparison, OAuth parameters, and Host/SNI semantics use the logical identity unless a specific protocol requires otherwise.

### 4. Policy applies to secondary network destinations too

Remote MCP/OAuth discovery can introduce additional URLs:

- protected-resource metadata;
- authorization-server metadata;
- redirects;
- authorization endpoint;
- token endpoint;
- CIMD fetch relationships.

Aviary must not treat server-provided URLs as automatically trusted merely because the initial endpoint was allowed.

Secondary destinations are checked against the appropriate configured policy before Aviary contacts them. Deployments may explicitly allow additional OAuth/identity-provider domains.

Transport rewrites may also apply to those destinations when configured.

### 5. DNS/private-address behavior is configurable policy

A hostname may resolve to public or private addresses, and deployments may intentionally route allowed names to internal addresses.

Therefore the architecture does not impose a universal "public IP only" rule.

The implementation must provide a clear hook for deployment policy to validate and/or rewrite resolved destinations before dialing.

This also provides the place to address DNS rebinding or environment-specific routing requirements.

### 6. Redirects cannot escape policy implicitly

HTTP redirects are either:

- rejected by default when the target does not satisfy policy; or
- followed only after evaluating the new logical target against policy.

A redirect must never turn an allowed initial URL into unrestricted internal network access.

### 7. Logging separates logical and transport destinations without leaking credentials

Connection diagnostics may log:

- connection alias;
- logical host/resource;
- selected rewrite rule/transport target;
- status/error class.

Logs must not contain bearer tokens, authorization codes, PKCE verifiers, sensitive callback parameters, or credential-bearing response bodies.

## Consequences

### Positive

- Users can connect arbitrary MCP servers within an administrator-defined trust boundary.
- Altinity wildcard domains can be enabled without enumerating every cluster.
- Internal SNI proxy/routing deployments are supported without changing OAuth resource identity.
- Security policy is explicit and deployment-specific instead of hidden in ad-hoc client code.

### Negative

- URL policy and HTTP redirect handling become security-sensitive code.
- OAuth discovery may require additional allowed domains.
- Rewrites make debugging logical-vs-transport identity more complex.
- DNS policy must be designed carefully for deployments that mix public and private routing.

## Rejected alternatives

### Allow any URL without policy

Rejected because `mcp_connect(url)` would become unrestricted outbound network access for any user who can invoke the agent.

### Hard-code `*.altinity.cloud`

Rejected because Aviary should remain a generic remote MCP client and deployments need different trust boundaries.

### Reject all private IP destinations

Rejected because private routing and SNI-proxy deployments may be intentional. The deployment policy decides what is allowed.
