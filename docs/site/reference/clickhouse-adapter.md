# Direct ClickHouse adapter

The direct adapter uses `github.com/ClickHouse/clickhouse-go/v2` because the
standard HTTP client does not provide ClickHouse's typed result decoding and
query lifecycle. It accepts only HTTPS endpoints approved by `connections`.

Deploy a dedicated account with least-privilege read and inspection grants and
a server profile enforcing `readonly = 1`. The initial verifier accepts only
the effective `SHOW GRANTS FINAL` forms `GRANT SELECT ON ...` and `GRANT SHOW
ON ...`; it fails closed for role, `ALL`, administrative, or unrecognized
grants, including comma-separated access lists. Do not give this account
administrative, access-management, backup, named-collection, or settings
override authority: a client-side readonly setting does not make a privileged
account safe. The adapter checks the effective readonly setting but deployment
grants remain the server-side security boundary.

For private certificate authorities, install the CA in the host trust store.
The internal adapter also accepts a trusted CA bundle path, but Slack attachment
configuration does not yet expose per-target CA selection. TLS verification is
always enabled.

Configure canonical tool permission names `clickhouse_query`,
`clickhouse_inspect`, and (when using preparation) `artifact_read`. The model sees
generation-specific query/inspection names; stale names cannot follow a replaced
attachment. HTTPS policy defaults to deny and uses explicit host, port and CIDR
rules. Wildcards match subdomains at any depth, never the apex. Rewrites match
the logical host, replace its TCP destination port and use their own destination
CIDRs while retaining the logical Host and TLS identity.

Each operation creates and closes its own driver connection. Credentials are
validated against the effective readonly setting and conservative grants before
private setup completes. The database administrator must keep that account
restricted and configure execution, memory and result limits on the server.
Aviary does not make unrestricted credentials safe or police server-side table
functions through its outbound connection policy.

Unposted results and preparation evidence are kept out of shared session history,
provider continuation and tool-call argument logs. Built-in shared-memory writes,
nested agent runs, session publication, shared browser/lab tools and web search are
unavailable during a private turn. These tools have shared state or diagnostics;
they are not a private evidence publication mechanism. Read-only local tools and
explicitly configured trusted deployment executables remain available.
Configured host executables are trusted code, not a sandbox. A final Slack answer
is recorded in shared history only after successful delivery. Verbose Slack output also omits private tool details and progress; only the
final answer is published.

The driver normally derives `max_execution_time` from a Go context deadline.
The adapter deliberately hides that deadline from the driver while retaining
context cancellation, because an immutable read-only profile can reject a
client attempt to override its server-side execution limit.

The Go deadline bounds client work. To cancel HTTP SELECT execution after a
client disconnect, also set `cancel_http_readonly_queries_on_client_close = 1`
in the database profile. Require server `max_execution_time`, memory and result
limits regardless: disconnects and network failures are not a substitute for
server limits. These are profile requirements, not query settings sent by Aviary.
