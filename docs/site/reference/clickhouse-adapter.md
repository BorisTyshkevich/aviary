# Direct ClickHouse adapter

The direct adapter uses `github.com/ClickHouse/clickhouse-go/v2` because the
standard HTTP client does not provide ClickHouse's typed result decoding and
query lifecycle. It accepts only HTTPS endpoints approved by `connections`.

Every ClickHouse HTTPS connection sends `readonly=2`. Setup checks that the
effective setting is exactly 2. Aviary does not inspect or change the account's
grants. ClickHouse mode 2 blocks ordinary table writes and DDL while allowing
setting changes and temporary tables. On the tested cluster, even
`INSERT INTO FUNCTION null(...)` succeeds under mode 2. Readonly users can still
use `KILL QUERY` on their own queries.
Broader grants can also expose external table functions and data sources, and
mode 2 can change audit or resource settings unless the server constrains them.
Operators who need a stronger boundary should restrict grants and settings at
the database. Credential-forwarding preparation hooks run in a separate process
and must apply `readonly=2` to their own ClickHouse requests.

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
validated against the effective `readonly=2` setting before private setup
completes. The database administrator controls grants and server-side execution,
memory and result limits. Aviary does not police server-side table functions
through its outbound connection policy.

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
context cancellation, so it does not add another ClickHouse setting.

The Go deadline bounds client work. Operators can set
`cancel_http_readonly_queries_on_client_close = 1` in the database profile to
cancel HTTP SELECT execution after a client disconnect. Server-side execution,
memory and result limits are recommended because client disconnects and network
failures do not reliably bound work on the server. Aviary sends none of these
resource settings on the connection.

Slack connection setup uses `connect [clickhouse] URL [username]`. The optional
username is not a secret and belongs in the command. Otherwise the login comes
from the authenticated sender's Slack profile email. There is exactly one DM
password prompt, never a separate username prompt. An unavailable email produces
an actionable setup error instead of treating a password as a username.
Successful setup posts one confirmation in the original channel thread after
the optional post-connect collector finishes. Validated version and uptime may
appear there without an LLM call. Its private evidence snapshot remains bound
to the credential and target for later turns, without implicit refresh. A failed
password check leaves the same private prompt available for a corrected password
until it expires or the selected target changes. The DM receives no success reply.

Mode 2 permits setting changes, so operators can constrain resource settings
with positive minima and maximums. For example, use
`max_execution_time=30 MIN 1 MAX 30`; zero disables several limits. The adapter
does not rely on SQL classification to enforce ordinary read-only behavior.
Accounts pinned to an incompatible value such as `readonly=1` cannot connect.
