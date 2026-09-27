# Connections and artifacts

Personal database tools require the authenticated Slack sender, originating thread,
current attachment generation, and that sender's private credential. MCP bearer
access and an agent/session ID do not impersonate a Slack sender. Control-plane
and scheduled calls cannot obtain personal database authority.

Select targets and complete private setup through the deterministic
[Slack commands](/guide/channels#connect-a-database-in-a-slack-thread). See the
[adapter requirements](/reference/clickhouse-adapter) and
[preparation protocol](/reference/preparation-hooks).

## ClickHouse query and inspection

Configure stable permission names `clickhouse_query` and `clickhouse_inspect`.
The agent sees `clickhouse_<generation>__query` and
`clickhouse_<generation>__inspect`. The runtime binds the generation; stale names
cannot route to another attachment. Missing credentials omit these tools.

| Argument | Description |
| --- | --- |
| `sql` | SQL text for query; inspection uses a fixed `system.columns` query |
| `max_rows` | Default 1,000; maximum 10,000 |
| `max_bytes` | Default 64 KiB; maximum 1 MiB for columns and encoded rows, excluding the small result envelope |
| `timeout_ms` | Default and maximum 30,000; the server profile also enforces limits |

Results contain the selected `target`, `generation`, typed `columns`, `rows`,
`query_id`, and `truncated`. Endpoint, credential and TLS options cannot be
supplied through tool arguments. Errors expose stable categories and numeric
server codes, without raw server error bodies or SQL echoes.

The canonical MCP tools also require a `generation` argument and enforce the
same trusted runtime authority. Knowing a generation alone grants no access.

## artifact_read

A configured preparation hook supplies a scoped evidence index before the first
model request. `artifact_read` is available within that prepared turn, subject to
effective tool permissions.

| Argument | Description |
| --- | --- |
| `run_id` | Published run from an authorized evidence index |
| `path` | Manifest-listed relative file path |
| `max_bytes` | Default 64 KiB; maximum 1 MiB; larger files are rejected rather than silently clipped |

The response includes content and provenance. Reads must match the complete
execution scope, target generation and credential version. Expired, evicted,
traversing, unlisted and cross-scope reads fail. Ordinary file and channel-file
tools cannot read or publish private runtime storage.

Personal evidence stays out of shared history and provider continuation. Built-in
shared storage/publication and shared browser/search tools are unavailable during
personal evidence turns. Administrator-authorized executables remain trusted
host code. Non-personal control and scheduled preparation preserves ordinary
session/job history and receives no personal credentials.
