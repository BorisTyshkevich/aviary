# MCP Tool Reference

Aviary exposes all of its capabilities as MCP tools. Any MCP-compatible client — Claude Code, another LLM, or a custom integration — can connect to the running server and invoke these tools directly.

Connect at `https://localhost:16677/mcp`. The token from
`~/.config/aviary/token` is an administrator credential and exposes the full
control plane. Integrations should use a scoped credential created with
`aviary client add` instead.

## Scoped inbound clients

```sh
aviary client add investigation-peer --protocols mcp --tools agent_run,ping --agents expert
aviary client rotate investigation-peer
aviary client remove investigation-peer
```

Add and rotate print the raw token once to local stdout after persistence.
Store it privately and send it only as `Authorization: Bearer <client-token>`.
Client cookies, query tokens, login and administrator APIs are denied. A client
lists and calls only its exact grants: `ping` and/or `agent_run`. Tool groups,
wildcards and all other tools are rejected as client grants.

`agent_run.name` must identify a granted configured agent. Omitted `session`
uses that client's own default conversation. A `session` is an exact logical
name within its client/agent namespace; `session_id` must resolve to an
existing persisted owned conversation. A stop message can stop only that
owned conversation. `X-Aviary-Agent-ID` does not affect client requests.
MCP transport sessions also bind to the authenticated principal across POST,
GET and DELETE, independently of conversation ownership.

Rotation invalidates old credentials and closes their transports while admitted
runs continue. The new token retains conversation ownership. Removal, or
removing the `agent_run` or relevant agent grant, cancels affected runs.
Client runs are synchronous and are not replayed after server/runner restart;
rotation or disconnection does not provide a recoverable task result.
Progress notifications remain supported, with tool progress limited to safe
registered names and states.

The external client grants do not restrict the agent's internal tool catalog:
the agent executes under its configured permissions. Conversation ownership
does not isolate agent-global memory or workspace. Use separate agents when
those resources must be private. See [security and permissions](../../guide/security-permissions)
and the [client configuration reference](../config#server-clients).

## Tool Categories

| Category | Tools | Description |
| --- | --- | --- |
| [Agent Tools](./agents) | `agent_list`, `agent_run`, `agent_stop`, `agent_run_script`, `agent_get`, `agent_add`, `agent_update`, `agent_delete`, `agent_template_sync`, `agent_rules_get`, `agent_rules_set` | Lifecycle, execution, and configuration of agents |
| [Session Tools](./sessions) | `session_list`, `session_create`, `session_messages`, `session_history`, `session_stop`, `session_remove`, `session_send`, `session_set_target` | Conversation history and channel delivery |
| [Task and Job Tools](./tasks-and-jobs) | `task_list`, `task_run`, `task_schedule`, `task_stop`, `task_compile_query`, `task_compile_get`, `job_list`, `job_query`, `job_logs`, `job_run_now` | Scheduled automation and execution history |
| [Browser and Channel Tools](./browser-and-channels) | `browser_open`, `browser_tabs`, `browser_navigate`, `browser_wait`, `browser_click`, `browser_keystroke`, `browser_fill`, `browser_text`, `browser_query`, `browser_screenshot`, `browser_resize`, `browser_eval`, `browser_close`, `channel_send_file` | Browser automation and file delivery to channels |
| [Files and Notes Tools](./files-and-notes) | `agent_file_list`, `agent_file_read`, `agent_file_write`, `agent_file_delete`, `file_read`, `file_write`, `file_append`, `file_truncate`, `file_delete`, `file_copy`, `file_move`, `exec` | Workspace files, filesystem access, and command execution |
| [Connections and Artifacts](./connections-and-artifacts) | `clickhouse_query`, `clickhouse_inspect`, `artifact_read` | Scoped personal queries and preparation evidence |
| [Auth Tools](./auth) | `auth_set`, `auth_get`, `auth_list`, `auth_delete`, `auth_login_anthropic`, `auth_login_anthropic_complete`, `auth_login_gemini`, `auth_login_openai`, `auth_login_github_copilot`, `auth_login_github_copilot_complete` | Credential storage and OAuth login flows |
| [Server and Config Tools](./server-and-config) | `ping`, `server_status`, `server_version_check`, `server_upgrade`, `config_get`, `config_save`, `config_restore_latest_backup`, `config_validate` | Server health, upgrades, and configuration management |
| [Usage and Skills Tools](./usage-and-skills) | `usage_query`, `skills_list`, `web_search` | Token analytics, skill discovery, and web search |

## Permissions

Which tools are available to an agent depends on its [permissions preset](/reference/config#agentspermissions):

| Preset | Available groups |
| --- | --- |
| `standard` _(default)_ | agent, session, task, job, browser, search, skills, usage, clickhouse, artifact |
| `full` | all tools |
| `minimal` | session, task, job, search, usage, clickhouse, artifact |

The `standard` preset blocks the `auth`, `exec`, `file`, `server`, and `chlab` groups. An explicit tool allowlist cannot exceed the preset. Personal connection and artifact tools also enforce their runtime scope; the control-panel tool runner cannot impersonate a Slack sender.

The Settings → Providers panel and the Tools runner in the control panel expose all tools regardless of agent permissions.
