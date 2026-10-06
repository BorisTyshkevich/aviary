# Configuration Reference

All configuration lives in `aviary.yaml`, located at `~/.config/aviary/aviary.yaml` by default. The path can be overridden with the `--config` flag.

## Top-Level Structure

```yaml
server:    { ... }
agents:    [ ... ]
models:    { ... }
browser:   { ... }
search:    { ... }
scheduler: { ... }
connections: { ... }
skills:    { ... }
```

---

## connections

Controls generic outbound endpoint authorization for connection-backed features.
The policy is fail-closed: each logical HTTPS host and port, and every resolved
dial address, must be explicitly allowed. It is not specific to MCP.

```yaml
connections:
  network:
    allow:
      - host: "*.example.com"
        ports: [8443]
        cidrs: ["198.51.100.0/24"]
    rewrites:
      - host: "db.example.com"
        connect_via: "sni-proxy.internal:443"
        cidrs: ["10.0.0.0/8"]
```

`host` is exact or a left-most wildcard. `rewrites` preserve the logical URL,
HTTP Host header, TLS SNI, and certificate verification while dialing only the
authorized rewrite destination. URL userinfo, queries, fragments, ambient HTTP
proxies, and redirects are refused. A resolved address is pinned for the
connection so DNS cannot change it after authorization.

---

## server

Controls the HTTP server.

```yaml
server:
  port: 16677
  external_access: false
  no_tls: false
  failed_task_timeout: "6h"
  tls:
    cert: /path/to/cert.pem
    key:  /path/to/key.pem
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `port` | int | `16677` | Port the server listens on |
| `external_access` | bool | `false` | Bind to `0.0.0.0` instead of `127.0.0.1` |
| `no_tls` | bool | `false` | Disable TLS and serve plain HTTP |
| `failed_task_timeout` | string | `"6h"` | Maximum age of a pending checkpoint before the agent gives up. Accepts Go duration strings (`"30m"`, `"6h"`, `"24h"`). |
| `tls.cert` | string | | Path to TLS certificate file |
| `tls.key` | string | | Path to TLS private key file |

### server.clients

Scoped inbound MCP callers are managed locally with `aviary client`. Each
entry contains a generated immutable `id`, unique operator-facing `name`,
versioned `token_hash` (`sha256:` followed by 64 lowercase hexadecimal
characters), `protocols: [mcp]`, exact `tools`, and allowed configured `agents`.
Only `ping` and `agent_run` are valid tools. `agent_run` requires a nonempty
agent grant; a ping-only client can omit agents. IDs, names and token hashes
must be unique. Names match `[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}`; IDs match
`client_[0-9a-f]{32}`. Raw client tokens never persist in this configuration.

Clients authenticate only by bearer header on `/mcp`. The administrator token
continues to authorize the full control plane. See the [MCP reference](./mcp/)
for conversation ownership and execution policy boundaries.

Validated reloads install a coherent client policy. Rotation preserves identity
and admitted runs while closing old streams. Removing a client or its execution
scope cancels affected runs. Client CLI success includes acknowledgment from the
running server; offline commands explicitly report persistence for next startup.
An acknowledgment failure reports an error even if the disk update succeeded.

---

## agents

A list of agent definitions.

```yaml
agents:
  - name: assistant
    model: anthropic/claude-sonnet-4-6
    fallbacks:
      - openai/gpt-4o
    memory: shared
    memory_tokens: 4096
    compact_keep: 20
    working_dir: ~/workspace
    rules: |
      You are a helpful assistant.
    permissions: { ... }
    channels: [ ... ]
    tasks: [ ... ]
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `name` | string | _(required)_ | Unique agent name |
| `model` | string | | Model ID (e.g. `anthropic/claude-sonnet-4-6`). Falls back to `models.defaults.model`. |
| `fallbacks` | []string | | Ordered fallback model IDs if the primary model is unavailable |
| `memory` | string | | Memory pool: `"shared"`, `"private"`, or a named pool (e.g. `"team-memory"`) |
| `memory_tokens` | int | | Maximum tokens of memory content injected into each prompt |
| `compact_keep` | int | | Number of recent messages to retain during context compaction |
| `working_dir` | string | | Default working directory for file-path resolution. Supports `~` and environment variables. Defaults to the agent's data directory. |
| `rules` | string | | Inline markdown rules or a path to a file (e.g. `"./RULES.md"`) injected at the top of every system prompt. Paths are resolved relative to `working_dir`. |

### agents[].permissions

Restricts which tools an agent may use.

```yaml
permissions:
  preset: standard
  tools:
    - agent_run
    - file_read
  disabled_tools:
    - exec
  filesystem:
    allowed_paths:
      - "./workspace/**"
      - "!./workspace/private/**"
  exec:
    allowed_commands:
      - "git *"
      - "!git push *"
    shell_interpolate: false
    shell: /bin/bash
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `preset` | string | `"standard"` | Base tool surface: `"full"`, `"standard"`, or `"minimal"` |
| `tools` | []string | | Explicit tool allowlist. When non-empty, only the listed tools are offered. |
| `disabled_tools` | []string | | Tools to remove from the available set regardless of preset |
| `filesystem.allowed_paths` | []string | | Ordered allow/deny glob rules for `file_*` tools. Rules use gitignore-style globbing; prefix with `!` to deny. Relative paths resolve to the agent's data directory. |
| `exec.allowed_commands` | []string | | Ordered glob rules matched against the raw command string. Prefix with `!` to deny. |
| `exec.shell_interpolate` | bool | `false` | Allow shell variable interpolation in commands |
| `exec.shell` | string | | Shell binary to use for command execution |

**Preset levels:**

| Preset | Description |
| --- | --- |
| `"standard"` _(default)_ | Blocks higher-risk local and server tools (filesystem writes, exec, config mutations) |
| `"full"` | All tools available |
| `"minimal"` | Only the smallest safe subset of tools |

**Path prefix shortcuts** for `allowed_paths`:

| Prefix | Resolves to |
| --- | --- |
| `./` or relative | Agent's data directory (`~/.config/aviary/agents/<name>/`) |
| `~/` | User home directory |
| Absolute path | Used as-is |

### agents[].channels

A list of messaging channel connections for this agent.

```yaml
channels:
  - type: slack
    id: workspace-bot
    url: xapp-...
    token: xoxb-...
    model: anthropic/claude-haiku-4-5-20251001
    react_to_emoji: true
    send_read_receipts: true
    group_chat_history: 50
    disabled_tools:
      - exec
    allow_from:
      - from: "@alice"
        allowed_groups: "#alerts,#engineering"
        mention_prefixes:
          - "hey bot*"
        respond_to_mentions: true
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `type` | string | _(required)_ | Channel type: `"slack"`, `"discord"`, or `"signal"` |
| `enabled` | bool | `true` | Whether this channel connection is active |
| `token` | string | | Bot token for the channel. For Slack this is the `xoxb-...` token. For Discord this is the bot token from the Discord Developer Portal. |
| `id` | string | | Aviary's configured channel/integration ID. Used when routing task output, e.g. `slack:workspace-bot:#alerts`. |
| `url` | string | | Channel transport address. For Slack this is the App-Level token (`xapp-...`) used by Socket Mode. For Signal this is the `signal-cli` daemon address. Discord does not use `url`. |
| `model` | string | | Override model for all messages on this channel |
| `fallbacks` | []string | | Override fallbacks for all messages on this channel |
| `show_typing` | bool | `true` | Show Signal typing or fixed generic Slack assistant status for the selected channel route |
| `tool_progress` | `off` / `name` / `sql` | `off` | Slack only: temporary tool progress in the original thread. `name` shows tool name, state, and elapsed time; `sql` also shows safe inputs with SQL literals and comments redacted. |
| `tool_progress_max_calls` | integer | `100` | Slack only: maximum tool invocations shown across temporary progress messages; 1–1000 |
| `tool_progress_max_chars` | integer | `2800` | Slack only: maximum UTF-8 bytes in each progress message; 500–3900 |
| `reply_prefix` | string | | Slack only: text placed, followed by one space, before every message an agent run posts or edits on this route, for example `"🔒"`. A single line of at most 64 UTF-8 bytes without surrounding whitespace. |
| `reply_prefix_markers` | []string | | Slack only; requires `reply_prefix`. When set, the prefix applies only to runs whose question text contains one of these markers, for example `[":lock:", "🔒"]`. Up to 16 single-line markers of at most 64 UTF-8 bytes. |
| `separate_top_level_sessions` | bool | `false` | For Slack, start a distinct session for each top-level message thread instead of sharing one channel session |
| `react_to_emoji` | bool | `true` | Treat emoji reactions on the agent's own messages as prompts |
| `reply_to_replies` | bool | `true` | On Signal, respond to replies to the agent's messages. On Slack, let authorized participants continue a thread claimed by an explicit bot mention without another mention. |
| `ignore_other_user_mentions` | bool | `false` | On Slack, ignore claimed-thread replies that directly mention a user other than the owning bot. Requires `reply_to_replies: true`. Explicit mentions of another configured bot can still route to that bot. |
| `send_read_receipts` | bool | `true` | Send read receipts for messages the agent will act on |
| `group_chat_history` | int | `50` | Number of recent group chat messages retained as context. Set to `-1` to disable. |
| `disabled_tools` | []string | | Tools disabled for messages arriving on this channel |
| `allow_from` | []AllowFromEntry | | Sender and group filtering rules (see below) |

### Slack-specific Notes

- `id` is not a Slack workspace ID or channel ID. It is your Aviary integration name for that Slack connection.
- `url` must contain the Slack App-Level token (`xapp-...`) when `type: slack`.
- `token` must contain the Slack Bot token (`xoxb-...`) when `type: slack`.
- Slack apps using Events API and Socket Mode cannot send classic typing indicators, but `show_typing` enables fixed generic Slack assistant status on supported surfaces while Aviary is working. The selected agent/channel route controls this setting; status text never includes tool calls or their inputs or results.
- `tool_progress` is optional and independent of `show_typing`. It applies only to the selected Slack route. `off` emits no progress; `name` shows registered tool name, state, and elapsed time; `sql` additionally shows safe input details, including SQL with literals and comments redacted. Raw tool results and errors stay private. Progress spans multiple temporary messages when needed, each bounded by `tool_progress_max_chars`. It paginates calls through `tool_progress_max_calls` while the run is active; additional calls are counted on the last page. Temporary progress is removed after confirmed final delivery when Slack permits cleanup.
- `reply_prefix` applies only to the selected Slack route. It leads final answers, each split answer part, the visible introduction of an attached Markdown answer (not the file itself), fixed stop/failure/restart notices, post-restart recovery notices, and temporary progress messages. Connection setup prompts are not agent replies and are not prefixed. With `reply_prefix_markers`, each run decides once from its own question text (not linked or thread context), and that decision also covers its notices after a restart. Slack delivers a typed 🔒 as the shortcode `:lock:`, so list both forms. A thread reply without a marker is answered without the prefix even when the thread root had one. To mark one Slack channel differently from another on the same app, add a second channel entry for the same agent with the same tokens, a distinct `id`, and `allowed_groups` limited to that channel.
- Replace older boolean `tool_progress` values with one of the three named modes before restarting Aviary. Boolean values fail config validation. The former `true` setting displayed safe input details; `sql` is its explicit replacement and also applies to personal connected turns with an active target lease.
- The first authorized explicit bot mention or literal configured mention prefix claims a Slack thread for one agent when `reply_to_replies` is true. A bot mention can claim a channel-scoped catch-all rule or a rule configured to respond to mentions; a prefix-gated rule also requires its prefix. Wildcard mention prefixes remain ordinary message filters and do not claim or transfer threads. Authorized participants can continue a claimed thread without another mention. Sender, channel, exclude, and tool rules apply to each reply. Set `ignore_other_user_mentions: true` to keep a claimed thread's owner from responding when a reply directly tags another user, even if it also tags the owner. A mention of another bot routes that message when the other bot has an enabled channel rule that accepts its mention; it does not change the thread owner. Ambiguous explicit targets are ignored. Unclaimed threads still follow ordinary `allow_from` rules; mention-gated agents ignore their untagged replies. Set `reply_to_replies: false` to require a fresh bot mention or literal prefix for every reply.
- `ignore_other_user_mentions` controls reply routing and setup prompts. It does not change `group_chat_history`, which records channel messages for later context.
- Thread claims survive restarts and expire 90 days after the Slack thread root. Removing, disabling, or re-enabling the owning channel spec leaves its old claim without an active owner; start a fresh thread to establish new affinity.
- Set `separate_top_level_sessions: true` to keep each new top-level Slack message and its thread replies in a separate session.
- `users:read` is required on the Slack bot token if you want Aviary to resolve Slack user names for name-based routing.
- Slack Event Subscriptions should include both message events and the `app_mention` event if you want the bot to answer `@bot` mentions in channels.
- Slack scheduled task delivery routes use the form `slack:<configured-id>:<slack-channel-id>`.
- For Slack, Aviary accepts either raw IDs or friendly names in many places:
  `@alice` for users, and `#alerts` for channels in the common case. Raw Slack IDs still work when needed.

### Discord-specific Notes

- Enable the **Message Content Intent** for the bot in the Discord Developer Portal.
- `token` must contain the Discord bot token when `type: discord`.
- `id` is Aviary's configured integration name for that Discord connection, not a Discord channel ID.
- `allow_from[].from` should contain Discord user IDs.
- `allow_from[].allowed_groups` should contain Discord channel IDs.
- Discord scheduled task delivery routes use the form `discord:<configured-id>:<discord-channel-id>`.

**allow_from entries:**

Each entry controls which senders and groups can trigger the agent.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `from` | string | _(required)_ | Sender ID (phone number, user ID) or `"*"` for any sender |
| `enabled` | bool | `true` | Whether this entry is active |
| `allowed_groups` | string | | Comma-separated group/channel IDs or `"*"` for any group. When empty, only direct messages match. |
| `mention_prefixes` | []string | | Glob patterns matched against group message text. At least one must match (unless `respond_to_mentions` triggers). |
| `exclude_prefixes` | []string | | Glob patterns; messages matching any pattern are silently dropped |
| `respond_to_mentions` | bool | `false` | Also forward group messages that directly @mention the bot |
| `mention_prefix_group_only` | bool | `true` | When `true`, prefix/mention filtering applies only to group messages; direct messages from allowed senders are always forwarded. |
| `restrict_tools` | []string | | Override the tool allowlist for messages matching this entry |
| `model` | string | | Override model for messages matching this entry |
| `fallbacks` | []string | | Override fallbacks for messages matching this entry |

A plain string in `allow_from` is equivalent to `{ from: "<string>" }`.

### agents[].tasks

A list of scheduled or file-watch tasks for this agent.

```yaml
tasks:
  - name: daily-summary
    schedule: "0 9 * * *"
    prompt: "Summarize what happened yesterday and post to Slack."
    target: slack:workspace-bot:#alerts

  - name: process-new-files
    watch: "./inbox/*.csv"
    type: script
    script: |
      local files = aviary.changed_files()
      for _, f in ipairs(files) do
        aviary.run_agent("processor", "Process file: " .. f)
      end
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `name` | string | _(required)_ | Unique task name within this agent |
| `enabled` | bool | `true` | Whether this task is active |
| `type` | string | `"prompt"` | Task type: `"prompt"` or `"script"` |
| `schedule` | string | | Cron expression for time-based scheduling. Supports standard 5-field cron and shortcuts like `@hourly`, `@daily`, `@weekly`. |
| `start_at` | string | | ISO 8601 datetime for the first run (e.g. `"2026-01-01T09:00:00Z"`) |
| `run_once` | bool | `false` | Run the task once then disable it |
| `watch` | string | | File glob pattern; the task runs when matching files change |
| `prompt` | string | | Prompt text sent to the agent (for `type: prompt`) |
| `script` | string | | Lua script executed directly (for `type: script`) |
| `target` | string | | Target session or output destination. Use `session:<name>` for a session or `<channel-type>:<configured-channel-id>:<delivery-id>` for channel delivery. |

---

## models

Provider credentials and default model settings.

```yaml
models:
  providers:
    anthropic:
      auth: auth:anthropic:default
    openai:
      auth: auth:openai:default
    gemini:
      auth: auth:gemini:default
    github-copilot:
      auth: auth:github-copilot:default
  defaults:
    model: anthropic/claude-sonnet-4-6
    fallbacks:
      - openai/gpt-4o
```

| Field | Type | Description |
| --- | --- | --- |
| `providers.<name>.auth` | string | Credential reference in the form `auth:<key>` (see `aviary auth set`) |
| `defaults.model` | string | Default model used by agents that do not specify one |
| `defaults.fallbacks` | []string | Default fallback models used by agents that do not specify their own |

**Supported providers:** `anthropic`, `openai`, `gemini`, `github-copilot`

---

## browser

Browser automation settings.

```yaml
browser:
  binary: /usr/bin/chromium
  cdp_port: 9222
  profile_directory: Default
  headless: false
  reuse_tabs: true
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `binary` | string | _(auto-detected)_ | Path to a Chrome or Chromium binary |
| `cdp_port` | int | `9222` | Chrome DevTools Protocol debugging port |
| `profile_directory` | string | `~/.config/aviary/browser` | Chrome user data directory |
| `headless` | bool | `false` | Run Chrome in headless mode |
| `reuse_tabs` | bool | `true` | Reuse an existing page tab in `browser_open` when the requested URL exactly matches the current URL |

---

## search

Web search backend settings.

```yaml
search:
  web:
    brave_api_key: BSA...
```

| Field | Type | Description |
| --- | --- | --- |
| `search.web.brave_api_key` | string | Brave Search API key for web search |

---

## scheduler

Task execution settings.

```yaml
scheduler:
  concurrency: auto
  precompute_tasks: true
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `concurrency` | int or `"auto"` | `"auto"` | Maximum concurrent task jobs. `"auto"` uses `runtime.NumCPU()`. |
| `precompute_tasks` | bool | `true` | Pre-compile prompt tasks when they are scheduled, rather than at run time |

---

## skills

Enables and configures installed skill runtimes. Keys are skill names.

Installed disk skills are loaded from `~/.config/aviary/skills` and `~/.agents/skill`. Search for published skills with `npx skills find` or on [skills.sh](https://skills.sh/), then install one globally with a command like `npx skills add --global -a universal owner/skill-name`.

```yaml
skills:
  my-skill:
    enabled: true
    settings:
      api_url: https://example.com/api
      timeout: 30
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `<name>.enabled` | bool | `false` | Whether this skill is active |
| `<name>.settings` | map | | Skill-specific key/value settings passed to the skill runtime |

---

## Full Example

```yaml
server:
  port: 16677
  external_access: false
  failed_task_timeout: "6h"

models:
  providers:
    anthropic:
      auth: auth:anthropic:default
  defaults:
    model: anthropic/claude-sonnet-4-6

agents:
  - name: assistant
    model: anthropic/claude-sonnet-4-6
    memory: private
    memory_tokens: 2048
    working_dir: ~/projects/my-app
    rules: |
      You are a helpful coding assistant with access to the project workspace.
    permissions:
      preset: standard
      filesystem:
        allowed_paths:
          - "./src/**"
          - "./tests/**"
      exec:
        allowed_commands:
          - "go test *"
          - "go build *"

  - name: lobby
    model: anthropic/claude-sonnet-4-6
    memory: shared
    channels:
      - type: slack
        id: workspace-bot
        url: xapp-your-app-level-token
        token: xoxb-your-bot-token
        allow_from:
          - from: "*"
            allowed_groups: "#alerts"
            respond_to_mentions: true
    tasks:
      - name: morning-standup
        schedule: "0 9 * * 1-5"
        prompt: "Post a good morning message to the team."
```
