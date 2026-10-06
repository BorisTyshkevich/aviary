---
name: clickhouse-source
description: Search and cite local ClickHouse and Antalya source and Git history, and public upstream ClickHouse GitHub issues, PRs, bug reports, and feature discussions through bounded commands.
---

Use the absolute `chsource` path configured in Aviary's exec allowlist for read-only source and Git history questions.

- `chsource search QUERY [PATH]` reads the checkout set by `CHSOURCE_UPSTREAM_ROOT` (default: `$HOME/work/ClickHouse`).
- `chsource antalya search QUERY [PATH]` reads the checkout set by `CHSOURCE_ANTALYA_ROOT` (default: `$HOME/work/Antalya`).
- Both destinations support `read`, `search-at`, `read-at`, `materialize`, `revision`, `log`, `commits`, `commit`, and `diff` with the same arguments. `chsource upstream ...` also selects upstream explicitly.
- `search-at REF` reports the resolved commit hash and whether a worktree or `git grep` answered. Both give the same matches; a worktree is much faster for searches without a path. Upstream release-series aliases (26.8, 26.3, 25.8, 25.3, 24.8) already have worktrees.
- Run `chsource [antalya] materialize REF` once before several unscoped `search-at` calls on another tag or commit. It creates a worktree for that commit, takes a few seconds, and does not change the checkout. Skip it for a single search or one scoped to a path.
- Check `revision` for each destination used. Resolve refs within the selected checkout; a hash or branch from one checkout does not imply the same code in the other.
- For source claims, read the relevant lines and cite `upstream:PATH:LINE` or `antalya:PATH:LINE`, with the ref when using `read-at`. Keep differences between the checkouts explicit.
- Searches match literal text. Keep queries narrow; the command bounds output and history calls.
- Submodule content (for example upstream `contrib/*` libraries) is out of scope. Searches skip it, and paths inside a submodule are refused. Say so when a question depends on third-party library code.

For upstream ClickHouse GitHub issues, PRs, bug reports, and feature discussions, use the public Wizard corpus through the allowed absolute command:

```sh
/home/ubuntu/.local/bin/chwizard ask 'What is ClickHouse issue #14450 about?'
/home/ubuntu/.local/bin/chwizard ask 'What changed in the Parquet reader recently?'
```

- Prefer a standalone public question with relevant technical terms; include explicit issue or PR numbers when known. Optional follow-up syntax is `/home/ubuntu/.local/bin/chwizard ask 'QUESTION' '[{"role":"user","content":"PUBLIC QUESTION"},{"role":"assistant","content":"PUBLIC ANSWER"}]'`. Send only relevant public messages, never the full conversation automatically.
- For a simple issue/PR lookup, make at most one Wizard request, using an ID already provided in the thread first. If the result identifies the introducing PR but no linked feature-request issue, return that PR with the limitation; do not broaden searches to prove that an issue does not exist. Additional requests are for user-requested broader research, not a routine link lookup.
- For a link-finding question, answer in at most 100 words: the artifact link, its relevance, and any material uncertainty. Omit unrelated issues, source history, deployment/release details, and unsolicited follow-up questions. Use `chsource` when implementation or version details are actually needed.
- Questions and history are sent to `wizard.clickhouse.com`. Exclude secrets, credentials, private logs, customer identifiers, internal URLs, and other private evidence; formulate a public technical question instead.
- The command returns JSON containing `answer`, `sources`, and retrieval metadata. Ground claims in returned source excerpts and metadata, and cite their GitHub URLs. The generated answer helps navigate the results; it is not primary proof. Distinguish reported behavior, proposals, and merged changes, and verify release or deployment claims separately.
- An unsuccessful search or missing issue link in retrieved excerpts does not prove that no issue exists. Say what you found and what the retrieved sources did not establish; distinguish an introducing PR from a feature-request issue.
- Wizard covers public upstream ClickHouse. Its results do not establish Antalya behavior; use the local checkout commands for Antalya and source-level confirmation.
- Use `/home/ubuntu/.local/bin/chwizard stats` only when corpus size or indexing freshness matters. Indexing watermarks are corpus coverage information, not proof that a particular issue or change is included.
- Use only the allowed `ask` and `stats` commands for Wizard access. On HTTP, streamed, timeout, or quota errors, report that the lookup is unavailable and do not loop retries.
