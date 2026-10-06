# ClickHouse source helper

`chsource` gives agents bounded, read-only access to the local upstream ClickHouse and Altinity Antalya checkouts and their Git history. `SKILL.md` is the `clickhouse-source` skill that tells agents how to use it.

| File | Purpose |
|---|---|
| `chsource` | Agent command: search, read, history, and `materialize` |
| `chsource-daily-pull` | Cron job: fast-forward a checkout, then refresh upstream release-series worktrees |
| `SKILL.md` | The `clickhouse-source` Aviary skill |
| `chsource_test.go` | Fixture-repository tests run by `pnpm test:go` |

## Install

```sh
install -m 0755 support/clickhouse-source/chsource support/clickhouse-source/chsource-daily-pull ~/.local/bin/
install -D -m 0644 support/clickhouse-source/SKILL.md ~/.config/aviary/skills/clickhouse-source/SKILL.md
```

`/etc/cron.d/clickhouse-daily-pull`:

```cron
SHELL=/bin/bash
MAILTO=""
GIT_TERMINAL_PROMPT=0
0 5 * * * ubuntu /home/ubuntu/.local/bin/chsource-daily-pull upstream
10 5 * * * ubuntu /home/ubuntu/.local/bin/chsource-daily-pull antalya
```

Each run appends to `<checkout>/tmp/git-pull-YYYY-MM-DD.log`. `MAILTO` is empty, so check that log when a checkout looks stale.

Allow these agent exec patterns next to the existing read-only operations: `chsource materialize *`, `chsource upstream materialize *`, and `chsource antalya materialize *`. Never allow `refresh`; only the daily job runs it.

## Requirements

Git 2.38 or later (for `ls-files --format`), ripgrep, `flock`, and GNU `grep`/`sed` (both use `-z`).

## Environment

| Variable | Default |
|---|---|
| `CHSOURCE_UPSTREAM_ROOT` | `$HOME/work/ClickHouse` |
| `CHSOURCE_ANTALYA_ROOT` | `$HOME/work/Antalya` |
| `CHSOURCE_TREES_ROOT` | `$HOME/work/chsource-trees` |
| `CHSOURCE_RG_BIN` | `$HOME/.local/bin/rg`, then `rg` on `PATH` |

## Committed-tree search

`search-at REF` resolves REF to a commit. If `$CHSOURCE_TREES_ROOT/<source>/<commit>` is a worktree whose `HEAD` is that commit, ripgrep searches its tracked files. Otherwise `git grep` searches the commit's tree. The header names the commit and the engine (`via worktree` or `via git grep`), and both print `PATH:LINE:TEXT`.

`git grep` has to inflate every blob from the pack. On the upstream checkout, an unscoped search takes about 1.2 to 5 seconds that way, against 0.1 to 0.7 seconds with ripgrep over a worktree. With a path, both take tens of milliseconds. `bounded_git` stops reading at the output limit, so a common token no longer forces a full scan.

Design decisions:

- **Trees are keyed by commit hash, not alias.** Lookup goes through tag resolution, so a moving alias needs no link swap. Between a fetch and a refresh, a new tag has no tree yet, and `search-at` falls back to `git grep`.
- **A tree is published only once it is complete.** `materialize` and `refresh` create the worktree under `.tmp-<hash>-<pid>` and `git worktree move` it into place while holding `.lock`. A reader never sees a partial checkout.
- **Superseded trees get a grace run.** `refresh` records the commits that aliases left in `.retired` and removes them on the next run, unless an alias or `materialize` (`.on-demand/<hash>`) still uses them. A search in flight during an update therefore keeps its files.
- **Modified trees are rebuilt.** `refresh` rebuilds any alias tree whose `git status` shows tracked changes. Searches check only `HEAD`, which keeps each call fast.
- **Submodule content is out of scope.** No command searches or reads files inside a submodule (for example `contrib/*` upstream), even when it is initialized in the checkout. `search` passes only non-gitlink tracked files to ripgrep, which already matches `git grep` on a commit tree and the worktrees, where submodules are empty. A path that is a submodule or lies inside one fails with `path is inside submodule …; submodule content is out of scope`. A match the agent cannot open, and cannot cite at a known commit, wastes calls and the match budget.
- **On-demand trees are uncapped.** They have no size cap or expiry yet; see [#56](https://github.com/BorisTyshkevich/aviary/issues/56).
- **`read-at` always reads the blob with `git cat-file`.** That is fast and cannot drift from the commit.

Worktrees share the checkout's object store; each tree costs only its checked-out files. Upstream release trees range from about 0.3 GB (24.8) to 1.1 GB (26.8), and the five series trees total about 2.5 GB.
