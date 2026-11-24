# tgaud: developer notes

`tgaud` is the Telegram Bot API service that records every join and leave of the owner's chats into PostgreSQL
18.6, next to the MTProto userbot `tgstats`. What it does and why is in `docs/design/design.md` (the rules),
`docs/design/summary.md` (the owner's view) and `docs/deploy-contract.md` (what the deploy repo relies on).
This file is how the code is put together and where each part plugs in.

## Tests

```sh
mise run ci                       # = uv run pytest: tgstats and tgaud, everything
```

The database tests need PostgreSQL 18.6 and `psql`:

- **With `TGAUD_TEST_ADMIN_URL`** (a superuser, or a login with CREATEROLE and CREATEDB) the tests use that
  server, e.g. `TGAUD_TEST_ADMIN_URL=postgresql://postgres:PASSWORD@127.0.0.1:55433/postgres uv run pytest`.
- **Without it** they start `docker.io/library/postgres:18.6-alpine3.24` (the local image) with podman on a free
  127.0.0.1 port with the production limits (512 MB, `shared_buffers` 64MB, `work_mem` 2MB, no parallel
  workers), and remove it at the end.

Each test session makes one tenant login like `~/projects/ansible/postgresql` does (NOSUPERUSER NOCREATEDB
NOCREATEROLE, CONNECTION LIMIT 10) with a unique name, and databases owned by it (PUBLIC revoked). `admin.sql`
runs as the admin (the roles `bi_reader` and `collect_writer` are cluster-wide and created only if missing),
migrations run as the tenant. Everything the session made is dropped at the end. Several worktrees can test
against one server at once. `psql` is the local binary if there is one, else the postgres image run with
podman (`--network=host`); without podman or psql the database tests fail, they do not skip.

Fixtures (`tests/tgaud/conftest.py`): `pg` (the server: `make_db()`, `psql()`, `tenant_url()`), `tgaud_dsn`
(one migrated database per session: never commit test data to it), `empty_dsn` and `migrated_dsn` (a fresh
database per test), `history_fixture` (the hand-checked fixture of `docs/design/verify.md` 2, loaded once per
session as `evidence/run-fixture.sh` does: read it in a transaction and roll back).

The fake Bot API (`tests/tgaud/botapi_fake.py`, never the real Telegram): `FakeBotApi` is an
`httpx.MockTransport` (`fake.api()` gives a wired `BotApi`), `FakeBotServer` a local HTTP server (`fake.url` for
`TGAUD_BOT_API_URL`, also from a container) that can hold or trickle a response. Both script answers per method
(`ok`, `error(code, ..., retry_after=)`, `updates`, `respond`) and record `calls`.

`tests/tgaud/serve_runner.py` runs `tgaud serve` through the real CLI with job modules from
`tests/tgaud/serve_fakes.py` put into `sys.modules` under the real names; that is how a test replaces a job.

## Layout

```
src/tgaud/
  config.py      every variable of the contract's Environment table; Secret; logging with redaction
  db.py          connect(config, job), migrate, schema_status, the service lock, the generation fence, maintenance
  sqlscript.py   the one lexer of psql-style SQL (migrations, queries.sql, admin.sql)
  sql.py         the loader of sql/queries.sql: block(id), run(conn, id, **params)
  botapi.py      BotApi on httpx, ApiError, outcome classes
  metrics.py     every metric of design.md 9, Health, the /metrics + /health listener
  history.py     lock_chats(conn, chat_ids); rebuild_pairs(conn, pairs) -> RebuildResult (history task)
  jobs/          the process model (__init__.py is the brief's "jobs.py") and one module per job
  commands/      report commands of the CLI (cli task)
  cli.py         argparse entry: serve, migrate, botcheck, --selfcheck, register()
  selfcheck.py   the offline check
  sql/           migrations/NNN_name.sql, queries.sql, admin.sql
```

## Process model (`tgaud serve`)

Three threads, each with its own connection (the contract's budget is 4 with the web UI):

| Thread | Runs | Connection |
|---|---|---|
| ingest | `tgaud.jobs.ingest.identify(ctx, conn)` and `startup(ctx, conn)` once, then `run(ctx, conn)`: the long poll | holds the service lock; never reopened: losing it ends the process (exit 1) |
| worker | `Scheduler` over `reconciler`, `prober`, `sampler` | reopened with backoff after a loss |
| batch | `Scheduler` over `chat_day`, `audience`, `partitions` | same |

A scheduled job module exports `make(ctx: JobContext) -> Job`; a `Job` has `name`, `due(now) -> bool` (cheap,
no I/O) and `step(conn)`, a bounded piece of work (target under 5 s) that commits and is idempotent (a commit
whose outcome is unknown after a lost connection is retried). The scheduler runs due jobs round-robin, opens
every step's transaction with the generation fence, commits after the step, records
`tgaud_job_last_success_timestamp_seconds{job}` and `tgaud_db_transaction_seconds{job}`, and on a failure logs
it, counts `tgaud_job_failures_total{job}`, rolls back (or discards a lost connection) and goes on with the
other jobs. `JobContext` carries `config`, `api` (a `BotApi`), `health`, `stop` (an Event), `generation`,
`clock`, `fence(conn)` and `fatal(code, reason)`.

**One serve, fenced by a generation.** `serve` takes `SERVE_LOCK` (a session advisory lock) on its ingest
connection; a second serve exits 2 at once. Then `ingest.identify(ctx, conn)` runs the identity guards (bot id
from getMe and the link key fingerprint against `tg.bot_state`; on the first start it records both); a
mismatch raises `StartupError` (exit 2) with nothing written, not even the generation. Only then does serve
increment `tg.bot_state.serve_generation` (`SELECT ... FOR UPDATE`), remember the value, and call
`ingest.startup(ctx, conn)` for the other startup writes. Every job transaction (ingest's per update, every scheduler
step, the metric cache) starts with `ctx.fence(conn)` = `SELECT serve_generation FROM tg.bot_state FOR KEY
SHARE`; when the value is not the process's own it raises `StaleGeneration` and the process exits 1. A
takeover's `FOR UPDATE` waits for in-flight job transactions, and after it an old process can commit nothing.
A step that commits more than once calls `ctx.fence(conn)` again first thing in each later transaction.
`tgaud.db.maintenance(conn)` (for `forget-user`) takes the same lock without waiting ("stop the service
first") and bumps the generation too.

**Health.** `/health` is 200 only while the last successful getUpdates and the last successful ingest commit
are both younger than 180 s, measured on a monotonic clock (a wall-clock step changes nothing); ingest marks them with `ctx.health.mark_poll_ok()` and `ctx.health.mark_commit()`
(an empty poll's commit counts). No other job can make the service healthy. The body always ends with
`activation=<TGAUD_ACTIVATION_ID>`.

**Shutdown.** The SIGTERM handler only sets a flag; the C-level handler also writes to a wakeup pipe, so the
supervisor thread (started first in `tgaud serve`, before argument parsing, config, schema check and startup)
notices within 100 ms even when the main thread is blocked. The supervisor never logs or flushes on its own
thread, so a stalled log sink (a full stderr pipe) cannot hold it from the deadline. It sets `ctx.stop`, cancels in-flight SQL and closes the Bot API
client (each in a daemon thread), and calls `os._exit(1)` if the process still runs 9 s after the signal.
Long polls use `timeout=8`. Correctness never depends on a graceful stop: every unit of work is a transaction.

**Exit codes** (contract): serve 0 after a signal, 1 failure, 2 config error / another serve / startup refused
(identity guards), 3 schema behind; migrate 0/1; botcheck 0/1; reports 0, 2 usage, 1 failure.

## Where the later tasks plug in

| Task | Edits |
|---|---|
| history | `tgaud/history.py`: `rebuild_pairs` behind the documented signature; migration 003 if it needs one |
| ingest | `tgaud/jobs/ingest.py` (`identify`, `startup`, `run`; the core stub already has the two identity guards in `identify`); its fix of `tg.move_chat_history` in 001 (must set `chat_alias.moved_at = now()`, see below); migration 007 |
| reconcile | `tgaud/jobs/reconciler.py`; migration 004 |
| probe-sample | `tgaud/jobs/prober.py`, `tgaud/jobs/sampler.py` (and the metric cache); migration 005 |
| rollups | `tgaud/jobs/chat_day.py`, `audience.py`, `partitions.py` (R1, R2 in Python, R4); migration 006 |
| cli | modules in `tgaud/commands/` that call `tgaud.cli.register(name, help, add_arguments, run)`; migration 008 |
| web | the web listener, started in `tgaud.jobs.serve` where the comment says, after `config.web_proxy_token_or_fail()`; migration 009 |
| image | Containerfile, Forgejo CI; adds the container check to the `ci` task of `mise.toml` |

## Migrations

- Files `src/tgaud/sql/migrations/NNN_name.sql`, applied in order by `tgaud migrate` as the tenant, each in one
  transaction together with its row in `public.tgaud_migration(version, name, sha256, applied_at)`, under an
  advisory lock. The runner owns the transaction: an exact top-level `BEGIN;` first and `COMMIT;` last (as in
  001) are removed; any other transaction control (`PREPARE TRANSACTION`, `COMMIT PREPARED`, `END`,
  `ROLLBACK`, a `BEGIN` with options, ...), psql meta command or psql variable is refused, and the ledger row
  is written only while the runner's transaction is still open.
- A recorded migration whose file changed is refused before anything is applied. Never edit a released
  migration; add a new one. (Nothing is released yet, so tasks of this run still change 001 objects their
  brief names, in place.)
- **Every migration after 001 is additive** (deploy contract): new tables, nullable columns, new functions or
  views; no drop or rename of anything a released image uses. The schema is never rolled back; an image
  rollback after `migrate` must keep working. `serve` exits 3 when the database lacks one of the image's
  migrations and accepts a newer database with a warning.
- Numbers: core 002 (every addition more than one task uses), history 003, reconcile 004, probe-sample 005,
  rollups 006, ingest 007, cli 008, web 009.

002 (`002_shared.sql`) adds: `tg.bot_state.caught_up_at`, `.link_key_fingerprint`, `.serve_generation`;
`tg.member_event.actor_is_other` with its BEFORE INSERT trigger (the rebuild and the join route read it, never
`actor_id`); `tg.chat_alias.moved_at` nullable without default; `tg.snapshot.applied_through_user_id` and the
CHECK `reconciled_needs_grade` (a grade may exist without `reconciled_at`, not the reverse);
`tg.reconcile_run.chunk`, `.from_user_id`, `.through_user_id`; `tg.gap_progress`; `tg.rollup_dirty.version`
(bumped by a trigger on every write) and `tg.mark_dirty(chat_id, from_day) -> version`; `tg.erasure`,
`tg.erasure_hash(user_id)`, `tg.is_erased(user_id, at)`; the trigger `snapshot_member_open_only`, which rejects
member rows for a finished snapshot (it locks the snapshot row FOR SHARE, so a page and the finalizer are
ordered either way).

## Locks

- `tgaud.history.lock_chats(conn, chat_ids)`: transaction-scoped advisory locks on each chat id in ascending
  order. Every writer of evidence or spells takes it before reading what it rebuilds from, and keeps that
  transaction short. The single-bigint key space of `pg_advisory_xact_lock` is reserved for chat ids.
- tgaud's own locks use the two-int4 form with class `0x74676175`: `MIGRATE_LOCK` (…, 1), `SERVE_LOCK` (…, 2).

## The query loader

`tgaud.sql.run(conn, "16a", as_of=..., chat=..., day=...)` runs a block of `queries.sql` in the caller's
transaction (it never commits) and returns `Result(columns, rows)` of the last statement that produced rows
(`all_results=True` for each). Parameters are the psql variables of the file's header, with its defaults;
the derived `as_of_ts`, `since_ts`, `day_ts`, `scope_ids` are computed by the header's own SELECT. `:'name'`
is bound as an untyped string (the server infers the type, as with psql's literal), `:name` with the type in
`tgaud.sql.PARAM_TYPES`; `explain="EXPLAIN (ANALYZE, BUFFERS)"` is a checked syntax option. `BEGIN`/`COMMIT`
in a block are dropped, `SET LOCAL` stays until the caller's transaction ends. `block(id)` returns the
statements; it refuses R1 and R2, whose branches are chosen at run time, but `run` executes those the way
psql does (`\gset`, `\if`), which the rollups task can compare its Python port against.

## Corrections and choices made in the core task

- **Collector recipe (design.md 5 step 3).** `collect_writer` may only INSERT into `tg.snapshot_member`, and
  PostgreSQL needs SELECT on the conflict target's columns, so `ON CONFLICT (snapshot_id, user_id) DO NOTHING`
  fails with "permission denied for table snapshot_member". Use the target-free `ON CONFLICT DO NOTHING` (the
  primary key is the only unique index, so it is the same rule). Grants are unchanged. Tested as
  `collect_writer`.
- **The fence is `FOR KEY SHARE`**, not `FOR SHARE`: `FOR SHARE` conflicts with ingest's per-update
  `UPDATE tg.bot_state SET next_offset`, so ingest would wait for every worker and batch transaction and could
  deadlock with `lock_chats` or a `rollup_dirty` row. `FOR KEY SHARE` conflicts only with the takeover's
  `FOR UPDATE`, which is all the fence needs. Tested both ways.
- **`chat_alias.moved_at` has no default after 002**, as plan review 3 asks, so 001's `tg.move_chat_history`
  (which does not set it) stores NULL until the ingest task's fix of that function sets `moved_at = now()`.
- **An empty scope** (`scope_ids` NULL because no chat is tracked) becomes `{}` in the loader, so a report
  on a fresh database returns no rows instead of failing.
- **`--selfcheck` fills dummies only for unset variables**: an explicitly empty value (say `TGAUD_BOT_TOKEN=`)
  is validated as `serve` would and fails the check.
- **`tgaud_job_failures_total{job}`** is added to the metrics of design.md 9 (the scheduler counts failing
  steps).
