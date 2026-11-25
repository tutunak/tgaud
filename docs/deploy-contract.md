# tgaud deploy contract v1 (app repo tutunak/tgaud <-> deploy repo Ansible/tgaud)

Both sides build to this file. A change to it is a change to both repos.

## Image

- `ghcr.io/tutunak/tgaud`, tags `<40-hex git tree hash>` (every pull request that passed its checks, pushed by GitHub
  Actions after `--selfcheck` passed on that very build) and `X.Y.Z` (every release: a push to `main` adds the next
  version to the image of its tree, same digest). No `latest` and no commit sha tag.
- No `USER` line: rootless Podman maps uid 0 in the container to the host's `tgaud` account. The root file
  system is read-only; the process writes only to `/tmp`. No volume: all state is in PostgreSQL.
- `ENTRYPOINT ["tgaud"]`, `CMD ["serve"]`.

## Commands

| Command | Network | What it does | Exit |
|---|---|---|---|
| `tgaud serve` | yes | the long-running service: bot ingest, worker (reconciler, prober, sampler), batch rollups, `/metrics` and `/health` | 3 when the database lacks one of the image's migrations (it never migrates by itself; a database with newer migrations than the image is accepted with a warning, so a rollback of the image works); 2 config error; 1 other failure |
| `tgaud migrate` | yes (DB only) | applies pending migrations as the tenant login under an advisory lock; a no-op when current; prints the version | 0 ok, 1 failure |
| `tgaud --selfcheck` | **none** (`--network=none`, read-only root, `--tmpfs /tmp`, all capabilities dropped) | offline: parses the config from the environment, loads every bundled migration and query file, imports every module; prints one line starting `selfcheck ok:` | 0 ok, non-zero with the reason on stderr |
| `tgaud botcheck` | yes (Bot API only) | calls `getMe` with `TGAUD_BOT_TOKEN` and prints the bot id; no database | 0 ok, 1 failure |
| `tgaud <report> ...` | yes (DB, Bot API for `link create` and `probe`) | the CLI reports (`overlap`, `unique`, `chats-per-user`, `user`, `campaign ...`, ...) as one-off containers | 0 ok, 2 usage, 1 failure |

## Environment

Secrets come as Podman secrets with `type=env`; everything else from the env file of the unit.

| Variable | Source | Required | Default | Meaning |
|---|---|---|---|---|
| `TGAUD_DATABASE_URL` | env file | yes | - | `postgresql://tgaud@<host>:<port>/tgaud?sslmode=disable`, built from the tenant contract's `postgresql_tenant_conn`; no password in it |
| `TGAUD_DATABASE_PASSWORD` | secret `tgaud_db_password` | yes in production | - | the tenant login's password; overrides any password in the URL |
| `TGAUD_BOT_TOKEN` | secret `tgaud_bot_token` | yes (serve, link create, probe) | - | the Bot API token |
| `TGAUD_LINK_HMAC_KEY` | secret `tgaud_link_hmac_key` | yes (serve, link create) | - | at least 32 bytes as hex; bot-made invite links are stored only as HMAC-SHA256 digests |
| `TGAUD_WEB_PROXY_TOKEN` | secret `tgaud_web_proxy_token` | yes when the web UI listens | - | every web request must carry it in `X-Tgaud-Proxy-Token` (Caddy adds it); host loopback is shared by every local account |
| `TGAUD_METRICS_LISTEN` | env file | no | `0.0.0.0:9108` | `/metrics` and `/health` inside the container; the Quadlet publishes `127.0.0.1:9108:9108` |
| `TGAUD_WEB_LISTEN` | env file | no | empty = **off** | the web UI (phase 2) listens only when set; the role sets `0.0.0.0:8097` when `tgaud_web_enabled` (published `127.0.0.1:8097:8097`) and leaves it unset otherwise |
| `TGAUD_STORE_NAMES` | env file | no | `true` | `false` blanks names and usernames (design.md 11) |
| `TGAUD_LOG_LEVEL` | env file | no | `INFO` | logs go to stdout/stderr, one line per record (journald) |
| `TGAUD_BOT_API_URL` | env file | no | `https://api.telegram.org` | for tests only |
| `TGAUD_ACTIVATION_ID` | env file | no | `""` | an opaque id the deploy role gives each activation; `/health` echoes it, so the role can tell which inputs answered |
| `TGAUD_PROBE_RATE` | env file | no | `5` | `getChatMember` calls per second for the prober (0.1-100); the real limit is unknown: the owner measures it (design.md 13.3) and sets it here |

The selfcheck runs with dummy secret values only if the real ones are absent; it never connects anywhere.

## HTTP (metrics port)

- `GET /metrics`: Prometheus text, values cached by the service, never a database query per scrape.
- `GET /health`: `200 ok` when the last successful `getUpdates` and the last successful ingest commit (a
  successful empty poll counts) are both younger than 180 s; otherwise `503` and the reason in the body. The body
  always includes `activation=<TGAUD_ACTIVATION_ID>`. Other
  jobs' transactions don't count: a head update that keeps failing turns it 503.
- `tgaud serve` stops within 10 s of SIGTERM (long polls of at most 8 s, in-flight SQL cancelled).
- The web UI (phase 2) is a separate listener on `TGAUD_WEB_LISTEN`. It trusts the reverse proxy for
  authentication (Caddy `basic_auth`, tailnet only) and does no login of its own, but answers 403 to any request
  without the right `X-Tgaud-Proxy-Token`. Readiness: `GET /healthz` on the web listener, with the proxy token like
  every web request: `200 ok activation=<TGAUD_ACTIVATION_ID>` when the web app answers and its database
  connection runs `select 1`; `503` with the reason otherwise; `403` without the token. The HMAC key is provisioned once: changing it re-keys every bot invite
  link and breaks campaign attribution, so the role refuses a change on a deployed host.
- Identity guards in the app: on its first start `tgaud serve` records the bot id (`getMe`) and a fingerprint of
  `TGAUD_LINK_HMAC_KEY` (HMAC of a fixed label, never the key) in the database; afterwards it exits 2 with a clear
  message when the token belongs to another bot or the key differs. Nothing is written before both checks pass.

## Database

- Tenant on the shared PostgreSQL 18.6 through `ansible/postgresql` `tasks_from: tenant`: name `tgaud`,
  `connection_limit: 10`, `account: tgaud`, `units: [tgaud.service]`, `check_sql: select count(*) from tg.member_event`,
  `client_ips: "{{ tgaud_db_client_ips }}"` (default `[]`; the owner adds a laptop for the collect script).
- `tgaud serve` holds at most 4 connections (ingest 1, worker 1, batch 1, web 1 in phase 2). The rest of the
  limit is for one-off CLI runs, `tgaud migrate` and the owner's collect script.
- Migrations run as the tenant login (NOSUPERUSER, owner of the database and of `public`); the only extension is
  `btree_gist`, trusted on 18.6. No superuser step in v1. Every migration after the first keeps the previous image
  working (additive only: new tables, nullable columns, new functions or views; no drop or rename of what a
  released image uses), so a rollback of the image after `migrate` works; the schema is never rolled back. `admin.sql` (roles `bi_reader`, `collect_writer`) is not
  run by the role in v1.

## Deploy order (the role)

pull by tag -> resolve the digest -> `--selfcheck` in the new image (offline) -> `tgaud migrate` in a one-off container
of the new image (network on) -> pin the Quadlet to `image@sha256:...` -> restart `tgaud.service` -> wait until
`/health` answers 200 or a timeout -> on failure keep or restore the previous digest and fail the play.
