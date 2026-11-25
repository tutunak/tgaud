# tgstats

A Telegram **user bot**: it logs in as your own Telegram account, collects the member lists of every
group, supergroup and channel where you are creator or admin, stores them in a local SQLite file, and
answers two questions:

1. **Unique users**: for each chat, how many members are in that chat and in none of the other compared chats.
2. **Intersection**: for two chats, how many users are in both (plus percentages and the Jaccard index).

## Why it must be a user account

The Bot API cannot list the members of a chat; bots only see the people who write to them. Listing members
needs the MTProto API as a real account, so tgstats uses [Telethon](https://docs.telethon.dev) and logs in as
you (phone, code, and 2FA password if you have one). It only reads: it never posts, joins or leaves anything.

## Get an API id and hash

1. Log in at <https://my.telegram.org> with your phone number.
2. Open **API development tools** and create an application (any name).
3. Copy `api_id` and `api_hash`.

Put them in the environment or in a `.env` file in the directory you run tgstats from:

```sh
cp .env.example .env
# edit .env: TG_API_ID=123456  TG_API_HASH=0123abcd...
```

Real environment variables win over `.env`. Optional: `TG_SESSION` (default `tgstats.session`) and `TG_DB`
(default `tgstats.db`), or the `--session` / `--db` flags on each command.
A missing id or hash prints a one-line error pointing here instead of a traceback.

## Install

With [uv](https://docs.astral.sh/uv/):

```sh
uv sync
uv run tgstats --help
```

Or install the command: `uv tool install .` and run `tgstats ...`. The examples below use plain `tgstats`;
prefix with `uv run` inside the checkout.

## First login

```sh
tgstats login
```

Asks for your phone number, the code Telegram sends you, and your 2FA password if set, then prints who is
logged in. The session is saved to `tgstats.session`.

**The session file grants full access to your Telegram account. Keep it private**: do not commit it, share it
or put it in a synced public folder. tgstats sets it to owner-only permissions; `.gitignore` excludes
`*.session`, `.env`, `*.db` and `*.csv`. If it leaks, end the session in Telegram under
Settings > Devices.

## Commands

A `CHAT` argument is a numeric id, an `@username`, or an exact title. An unknown or ambiguous one is an error
that lists the candidates.

### `tgstats chats`

Lists the chats where you are creator or admin: id, type (group / supergroup / channel), title, @username,
the member count Telegram reports, and when each was last collected.

```sh
tgstats chats
```

### `tgstats collect [CHAT ...]`

Fetches the full member list of every admin chat (or only the named ones) and stores it as a **new
snapshot**. Old snapshots are kept; every analysis uses the latest snapshot of each chat.

```sh
tgstats collect                       # all admin chats
tgstats collect @mychannel "My Group" # only these
tgstats collect --deep                # also search by one, then two characters when the listing is short
tgstats collect --deep --expand-at 0  # ... and run every two-character query (slow)
```

It prints fetched vs reported per chat and marks a snapshot **INCOMPLETE** when fetched is clearly below
reported (more than 2% short). FloodWait errors are slept through (it prints the wait). If one chat fails
(no rights, private, an RPC error) the error is printed, the run continues, and the exit code is non-zero.

Telegram's member count is read twice, fresh each time (for basic groups too, never the dialog list's copy):
before the listing and after it. "Reported" is the larger of the two. Every snapshot also records what tgaud
needs to load it (see "Loading the members a chat already has" below): a random id, when the collection started
and finished, both counts, how it ran (`basic_group`, `listing` or `deep`), and for every member what the page
that first listed them said (when that page arrived, the join date, who invited them, a join request,
a subscription end, bot, deleted, premium, username and name). A member listed again on a later page keeps the
first page's record. A snapshot is also INCOMPLETE, whatever the counts say, when a search query failed during
`--deep` or the count after the listing could not be read: what was found is kept. A `tgstats.db` from an older
version gets the new columns the first time this version opens it; its old snapshots read back as before.

Before each request to Telegram, `collect` and `chats` pause for a random time between 1000 and 5000 ms
(picked to the millisecond), so requests don't arrive at a fixed rate. Change the range with
`--min-delay-ms` and `--max-delay-ms`, for example `tgstats collect --min-delay-ms 2000 --max-delay-ms 8000`.
Every request is paced, including the chat discovery and the login check at the start. Not paced: the handshake
Telethon sends when it connects (the first request, so there is nothing before it), and Telethon's own retries of a
request after a Telegram server error, which wait a fixed 2 s each, at most 5 times. A big chat takes longer:
200 members per request, so 10,000 members is 50 requests, about 2.5 minutes of pauses on average.

`--deep`: when the plain listing comes back short, it runs member searches and merges what they find. It
stops as soon as the snapshot is no longer incomplete. Two passes:

1. **Single characters**: 69 queries, one per Latin letter, digit (`a`..`z`, `0`..`9`) and Cyrillic letter.
2. **Two characters**, only if the snapshot is still incomplete after pass 1: `aa`, `ab`, ..., `a0`, ..., `99`,
   then `аа`, `аб`, ... A pair uses two characters of the same group (Latin letters and digits: 36 characters,
   36 x 36 = 1296 pairs; Cyrillic: 33 characters, 33 x 33 = 1089 pairs), never one of each. That is up to
   2385 pair queries, 2454 searches with pass 1, at most.

Pruning: a first character is expanded into its pair queries only when its single-character search returned at
least `--expand-at N` members (default 100). Anyone matched by `cx` is also matched by `c`, so a
single-character search that returned fewer results than any search cap cannot be hiding anyone its pairs
would find. Before pass 2 it prints how many queries will run and how many first characters were skipped,
then one line per expanded first character (`deep 'a?': 36 queries, +123 new, 5012 total`). `--expand-at 0`
expands every character: the full grid of 2385 queries. A negative value is an error.

Time: every request waits 1 to 5 s (about 3 s on average at the default pauses), so the full grid is about
2 hours per chat, more when a search needs several pages (200 members each). A flood wait adds its own sleep.
Pruning usually makes it much shorter. A failed query (other than a flood wait) prints the error and stops
the deep search; what was found so far is kept.

**Ctrl-C keeps a partial snapshot.** If you interrupt a collection, the members found so far for that chat are
stored as an **incomplete** snapshot, but only if they number more than the chat's latest snapshot (or the chat
has none yet); otherwise nothing is stored, so an early Ctrl-C never replaces a better snapshot. It prints which
happened. Chats after the interrupted one are not collected; the exit code is 130.

Limit: the searches match the characters above, so members whose names and usernames use none of these scripts
(for example Arabic or Chinese) are not found this way, and the snapshot can stay incomplete. Without `--deep`,
a run that ends with an incomplete snapshot tells you to try `tgstats collect --deep CHAT`.

### `tgstats unique [--among CHAT ...] [--chat CHAT] [--list] [--csv FILE]`

Per chat: how many of its members are in no other compared chat, out of its total. The compared set is every
collected chat, or the `--among` chats. `--chat` narrows the output to one chat (the comparison still uses
the whole set); `--list` prints the users (id, @username, name); `--csv` writes them to a file.

```sh
tgstats unique
tgstats unique --among @mychannel "My Group" --chat @mychannel --list --csv unique.csv
```

### `tgstats intersect CHAT_A CHAT_B [--list] [--csv FILE]`

The number of common users, that number as a percentage of A and of B, and the Jaccard index
(common / union).

```sh
tgstats intersect @mychannel "My Group" --csv common.csv
```

### `tgstats overlap [--among CHAT ...]`

A matrix of pairwise common-user counts for all collected chats (or the `--among` ones); the diagonal is each
chat's own member count.

```sh
tgstats overlap
```

### Bots and deleted accounts

They are left out of `unique`, `intersect` and `overlap` by default. Add `--include-bots` to keep them.

## Completeness caveat

Telegram may return **fewer members than a chat has**: large supergroups cap the plain member listing, and
channel subscriber lists may be capped too. `collect` shows fetched vs reported; `unique`, `intersect` and
`overlap` print a warning naming every compared chat whose latest snapshot is incomplete. What that means:

- Absolute common-user counts (`intersect`, `overlap`) are a **lower bound**.
- Unique counts, percentages and the Jaccard index can be **wrong in either direction, even for a chat whose
  own snapshot is complete**: a user missing from B's snapshot looks unique to A.

Examples: if the truth is A = B = {1, 2} but B's snapshot only has {1}, tgstats reports one user unique to A
where the truth is zero. If the truth is A = {1, 2, 3} and B = {1, 2, 4} but both snapshots only have {1, 2},
tgstats reports 100% overlap and Jaccard 1 where the truth is 66.7% and 0.5.

Treat results involving an incomplete snapshot as estimates.

## Development

```sh
uv run pytest
```

The tests are offline: they use a fixture database and a fake client, never the network.

## tgaud

`tgaud` is the second program in this repository: a Telegram **Bot API** service that records every join and leave
of your channels and groups into PostgreSQL 18.6 with full history (one row per stay, with how sure each start and
end is), and the analytics on top: overlap, unique members, campaign and giveaway joiners, short stays, drift
against Telegram's member count, `/metrics` for Prometheus. It runs as one container (`tgaud serve`) next to the
database; `tgstats collect` and the web UI's Import page seed it with the members from before the bot (below).

Read, in this order:

- `docs/design/summary.md`: what it answers, for the owner;
- `docs/design/design.md`: the rules (history model, snapshot contract, probes, metrics);
- `docs/deploy-contract.md`: the image, commands, environment and deploy order the Ansible role relies on;
- `docs/tgaud.md`: how the code is put together, and the tests.

### Loading the members a chat already has

The Bot API cannot list a chat's members, so tgaud only knows the people who joined or left since the bot became an
admin: right after setup its reports show a handful of known members against Telegram's thousands. Load the rest
with a member list taken by `tgstats` with your own account:

1. **Collect** with this version of tgstats: `tgstats collect` (or `tgstats collect CHAT ...`; `--deep` when a
   listing comes back short). Snapshots collected by an older tgstats are not imported (they record only when a
   collection ended, which can contradict leaves tgaud saw meanwhile), so collect again after upgrading.
2. **Upload** the `tgstats.db` on the web UI's **Import** page. At most 32 MiB per file; a file that has grown past
   that can be collected into a fresh one: `tgstats collect --db fresh.db`, then upload `fresh.db`.
3. **Read the result.** The page lists every chat in the file and what happened to its latest snapshot:
   - *imported*: its members were written (the count shows how many; people removed with `tgaud forget-user` are
     left out);
   - *skipped: not tracked*: tgaud records nothing for that chat (the bot is not an admin there);
   - *skipped: already imported*: uploading the same file again imports nothing new;
   - *skipped: not newer*: tgaud already has a snapshot of that chat started at the same time or later
     (snapshots are applied in start order, so an older one is never put behind a newer one);
   - *refused*: old format (collect again), bad times or values (a clock far ahead, a damaged file), a migrated
     group (collect the supergroup), or too large for one import (more than 200 000 members in one snapshot);
   - *failed* or *unknown*: a database problem; upload the same file again, and chats already imported are skipped.

   A file that is not a tgstats.db at all (another SQLite file, a damaged one, one with extra tables or views) is
   refused whole, and nothing is written.

The reconciler then applies each imported snapshot, usually within minutes (it waits until the bot has caught up
with the updates of the collection's time). Every listed member becomes a known stay starting at the time the page
listed them, with Telegram's join date as a hint; members who joined or left during the collection keep what the
bot recorded. A complete listing whose count matches Telegram's (good) also lets tgaud doubt the stays it does not
list and check them with `getChatMember`; an incomplete one only adds members. The overview's **Known** and
**Coverage** columns and the coverage report (`tgaud coverage`) then show known against Telegram's count. Collect
and upload again now and then: each newer snapshot confirms the stays and finds leaves the bot missed.

Tests (PostgreSQL 18.6 and podman; never the real Telegram, always the fake Bot API in `tests/tgaud/`):

```sh
mise run ci                                    # everything CI runs: pytest, the image check, the container end-to-end run,
                                               # actionlint, the release scripts' tests
TGAUD_TEST_ADMIN_URL=postgresql://postgres:PASSWORD@127.0.0.1:5432/postgres mise run ci   # use a running server
uv run pytest tests/tgaud/test_end_to_end.py   # one scripted day through `tgaud serve`
```

Without `TGAUD_TEST_ADMIN_URL` the tests start a throwaway PostgreSQL container with podman.

### CI and releases

GitHub Actions, as in echopan. Pull requests to `main` run `.github/workflows/ci.yml`: the tests, then the image. A push
to `main` runs only `.github/workflows/release.yml`: no tests, no build. `mise run ci` is the same suite locally.

- `lint` is actionlint, `tests` is `uv run pytest -rs` with a real PostgreSQL 18.6 (limits as in the local fixture,
  checked before pytest runs), `container` is the image check and the container end-to-end run with podman, and
  `release-scripts` tests the scripts below against a throwaway registry.
- `publishing` (needs every job above; logs in to ghcr.io with `GITHUB_TOKEN`) builds the image, runs
  `--selfcheck` on that build the way the deploy role runs it, and only then pushes `ghcr.io/tutunak/tgaud:<40-hex git
  tree hash>`. On a pull request the checkout is GitHub's merge ref, the tree `main` gets if the PR is merged as it is.
  A tag the registry already has isn't built again; when the registry can't say, the job stops.
- `release` computes the tree hash of the commit, stops (naming the image) when the registry lacks that tag, picks the
  version, adds it to the image at the manifest level (`docker buildx imagetools create`: no pull, same digest) and only
  then pushes the git tag. The version is the `X.Y.Z` tag already on the commit (a re-run bumps nothing), else the highest
  `X.Y.Z` on the remote plus one patch, `1.0.0` when there is none; plain `X.Y.Z`, no `v`. Releases run one at a time
  (`concurrency: {group: release, queue: max}`); never set `cancel-in-progress`.
- Images: `ghcr.io/tutunak/tgaud:<tree hash>` for every pull request that passed, `ghcr.io/tutunak/tgaud:X.Y.Z` for every
  release. There is no `latest`. A git tag `X.Y.Z` means the image carries that version; an image `X.Y.Z` without a git
  tag is a stopped run's leftover, and the next release reuses the number.

What it relies on from the owner:

- **Branch rules** (GitHub settings): pull requests must be up to date with `main` before merging, the required checks are
  every test job (`lint`, `tests`, `container`, `release-scripts`) **and `publishing`**, and merges use rebase. Otherwise the
  merged tree has no image and the release stops, saying so. A direct push to `main` stops the same way.
- **Pull requests from forks** get a read-only token, so their `publishing` fails: push the reviewed branch to this repo
  and merge it through a same-repository pull request instead.
- **Tags**: the release workflow is the only writer of `X.Y.Z` git tags. Never tag by hand, and never move a pushed tag.
- **The package**: `ghcr.io/tutunak/tgaud` is created by the first `publishing` run and linked to this repository. Its
  visibility must be set to public in the package settings for anonymous pulls. Unverified: whether GitHub makes a package
  public by itself for a public repository; check after the first run. If the package already exists without having been created by a workflow run of this repo, add the repo
  under Package settings, Manage Actions access, with role Write.
- **Recovery**: a release that was cancelled (the queue full, by hand) or failed on a registry or network error is fixed by
  re-running it, which is safe: a commit that already has a version keeps it, and one without gets the next patch, even
  after newer commits were released (version order is then release order, not commit order). A release stopped for a
  missing image isn't fixed by a re-run, since no run built that tree: the next pull request merged up to date with `main`
  releases a tree that contains the commit.
