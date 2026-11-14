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
tgstats collect --deep                # also search by single characters when the listing is short
```

It prints fetched vs reported per chat and marks a snapshot **INCOMPLETE** when fetched is clearly below
reported (more than 2% short). FloodWait errors are slept through (it prints the wait). If one chat fails
(no rights, private, an RPC error) the error is printed, the run continues, and the exit code is non-zero.

Before each request to Telegram, `collect` and `chats` pause for a random time between 1000 and 5000 ms
(picked to the millisecond), so requests don't arrive at a fixed rate. Change the range with
`--min-delay-ms` and `--max-delay-ms`, for example `tgstats collect --min-delay-ms 2000 --max-delay-ms 8000`.
Every request is paced, including the chat discovery and the login check at the start. Not paced: the handshake
Telethon sends when it connects (the first request, so there is nothing before it), and Telethon's own retries of a
request after a Telegram server error, which wait a fixed 2 s each, at most 5 times. A big chat takes longer:
200 members per request, so 10,000 members is 50 requests, about 2.5 minutes of pauses on average.

`--deep`: when the plain listing comes back short, it also runs member searches for each Latin letter,
digit and Cyrillic letter and merges the results, stopping once the reported count is reached. This can get
past the cap on big supergroups but is slow and still may not reach everyone.

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
