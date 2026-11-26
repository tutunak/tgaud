# Snapshot import: tgstats.db uploads on the web UI

Status: **implemented** (herd run 20261125-223155, task snapshot-import). How it is built: docs/tgaud.md, "The Import
page"; the owner's steps: README, "Loading the members a chat already has".

## Problem

tgaud's reports showed `known 1` against Telegram's 7 000-13 000 per channel: the Bot API cannot list a chat's
members, so tgaud learns people only from joins and leaves after the bot became an admin. The design's answer is a
member snapshot taken with the owner's own account (MTProto) and reconciled by tgaud (design.md 4-6, section 5 "the
snapshot contract"). Its writer was never built.

## The flow the owner chose

Collect locally with `tgstats collect`, then upload the resulting `tgstats.db` on a new **Import** page of the web
UI. No database access from the owner's machine, no change to the Ansible role.

## Decisions

- **The file records the contract.** `tgstats collect` stores per snapshot a random uuid (made when the collection
  starts), `started_at` (before the first count read), `finished_at`, both counts (before and after the listing,
  read fresh, basic groups included) and the method (`basic_group`, `listing`, `deep`); per member, with the
  membership row, the observation of the page that listed them: `seen_at`, `joined_date` (NULL for creators),
  `inviter_id`, `via_request`, `subscription_until`, bot, deleted, premium, username and names. **The first
  observation wins** (as `ON CONFLICT DO NOTHING`). A failed page or query that the run went on after, or a final
  count that failed or came back without the count, makes the snapshot incomplete whatever the counts say. Old
  files get the columns on open and read back as before.
- **Old-format snapshots are refused**, never imported: they carry only `taken_at`, roughly the end of the
  collection, so their sightings can contradict leaves and erasures recorded during it. The page tells the owner to
  collect again.
- **The latest snapshot of each chat in the file**, by `started_at` (ties: new format, then uuid; never the SQLite
  row id), for chats tgaud tracks. Other snapshots in the file, and tracked chats the file lacks, are untouched.
- **Ordering and duplicates.** The reconciler applies a chat's snapshots in `started_at` order, so a snapshot not
  newer than one tgaud has for the chat is skipped. The uuid (`tg.snapshot.collect_uuid`, migration 011, unique) is
  looked up first, globally: re-uploading imports nothing new, racing uploads import once, and a lost commit
  acknowledgement is settled by the next upload even after a migration re-keyed the chat. A retired source (an
  `old_chat_id` of `tg.chat_alias`) is refused. All of these checks run inside the chat's transaction, after the
  generation fence and `history.lock_chats`.
- **Later versions of one collection** (reimport task, migration 012; the owner's decision). A file uploaded while
  `tgstats collect` was still running was imported with a fraction of the members; the finished file has the same
  uuid. It is imported as a new snapshot of the chat right after the stored version, listing only the members no
  stored version listed (`version_of` = the uuid, `collect_uuid` NULL, `complete = false`), when the uuid's claim
  (the `collect_uuid` row's chat and source) is the uploaded chat, `started_at` is equal, `finished_at` is later
  than every stored version's and the newest version is the chat's newest snapshot. The same `finished_at` is
  already imported, an earlier one an older version, a claim elsewhere or another start already imported (nothing
  written); a later snapshot of the chat makes it not newer. `tg.lock_collection(uuid)`, taken after the chat lock,
  serializes every import of one uuid, so two racing first imports for different chats end with one claim. Nothing
  is deleted or undone, and the reconciler, history and prune rule are unchanged: deleting and recreating the
  partial snapshot cannot be undone once it is pruned, and listing everyone again would replay shared observations
  (S3 cases per snapshot). Details: docs/tgaud.md, "The Import page".
- **Whole-file versus per-snapshot errors.** Not SQLite, missing or extra tables or columns (generated ones
  included), a view, trigger or virtual table, a wrong type or shape in a member row of any chat's latest snapshot
  (even a refused chat's), a NULL bot or deleted flag: the whole file is refused, nothing written. Bad times
  (`started_at <= seen_at <= finished_at <= now + 5 min`, or out of range), negative counts, a `fetched` that is not
  the number of member rows, a snapshot marked complete without its final count, or a malformed uuid refuse that
  snapshot only.
- **Erasure.** Nobody erased by `tg.forget_user` is imported, whatever their `seen_at` says (the collector's clock
  proves nothing); erased inviters are cleared; `fetched` counts the rows that survived.
- **Names follow `TGAUD_STORE_NAMES`**: off, no name or username is written anywhere.
- **Size.** 32 MiB per upload (a 32 MiB file holds about 350 000 members without names, 260 000 with typical
  names). One import at a time (409 before the body is read): the container's only writable place is a
  memory-backed `/tmp`, so the spool budget is one file. Each chat is one transaction, bounded in total by 30 s
  (lock and statement timeouts from the time left, the fence's and the chat lock's waits included), well under the
  web connection's 75 s lock wait. **Member ceiling: 200 000 per snapshot**: measured at 7.3 s (the largest chat a
  32 MiB file holds, 352 960 members: 12.6 s) with a report waiting on the connection; a chat above it is refused
  with the reason. A file that grows past 32 MiB is collected into a fresh one with `tgstats collect --db`.
- **Transport.** Only `POST /import` takes a multipart body; its token, method and origin are checked before the body
  is read, the body is streamed into one spool file under a 2-minute deadline, every refusal is a normal error page
  the client can still read, and the spool file is deleted on success, error and abort alike. Every other route keeps
  its 64 KiB form limit.
- **Reconciler rules unchanged but one, proved by a test:** the M2 cutoff. A snapshot committed before ingest
  recorded a migration to an untracked supergroup, with pages stamped after `migrated_at`, reopened spells on the old
  id (S4). A sighting of an old id after its M2 migration is now no evidence for it (design.md 4, M2).

## Deferred

- The result page's "within minutes" does not allow for delayed ingest or an unfinished earlier snapshot of the chat;
  the page states what was written, and the linked coverage report shows the real state.
- Files over 32 MiB: collect into a fresh file with `--db` (README).
