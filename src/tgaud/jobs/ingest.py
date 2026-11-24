"""Ingest (its own thread, on the connection that holds the service lock). Filled by the ingest task.

serve calls, in this order, with the service lock held:

    identify(ctx, conn) the identity guards of the deploy contract, before the generation is claimed: getMe's
                        bot id and ctx.config.link_key_fingerprint() against tg.bot_state; on a mismatch raise
                        tgaud.jobs.StartupError (exit 2) having written nothing. On the first start (no row) it
                        records both. It must leave exactly one tg.bot_state row and commit or roll back.
                        No other writes here: a refused start must not change the database, not even the
                        serve generation, which serve claims only after this returns.
    startup(ctx, conn)  once, after the claim: the other startup writes (rights checks, ...); each transaction
                        starts with ctx.fence(conn).
    run(ctx, conn)      the long-polling loop until ctx.stop is set: ctx.api.get_updates(timeout=8, ...), one
                        transaction per update that starts with ctx.fence(conn) and ends with the offset;
                        ctx.health.mark_poll_ok() after each successful poll and ctx.health.mark_commit() after
                        each successful commit (an empty poll counts). Returning or raising while ctx.stop is not
                        set ends the process with exit 1 (the connection, and with it the lock, is never
                        reopened; systemd restarts the unit).

Core stub: identify implements the two guards, startup does nothing, run idles until the stop.
"""

from __future__ import annotations

import psycopg

from tgaud.botapi import ALLOWED_UPDATES
from tgaud.jobs import JobContext, StartupError


def identify(ctx: JobContext, conn: psycopg.Connection) -> None:
    if ctx.api is None:
        raise StartupError("ingest needs TGAUD_BOT_TOKEN")
    bot_id = int(ctx.api.get_me()["id"])
    fingerprint = ctx.config.link_key_fingerprint()
    rows = conn.execute("SELECT bot_id, link_key_fingerprint FROM tg.bot_state").fetchall()
    conn.rollback()
    if len(rows) > 1:
        raise StartupError(f"tg.bot_state has {len(rows)} rows; expected one bot")
    if rows:
        known_bot, known_fp = rows[0]
        if known_bot != bot_id:
            raise StartupError(f"TGAUD_BOT_TOKEN belongs to bot {bot_id}, but this database records bot "
                               f"{known_bot}: refusing to start")
        if known_fp is not None and known_fp != fingerprint:
            raise StartupError("TGAUD_LINK_HMAC_KEY differs from the key this database was started with "
                               "(it would re-key every bot invite link): refusing to start")
        if known_fp is not None:
            return
    # first start, or a row from before the fingerprint was recorded: both checks passed
    conn.execute("""INSERT INTO tg.bot_state (bot_id, allowed_updates, link_key_fingerprint) VALUES (%s, %s, %s)
                    ON CONFLICT (bot_id) DO UPDATE SET link_key_fingerprint = excluded.link_key_fingerprint
                    WHERE tg.bot_state.link_key_fingerprint IS NULL""",
                 (bot_id, list(ALLOWED_UPDATES), fingerprint))
    conn.commit()


def startup(ctx: JobContext, conn: psycopg.Connection) -> None:
    pass


def run(ctx: JobContext, conn: psycopg.Connection) -> None:
    while not ctx.stop.wait(1.0):
        pass
