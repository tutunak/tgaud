"""Ingest (its own thread, on the connection that holds the service lock). Filled by the ingest task.

    startup(ctx, conn)  once, before any other thread starts: the identity guards of the deploy contract
                        (bot id from getMe, tg.bot_state.link_key_fingerprint against
                        ctx.config.link_key_fingerprint()); raise tgaud.jobs.StartupError (exit 2) on a mismatch.
                        It must leave exactly one tg.bot_state row: serve claims the generation on it next.
    run(ctx, conn)      the long-polling loop until ctx.stop is set: ctx.api.get_updates(timeout=8, ...), one
                        transaction per update that starts with ctx.fence(conn) and ends with the offset;
                        ctx.health.mark_poll_ok() after each successful poll and ctx.health.mark_commit() after
                        each successful commit (an empty poll counts). Returning or raising while ctx.stop is not
                        set ends the process with exit 1 (the connection, and with it the lock, is never
                        reopened; systemd restarts the unit).

Core stub: startup records the bot from getMe (no identity checks), run idles until the stop.
"""

from __future__ import annotations

import psycopg

from tgaud.botapi import ALLOWED_UPDATES
from tgaud.jobs import JobContext


def startup(ctx: JobContext, conn: psycopg.Connection) -> None:
    if ctx.api is None:
        raise RuntimeError("ingest needs TGAUD_BOT_TOKEN")
    me = ctx.api.get_me()
    conn.execute("INSERT INTO tg.bot_state (bot_id, allowed_updates) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                 (int(me["id"]), list(ALLOWED_UPDATES)))
    conn.commit()


def run(ctx: JobContext, conn: psycopg.Connection) -> None:
    while not ctx.stop.wait(1.0):
        pass
