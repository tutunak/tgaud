"""Evidence written the way the service's writers write it, for the history tests.

`World` makes one chat's evidence in an open transaction: events (ingest), snapshots with their member rows (the
collect script) folded into tg.sighting runs by the writer contract of migration 003 (the reconciler), probe
answers (the prober), gaps and aliases. It never commits; tests commit or roll back.
"""

from __future__ import annotations

import itertools
from datetime import datetime, timedelta, timezone

import psycopg

from tgaud import history

T0 = datetime(2026, 9, 1, 10, 0, tzinfo=timezone.utc)
CHAT = -1009000000001
COLS = ", ".join(history.SPELL_COLUMNS)


def at(minutes: float = 0, **kw) -> datetime:
    return T0 + timedelta(minutes=minutes, **kw)


class World:
    def __init__(self, conn: psycopg.Connection, chat_id: int = CHAT, *, chat_type: str = "supergroup") -> None:
        self.c = conn
        self.chat_id = chat_id
        self._update = itertools.count(700000)
        self._event = itertools.count(900000)
        self.users: set[int] = set()
        conn.execute("SELECT tg.ensure_partitions('member_event', 'month', '2026-01-01', '2027-07-01')")
        self.add_chat(chat_id, chat_type)

    def add_chat(self, chat_id: int, chat_type: str = "supergroup") -> None:
        self.c.execute("""INSERT INTO tg.chat (chat_id, chat_type, title, tracked) VALUES (%s, %s, 'history test', true)
                          ON CONFLICT (chat_id) DO NOTHING""", (chat_id, chat_type))

    def user(self, user_id: int) -> int:
        if user_id not in self.users:
            self.c.execute("""INSERT INTO tg.tg_user (user_id, first_seen_at, last_seen_at) VALUES (%s, %s, %s)
                              ON CONFLICT (user_id) DO NOTHING""", (user_id, at(-60 * 24 * 365), at(60 * 24 * 365)))
            self.users.add(user_id)
        return user_id

    # --- ingest ---------------------------------------------------------------------------------------------
    def event(self, user: int, t: datetime, old: str, new: str, *, old_m: bool | None = None,
              new_m: bool | None = None, actor: int | None = None, link: int | None = None, req: bool = False,
              folder: bool = False, update_id: int | None = None, event_id: int | None = None,
              chat: int | None = None) -> int:
        self.user(user)
        event_id = next(self._event) if event_id is None else event_id
        self.c.execute("""
            INSERT INTO tg.member_event (event_id, chat_id, user_id, event_at, update_id, old_status, old_is_member,
                                         new_status, new_is_member, actor_id, invite_link_id, via_join_request,
                                         via_folder_link)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                       (event_id, chat or self.chat_id, user, t, next(self._update) if update_id is None else update_id,
                        old, old_m, new, new_m, actor, link, req, folder))
        return event_id

    def join(self, user: int, t: datetime, **kw) -> int:
        return self.event(user, t, kw.pop("old", "left"), "member", **kw)

    def leave(self, user: int, t: datetime, **kw) -> int:
        return self.event(user, t, "member", kw.pop("new", "left"), **kw)

    # --- collect script + reconciler ---------------------------------------------------------------------------
    def snapshot(self, started: datetime, listed: dict[int, tuple[datetime, datetime | None]], *,
                 finished: datetime | None = None, good: bool = True, through: int | None | str = "all",
                 fold_through: int | None | str = "same", snapshot_id: int | None = None) -> int:
        """A finished snapshot listing {user: (seen_at, participant date)}, graded good or partial.

        through: "all" = reconciled; an int = applied_through_user_id mid-reconcile; None = graded, nothing applied.
        Listed users up to the cursor are folded into sighting runs (the reconciler's chunks).
        """
        finished = finished or max([started + timedelta(minutes=30), *(s for s, _ in listed.values())])
        sid = self.c.execute("""
            INSERT INTO tg.snapshot (snapshot_id, chat_id, started_at, method, reported_at_start)
            OVERRIDING SYSTEM VALUE VALUES (coalesce(%s, nextval(pg_get_serial_sequence('tg.snapshot', 'snapshot_id'))),
                                            %s, %s, 'listing', %s)
            RETURNING snapshot_id""", (snapshot_id, self.chat_id, started, len(listed))).fetchone()[0]
        for u, (seen, hint) in listed.items():
            self.user(u)
            self.c.execute("""INSERT INTO tg.snapshot_member (snapshot_id, user_id, seen_at, joined_date)
                              VALUES (%s, %s, %s, %s)""", (sid, u, seen, hint))
        self.c.execute("""UPDATE tg.snapshot SET finished_at = %s, reported_at_end = %s, fetched = %s, complete = %s,
                                 quality = %s WHERE snapshot_id = %s""",
                       (finished, len(listed), len(listed), good, "good" if good else "partial", sid))
        if through == "all":
            self.c.execute("UPDATE tg.snapshot SET reconciled_at = %s WHERE snapshot_id = %s", (finished, sid))
        elif through is not None:
            self.c.execute("UPDATE tg.snapshot SET applied_through_user_id = %s WHERE snapshot_id = %s", (through, sid))
        limit = through if fold_through == "same" else fold_through
        for u in sorted(listed):
            if limit == "all" or (limit is not None and u <= limit):
                self.fold(sid, u)
        return sid

    def finish_reconcile(self, sid: int, users: list[int] | None = None) -> None:
        """Apply the rest of a snapshot (fold the remaining listed users) and set reconciled_at."""
        for u in users or []:
            self.fold(sid, u)
        self.c.execute("UPDATE tg.snapshot SET reconciled_at = finished_at, applied_through_user_id = NULL "
                       "WHERE snapshot_id = %s", (sid,))

    def fold(self, sid: int, user: int) -> None:
        """The writer contract of migration 003: extend the user's run or start a new one."""
        seen, hint = self.c.execute("SELECT seen_at, joined_date FROM tg.snapshot_member WHERE snapshot_id = %s "
                                    "AND user_id = %s", (sid, user)).fetchone()
        prev = self.c.execute("""
            SELECT p.snapshot_id FROM tg.snapshot s JOIN tg.snapshot p ON p.chat_id = s.chat_id
              AND p.finished_at IS NOT NULL AND (p.started_at, p.snapshot_id) < (s.started_at, s.snapshot_id)
            WHERE s.snapshot_id = %s ORDER BY p.started_at DESC, p.snapshot_id DESC LIMIT 1""", (sid,)).fetchone()
        run = self.c.execute("""
            SELECT first_snapshot_id, last_seen_at, joined_date, times_seen FROM tg.sighting
            WHERE chat_id = %s AND user_id = %s AND last_snapshot_id = %s""",
                             (self.chat_id, user, prev[0] if prev else None)).fetchone()
        if run and run[2] == hint and seen >= run[1]:
            self.c.execute("""
                UPDATE tg.sighting SET interior_seen_at = CASE WHEN times_seen >= 2
                                                               THEN interior_seen_at || last_seen_at
                                                               ELSE interior_seen_at END,
                       last_seen_at = %s, last_snapshot_id = %s, times_seen = times_seen + 1
                WHERE chat_id = %s AND user_id = %s AND first_snapshot_id = %s""",
                           (seen, sid, self.chat_id, user, run[0]))
        else:
            self.c.execute("""
                INSERT INTO tg.sighting (chat_id, user_id, first_seen_at, first_snapshot_id, last_seen_at,
                                         last_snapshot_id, times_seen, joined_date)
                VALUES (%s, %s, %s, %s, %s, %s, 1, %s)
                ON CONFLICT (chat_id, user_id, first_snapshot_id) DO NOTHING""",
                           (self.chat_id, user, seen, sid, seen, sid, hint))

    def prune(self) -> int:
        """R3: drop the member rows of every snapshot (the sightings stay)."""
        n = self.c.execute("DELETE FROM tg.snapshot_member m USING tg.snapshot s WHERE s.snapshot_id = m.snapshot_id "
                           "AND s.chat_id = %s", (self.chat_id,)).rowcount
        self.c.execute("UPDATE tg.snapshot SET members_pruned_at = now() WHERE chat_id = %s", (self.chat_id,))
        return n

    # --- prober, gaps, migrations ---------------------------------------------------------------------------
    def probe(self, user: int, answered: datetime | None, outcome: str, status: str | None = None, *,
              sent: datetime | None = None, reason: str = "manual") -> int:
        self.user(user)
        if outcome in ("in", "out") and status is None:
            status = "member" if outcome == "in" else "left"
        return self.c.execute("""
            INSERT INTO tg.probe (chat_id, user_id, reason, outcome, answered_at, status_seen, sent_at, error_code,
                                  attempts)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 1) RETURNING probe_id""",
                              (self.chat_id, user, reason, outcome, answered, status, sent,
                               "timeout" if outcome == "error" else None)).fetchone()[0]

    def gap(self, start: datetime, end: datetime | None = None, *, all_chats: bool = False,
            reason: str = "service_down") -> None:
        self.c.execute("INSERT INTO tg.gap (chat_id, period, reason) VALUES (%s, tstzrange(%s, %s), %s)",
                       (None if all_chats else self.chat_id, start, end, reason))

    # --- rebuild and read -------------------------------------------------------------------------------------
    def rebuild(self, users: list[int] | None = None) -> history.RebuildResult:
        return history.rebuild_pairs(self.c, [(self.chat_id, u) for u in (users or sorted(self.users))])

    def spells(self, user: int, chat: int | None = None) -> list[history.Spell]:
        rows = self.c.execute(f"SELECT {COLS} FROM tg.membership WHERE chat_id = %s AND user_id = %s "
                              "ORDER BY known_from, joined_at NULLS FIRST", (chat or self.chat_id, user)).fetchall()
        # in UTC: in another session zone, datetimes of one tzinfo would compare by wall time
        return [history.Spell(*(v.astimezone(timezone.utc) if isinstance(v, datetime) else v for v in r))
                for r in rows]
