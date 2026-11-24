"""Database helpers: connections, the migration runner, schema status, the service lock and generation.

Advisory lock key spaces (one database per tenant, so these are the only users):
  - single bigint keys `pg_advisory_xact_lock(chat_id)`: reserved for chat ids (tgaud.history.lock_chats);
  - two int4 keys `(TGAUD_LOCK_CLASS, n)`: tgaud's own locks, MIGRATE_LOCK = n 1, SERVE_LOCK = n 2.
PostgreSQL keeps the two forms apart (different lock tags), so a chat id never collides with a tgaud lock.

The service: `tgaud serve` holds SERVE_LOCK exclusively on its ingest connection for its lifetime, then
increments `tg.bot_state.serve_generation` (SELECT ... FOR UPDATE) and remembers the value. Every job
transaction starts with `check_generation(conn, generation)` (SELECT ... FOR KEY SHARE): the takeover's FOR UPDATE
waits for in-flight job transactions, and after it commits an old process can commit nothing and exits.
FOR KEY SHARE, not FOR SHARE: it conflicts only with FOR UPDATE, so ingest's per-update
`UPDATE tg.bot_state SET next_offset` (FOR NO KEY UPDATE) never waits for a worker or batch transaction (with
FOR SHARE it would, and could deadlock with lock_chats or a rollup_dirty row). `maintenance(conn)` takes SERVE_LOCK too and also bumps the generation.
"""

from __future__ import annotations

import contextlib
import hashlib
import importlib.resources
import logging
import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from importlib.resources.abc import Traversable

import psycopg
from psycopg import pq

from tgaud import sqlscript
from tgaud.config import Config

log = logging.getLogger(__name__)

TGAUD_LOCK_CLASS = 0x74676175  # "tgau"
MIGRATE_LOCK = (TGAUD_LOCK_CLASS, 1)
SERVE_LOCK = (TGAUD_LOCK_CLASS, 2)

LEDGER_DDL = """
CREATE TABLE IF NOT EXISTS public.tgaud_migration (
  version     integer PRIMARY KEY,
  name        text NOT NULL,
  sha256      text NOT NULL,
  applied_at  timestamptz NOT NULL DEFAULT now()
)"""
_MIGRATION_FILE = re.compile(r"^(?P<version>[0-9]{3})_(?P<name>[a-z0-9_]+)\.sql$")
_TXN_CONTROL = {"BEGIN", "START", "COMMIT", "END", "ROLLBACK", "ABORT"}


class MigrationError(RuntimeError):
    pass


class StaleGeneration(RuntimeError):
    """Another `tgaud serve` (or a maintenance command) took over; this process must commit nothing more."""


class MaintenanceBlocked(RuntimeError):
    pass


def connect(config: Config, job: str) -> psycopg.Connection:
    """A connection for one job: autocommit off, application_name tgaud-<job>."""
    return psycopg.connect(config.dsn(job), autocommit=False)


def sql_root() -> Traversable:
    """The bundled SQL files (src/tgaud/sql), readable from a wheel."""
    return importlib.resources.files("tgaud") / "sql"


# ------------------------------------------------------------------------------------------------------
# Migrations
# ------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    filename: str
    sha256: str
    text: str = field(repr=False)

    def statements(self) -> list[sqlscript.Statement]:
        """The file's statements; an exact top-level `BEGIN;` first and `COMMIT;` last (as in 001) are removed,
        because the runner owns the transaction. Any other transaction control (PREPARE TRANSACTION, END,
        ROLLBACK, a BEGIN with options, ...), meta command or psql variable is refused."""
        stmts = [s for s in sqlscript.statements_only(sqlscript.parse(self.text, self.filename), self.filename)]
        if stmts and _is_exact(stmts[0], _WRAPPER_BEGIN):
            if len(stmts) < 2 or not _is_exact(stmts[-1], _WRAPPER_COMMIT):
                raise MigrationError(f"{self.filename}:{stmts[0].line}: a top-level BEGIN must be closed by a "
                                     "final COMMIT")
            stmts = stmts[1:-1]
        for s in stmts:
            if _is_txn_control(s):
                raise MigrationError(f"{self.filename}:{s.line}: transaction control ({s.head}) inside a "
                                     "migration; the runner owns the transaction")
        return stmts


_WRAPPER_BEGIN = ("BEGIN", "BEGIN WORK", "BEGIN TRANSACTION")
_WRAPPER_COMMIT = ("COMMIT", "COMMIT WORK", "COMMIT TRANSACTION")


def _is_exact(s: sqlscript.Statement, forms: tuple[str, ...]) -> bool:
    return " ".join(s.code.upper().split()) in forms


def _is_txn_control(s: sqlscript.Statement) -> bool:
    """BEGIN, START, COMMIT (and COMMIT PREPARED), END, ROLLBACK (and ROLLBACK PREPARED / TO), ABORT,
    PREPARE TRANSACTION."""
    words = s.head.split(" ")
    return words[0] in _TXN_CONTROL or words[:2] == ["PREPARE", "TRANSACTION"]


def load_migrations(root: Traversable | None = None) -> list[Migration]:
    """Every sql/migrations/NNN_name.sql in version order."""
    mdir = (root or sql_root()) / "migrations"
    out: list[Migration] = []
    for entry in mdir.iterdir():
        m = _MIGRATION_FILE.match(entry.name)
        if not m:
            if entry.name.endswith(".sql"):
                raise MigrationError(f"migration file name {entry.name!r} is not NNN_name.sql")
            continue
        data = entry.read_bytes()
        out.append(Migration(int(m.group("version")), m.group("name"), entry.name,
                             hashlib.sha256(data).hexdigest(), data.decode("utf-8")))
    out.sort(key=lambda x: x.version)
    versions = [x.version for x in out]
    if len(set(versions)) != len(versions):
        raise MigrationError(f"duplicate migration versions: {versions}")
    return out


@contextlib.contextmanager
def _idle(conn: psycopg.Connection) -> Iterator[None]:
    if conn.autocommit:
        raise ValueError("this helper needs a connection with autocommit off")
    if conn.info.transaction_status != pq.TransactionStatus.IDLE:
        raise ValueError("this helper needs a connection without an open transaction")
    yield


def _ledger(conn: psycopg.Connection) -> dict[int, tuple[str, str]]:
    if conn.execute("SELECT to_regclass('public.tgaud_migration')").fetchone()[0] is None:
        return {}
    rows = conn.execute("SELECT version, name, sha256 FROM public.tgaud_migration").fetchall()
    return {v: (n, h) for v, n, h in rows}


@dataclass(frozen=True)
class MigrateResult:
    applied: list[int]
    version: int | None  # the highest recorded version after the run


def migrate(conn: psycopg.Connection, migrations: list[Migration] | None = None, *,
            _before_record: Callable[[Migration], None] | None = None) -> MigrateResult:
    """Apply pending migrations in order under MIGRATE_LOCK, each in one transaction with its ledger row.

    Before anything is applied, every recorded migration that the image has must still have the recorded
    checksum (MigrationError otherwise). Versions recorded but unknown to the image (a newer database) are left
    alone. `_before_record` is a test hook called after a migration's last statement, before its ledger row.
    """
    migrations = load_migrations() if migrations is None else migrations
    with _idle(conn):
        pass
    conn.execute("SELECT pg_advisory_lock(%s, %s)", MIGRATE_LOCK)
    conn.commit()
    try:
        conn.execute(LEDGER_DDL)
        conn.commit()
        recorded = _ledger(conn)
        conn.commit()
        for m in migrations:
            if m.version in recorded and recorded[m.version][1] != m.sha256:
                raise MigrationError(f"{m.filename} was changed after it was applied (recorded sha256 "
                                     f"{recorded[m.version][1][:12]}..., file {m.sha256[:12]}...); a released "
                                     "migration must never change, add a new one")
        applied: list[int] = []
        for m in migrations:
            if m.version in recorded:
                continue
            stmts = m.statements()
            try:
                for s in stmts:
                    try:
                        conn.execute(s.text)
                    except psycopg.Error as e:
                        raise MigrationError(f"{m.filename}:{s.line}: {e}".rstrip()) from e
                if _before_record is not None:
                    _before_record(m)
                if conn.info.transaction_status != pq.TransactionStatus.INTRANS:
                    # a statement ended the runner's transaction: the ledger must not record what may be
                    # half applied, prepared or committed outside it
                    raise MigrationError(f"{m.filename}: the migration ended the runner's transaction "
                                         f"(status {conn.info.transaction_status.name}); not recorded")
                conn.execute("INSERT INTO public.tgaud_migration (version, name, sha256) VALUES (%s, %s, %s)",
                             (m.version, m.name, m.sha256))
                conn.commit()
            except BaseException:
                _rollback_quietly(conn)
                raise
            log.info("applied migration %s", m.filename)
            applied.append(m.version)
        final = max(_ledger(conn) or {0: None})
        conn.commit()
        return MigrateResult(applied, final or None)
    finally:
        _rollback_quietly(conn)
        if not conn.closed and not conn.broken:
            conn.execute("SELECT pg_advisory_unlock(%s, %s)", MIGRATE_LOCK)
            conn.commit()


def _rollback_quietly(conn: psycopg.Connection) -> None:
    try:
        if not conn.closed and not conn.broken:
            conn.rollback()
    except psycopg.Error:
        pass


@dataclass(frozen=True)
class SchemaStatus:
    state: str  # "behind" | "current" | "ahead"
    applied: list[int]
    image: list[int]
    missing: list[int]  # in the image, not in the database
    extra: list[int]  # in the database, not in the image (a newer image migrated it)
    changed: list[int]  # recorded with another checksum than the image's file

    @property
    def latest_applied(self) -> int | None:
        return max(self.applied) if self.applied else None


def schema_status(conn: psycopg.Connection, migrations: list[Migration] | None = None) -> SchemaStatus:
    """Compare the ledger with the image's migrations (contract: serve exits 3 when behind, warns when ahead).
    Reads only; runs in its own short transaction on an idle connection."""
    migrations = load_migrations() if migrations is None else migrations
    with _idle(conn):
        recorded = _ledger(conn)
        conn.rollback()
    image = {m.version: m for m in migrations}
    missing = sorted(set(image) - set(recorded))
    extra = sorted(set(recorded) - set(image))
    changed = sorted(v for v in set(image) & set(recorded) if recorded[v][1] != image[v].sha256)
    state = "behind" if missing else "ahead" if extra else "current"
    return SchemaStatus(state, sorted(recorded), sorted(image), missing, extra, changed)


# ------------------------------------------------------------------------------------------------------
# Service lock and generation fence
# ------------------------------------------------------------------------------------------------------


def try_serve_lock(conn: psycopg.Connection) -> bool:
    """Take SERVE_LOCK exclusively for the session, without waiting. False when another session holds it."""
    with _idle(conn):
        ok = conn.execute("SELECT pg_try_advisory_lock(%s, %s)", SERVE_LOCK).fetchone()[0]
        conn.commit()
    return bool(ok)


def claim_generation(conn: psycopg.Connection) -> int | None:
    """Increment tg.bot_state.serve_generation and return the new value; None when tg.bot_state has no row
    yet (the first start, before ingest recorded the bot). FOR UPDATE waits for in-flight job transactions of
    an older process (they hold the row FOR KEY SHARE); after this commits, those can commit nothing."""
    with _idle(conn):
        rows = conn.execute("SELECT serve_generation FROM tg.bot_state FOR UPDATE").fetchall()
        if not rows:
            conn.rollback()
            return None
        gen = max(r[0] for r in rows) + 1
        conn.execute("UPDATE tg.bot_state SET serve_generation = %s", (gen,))
        conn.commit()
    return gen


def check_generation(conn: psycopg.Connection, generation: int) -> None:
    """The first statement of every job transaction: lock the generation row FOR KEY SHARE and compare.
    Raises StaleGeneration after a takeover. Leaves the transaction open (the lock lasts until it ends)."""
    rows = conn.execute("SELECT serve_generation FROM tg.bot_state FOR KEY SHARE").fetchall()
    if not rows or any(r[0] != generation for r in rows):
        found = sorted({r[0] for r in rows})
        raise StaleGeneration(f"serve generation is {found or 'unset'}, this process has {generation}: "
                              "another tgaud serve or a maintenance command took over")


@contextlib.contextmanager
def maintenance(conn: psycopg.Connection) -> Iterator[None]:
    """For commands that need the service stopped (forget-user): takes SERVE_LOCK without waiting, fails fast
    with MaintenanceBlocked while `tgaud serve` runs, and bumps the generation so a dying old process can commit
    nothing. The lock is released when the block ends."""
    if not try_serve_lock(conn):
        raise MaintenanceBlocked("stop the service first: a tgaud serve holds the service lock")
    try:
        claim_generation(conn)
        yield
    finally:
        _rollback_quietly(conn)
        if not conn.closed and not conn.broken:
            conn.execute("SELECT pg_advisory_unlock(%s, %s)", SERVE_LOCK)
            conn.commit()
