import dataclasses
import threading

import psycopg
import pytest

from tgaud import db


def _ledger(c):
    return c.execute("SELECT version, name, sha256, applied_at FROM public.tgaud_migration ORDER BY version").fetchall()


def test_migrate_empty_database_as_the_tenant_then_noop(empty_dsn):
    with psycopg.connect(empty_dsn) as c:
        role = c.execute("SELECT rolsuper, rolcreatedb, rolcreaterole FROM pg_roles WHERE rolname = current_user").fetchone()
        assert role == (False, False, False)
        c.rollback()
        first = db.migrate(c)
        assert first.applied == [1, 2] and first.version == 2
        ledger = _ledger(c)
        assert [(v, n) for v, n, _, _ in ledger] == [(1, "initial"), (2, "shared")]
        assert c.execute("SELECT to_regclass('tg.membership'), to_regclass('tg.erasure')").fetchone() == (
            "tg.membership", "tg.erasure")
        c.commit()
        second = db.migrate(c)
        assert second.applied == [] and second.version == 2
        assert _ledger(c) == ledger
        c.rollback()


def test_concurrent_migrate_applies_once(empty_dsn):
    results, errors = [], []

    def run():
        try:
            with psycopg.connect(empty_dsn) as c:
                results.append(db.migrate(c).applied)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=run) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(120)
    assert not errors, errors
    assert sorted(results) == [[], [1, 2]]


def test_changed_migration_is_refused_before_anything_applies(migrated_dsn):
    image = db.load_migrations()
    changed = [dataclasses.replace(image[0], sha256="0" * 64), *image[1:]]
    extra = dataclasses.replace(image[1], version=3, name="later", filename="003_later.sql",
                                text="CREATE TABLE tg.should_not_exist (x int);", sha256="1" * 64)
    with psycopg.connect(migrated_dsn) as c:
        with pytest.raises(db.MigrationError, match="001_initial.sql was changed"):
            db.migrate(c, [*changed, extra])
        assert c.execute("SELECT to_regclass('tg.should_not_exist')").fetchone()[0] is None
        assert [r[0] for r in _ledger(c)] == [1, 2]
        c.rollback()
        st = db.schema_status(c, changed)
        assert st.changed == [1]


def test_failure_after_the_last_statement_rolls_back_ddl_and_ledger(empty_dsn):
    image = db.load_migrations()

    def boom(m):
        assert c.execute("SELECT to_regclass('tg.membership')").fetchone()[0] == "tg.membership"
        raise RuntimeError("injected before the ledger insert")

    with psycopg.connect(empty_dsn) as c:
        with pytest.raises(RuntimeError, match="injected"):
            db.migrate(c, image[:1], _before_record=boom)
        assert c.execute("SELECT to_regnamespace('tg'), to_regclass('tg.membership')").fetchone() == (None, None)
        assert _ledger(c) == []
        c.rollback()
        assert db.migrate(c).applied == [1, 2]


def test_schema_status_behind_current_ahead(empty_dsn):
    image = db.load_migrations()
    with psycopg.connect(empty_dsn) as c:
        st = db.schema_status(c)
        assert (st.state, st.applied, st.missing) == ("behind", [], [1, 2])
        db.migrate(c, image[:1])
        st = db.schema_status(c)
        assert (st.state, st.missing) == ("behind", [2])
        db.migrate(c)
        assert db.schema_status(c).state == "current"
        c.execute("INSERT INTO public.tgaud_migration (version, name, sha256) VALUES (999, 'future', 'x')")
        c.commit()
        st = db.schema_status(c)
        assert (st.state, st.extra, st.missing) == ("ahead", [999], [])
        # a newer database is left alone by migrate
        assert db.migrate(c).applied == []


def _m(text: str) -> db.Migration:
    return db.Migration(9, "t", "009_t.sql", "x", text)


def test_runner_strips_only_the_outer_transaction():
    stmts = _m("-- c\nBEGIN;\nCREATE TABLE a (x int);\nCREATE FUNCTION f() RETURNS int LANGUAGE plpgsql AS $$\n"
               "BEGIN\n  RETURN 1;\nEND $$;\nCOMMIT;\n").statements()
    assert [s.head.split()[0] for s in stmts] == ["CREATE", "CREATE"]
    assert "BEGIN\n  RETURN 1;" in stmts[1].text
    assert len(_m("CREATE TABLE a (x int);").statements()) == 1


@pytest.mark.parametrize("text", [
    "BEGIN;\nCREATE TABLE a (x int);\n",                    # BEGIN without the final COMMIT
    "CREATE TABLE a (x int);\nCOMMIT;\nCREATE TABLE b ();",  # transaction control in the middle
    "\\set x 1\nCREATE TABLE a (x int);",                     # psql meta command
    "CREATE TABLE a (x int DEFAULT :v);",                     # psql variable
    "CREATE TABLE a (x text DEFAULT 'oops);",                 # unterminated literal
])
def test_runner_refuses_what_it_cannot_own(text):
    with pytest.raises(Exception) as e:
        _m(text).statements()
    assert isinstance(e.value, (db.MigrationError, ValueError))


def test_bundled_001_is_the_design_schema_with_an_origin_line():
    from pathlib import Path

    m = db.load_migrations()[0]
    first, rest = m.text.split("\n", 1)
    assert first.startswith("-- origin:")
    design = Path(__file__).resolve().parents[2] / "docs" / "design"
    assert rest.startswith("-- schema.sql: PostgreSQL")
    assert "BEGIN;" in rest and rest.rstrip().endswith("COMMIT;")
    assert (design / "design.md").exists()
