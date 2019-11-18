from pathlib import Path

from pgnotify import await_pg_notifications
from pytest import raises
from sqlalchemy.exc import ProgrammingError

import results

FIXTURES = Path("tests/FIXTURES")

DIFFERENCES = """create table "public"."t" (
    "x" integer
);


"""


CREATION_STATEMENTS = """create schema if not exists "public";

create extension if not exists "plpgsql" with schema "pg_catalog" version '1.0';

create table "public"."t" (
    "x" integer
);


"""


DROP_STATEMENTS = """drop table "public"."t";

drop extension if exists "plpgsql";

drop schema if exists "public";

"""


def test_db(tmpdb):
    db = results.db(tmpdb)

    x = db.ss_from_file(FIXTURES / "sql/select1.sql")

    assert x.scalar() == 1
    assert x[0].scalar() == 1
    assert x[0].x == 1
    assert x[0]["x"] == 1
    assert x.values_for("x") == [1]
    assert list(x[0]) == [1]

    with raises(AttributeError):
        x[0].y

    assert (
        db.paged_from_file(
            FIXTURES / "sql/select1.sql",
            bookmark=None,
            ordering="x",
            per_page=1,
            backwards=False,
        ).scalar()
        == 1
    )
    assert results.file_text(FIXTURES / "sql/select1.sql") == "select 1 as x\n"
    OTHER_DB = tmpdb + "x"
    other = results.db(OTHER_DB)
    other.create_database()

    assert db.has_same_structure_as(other.db_url) is True
    other.sync_db_structure_to_definition("create table t(x integer);", confirm=False)
    assert db.has_same_structure_as(other.db_url) is False

    assert other.ss("select sum(x) from t").scalar() is None
    assert other.schema_hash() == "3fb6449d1d7b87bebd9b146c53fac72955370d2d"

    assert db.db_differences(other.db_url) == DIFFERENCES
    assert other.creation_statements() == CREATION_STATEMENTS
    assert other.drop_statements() == DROP_STATEMENTS

    db.sync_db_structure_to_target_db(other.db_url, confirm=False)
    assert db.has_same_structure_as(other.db_url) is True

    db.sync_db_structure_to_definition(
        "create extension if not exists pg_trgm;",
        create_extensions_only=True,
        confirm=False,
    )

    i = db.inspect()
    assert list(sorted(i.extensions)) == ["pg_trgm", "plpgsql"]
    assert db.has_same_structure_as(other.db_url) is False
    db.ss("drop extension pg_trgm")
    assert db.has_same_structure_as(other.db_url) is True

    with raises(ProgrammingError):
        other.insert("t", [dict(x=1)], upsert_on=["x"])

    other.sync_db_structure_to_definition(
        "create table t(x integer unique, y text);", confirm=False
    )

    def setup_db(db_url):
        results.db(db_url).ss("create table t(x integer unique, y text);")

    db.sync_db_structure_to_setup_method(setup_db, confirm=False)
    assert db.has_same_structure_as(other.db_url) is True

    with raises(ValueError):
        other.insert("t", [])

    with raises(ValueError):
        other.insert("t", [{",x": 2, "y": "b"}])

    inserted = other.insert("t", dict(x=2, y="b"))
    inserted = inserted.one()
    assert inserted.x == 2
    assert inserted.y == "b"

    inserted = other.insert(
        "t", [dict(x=1, y="a"), dict(x=2, y="a"), dict(x=4, y="a")], upsert_on=["x"]
    )
    assert inserted is None

    inserted = other.insert(
        "t",
        [dict(x=1, y="a"), dict(x=2, y="a"), dict(x=4, y="a")],
        upsert_on=["x"],
        returning=True,
    )
    inserted = inserted.one()
    assert inserted.x == 4
    assert inserted.y == "a"

    assert len(other.ss("select * from t")) == 3

    unique_y = other.ss("select distinct y from t")
    assert len(unique_y) == 1
    assert unique_y.scalar() == "a"

    defaults = other.insert("t", {})

    assert defaults.one() == dict(x=None, y=None)

    with raises(ValueError):
        other.insert("t", None)


def test_pg_notify(tmpdb):
    db = results.db(tmpdb)

    for n in await_pg_notifications(
        tmpdb, channels=["channel"], timeout=1, yield_on_timeout=True
    ):

        if not n:
            db.pg_notify("channel", "payload")

        else:
            n.channel == "channel"
            n.payload == "payload"
            break


def test_explain(tmpdb):
    from results.connections import explain_prefix

    p = explain_prefix(analyze=True)
    assert p == "EXPLAIN (ANALYZE)"
    p = explain_prefix(analyze=True, format="json", buffers=True, verbose=True)
    assert p == "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON, VERBOSE)"

    db = results.db(tmpdb)
    explained = db.explain("select 1")
    col = explained["QUERY PLAN"]
    joined = "\n".join(col)

    assert joined == "Result  (cost=0.00..0.01 rows=1 width=4)"

    explained = db.explain(
        "select 1", analyze=True, format="json", buffers=True, verbose=True
    )

    assert "Plan" in explained.scalar()[0]
