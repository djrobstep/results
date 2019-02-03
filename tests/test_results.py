from pathlib import Path

from pytest import raises

import results
from results import db

FIXTURES = "tests/FIXTURES"


def test_file_loading(tmpdir):
    fixture_files = results.files(FIXTURES)

    with raises(ValueError):
        results.from_files(fixture_files, ignore_unopenable=False)

    results.from_files(fixture_files)


def test_db(tmpdb):
    s = results.db(tmpdb)

    wun = s.ss("select 1")
    s.ss("create table t(id int primary key)")
    assert wun.scalar() == 1

    s.raw_from_file(Path(FIXTURES) / "sql/select1.sql")


def test_empty(tmpdb):
    db = results.db(tmpdb)
    empty = db.ss("select 1 as bollocks limit 0")
    assert empty.keys() == ["bollocks"]

    assert results.Results([]).keys() == []


def test_function_calls(tmpdbwithfunctions):
    tmpdb = db(tmpdbwithfunctions)

    assert tmpdb.procs.inc_f(1).scalar() == 2

    with tmpdb.transaction() as t:
        result = t.procs.inc_f(1)
        assert result.scalar() == 2


def test_result_object(tmpdir, sample):
    r = results.Results([])
    assert r.keys() == []

    EXPECTED_CSV = """first_name,last_name
Ice,T
Ice,Cube
,
"""

    r = results.Results(sample)

    assert r.keys() == ["First Name ", " Last_Name"]

    r = r.with_standardized_keys()

    assert r.keys() == ["first_name", "last_name"]

    r.strip_all_values()

    assert r[2] == dict(first_name="", last_name="")

    assert r.grouped_by("first_name") == {
        "Ice": results.Results(
            [
                dict(first_name="Ice", last_name="T"),
                dict(first_name="Ice", last_name="Cube"),
            ]
        ),
        "": results.Results([dict(first_name="", last_name="")]),
    }

    r.set_blanks_to_none()

    assert r.by_key("last_name", "first_name") == {
        "T": "Ice",
        "Cube": "Ice",
        None: None,
    }

    assert r[2] == dict(first_name=None, last_name=None)

    assert r.csv == EXPECTED_CSV

    outpath = tmpdir / "test.csv"
    r.save_csv(outpath)
    assert Path(outpath).read_text() == EXPECTED_CSV

    r.delete_key("last_name")
    assert r.keys() == ["first_name"]
    r.delete_keys(["first_name"])
    assert r.keys() == []


def test_single_result():
    r = results.Result()
    r.first_name = "Abc"
    assert list(r.keys()) == ["first_name"]
    assert r.first_name == "Abc"
    assert hasattr(r, "first_name")


def test_key_superset():
    r = results.Results([dict(a=1, b=2), dict(b=3, c=4)])
    assert r.keys() == ["a", "b"]
    r = r.with_key_superset()
    assert r.keys() == ["a", "b", "c"]


def test_clean_whitespace():
    r = results.Results([dict(a=" a\nb\n", b="  ")])
    r.standardize_spaces()
    x = r.one()
    assert x.a == "a b"
    assert x.b == ""


def test_slice(sample):
    r = results.Results(sample)
    assert r[1:2] == results.Results([sample[1]])


def test_hierarchical():
    ORIGINAL = [
        dict(state="NSW", party="Pink", candidate="A"),
        dict(state="NSW", party="Pink", candidate="B"),
        dict(state="VIC", party="Pink", candidate="C"),
        dict(state="VIC", party="Pink", candidate="D"),
        dict(state="VIC", party="Yellow", candidate="E"),
    ]

    HIERARCHICAL = [
        dict(state="NSW", party="Pink", candidate="A"),
        dict(state="", party="", candidate="B"),
        dict(state="VIC", party="Pink", candidate="C"),
        dict(state="", party="", candidate="D"),
        dict(state="", party="Yellow", candidate="E"),
    ]

    r = results.Results(ORIGINAL)
    r.make_hierarchical()
    assert r == results.Results(HIERARCHICAL)
