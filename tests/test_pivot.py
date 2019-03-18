import results
from results.pivoting import ordering


def test_grouped():
    GROUPS = [list("bcef"), list("abdef"), list("abc"), list("ae")]

    assert ordering(GROUPS) == list("abcdef")

    GROUPS = [list("abdef"), list("bcef"), list("abc"), list("ae")]

    assert ordering(GROUPS) == list("abdcef")


def test_pivot():
    UNPIVOTED = [
        dict(state="NSW", party="Pink", votes=10),
        dict(state="NSW", party="Orange", votes=1),
        dict(state="VIC", party="Pink", votes=11),
        dict(state="VIC", party="Orange", votes=2),
        dict(state="VIC", party="Yellow", votes=5),
    ]

    PIVOTED = [
        {"state": "NSW", "Pink": 10, "Orange": 1, "Yellow": None},
        {"state": "VIC", "Pink": 11, "Orange": 2, "Yellow": 5},
    ]

    r = results.Results(UNPIVOTED)
    pivoted = results.Results(PIVOTED)
    assert r.pivoted() == results.Results(PIVOTED)

    pivoted.replace_values(None, 0)
    PIVOTED[0]["Yellow"] = 0
    assert pivoted == PIVOTED


def test_pivot_multi_down():
    UNPIVOTED = [
        dict(year=2015, state="NSW", party="Pink", votes=10),
        dict(year=2015, state="NSW", party="Orange", votes=1),
        dict(year=2015, state="VIC", party="Magenta", votes=3),
        dict(year=2015, state="VIC", party="Pink", votes=11),
        dict(year=2015, state="VIC", party="Orange", votes=2),
        dict(year=2018, state="VIC", party="Yellow", votes=5),
    ]

    PIVOTED = [
        {
            "state": "NSW",
            "year": 2015,
            "Magenta": None,
            "Pink": 10,
            "Orange": 1,
            "Yellow": None,
        },
        {
            "state": "VIC",
            "year": 2015,
            "Magenta": 3,
            "Pink": 11,
            "Orange": 2,
            "Yellow": None,
        },
        {
            "state": "VIC",
            "year": 2018,
            "Magenta": None,
            "Pink": None,
            "Orange": None,
            "Yellow": 5,
        },
    ]

    r = results.Results(UNPIVOTED)
    pivoted = results.Results(PIVOTED)
    assert pivoted.keys() == ["state", "year", "Magenta", "Pink", "Orange", "Yellow"]
    assert r.pivoted() == results.Results(PIVOTED)


def test_otherbad():
    BAD = [{"Area": "A", "Count": 2}, {"Area": "B", "Count": 1}]
    assert results.Results(BAD) == results.Results(BAD).pivoted()


def test_badcase():
    BAD = [
        dict(year=2015, state="NSW", party="Pink", votes=10),
        dict(year=2015, state="NSW", party="Orange", votes=1),
        dict(year=2015, state="VIC", party="Magenta", votes=3),
        dict(year=2018, state="VIC", party="Pink", votes=10),
        dict(year=2018, state="VIC", party="Magenta", votes=3),
        dict(year=2018, state="NSW", party="Yellow", votes=1),
        dict(year=2018, state="NSW", party="Orange", votes=1),
    ]

    PIVOTED = [
        {
            "Magenta": None,
            "Orange": 1,
            "Pink": 10,
            "Yellow": None,
            "state": "NSW",
            "year": 2015,
        },
        {
            "Magenta": 3,
            "Orange": None,
            "Pink": None,
            "Yellow": None,
            "state": "VIC",
            "year": 2015,
        },
        {
            "Magenta": 3,
            "Orange": None,
            "Pink": 10,
            "Yellow": None,
            "state": "VIC",
            "year": 2018,
        },
        {
            "Magenta": None,
            "Orange": 1,
            "Pink": None,
            "Yellow": 1,
            "state": "NSW",
            "year": 2018,
        },
    ]

    pivoted = results.Results(BAD).pivoted()
    EXPECTED = results.Results(PIVOTED)
    assert pivoted == EXPECTED

    assert pivoted.keys() == ["year", "state", "Pink", "Magenta", "Yellow", "Orange"]
