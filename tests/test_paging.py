import results
from results.paging import recursive_comparison

PAGING_QUERY = """
select
    *
from
    (values (5, 6, 7), (1,2,3), (5,9,10), (5, 11, 12))
    t(a,b,c)
"""


def test_paging(tmpdb):
    db = results.db(tmpdb)

    with db.transaction() as t:
        PER_PAGE = 1

        page = t.paged(
            PAGING_QUERY,
            per_page=PER_PAGE,
            bookmark=(5, 9, 10),
            backwards=True,
            ordering="a, b, c",
        )
        p = page.paging
        assert p.has_prev is True
        assert p.has_next is True
        assert p.has_after is True
        assert p.has_before is True
        assert p.backwards is True
        assert p.at_start is False
        assert p.at_end is False
        assert p.past_end is False
        assert p.past_start is False
        assert p.is_empty is False
        assert p.is_full is True
        assert p.next == (5, 6, 7)
        assert p.prev == (5, 6, 7)
        assert p.discarded_item == dict(a=1, b=2, c=3)
        assert p.current == (5, 9, 10)
        assert p.current_reversed == (1, 2, 3)

        assert len(page) == PER_PAGE
        assert page.paging.results == page

        page = t.paged(
            PAGING_QUERY,
            per_page=2,
            bookmark=(5, 9, 10),
            backwards=False,
            ordering="a desc, b, c",
        )
        p = page.paging
        assert p.has_prev is True
        assert p.has_next is False
        assert p.has_after is False
        assert p.has_before is True
        assert p.backwards is False
        assert p.at_start is False
        assert p.at_end is True
        assert p.past_end is False
        assert p.past_start is False
        assert p.is_empty is False
        assert p.is_full is True
        assert p.next == (1, 2, 3)
        assert p.prev == (5, 11, 12)
        assert p.discarded_item is None
        assert p.current == (5, 9, 10)
        assert p.current_reversed is None

    page = db.paged(
        PAGING_QUERY,
        per_page=3,
        bookmark=(5, 11, 12),
        backwards=True,
        ordering="a desc, b, c",
    )
    p = page.paging

    assert p.has_prev is False
    assert p.has_next is True
    assert p.has_after is False
    assert p.has_before is True
    assert p.backwards is True
    assert p.at_start is True
    assert p.at_end is False
    assert p.past_end is False
    assert p.past_start is True
    assert p.is_empty is False
    assert p.is_full is False
    assert p.next == (5, 9, 10)
    assert p.prev == (5, 6, 7)
    assert p.discarded_item is None
    assert p.current == (5, 11, 12)
    assert p.current_reversed is None


def test_recursive_comparison():
    r = recursive_comparison(['"A"', '"B"'], range(1, 3))
    assert r == """("A" > 1 or ("A" = 1 and "B" > 2))"""
    r = recursive_comparison(['"A"', '"B"', '"C"'], range(1, 4))
    assert (
        r
        == """("A" > 1 or ("A" = 1 and "B" > 2) or ("A" = 1 and "B" = 2 and "C" > 3))"""
    )
    r = recursive_comparison(['"A"', '"B"', '"C"', '"D"'], range(1, 5))
    assert (
        r
        == """("A" > 1 or ("A" = 1 and "B" > 2) or ("A" = 1 and "B" = 2 and "C" > 3) or ("A" = 1 and "B" = 2 and "C" = 3 and "D" > 4))"""
    )
