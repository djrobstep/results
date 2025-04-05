# Copyright results Pty Ltd 2022

from itertools import zip_longest

from .rows import Rows

ASC_OR_DESC = ("desc", "asc")


def quoted(column_name):
    """Quote a column name for SQL."""
    return f'"{column_name}"'


def parse_order_by_col(column_spec):
    """Parse a single column specification from ORDER BY clause."""
    column_spec = column_spec.strip()
    tokens = column_spec.rsplit(None, 1)

    if len(tokens) > 1:
        possible_direction = tokens[-1].lower()
        if possible_direction in ASC_OR_DESC:
            column_spec = tokens[0]
            descending = possible_direction == "desc"
        else:
            descending = False
    else:
        descending = False

    column_spec = column_spec.strip().strip('"')
    return column_spec, descending


def parse_order_by(order_by):
    """Parse an ORDER BY clause into column specifications."""
    columns = order_by.split(",")
    return [parse_order_by_col(col) for col in columns]


def reversed_order_by(order_by):
    """Reverse the direction of all columns in an ORDER BY specification."""
    return [(column, not descending) for column, descending in order_by]


def order_by_from_parsed(columns):
    """Convert parsed ORDER BY columns back to SQL string."""
    column_list = [
        quoted(column) + (" desc" if descending else "")
        for column, descending in columns
    ]
    return ", ".join(column_list)


PAGED_QUERY = """
select * from
(
{q}
) unpaged_table
{bookmark}
{order_by}
{limit}
"""

PARAM_PREFIX = "paging_"


def get_paged_query(
    query, params, order_by, bookmark=None, per_page=10, backwards=False
):
    query, paging_query_params = paging_wrapped_query(
        query, order_by, bookmark, per_page, backwards
    )
    params.update(paging_query_params)

    return query, params


def get_paged_rows(rows, paging_params):
    rows.paging = Paging(rows, **paging_params)
    return rows


def get_page(
    cursor,
    query,
    *args,
    bookmark=None,
    order_by,
    per_page,
    backwards,
    **kwargs,
):
    query, page_params = paging_wrapped_query(
        query, order_by, bookmark, per_page, backwards
    )

    argslist = list(args)
    try:
        params = argslist[0]
    except IndexError:
        argslist.append({})
        params = argslist[0]

    params.update(page_params)

    argslist[0] = params
    args = tuple(argslist)

    results, d = cursor.qd(query, *args, **kwargs)

    results = Rows(results)

    return results


def bind_pairs_iter(cols, bookmark, swap_on_descending=False):
    for i, zipped in enumerate(zip(cols, bookmark)):
        col, val = zipped
        name, is_descending = col
        lowercase_name = name.lower()
        bind = f"%({PARAM_PREFIX}{lowercase_name})s"

        if swap_on_descending and is_descending:
            yield name, bind
        else:
            yield bind, name


def make_bookmark_where_clause(cols, bookmark):
    if bookmark is None:
        return ""

    pairslist = bind_pairs_iter(cols, bookmark, swap_on_descending=True)

    b, a = zip(*pairslist)
    if len(a) > 1 or len(b) > 1:
        a, b = ", ".join(a), ", ".join(b)
        return f"where row({a}) > row({b})"

    else:
        return f"where {a[0]} > {b[0]}"


def paging_params(cols, bookmark):
    names = [PARAM_PREFIX + c[0].lower() for c in cols]
    return dict(zip_longest(names, bookmark or []))


def paging_wrapped_query(query, order_by, bookmark, per_page, backwards):
    cols = parse_order_by(order_by)
    if backwards:
        cols = reversed_order_by(cols)

    bookmark_clause = make_bookmark_where_clause(cols, bookmark)
    order_list = order_by_from_parsed(cols)
    order_by = f"order by {order_list}"

    limit = f"limit {per_page + 1}"

    params = paging_params(cols, bookmark)
    formatted = PAGED_QUERY.format(
        q=query, bookmark=bookmark_clause, order_by=order_by, limit=limit
    )
    return formatted, params


class Paging:
    def __init__(
        self, results, *, order_by, per_page=10, bookmark=None, backwards=False
    ):
        self.results = results
        self.per_page = per_page
        self.order_by = order_by
        self.bookmark = bookmark
        self.backwards = backwards
        self.parsed_order_by = parse_order_by(order_by)
        self.order_keys = [c[0] for c in self.parsed_order_by]
        # self.d = d

        try:
            self.discarded_item = results.pop(per_page)
            self.has_more = True
        except IndexError:
            self.discarded_item = None
            self.has_more = False

        if backwards:
            results.reverse()
        self.count = len(self.results)

        if self.count:
            self.names = list(self.results.keys())
            self.names_i = {n: i for i, n in enumerate(self.names)}
        else:
            self.names = None
            self.names_i = None

    @property
    def has_after(self):
        return self.discarded_item is not None

    @property
    def has_before(self):
        return self.bookmark is not None

    @property
    def has_next(self):
        if self.backwards:
            return self.has_before
        else:
            return self.has_after

    @property
    def has_prev(self):
        if self.backwards:
            return self.has_after
        else:
            return self.has_before

    @property
    def at_start(self):
        return not self.has_prev

    @property
    def at_end(self):
        return not self.has_next

    @property
    def is_all(self):
        return self.at_start and self.at_end

    @property
    def is_full(self):
        return self.count == self.per_page

    @property
    def past_start(self):
        return self.backwards and self.at_start and not self.is_full

    @property
    def past_end(self):
        return not self.backwards and self.at_end and not self.is_full

    @property
    def next(self):
        if not self.is_empty:
            return self.get_bookmark(self.results[-1])

    @property
    def prev(self):
        if not self.is_empty:
            return self.get_bookmark(self.results[0])

    @property
    def is_empty(self):
        return not bool(self.count)

    @property
    def current(self):
        return self.bookmark

    @property
    def current_reversed(self):
        if self.discarded_item:
            return self.get_bookmark(self.discarded_item)

    def get_bookmark(self, result_row):
        return tuple(result_row[self.names_i[k]] for k in self.order_keys)
