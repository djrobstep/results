import itertools

from .resultset import Results


class item:
    def __init__(self, i):
        self.i = i

    @property
    def after(self):
        aft = []

        x = self.next

        while x and x.i != self.i and x.i not in aft:
            aft.append(x.i)
            x = x.next

        return aft

    @property
    def before(self):
        bef = []

        x = self.prev

        while x and x.i != self.i and x.i not in bef:
            bef.append(x.i)
            x = x.next

        return bef


class items:
    def __init__(self, _list):

        full = []

        previous = None

        for x in _list:
            i = item(x)

            i.prev = previous

            if previous:
                previous.next = i

            i.next = None
            full.append(i)
            previous = i
        self._list = full


def ordering(groups):
    glist = [items(_) for _ in groups]
    allitems = itertools.chain(*groups)

    d_all = {k: None for k in allitems}

    d_before = {k: set() for k in d_all.keys()}

    for g in glist:
        for i in g._list:
            val = i.i
            d_before[val] = d_before[val] | set(i.before)

    _result = []

    while True:
        if not d_all:
            break

        for k in list(d_all.keys()):
            if d_before[k] - set(_result):
                continue
            _result.append(k)

            d_all.pop(k)

    return _result


def pivoted(_results):
    r = _results
    try:
        *down, across, values = r.keys()
    except ValueError:
        raise ValueError(
            "Pivoting requires at least 3 columns for input: 1 or more for row labels, 1 for column labels, and one values column"
        )

    downvalues = r.distinct_values(columns=down)

    acrossvalues = r.distinct_values(across)

    key_cols = down + [across]

    def cell_key(row):
        return tuple(row[k] for k in key_cols)

    d = {cell_key(row): row[values] for row in r}

    def pivoted_it():
        for downvalue in downvalues:
            out = {k: v for k, v in zip(down, downvalue)}

            row = {
                acrossvalue: d.get(tuple(list(downvalue) + [acrossvalue]), None)
                for acrossvalue in acrossvalues
            }
            out.update(row)
            yield out

    return Results(pivoted_it())
