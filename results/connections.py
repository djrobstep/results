from contextlib import contextmanager
from functools import partial
from pathlib import Path

from schemainspect import get_inspector
from sqlbag import S, raw_execute

from .inserting import insert
from .migration import SchemaManagement
from .paging import Paging, paging_wrapped_query
from .result import Result
from .resultset import resultproxy_to_results


def build_proc_call_query(_proc_name, *args, **kwargs):
    _proc_name = _proc_name.replace("__", ".")
    params = {f"positional{i}": x for i, x in enumerate(args)}
    params.update(**dict(kwargs))
    paramnames = params.keys()
    bindparams = [f":{name}" for name in list(paramnames)]
    paramspec = ", ".join(bindparams)
    query = f"select * from {_proc_name}({paramspec})"
    return query, params


class transactionprocs:
    def __init__(self, sess):
        self.session = sess

    def __getattr__(self, name):
        return partial(self.call, name)

    def call(self, _proc_name, *args, **kwargs):
        query, params = build_proc_call_query(_proc_name, *args, **kwargs)

        return self.session.ex(query, params)


EXPLAIN_DEFAULTS = dict(
    analyze=False,
    verbose=False,
    costs=True,
    buffers=False,
    timing=True,
    format="TEXT",
    summary=None,
)


def explain_prefix(**kwargs):

    # summary on by default if analyze is on
    defaults = dict(EXPLAIN_DEFAULTS)
    defaults["summary"] = defaults["analyze"]

    flags = {
        k: kwargs.get(k)
        for k, default in defaults.items()
        if kwargs.get(k) and kwargs.get(k) != default
    }

    def make_flag(k, v):
        if isinstance(v, bool):
            return k.upper()
        return f"{k} {v}".upper()

    flags = ", ".join(make_flag(k, v) for k, v in sorted(flags.items()))

    prefix = "EXPLAIN"
    if flags:
        prefix += f" ({flags})"

    return prefix


class transaction:
    def __init__(self, s):
        self.s = s
        self.procs = transactionprocs(self)

    def ex(self, *args, execution_options=None, **kwargs):
        if execution_options:
            _resultproxy = self.s.connection(
                execution_options=execution_options
            ).execute(*args, **kwargs)
        else:
            _resultproxy = self.s.execute(*args, **kwargs)
        results = resultproxy_to_results(_resultproxy)
        return results

    def raw_ex(self, *args, execution_options=None, **kwargs):
        if execution_options:
            return self.s.connection(execution_options=execution_options).execute(
                *args, **kwargs
            )
        else:
            return self.s.execute(*args, **kwargs)

    def paged(
        self,
        query,
        *args,
        bookmark=None,
        ordering,
        per_page,
        backwards,
        use_top=False,
        supports_row=True,
        **kwargs,
    ):
        query, page_params = paging_wrapped_query(
            query, ordering, bookmark, per_page, backwards, use_top, supports_row
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

        results = self.ex(query, *args, **kwargs)
        results.paging = Paging(results, per_page, ordering, bookmark, backwards)

        return results

    def insert(self, table, rowlist, upsert_on=None, returning=None):
        return insert(self.s, table, rowlist, upsert_on, returning)

    def pg_notify(self, channel, payload=None):
        return self.ex(
            """
            select pg_notify(:channel, :payload)
        """,
            dict(channel=channel, payload=payload),
        )

    def explain(self, *args, **kwargs):
        args = list(args)

        stmt = args[0]

        prefix = explain_prefix(**kwargs)

        for k in EXPLAIN_DEFAULTS:
            kwargs.pop(k, None)

        args[0] = f"{prefix} {stmt}"
        return self.ex(*args, **kwargs)


class procs:
    def __init__(self, db):
        self.db = db

    def __getattr__(self, name):
        return partial(self.call, name)

    def call(self, _proc_name, *args, **kwargs):
        _proc_name = _proc_name.replace("__", ".")

        with self.db.transaction() as s:
            query, params = build_proc_call_query(_proc_name, *args, **kwargs)
            return s.ex(query, params)


class db(SchemaManagement):
    def __init__(self, *args, **kwargs):
        self._args = args
        if args and args[0].startswith("postgres"):
            kwargs.setdefault("executemany_mode", "batch")
        self._kwargs = kwargs

    @contextmanager
    def transaction(self):
        with S(*self._args, **self._kwargs) as s:
            yield transaction(s)

    @property
    def procs(self):
        return procs(self)

    @property
    def db_url(self):
        return self._args[0]

    def ss(self, *args, **kwargs):
        with self.transaction() as t:
            return t.ex(*args, **kwargs)

    def ss_iter(self, *args, **kwargs):
        with self.transaction() as t:
            for row in t.raw_ex(*args, **kwargs):
                yield Result(row)

    def paged(self, *args, **kwargs):
        with self.transaction() as t:
            return t.paged(*args, **kwargs)

    def raw_from_file(self, f):
        sql = Path(f).read_text()
        return self.raw(sql)

    def ss_from_file(self, f, *args, **kwargs):
        sql = Path(f).read_text()
        return self.ss(sql, *args, **kwargs)

    def paged_from_file(self, f, *args, **kwargs):
        sql = Path(f).read_text()
        return self.paged(sql, *args, **kwargs)

    def insert(self, table, rowlist, upsert_on=None, returning=None):
        with self.transaction() as s:
            inserted = s.insert(table, rowlist, upsert_on, returning)
        return inserted

    def raw(self, sql):
        with S(*self._args, **self._kwargs) as s:
            _results = raw_execute(s, sql)
            return _results

    def inspect(self):
        with S(*self._args, **self._kwargs) as s:
            i = get_inspector(s)
        return i

    def pg_notify(self, channel, payload=None):
        with self.transaction() as t:
            return t.pg_notify(channel, payload)

    def explain(self, *args, **kwargs):
        with self.transaction() as t:
            return t.explain(*args, **kwargs)
