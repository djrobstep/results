import atexit
import getpass
from contextlib import contextmanager
from inspect import currentframe
from pathlib import Path
from threading import get_ident
import multiprocessing

from psycopg import connect as pgconnect
from psycopg_pool import ConnectionPool

from .constants import type_codes
from .createdrop import DatabaseCreateDrop
from .inserting import Inserting
from .notify import ListenNotify
from .paging import get_paged_query, get_paged_rows
from .psyco import reformat_bind_params
from .rows import Rows
from .schemas import Schemas
from .urls import URL

conns = {}


def pool_shutdown():
    for pool in conns.values():
        pool.close()


atexit.register(pool_shutdown)


def connection_check(conn):
    with conn.cursor() as curs:
        curs.execute("SELECT 1")


def reconnect_failed(connection_pool, **kwargs):
    connection_pool.queue.put("failed")


@contextmanager
def pooled_connection(db_url):
    if db_url not in conns:

        def configure_check(connection, **kwargs):
            pool.queue.put("ok")

        pool = ConnectionPool(
            db_url,
            min_size=1,
            max_size=1024,
            open=False,
            configure=configure_check,
            check=connection_check,
            reconnect_timeout=0,
            reconnect_failed=reconnect_failed,
        )

        queue = multiprocessing.Queue()

        pool.queue = queue

        pool.open()

        conns[db_url] = pool
        result = pool.queue.get()

        if result != "ok":
            raise ValueError("Connection failed")

    else:
        pool = conns[db_url]

    with pool.connection() as conn:
        yield conn


def put_conn(conn, db_url):
    tid = get_ident()

    pool = conns[db_url]
    pool.putconn(conn, tid)


def get_inspected(t, **kwargs):
    from results.schemainspect import get_inspector

    return get_inspector(t.c, **kwargs)


class Transaction(Inserting):
    def __init__(self):
        self._back = 0

    def inspect(self, **kwargs):
        return get_inspected(self, **kwargs)

    def ex(self, *args, **kwargs):
        self.c.execute(*args, **kwargs)

    def execute(self, *args, **kwargs) -> Rows:
        self.c.execute(*args, **kwargs)

        if self.c.description is None:
            return None

        fetched = self.c.fetchall()
        descr = list(self.c.description)

        rows = Rows(fetched)

        rows.paging = None

        column_info = {c.name: type_codes.get(c.type_code, "unknown") for c in descr}

        rows.column_info = column_info

        return rows

    def executemany(self, *args, **kwargs) -> Rows:
        self.c.executemany(*args, **kwargs, returning=True)

        if not self.c.description:
            return None

        fetched = []

        while True:
            for row in self.c.fetchall():
                fetched.append(row)
            if not self.c.nextset():
                break

        if self.c.description is None:
            return None

        descr = list(self.c.description)

        rows = Rows(fetched)

        rows.paging = None

        column_info = {c.name: type_codes.get(c.type_code, "unknown") for c in descr}

        rows.column_info = column_info

        return rows

    @property
    def db_version(self) -> int:
        return self.c.connection.pgconn.server_version // 10000

    def qvalues(self, query, values, fetch=True):
        keylist = list(values[0].keys())

        keylist = [f":{_}" for _ in keylist]

        param_string = f"({', '.join(k for k in keylist)})"

        template = reformat_bind_params(param_string)

        query = query.replace("[values]", template)

        executed = self.executemany(query, values)

        return executed

    def pages(self, *args, paging: dict, **kwargs):
        bookmark = None

        while True:
            paging["bookmark"] = bookmark
            page = self.q(*args, paging=paging, **kwargs)

            if not page:
                break

            yield page

            if page.paging.has_next:
                bookmark = page.paging.next
            else:
                break

    def all_pages(self, *args, paging: dict, **kwargs):
        return list(self.pages(*args, paging=paging, **kwargs))

    def q(
        self,
        query,
        params=None,
        *,
        context=False,
        paging=None,
        fail_on_empty=False,
        colon_bind_params=True,
        **kwargs,
    ):
        if not query.strip():
            if fail_on_empty:
                raise ValueError("Error: Empty query.")
            else:
                return None

        query = query.replace("%", "%" + "%")

        if context:
            frame = currentframe()

        try:
            if context:
                if self._back:
                    fback = frame.f_back.f_back
                else:
                    fback = frame.f_back

                caller_locals = fback.f_locals
                _params = caller_locals
            else:
                _params = {}

            if params:
                _params.update(params)

            if kwargs:
                _params.update(kwargs)

            if _params and colon_bind_params:
                query = reformat_bind_params(query)

            if paging:
                query, _params = get_paged_query(query, _params, **paging)

            rows = self.execute(query, _params)

            if paging:
                rows = get_paged_rows(rows, paging)

            return rows
        finally:
            if context:
                del frame

    def q_from_file(self, path, *args, **kwargs):
        query = Path(path).expanduser().resolve().read_text()
        return self.q(query, *args, **kwargs)

    def notify(self, channel, payload):
        self.q(
            """select pg_notify(:channel, :payload)""",
            dict(channel=channel, payload=payload),
        )


@contextmanager
def autocommit_transaction(db_url):
    conn = pgconnect(db_url)

    conn.autocommit = True

    try:
        with conn.cursor() as curs:
            t = Transaction()
            t.c = curs
            yield t
    finally:
        conn.close()


@contextmanager
def autocommit_connection(db_url):
    conn = pgconnect(db_url)

    conn.autocommit = True

    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def transaction(db_url):  # , cursor_factory=DictCursor):
    with pooled_connection(db_url) as conn:
        with conn.cursor() as curs:
            t = Transaction()
            t.c = curs
            yield t


def db(url=None):
    return Database(url)


class Database(DatabaseCreateDrop, Schemas, ListenNotify):
    def __init__(self, url=None):
        if url is None:
            url = getpass.getuser()

        _url = URL(url)

        if not _url.scheme or _url.scheme == "postgres":
            _url.scheme = "postgresql"

        self.URL = _url
        self.url = str(_url)

    @property
    def url_object(self):
        return URL(self.url)

    def sibling(self, name, allow_self=False):
        sibling_url = URL(self.url)
        sibling_url.path = name

        is_self = sibling_url.path == self.url_object.path

        if is_self and allow_self is False:
            raise ValueError("sibling must not be the same database")
        return db(str(sibling_url))

    @property
    def name(self):
        return self.url_object.relative_path

    @contextmanager
    def t(self):
        with transaction(self.url) as t:
            yield t

    @contextmanager
    def t_autocommit(self):
        with autocommit_transaction(self.url) as t:
            yield t

    @contextmanager
    def c_autocommit(self):
        with autocommit_connection(self.url) as t:
            yield t

    def autocommit(self, *args, **kwargs):
        with self.t_autocommit() as t:
            t.q(*args, **kwargs)

    def __getattr__(self, name):
        def method(*args, **kwargs):
            with self.t() as t:
                t._back = 1
                m = getattr(t, name)
                return m(*args, **kwargs)

        return method

    def __repr__(self):
        return f"db(url={self.url})"
