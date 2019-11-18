import string

from .fileutil import is_mapping
from .resultset import resultproxy_to_results

INSERT = """
    insert into
        {table} ({colspec})
    values
        ({valuespec})
"""


INSERT_DEFAULT = """
    insert into
        {table}
    default values
"""


INSERT_UPSERT = """
    on conflict ({upsertkeyspec})
    do update set
        {upsertspec}
"""


INSERT_UPSERT = """
    on conflict ({upsertkeyspec})
    do update set
        {upsertspec}

"""


INSERT_UPSERT_DO_NOTHING = """
    on conflict ({upsertkeyspec})
    do nothing
"""


VALID = {k: None for k in (string.ascii_letters + string.digits + "_")}


def valid(key):
    clist = [_ for _ in list(key) if _ in VALID]
    v = "".join(clist)
    if v != key:
        raise ValueError(f'invalid key: "{key}"')
    return v


def insert(s, table, rows, upsert_on=None, returning=None):

    if is_mapping(rows):
        rows = [rows]

    if not rows:
        raise ValueError("empty list of rows, nothing to upsert")

    if returning is None:
        returning = not len(rows) > 1

    keys = [valid(k) for k in rows[0].keys()]

    if keys:
        colspec = ", ".join([f'"{k}"' for k in keys])
        valuespec = ", ".join(":{}".format(k) for k in keys)
        q = INSERT.format(table=table, colspec=colspec, valuespec=valuespec)
    else:
        q = INSERT_DEFAULT.format(table=table)

    if upsert_on:
        upsert_keys = list(keys)

        for k in upsert_on:
            upsert_keys.remove(k)

        upsertkeyspec = ", ".join([f'"{k}"' for k in upsert_on])

        if upsert_keys:
            upsertspec = ", ".join(f'"{k}" = excluded."{k}"' for k in upsert_keys)

            q_upsert = INSERT_UPSERT.format(
                upsertkeyspec=upsertkeyspec, upsertspec=upsertspec
            )
        else:
            q_upsert = INSERT_UPSERT_DO_NOTHING.format(upsertkeyspec=upsertkeyspec)

        q = q + q_upsert
    if returning:
        q += " returning *"

    rp = s.execute(q, rows)
    return resultproxy_to_results(rp)
