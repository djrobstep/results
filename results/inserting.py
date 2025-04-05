from collections.abc import Mapping

from .rows import Rows
from .syntax import quoted_identifier as qi

INSERT = """
    insert into
        {table} ({colspec})
    values
        [values]
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

from string import ascii_lowercase, digits

ALLOWED = set(ascii_lowercase + digits + "_")
LOWERCASE = set(ascii_lowercase)


def renamed_key(k: str):
    k = k.lower()

    filtered = "".join(_ for _ in k if _ in ALLOWED)

    if not filtered:
        return "k"

    if filtered[0] not in LOWERCASE:
        filtered = f"k{filtered}"

    return filtered


def key_renaming(keylist):
    d = {}

    rekeys = set()

    for k in keylist:
        renamed = renamed_key(k)
        modified = renamed

        i = 0

        while True:
            if modified not in rekeys:
                rekeys.add(modified)

                if k != modified:
                    d[k] = modified
                break
            else:
                modified = modified + f"_{i}"
                i += 1

    return d


class Inserting:
    def insert(self, table, rows, upsert_on=None, returning=None, update_cols=None):
        if isinstance(rows, Mapping):
            rows = [rows]

        if isinstance(upsert_on, str):
            upsert_on = [upsert_on]

        if not rows:
            raise ValueError("empty list of rows, nothing to upsert")

        if returning is None:
            returning = not len(rows) > 1

        if isinstance(rows, Rows):
            rows = rows.as_dicts()

        keys = list(rows[0].keys())

        if renaming_map := key_renaming(keys):
            rows = [{renaming_map[k]: v for k, v in d.items()} for d in rows]

        if keys:
            colspec = ", ".join([qi(k) for k in keys])

            if renaming_map:
                valuespec = ", ".join(f":{renaming_map[k]}" for k in keys)
            else:
                valuespec = ", ".join(f":{k}" for k in keys)
            q = INSERT.format(table=table, colspec=colspec, valuespec=valuespec)
        else:
            q = INSERT_DEFAULT.format(table=table)

        if upsert_on:
            if update_cols is None:
                upsert_keys = list(keys)
            else:
                upsert_keys = list(update_cols)

            for k in upsert_on:
                if k in upsert_keys:
                    upsert_keys.remove(k)

            upsertkeyspec = ", ".join([qi(k) for k in upsert_on])

            if upsert_keys:
                upsertspec = ", ".join(
                    f"{qi(k)} = excluded.{qi(k)}" for k in upsert_keys
                )

                q_upsert = INSERT_UPSERT.format(
                    upsertkeyspec=upsertkeyspec, upsertspec=upsertspec
                )
            else:
                q_upsert = INSERT_UPSERT_DO_NOTHING.format(upsertkeyspec=upsertkeyspec)

            q = q + q_upsert
        if returning:
            q += " returning *"

        return self.qvalues(q, rows, fetch=bool(returning))
