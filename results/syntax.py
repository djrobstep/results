import string
import textwrap

safe_chars = {k for k in (string.ascii_lowercase + "_")}


def quoted_identifier(
    identifier, schema=None, identity_arguments=None, always_quote=False
):
    def qin(x):  # quote if needed
        if (not always_quote) and all(_ in safe_chars for _ in x):
            return x
        else:
            return f'"{x}"'

    s = qin(identifier.replace('"', '""'))
    if schema:
        s = "{}.{}".format(qin(schema.replace('"', '""')), s)
    if identity_arguments is not None:
        s = "{}({})".format(s, identity_arguments)
    return s


qi = quoted_identifier


def inplace_cte(rows, column_names_and_types: dict[str, str], ctename: str):
    # select * from (values (1,2)) as x(a,b);

    prefix = "p"

    def do_row(i, row):
        parts = [f":{prefix}_{i}_{j}" for j in range(len(row))]
        joined = ", ".join(parts)
        return f"({joined})"

    def get_values_expr(rows):
        return ",\n".join(do_row(i, _) for i, _ in enumerate(rows))

    cols_casts = ", ".join(f"{qi(k)}::{v}" for k, v in column_names_and_types.items())

    values_expr = get_values_expr(rows)

    colnames = ", ".join([qi(k) for k in column_names_and_types])
    sig = f"{ctename}({colnames})"

    sql = f"""\
select {cols_casts}
from 
(values 
{textwrap.indent(values_expr, "  ")}
) 
as {sig}"""

    params = {
        f"{prefix}_{i}_{j}": v for i, row in enumerate(rows) for j, v in enumerate(row)
    }

    return sql, params
