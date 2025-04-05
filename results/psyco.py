import re
import string

SAFE_CHARS = {char: None for char in (string.ascii_lowercase + "_")}


def reformat_bind_params(query, rewrite=True):
    """Reformat bind parameters in a SQL query."""
    pattern = re.compile(r"[^:](:[a-z0-9_]+)")

    parts = []
    remainder = 0

    for match in pattern.finditer(query):
        start, end = match.start(1), match.end(1)

        parts.append(query[remainder:start])

        if rewrite:
            varname = query[start + 1 : end]
            parts.append(f"%({varname})s")
        else:
            parts.append(query[start:end])

        remainder = end

    parts.append(query[remainder:])
    return "".join(parts)


def quoted_identifier(
    identifier, schema=None, identity_arguments=None, always_quote=False
):
    """Quote a SQL identifier if needed."""

    def quote_if_needed(text):
        if always_quote:
            return f'"{text}"'
        if all(char in SAFE_CHARS for char in text):
            return text
        else:
            return f'"{text}"'

    escaped_identifier = quote_if_needed(identifier.replace('"', '""'))

    if schema:
        escaped_schema = quote_if_needed(schema.replace('"', '""'))
        escaped_identifier = f"{escaped_schema}.{escaped_identifier}"

    if identity_arguments is not None:
        escaped_identifier = f"{escaped_identifier}({identity_arguments})"

    return escaped_identifier
