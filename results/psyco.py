import re
import string

SAFE_CHARS = {char: None for char in (string.ascii_lowercase + "_")}


_IDENT_CHARS = frozenset("abcdefghijklmnopqrstuvwxyz0123456789_")


def reformat_bind_params(query, rewrite=True):
    """Reformat :colon-style bind parameters to %(name)s style.

    Correctly skips string literals ('...'), dollar-quoted blocks ($$...$$),
    double-quoted identifiers ("..."), and :: type-cast operators.
    """
    result = []
    i = 0
    n = len(query)

    while i < n:
        c = query[i]

        # Single-quoted string literal: '...' with '' escape for internal quotes
        if c == "'":
            j = i + 1
            while j < n:
                if query[j] == "'":
                    if j + 1 < n and query[j + 1] == "'":
                        j += 2
                    else:
                        j += 1
                        break
                else:
                    j += 1
            result.append(query[i:j])
            i = j

        # Double-quoted identifier: "..." with "" escape
        elif c == '"':
            j = i + 1
            while j < n:
                if query[j] == '"':
                    if j + 1 < n and query[j + 1] == '"':
                        j += 2
                    else:
                        j += 1
                        break
                else:
                    j += 1
            result.append(query[i:j])
            i = j

        # Dollar-quoted string: $tag$...$tag$
        elif c == "$":
            j = i + 1
            while j < n and query[j] != "$":
                j += 1
            if j < n:
                tag = query[i : j + 1]
                close = query.find(tag, j + 1)
                if close != -1:
                    result.append(query[i : close + len(tag)])
                    i = close + len(tag)
                else:
                    result.append(c)
                    i += 1
            else:
                result.append(c)
                i += 1

        # Potential bind parameter: :identifier
        # Skip if preceded by : (cast) or a digit (array slice like [1:3])
        elif c == ":":
            prev = query[i - 1] if i > 0 else None
            next_c = query[i + 1] if i + 1 < n else None
            is_cast = prev == ":"
            is_array_slice = prev is not None and prev.isdigit()
            # Identifiers must start with a letter or underscore, not a digit
            starts_valid_ident = next_c is not None and (next_c.isalpha() or next_c == "_")
            if not is_cast and not is_array_slice and starts_valid_ident:
                j = i + 1
                while j < n and query[j] in _IDENT_CHARS:
                    j += 1
                if rewrite:
                    varname = query[i + 1 : j]
                    result.append(f"%({varname})s")
                else:
                    result.append(query[i:j])
                i = j
            else:
                result.append(c)
                i += 1

        else:
            result.append(c)
            i += 1

    return "".join(result)


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
