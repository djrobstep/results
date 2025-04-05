import csv
from io import StringIO

from .rows import Rows


def rows_from_text(text, format="csv", **kwargs):
    """Parse text data and return a Rows object."""
    reader = csv.reader(StringIO(text.strip()))

    column_names = next(reader)

    rows = Rows(row for row in reader)
    rows.column_info = {name: "text" for name in column_names}

    return rows
