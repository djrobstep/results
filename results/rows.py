import random
import string
from copy import deepcopy
from dataclasses import make_dataclass

from .formatting import format_table_of_dicts


def rando(length=10) -> str:
    """Generate a random string of lowercase letters."""
    return "".join(random.choice(string.ascii_lowercase) for _ in range(length))


def dedup(rows: list[tuple]):
    """Remove duplicate rows while preserving order."""
    return list({k: None for k in rows}.keys())


class DictRow(dict):
    def __str__(self) -> str:
        indent = 2

        maxlen = max(len(repr(_)) for _ in self) + indent

        items = [f"{repr(k).rjust(maxlen)}: {repr(v)}" for k, v in self.items()]

        mid = ",\n".join(items)

        return "{\n" + mid + "\n}\n"


class Rows(list):
    def __init__(self, *args, **kwargs):
        self.paging = None
        self.column_info = None

        super().__init__(*args, **kwargs)

    def __getitem__(self, x):
        if isinstance(x, (int, slice)):
            return super().__getitem__(x)

        if x not in self.column_info:
            raise ValueError(f"no such column: {x}")

        numbers = {c: i for i, c in enumerate(self.column_info)}

        i = numbers[x]

        return [_[i] for _ in self]

    def keys(self):
        return list(self.column_info)

    @property
    def d(self) -> list[dict]:
        return [DictRow(zip(self.column_info, _)) for _ in self]

    @property
    def dc(self) -> list:
        class_name = f"Row_{rando()}"
        _class = make_dataclass(class_name, list(self.column_info))
        return [_class(**d) for d in self.d]

    def as_dicts(self):
        return self.d

    def __str__(self) -> str:
        return "\n" + format_table_of_dicts(self.as_dicts())

    def __repr__(self) -> str:
        return str(self)

    def __add__(self, other):
        if self.column_info != other.column_info:
            raise ValueError("Cannot add Rows with mismatching columns")
        rows = Rows(list(deepcopy(self)) + list(deepcopy(other)))
        rows.column_info = self.column_info
        return rows

    @property
    def hierarchical(self) -> str:
        return format_table_of_dicts(self.as_dicts(), hierarchical=True)

    @property
    def guessed_sql_columns(self):
        from .typeguess import guess_sql_type_of_values

        return {k: guess_sql_type_of_values(self[k]) for k in self.column_info}

    @classmethod
    def from_dicts(cls, list_of_dicts):
        """Create a Rows object from a list of dictionaries."""
        first = list_of_dicts[0]
        column_names = list(first.keys())

        rows = cls(list(d.values()) for d in list_of_dicts)
        rows.column_info = {k: "text" for k in column_names}

        return rows

    def get_columns(self, clist, dedup=False):
        included_indexes = [i for i, x in enumerate(self.column_info) if x in clist]

        column_info = {k: v for k, v in self.column_info.items() if k in clist}

        if dedup:
            rows = Rows(
                list(
                    {
                        tuple(row[i] for i in included_indexes): None for row in self
                    }.keys()
                )
            )
        else:
            rows = Rows(tuple(row[i] for i in included_indexes) for row in self)

        rows.column_info = column_info
        return rows

    def rename(self, remapping):
        self.column_info = {remapping.get(k, k): v for k, v in self.column_info.items()}
