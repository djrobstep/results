import io
import itertools
from numbers import Number
from pathlib import Path

import csvx

from .annotations import AnnotationsMixin
from .cleaning import standardized_key_mapping
from .result import Result
from .sqlutil import create_table_statement
from .typeguess import guess_sql_column_type


def results(rows):
    return Results(rows)


def resultproxy_to_results(rp):
    if rp.returns_rows:
        cols = rp.context.cursor.description
        keys = [c[0] for c in cols]

        r = Results(rp)
        r._keys_if_empty = keys
        return r
    else:
        return None


class Results(list, AnnotationsMixin):
    def __init__(self, *args, **kwargs):
        try:
            given = args[0]
            given = [Result(_) for _ in given]

            args = list(args)
            args[0] = given
            args = tuple(args)

            self.paging = None
        except IndexError:
            pass
        self._keys_if_empty = None
        super().__init__(*args, **kwargs)

    def all_keys(self):
        keylist = dict()

        for row in self:
            rowkeys = row.keys()

            for key in rowkeys:
                if key not in keylist:
                    keylist[key] = True
        return list(keylist.keys())

    def by_key(self, key, value=None):
        def get_value(row):
            if value is None:
                return row
            else:
                return row[value]

        return {_[key]: get_value(_) for _ in self}

    def with_key_superset(self):
        all_keys = self.all_keys()

        def dict_with_all_keys(d):
            return {k: d.get(k, None) for k in all_keys}

        return Results([dict_with_all_keys(_) for _ in self])

    def with_renamed_keys(self, mapping):
        def renamed_key(x):
            if x in mapping:
                return mapping[x]
            return x

        def renamed_it():
            for row in self:
                d = {
                    renamed_key(k): v
                    for k, v in row.items()
                    if renamed_key(k) is not None
                }
                yield Result(d)

        return Results(list(renamed_it()))

    def standardized_key_mapping(self):
        return standardized_key_mapping(self.keys())

    def with_standardized_keys(self):
        return self.with_renamed_keys(self.standardized_key_mapping())

    def strip_values(self):
        for row in self:
            for k, v in row.items():
                if v and isinstance(v, str):
                    stripped = v.strip()

                    if stripped != v:
                        row[k] = stripped

    def strip_all_values(self):
        self.strip_values()

    def standardize_spaces(self):
        self.clean_whitespace()

    def clean_whitespace(self):
        for row in self:
            for k, v in row.items():
                if v and isinstance(v, str):
                    standardized = " ".join(v.split())

                    if standardized != v:
                        row[k] = standardized

    def delete_key(self, column=None):
        for row in self:
            try:
                del row[column]
            except KeyError:
                pass

    def delete_keys(self, columns=None):
        for row in self:
            for c in columns:
                try:
                    del row[c]
                except KeyError:
                    pass

    def set_blanks_to_none(self):
        for row in self:
            for k, v in row.items():
                if isinstance(v, str) and not v.strip():
                    row[k] = None

    def replace_values(self, before, after):
        for row in self:
            for k, v in row.items():
                if v == before:
                    row[k] = after

    def values_for(self, column=None, columns=None):
        if column is not None:
            values = [_[column] for _ in self]
        elif columns is not None:
            values = [tuple(_[c] for c in columns) for _ in self]
        else:
            values = list(self.values())
        return values

    def distinct_values(self, column=None, columns=None):
        values = self.values_for(column, columns)
        d = {k: True for k in values}
        return list(d.keys())

    @property
    def csv(self):
        f = io.StringIO()
        with csvx.DictWriter(f, lineterminator="\n") as w:
            w.write_dicts(self)
            value = f.getvalue()
        return value

    def save_csv(self, destination):
        Path(destination).write_text(self.csv)

    def save_xlsx(self, destination):
        from xlsxwriter import Workbook

        workbook = Workbook(destination)
        worksheet = workbook.add_worksheet()

        for r, row in enumerate([self.keys()] + self):
            for c, col in enumerate(row):
                worksheet.write(r, c, col)

        workbook.close()

    def keys(self):
        try:
            first = self[0]
        except IndexError:
            if self._keys_if_empty is None:
                return []
            else:
                return self._keys_if_empty
        return list(first.keys())

    def copy(self):
        return Results(self)

    def grouped_by(self, column=None, columns=None):
        def keyfunc(x):
            if column:
                return x[column]
            if columns:
                return tuple([x[k] for k in columns])

        copied = Results(self)

        copied.sort(key=keyfunc)

        def grouped_by_it():
            for k, g in itertools.groupby(copied, keyfunc):
                yield k, Results(g)

        return dict(grouped_by_it())

    def __getitem__(self, x):
        if isinstance(x, slice):
            return Results(list(self)[x])
        elif isinstance(x, Number):
            return list.__getitem__(self, x)
        else:
            return [_[x] for _ in self]

    def one(self):
        length = len(self)
        if not length:
            raise RuntimeError("should be exactly one result, but there is none")
        elif length > 1:
            raise RuntimeError("should be exactly one result, but there is multiple")
        return self[0]

    def scalar(self):
        return self.one()[0]

    def pivoted(self):
        from .pivoting import pivoted

        return pivoted(self)
        try:
            down, across, values = self.keys()
        except ValueError:
            raise ValueError("pivoting requires exactly 3 columns")

        downvalues = self.distinct_values(down)
        acrossvalues = self.distinct_values(across)

        d = {(row[down], row[across]): row[values] for row in self}

        def pivoted_it():
            for downvalue in downvalues:
                out = {down: downvalue}
                row = {
                    acrossvalue: d.get((downvalue, acrossvalue), None)
                    for acrossvalue in acrossvalues
                }
                out.update(row)
                yield out

        return Results(pivoted_it())

    def make_hierarchical(self):
        previous = None

        for r in self:
            original = Result(r)
            if previous:
                for k, v in r.items():
                    if previous[k] == v:
                        r[k] = ""
                    else:
                        break
            previous = original

    @property
    def md(self):
        from tabulate import tabulate

        return tabulate(self, headers="keys", tablefmt="pipe")

    def guessed_sql_column_types(self):
        return {k: guess_sql_column_type(self.values_for(k)) for k in self.keys()}

    def guessed_create_table_statement(self, name):
        guessed = self.guessed_sql_column_types()
        return create_table_statement(name, guessed)
