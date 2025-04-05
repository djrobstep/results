import re


def check_for_drop(s):
    return bool(re.search(r"(drop\s+)", s, re.IGNORECASE))


class Statements(list):
    def __init__(self, *args, **kwargs):
        super(Statements, self).__init__(*args, **kwargs)

    @property
    def sql(self):
        if not self:
            return ""

        return "\n\n".join(self) + "\n\n"

    def __add__(self, other):
        self += list(other)
        return self


class UnsafeMigrationException(Exception):
    pass
