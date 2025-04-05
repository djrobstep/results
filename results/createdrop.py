# from psycopg2.errors import DuplicateDatabase, InvalidCatalogName

from .psyco import quoted_identifier as qi

NO_YES_REALLY = (
    "You tried to drop a database without setting `yes_really_drop` to true."
)

import psycopg.errors


class DatabaseCreateDrop:
    def check_connection(self):
        return self.q("select 1")

    def can_connect(self):
        try:
            self.check_connection()
            return True
        except Exception:
            return False

    def create_db(
        self, name, *, fail_if_exists=False, drop_if_exists=False, yes_really_drop=False
    ):
        _name = qi(name)

        recreate = False

        if drop_if_exists and fail_if_exists:
            raise ValueError("Cannot both drop_if_exists and fail_if_exists")

        try:
            self.autocommit(f"create database {_name};")

        except psycopg.errors.DuplicateDatabase:
            if fail_if_exists:
                raise

            elif drop_if_exists:
                if not yes_really_drop:
                    raise RuntimeError(NO_YES_REALLY)
                self.drop_db(name, yes_really_drop=yes_really_drop)
                recreate = True

        if recreate:
            self.autocommit(f"create database {_name};")

        return self.sibling(name)

    def drop_db(
        self, name, *, fail_if_not_exists=False, yes_really_drop=False, force=False
    ):
        _name = qi(name)

        if not fail_if_not_exists:
            ifexist = "if exists "
        else:
            ifexist = ""

        if force:
            forceclause = " with (force)"
        else:
            forceclause = ""

        try:
            if not yes_really_drop:
                raise RuntimeError(NO_YES_REALLY)
            self.autocommit(f"drop database {ifexist}{_name}{forceclause};")
        except psycopg.errors.InvalidCatalogName:
            if fail_if_not_exists:
                raise
