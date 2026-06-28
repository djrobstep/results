"""
Tests for lockinfo — statement splitting, data model, and output formatting.
No live DB required.
"""
import pytest
from results.lockinfo import (
    LockEntry,
    StatementLockInfo,
    format_results,
    split_statements,
    LOCK_SEVERITY_INDEX,
    NOTABLE_THRESHOLD,
)


# ---------------------------------------------------------------------------
# split_statements
# ---------------------------------------------------------------------------

class TestSplitStatements:
    def test_basic_split(self):
        stmts = split_statements("SELECT 1; SELECT 2;")
        assert len(stmts) == 2

    def test_dollar_quoted_function(self):
        sql = "CREATE FUNCTION f() RETURNS void AS $$BEGIN NULL; END$$ LANGUAGE plpgsql; SELECT 1;"
        stmts = split_statements(sql)
        assert len(stmts) == 2
        assert "CREATE FUNCTION" in stmts[0]
        assert "SELECT 1" in stmts[1]

    def test_comments_ignored(self):
        stmts = split_statements("-- just a comment\nSELECT 1;")
        assert len(stmts) == 1

    def test_empty_string(self):
        assert split_statements("") == []

    def test_whitespace_only(self):
        assert split_statements("   \n  ;  \n") == []

    def test_single_statement_no_semicolon(self):
        stmts = split_statements("SELECT 1")
        assert len(stmts) == 1

    def test_preserves_statement_content(self):
        sql = "CREATE TABLE t (id integer NOT NULL);"
        stmts = split_statements(sql)
        assert len(stmts) == 1
        assert "CREATE TABLE" in stmts[0]


# ---------------------------------------------------------------------------
# LockEntry
# ---------------------------------------------------------------------------

class TestLockEntry:
    def _make(self, mode="AccessExclusiveLock", relname="users", schema="public", relkind="r"):
        return LockEntry(
            locktype="relation",
            mode=mode,
            relation_name=relname,
            schema_name=schema,
            relkind=relkind,
        )

    def test_severity_index_ordering(self):
        access_share = self._make("AccessShareLock")
        access_excl = self._make("AccessExclusiveLock")
        assert access_share.severity_index < access_excl.severity_index

    def test_notable_access_exclusive(self):
        assert self._make("AccessExclusiveLock").is_notable

    def test_notable_share_update_exclusive(self):
        assert self._make("ShareUpdateExclusiveLock").is_notable

    def test_not_notable_row_exclusive(self):
        assert not self._make("RowExclusiveLock").is_notable

    def test_not_notable_access_share(self):
        assert not self._make("AccessShareLock").is_notable

    def test_object_label_table(self):
        lock = self._make(relname="orders", schema="myschema", relkind="r")
        assert "orders" in lock.object_label
        assert "myschema" in lock.object_label
        assert "table" in lock.object_label

    def test_object_label_index(self):
        lock = self._make(relname="idx_orders_user", relkind="i")
        assert "index" in lock.object_label

    def test_object_label_non_relation(self):
        lock = LockEntry(locktype="advisory", mode="ExclusiveLock",
                         relation_name=None, schema_name=None, relkind=None)
        assert "advisory" in lock.object_label

    def test_mode_description_present(self):
        lock = self._make("AccessExclusiveLock")
        assert "blocks" in lock.mode_description.lower() or "exclusive" in lock.mode_description.lower()


# ---------------------------------------------------------------------------
# StatementLockInfo
# ---------------------------------------------------------------------------

class TestStatementLockInfo:
    def _make_lock(self, mode):
        return LockEntry("relation", mode, "t", "public", "r")

    def test_notable_locks_filtered(self):
        info = StatementLockInfo(
            statement="ALTER TABLE t ADD COLUMN x integer;",
            locks=[
                self._make_lock("RowExclusiveLock"),
                self._make_lock("AccessExclusiveLock"),
            ]
        )
        assert len(info.notable_locks) == 1
        assert info.notable_locks[0].mode == "AccessExclusiveLock"

    def test_notable_locks_empty_when_none(self):
        info = StatementLockInfo(
            statement="SELECT 1;",
            locks=[self._make_lock("AccessShareLock")]
        )
        assert info.notable_locks == []

    def test_max_severity_no_locks(self):
        info = StatementLockInfo(statement="SELECT 1;", locks=[])
        assert info.max_severity == -1

    def test_max_severity_with_locks(self):
        info = StatementLockInfo(
            statement="ALTER TABLE t ADD COLUMN x int;",
            locks=[
                self._make_lock("RowExclusiveLock"),
                self._make_lock("AccessExclusiveLock"),
            ]
        )
        assert info.max_severity == LOCK_SEVERITY_INDEX["AccessExclusiveLock"]

    def test_error_stored(self):
        info = StatementLockInfo(statement="BOGUS;", error="syntax error")
        assert info.error == "syntax error"


# ---------------------------------------------------------------------------
# format_results
# ---------------------------------------------------------------------------

class TestFormatResults:
    def _make_lock(self, mode, relname="users"):
        return LockEntry("relation", mode, relname, "public", "r")

    def test_empty_results(self):
        output = format_results([])
        assert "No notable locks" in output

    def test_no_locks_statement(self):
        results = [StatementLockInfo(statement="SELECT 1;", locks=[])]
        output = format_results(results)
        assert "no user-object locks" in output

    def test_notable_lock_shows_warning(self):
        results = [StatementLockInfo(
            statement="ALTER TABLE users ADD COLUMN x integer;",
            locks=[self._make_lock("AccessExclusiveLock")]
        )]
        output = format_results(results)
        assert "⚠" in output
        assert "AccessExclusiveLock" in output

    def test_no_notable_locks_shows_checkmark(self):
        results = [StatementLockInfo(
            statement="INSERT INTO t VALUES (1);",
            locks=[self._make_lock("RowExclusiveLock")]
        )]
        output = format_results(results)
        assert "✓" in output

    def test_error_shown(self):
        results = [StatementLockInfo(statement="BOGUS SQL;", error="syntax error at BOGUS")]
        output = format_results(results)
        assert "ERROR" in output
        assert "syntax error" in output

    def test_statement_numbered(self):
        results = [
            StatementLockInfo(statement="SELECT 1;", locks=[]),
            StatementLockInfo(statement="SELECT 2;", locks=[]),
        ]
        output = format_results(results)
        assert "[1]" in output
        assert "[2]" in output

    def test_verbose_shows_description(self):
        results = [StatementLockInfo(
            statement="ALTER TABLE t ADD COLUMN x int;",
            locks=[self._make_lock("AccessExclusiveLock")]
        )]
        output = format_results(results, verbose=True)
        # verbose mode adds a description line in parens
        assert "(" in output and ")" in output

    def test_summary_lists_notable_statements(self):
        results = [
            StatementLockInfo("SELECT 1;", locks=[self._make_lock("AccessShareLock")]),
            StatementLockInfo("ALTER TABLE t ADD COLUMN x int;",
                              locks=[self._make_lock("AccessExclusiveLock")]),
        ]
        output = format_results(results)
        assert "1 statement" in output
        assert "AccessExclusiveLock" in output

    def test_multiple_objects_grouped(self):
        results = [StatementLockInfo(
            statement="REINDEX TABLE t;",
            locks=[
                self._make_lock("AccessExclusiveLock", "users"),
                self._make_lock("AccessExclusiveLock", "idx_users"),
            ]
        )]
        output = format_results(results)
        assert "users" in output
        assert "idx_users" in output
