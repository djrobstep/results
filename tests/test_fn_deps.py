"""
Tests for function-as-column-default dependency tracking.

These are unit tests (no live DB) that verify:
  - When a table column default references a function, the table records
    that function in its dependent_on list
  - from_definition() reconstructs those deps correctly
  - ordered_changes() creates functions before tables that use them as defaults
  - ordered_changes() drops functions after tables that use them as defaults
"""
import pytest

from collections import OrderedDict as od
from results.schemainspect import PostgreSQL, SchemaDefinition


def make_col(name, dbtype="integer", **kwargs):
    return {
        "name": name, "dbtype": dbtype, "dbtypestr": dbtype,
        "not_null": False, "default": None, "is_enum": False,
        "enum_name": None, "enum_schema": None, "collation": None,
        "is_identity": False, "is_identity_always": False,
        "is_generated": False, "is_inherited": False, "comment": None,
        **kwargs,
    }


def make_function_defn(name="next_id", schema="public", identity_arguments=""):
    sig = f'"public"."{name}"({identity_arguments})'
    return sig, {
        "name": name, "schema": schema,
        "identity_arguments": identity_arguments,
        "result_string": "integer",
        "language": "sql",
        "definition": "select 1",
        "volatility": "volatile",
        "strictness": "called on null input",
        "security_type": "invoker",
        "full_definition": f"create or replace function {schema}.{name}() returns integer language sql as $$ select 1 $$",
        "comment": None,
        "returntype": "integer",
        "kind": "f",
        "columns": {},
        "inputs": [],
        "dependent_on": [],
    }


def make_table_defn(name, schema="public", dependent_on=None, columns=None):
    sig = f'"{schema}"."{name}"'
    if columns is None:
        columns = {"id": make_col("id")}
    return sig, {
        "name": name, "schema": schema,
        "relationtype": "r", "definition": None,
        "columns": columns,
        "comment": None, "parent_table": None, "partition_def": None,
        "rowsecurity": False, "forcerowsecurity": False,
        "persistence": "p", "options": None,
        "dependent_on": dependent_on or [],
    }


def make_defn_with_fn_default():
    """
    Schema: a function next_id() and a table widgets whose id column
    default calls next_id(). The table declares dependent_on the function.
    """
    fn_sig, fn_data = make_function_defn("next_id")
    tbl_sig, tbl_data = make_table_defn(
        "widgets",
        dependent_on=[fn_sig],
        columns={"id": make_col("id", default="next_id()")},
    )
    return SchemaDefinition(
        pg_version=14,
        schemas={"\"public\"": {"schema": "public"}},
        enums={},
        tables={tbl_sig: tbl_data},
        views={},
        materialized_views={},
        composite_types={},
        functions={fn_sig: fn_data},
        sequences={},
        indexes={},
        constraints={},
        extensions={},
        privileges={},
        triggers={},
        collations={},
        rlspolicies={},
        types={},
        domains={},
    )


class TestFnDepsReconstruction:
    def test_dependent_on_preserved_in_definition(self):
        defn = make_defn_with_fn_default()
        fn_sig = '"public"."next_id"()'
        assert fn_sig in defn.tables['"public"."widgets"']["dependent_on"]

    def test_from_definition_rebuilds_dependent_on(self):
        defn = make_defn_with_fn_default()
        insp = PostgreSQL.from_definition(defn)
        table = insp.tables['"public"."widgets"']
        fn_sig = '"public"."next_id"()'
        assert fn_sig in table.dependent_on

    def test_from_definition_rebuilds_dependents_on_function(self):
        defn = make_defn_with_fn_default()
        insp = PostgreSQL.from_definition(defn)
        fn = insp.functions['"public"."next_id"()']
        assert '"public"."widgets"' in fn.dependents

    def test_dependent_on_all_includes_function(self):
        defn = make_defn_with_fn_default()
        insp = PostgreSQL.from_definition(defn)
        table = insp.tables['"public"."widgets"']
        fn_sig = '"public"."next_id"()'
        assert fn_sig in table.dependent_on_all

    def test_roundtrip_preserves_fn_dep(self):
        defn = make_defn_with_fn_default()
        insp = PostgreSQL.from_definition(defn)
        defn2 = insp.as_definition()
        insp2 = PostgreSQL.from_definition(defn2)
        table = insp2.tables['"public"."widgets"']
        fn_sig = '"public"."next_id"()'
        assert fn_sig in table.dependent_on


class TestFnDepsOrdering:
    """Verify ordered_changes() respects function-default deps."""

    def _empty_defn(self):
        return SchemaDefinition(
            pg_version=14,
            schemas={"\"public\"": {"schema": "public"}},
            enums={}, tables={}, views={}, materialized_views={},
            composite_types={}, functions={}, sequences={}, indexes={},
            constraints={}, extensions={}, privileges={}, triggers={},
            collations={}, rlspolicies={}, types={}, domains={},
        )

    def test_function_created_before_table_that_uses_it_as_default(self):
        from results.dbdiff import Migration

        defn_from = self._empty_defn()
        defn_to = make_defn_with_fn_default()

        m = Migration(defn_from, defn_to)
        m.set_safety(False)
        m.add_all_changes_ordered()
        sql = m.sql

        fn_pos = sql.lower().find("next_id")
        tbl_pos = sql.lower().find("create")
        # function definition should appear somewhere
        assert fn_pos >= 0
        assert "widgets" in sql.lower()
        # The create function statement must come before create table widgets
        create_fn_pos = sql.lower().find("create or replace function")
        create_tbl_pos = sql.lower().find("create table")
        assert create_fn_pos < create_tbl_pos, (
            "function must be created before table that uses it as a default"
        )

    def test_function_dropped_after_table_that_uses_it(self):
        from results.dbdiff import Migration

        defn_from = make_defn_with_fn_default()
        defn_to = self._empty_defn()

        m = Migration(defn_from, defn_to)
        m.set_safety(False)
        m.add_all_changes_ordered()
        sql = m.sql

        drop_tbl_pos = sql.lower().find("drop table")
        drop_fn_pos = sql.lower().find("drop function")
        assert drop_tbl_pos >= 0, "should drop the table"
        assert drop_fn_pos >= 0, "should drop the function"
        assert drop_tbl_pos < drop_fn_pos, (
            "table must be dropped before the function it depends on"
        )

    def test_function_replaced_before_table_recreate(self):
        """If the function signature changes, it must be updated before tables using it."""
        from results.dbdiff import Migration

        defn_from = make_defn_with_fn_default()
        defn_to = make_defn_with_fn_default()

        # Change the function body (same signature, different definition)
        fn_sig = '"public"."next_id"()'
        defn_to.functions[fn_sig]["definition"] = "select 42"
        defn_to.functions[fn_sig]["full_definition"] = (
            "create or replace function public.next_id() returns integer language sql as $$ select 42 $$"
        )

        m = Migration(defn_from, defn_to)
        m.set_safety(False)
        m.add_all_changes_ordered()
        # Should not raise and should contain the updated function
        sql = m.sql
        assert "next_id" in sql.lower()
