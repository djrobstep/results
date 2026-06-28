"""
Tests for SchemaDefinition serialization and the from_definition() constructor.

These tests are purely unit tests — no live DB required.
They verify that:
  - SchemaDefinition round-trips cleanly through JSON and YAML
  - PostgreSQL.from_definition() reconstructs inspected objects faithfully
  - The reconstructed inspector can be diffed via Migration
  - add_all_changes_ordered() produces the same logical changes as add_all_changes()
    and in a valid dependency order
"""
import json

import pytest

from results.schemainspect import PostgreSQL, SchemaDefinition
from results.schemainspect.pg.obj import (
    InspectedConstraint,
    InspectedEnum,
    InspectedExtension,
    InspectedFunction,
    InspectedIndex,
    InspectedSchema,
    InspectedSelectable,
    InspectedSequence,
    InspectedTrigger,
    InspectedType,
)
from results.schemainspect.inspected import ColumnInfo
from collections import OrderedDict as od


# ---------------------------------------------------------------------------
# Fixtures: minimal hand-built SchemaDefinition and PostgreSQL objects
# ---------------------------------------------------------------------------

def make_simple_definition(pg_version=14):
    """A minimal SchemaDefinition with one table, one view, one enum."""
    return SchemaDefinition(
        pg_version=pg_version,
        schemas={"\"public\"": {"schema": "public"}},
        enums={
            "\"public\".\"status\"": {
                "name": "status", "schema": "public",
                "elements": ["active", "inactive"],
            }
        },
        tables={
            "\"public\".\"users\"": {
                "name": "users", "schema": "public",
                "relationtype": "r",
                "definition": None,
                "columns": {
                    "id": {
                        "name": "id", "dbtype": "integer", "dbtypestr": "integer",
                        "not_null": True, "default": None, "is_enum": False,
                        "enum_name": None, "enum_schema": None, "collation": None,
                        "is_identity": False, "is_identity_always": False,
                        "is_generated": False, "is_inherited": False, "comment": None,
                    },
                    "email": {
                        "name": "email", "dbtype": "text", "dbtypestr": "text",
                        "not_null": True, "default": None, "is_enum": False,
                        "enum_name": None, "enum_schema": None, "collation": None,
                        "is_identity": False, "is_identity_always": False,
                        "is_generated": False, "is_inherited": False, "comment": None,
                    },
                },
                "comment": None, "parent_table": None, "partition_def": None,
                "rowsecurity": False, "forcerowsecurity": False,
                "persistence": "p", "options": None, "dependent_on": [],
            }
        },
        views={
            "\"public\".\"active_users\"": {
                "name": "active_users", "schema": "public",
                "relationtype": "v",
                "definition": "SELECT id, email FROM users WHERE true",
                "columns": {},
                "comment": None, "parent_table": None, "partition_def": None,
                "rowsecurity": False, "forcerowsecurity": False,
                "persistence": None, "options": None,
                "dependent_on": ["\"public\".\"users\""],
            }
        },
        functions={},
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
        materialized_views={},
        composite_types={},
    )


# ---------------------------------------------------------------------------
# SchemaDefinition round-trip tests
# ---------------------------------------------------------------------------

class TestSchemaDefinitionRoundTrip:
    def test_to_dict_from_dict(self):
        defn = make_simple_definition()
        d = defn.to_dict()
        defn2 = SchemaDefinition.from_dict(d)
        assert defn == defn2

    def test_json_roundtrip(self):
        defn = make_simple_definition()
        s = defn.to_json()
        defn2 = SchemaDefinition.from_json(s)
        assert defn == defn2

    def test_yaml_roundtrip(self):
        pytest.importorskip("yaml")
        defn = make_simple_definition()
        s = defn.to_yaml()
        defn2 = SchemaDefinition.from_yaml(s)
        assert defn == defn2

    def test_empty_definition_roundtrip(self):
        defn = SchemaDefinition(pg_version=15)
        assert SchemaDefinition.from_dict(defn.to_dict()) == defn

    def test_pg_version_preserved(self):
        defn = make_simple_definition(pg_version=16)
        assert SchemaDefinition.from_json(defn.to_json()).pg_version == 16

    def test_json_is_valid_json(self):
        defn = make_simple_definition()
        parsed = json.loads(defn.to_json())
        assert "pg_version" in parsed
        assert "tables" in parsed


# ---------------------------------------------------------------------------
# PostgreSQL.from_definition() reconstruction tests
# ---------------------------------------------------------------------------

class TestFromDefinition:
    def setup_method(self):
        self.defn = make_simple_definition()
        self.insp = PostgreSQL.from_definition(self.defn)

    def test_pg_version(self):
        assert self.insp.pg_version == 14

    def test_schemas(self):
        assert "\"public\"" in self.insp.schemas

    def test_enums(self):
        assert "\"public\".\"status\"" in self.insp.enums
        e = self.insp.enums["\"public\".\"status\""]
        assert e.elements == ["active", "inactive"]

    def test_tables(self):
        assert "\"public\".\"users\"" in self.insp.tables
        t = self.insp.tables["\"public\".\"users\""]
        assert "id" in t.columns
        assert "email" in t.columns

    def test_views(self):
        assert "\"public\".\"active_users\"" in self.insp.views

    def test_view_dependency(self):
        view = self.insp.views["\"public\".\"active_users\""]
        assert "\"public\".\"users\"" in view.dependent_on

    def test_view_in_selectables(self):
        assert "\"public\".\"active_users\"" in self.insp.selectables

    def test_table_in_relations(self):
        assert "\"public\".\"users\"" in self.insp.relations

    def test_column_properties(self):
        col = self.insp.tables["\"public\".\"users\""].columns["id"]
        assert col.dbtype == "integer"
        assert col.not_null is True

    def test_from_definition_accepts_schemadefn_instance(self):
        insp = PostgreSQL.from_definition(self.defn)
        assert "\"public\".\"users\"" in insp.tables

    def test_from_definition_accepts_dict(self):
        insp = PostgreSQL.from_definition(self.defn.to_dict())
        assert "\"public\".\"users\"" in insp.tables


# ---------------------------------------------------------------------------
# Round-trip: PostgreSQL -> SchemaDefinition -> PostgreSQL
# (structural equality, not identity)
# ---------------------------------------------------------------------------

class TestDefinitionRoundTripViaInspector:
    def test_tables_survive_roundtrip(self):
        defn = make_simple_definition()
        insp = PostgreSQL.from_definition(defn)
        defn2 = insp.as_definition()
        insp2 = PostgreSQL.from_definition(defn2)
        assert set(insp2.tables.keys()) == set(insp.tables.keys())

    def test_enums_survive_roundtrip(self):
        defn = make_simple_definition()
        insp = PostgreSQL.from_definition(defn)
        defn2 = insp.as_definition()
        insp2 = PostgreSQL.from_definition(defn2)
        assert set(insp2.enums.keys()) == set(insp.enums.keys())


# ---------------------------------------------------------------------------
# Migration with SchemaDefinition inputs
# ---------------------------------------------------------------------------

class TestMigrationFromDefinition:
    def test_identical_definitions_produce_no_changes(self):
        from results.dbdiff import Migration
        defn = make_simple_definition()
        m = Migration(defn, defn)
        m.set_safety(False)
        m.add_all_changes_ordered()
        assert not m.statements

    def test_identical_definitions_produce_no_changes_deprecated_path(self):
        from results.dbdiff import Migration
        defn = make_simple_definition()
        m = Migration(defn, defn)
        m.set_safety(False)
        m.add_all_changes()  # deprecated — kept to verify the old path still works
        assert not m.statements

    def test_added_table_shows_up_in_diff(self):
        from results.dbdiff import Migration

        defn_from = make_simple_definition()

        defn_to = make_simple_definition()
        defn_to.tables["\"public\".\"posts\""] = {
            "name": "posts", "schema": "public",
            "relationtype": "r", "definition": None,
            "columns": {
                "id": {
                    "name": "id", "dbtype": "integer", "dbtypestr": "integer",
                    "not_null": True, "default": None, "is_enum": False,
                    "enum_name": None, "enum_schema": None, "collation": None,
                    "is_identity": False, "is_identity_always": False,
                    "is_generated": False, "is_inherited": False, "comment": None,
                }
            },
            "comment": None, "parent_table": None, "partition_def": None,
            "rowsecurity": False, "forcerowsecurity": False,
            "persistence": "p", "options": None, "dependent_on": [],
        }

        m = Migration(defn_from, defn_to)
        m.set_safety(False)
        m.add_all_changes_ordered()
        sql = m.sql
        assert "create" in sql.lower()
        assert "posts" in sql.lower()

    def test_dropped_table_shows_up_in_diff(self):
        from results.dbdiff import Migration

        defn_from = make_simple_definition()
        defn_to = make_simple_definition()
        del defn_to.tables["\"public\".\"users\""]
        # also remove the view that depends on it
        del defn_to.views["\"public\".\"active_users\""]

        m = Migration(defn_from, defn_to)
        m.set_safety(False)
        m.add_all_changes_ordered()
        sql = m.sql
        assert "drop" in sql.lower()

    def test_added_column_shows_up_in_diff(self):
        from results.dbdiff import Migration

        defn_from = make_simple_definition()
        defn_to = make_simple_definition()
        defn_to.tables["\"public\".\"users\""]["columns"]["name"] = {
            "name": "name", "dbtype": "text", "dbtypestr": "text",
            "not_null": False, "default": None, "is_enum": False,
            "enum_name": None, "enum_schema": None, "collation": None,
            "is_identity": False, "is_identity_always": False,
            "is_generated": False, "is_inherited": False, "comment": None,
        }
        m = Migration(defn_from, defn_to)
        m.set_safety(False)
        m.add_all_changes_ordered()
        sql = m.sql
        assert "add column" in sql.lower()
        assert "name" in sql.lower()


# ---------------------------------------------------------------------------
# add_all_changes_ordered(): smoke tests
# ---------------------------------------------------------------------------

class TestOrderedChanges:
    def test_ordered_identical_produces_no_changes(self):
        from results.dbdiff import Migration
        defn = make_simple_definition()
        m = Migration(defn, defn)
        m.set_safety(False)
        m.add_all_changes_ordered()
        assert not m.statements

    def test_ordered_added_table(self):
        from results.dbdiff import Migration

        defn_from = make_simple_definition()
        defn_to = make_simple_definition()
        defn_to.tables["\"public\".\"posts\""] = {
            "name": "posts", "schema": "public",
            "relationtype": "r", "definition": None,
            "columns": {
                "id": {
                    "name": "id", "dbtype": "integer", "dbtypestr": "integer",
                    "not_null": True, "default": None, "is_enum": False,
                    "enum_name": None, "enum_schema": None, "collation": None,
                    "is_identity": False, "is_identity_always": False,
                    "is_generated": False, "is_inherited": False, "comment": None,
                }
            },
            "comment": None, "parent_table": None, "partition_def": None,
            "rowsecurity": False, "forcerowsecurity": False,
            "persistence": "p", "options": None, "dependent_on": [],
        }

        m = Migration(defn_from, defn_to)
        m.set_safety(False)
        m.add_all_changes_ordered()
        sql = m.sql
        assert "posts" in sql.lower()

    def test_ordered_same_statements_as_unordered_for_simple_case(self):
        """For a simple diff with no cross-category deps, both methods should produce equivalent SQL."""
        from results.dbdiff import Migration

        defn_from = make_simple_definition()
        defn_to = make_simple_definition()
        defn_to.tables["\"public\".\"posts\""] = {
            "name": "posts", "schema": "public",
            "relationtype": "r", "definition": None,
            "columns": {
                "id": {
                    "name": "id", "dbtype": "integer", "dbtypestr": "integer",
                    "not_null": True, "default": None, "is_enum": False,
                    "enum_name": None, "enum_schema": None, "collation": None,
                    "is_identity": False, "is_identity_always": False,
                    "is_generated": False, "is_inherited": False, "comment": None,
                }
            },
            "comment": None, "parent_table": None, "partition_def": None,
            "rowsecurity": False, "forcerowsecurity": False,
            "persistence": "p", "options": None, "dependent_on": [],
        }

        m1 = Migration(defn_from, defn_to)
        m1.set_safety(False)
        m1.add_all_changes()

        m2 = Migration(defn_from, defn_to)
        m2.set_safety(False)
        m2.add_all_changes_ordered()

        # Both should contain the same statements (possibly in different order)
        assert set(m1.statements) == set(m2.statements)

    def test_view_drop_comes_before_table_drop(self):
        """When a table and its dependent view are both dropped, view drop must come first."""
        from results.dbdiff import Migration

        defn_from = make_simple_definition()
        defn_to = make_simple_definition()
        # Remove both table and its dependent view
        del defn_to.tables["\"public\".\"users\""]
        del defn_to.views["\"public\".\"active_users\""]

        m = Migration(defn_from, defn_to)
        m.set_safety(False)
        m.add_all_changes_ordered()
        sql = m.sql

        # Both drops should appear
        assert "drop" in sql.lower()
        # View drop should appear before table drop
        view_pos = sql.lower().find("active_users")
        table_pos = sql.lower().find("drop table")
        assert view_pos < table_pos, "view drop should precede table drop"


# ---------------------------------------------------------------------------
# SchemaDefinition as the source side of a diff
# ---------------------------------------------------------------------------

class TestDefinitionAsDiffSource:
    def test_definition_schemadiff_as_sql_identical(self):
        defn = make_simple_definition()
        assert defn.schemadiff_as_sql(defn) == ""

    def test_definition_schemadiff_as_sql_added_table(self):
        defn_from = make_simple_definition()
        defn_to = make_simple_definition()
        defn_to.tables['"public"."logs"'] = {
            "name": "logs", "schema": "public", "relationtype": "r",
            "definition": None,
            "columns": {"id": {
                "name": "id", "dbtype": "integer", "dbtypestr": "integer",
                "not_null": True, "default": None, "is_enum": False,
                "enum_name": None, "enum_schema": None, "collation": None,
                "is_identity": False, "is_identity_always": False,
                "is_generated": False, "is_inherited": False, "comment": None,
            }},
            "comment": None, "parent_table": None, "partition_def": None,
            "rowsecurity": False, "forcerowsecurity": False,
            "persistence": "p", "options": None, "dependent_on": [],
        }
        sql = defn_from.schemadiff_as_sql(defn_to)
        assert "logs" in sql.lower()
        assert "create" in sql.lower()

    def test_definition_schemadiff_as_statements_returns_statements(self):
        from results.dbdiff.statements import Statements
        defn = make_simple_definition()
        result = defn.schemadiff_as_statements(defn)
        assert isinstance(result, Statements)
        assert not result  # identical — no changes
