"""
General-coverage tests for the core diff and inspection logic.
No live DB required — all tests use hand-constructed objects.
"""
import pytest
from collections import OrderedDict as od

from results.schemainspect.inspected import ColumnInfo
from results.schemainspect.pg.obj import (
    InspectedCollation,
    InspectedConstraint,
    InspectedDomain,
    InspectedEnum,
    InspectedExtension,
    InspectedFunction,
    InspectedIndex,
    InspectedRowPolicy,
    InspectedSchema,
    InspectedSelectable,
    InspectedSequence,
    InspectedTrigger,
    InspectedType,
)
from results.dbdiff.util import differences
from results.dbdiff.changes import (
    statements_from_differences,
    get_enum_modifications,
    get_table_changes,
)
from results.dbdiff.statements import Statements


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_col(name="id", dbtype="integer", **kwargs):
    defaults = dict(
        dbtypestr=dbtype, pytype=None, default=None, not_null=False,
        is_enum=False, enum=None, collation=None, is_identity=False,
        is_identity_always=False, is_generated=False, is_inherited=False,
        can_drop_generated=True, can_set_expression=False, comment=None,
    )
    defaults.update(kwargs)
    return ColumnInfo(name=name, dbtype=dbtype, **defaults)


def make_table(name, schema="public", columns=None, **kwargs):
    cols = columns or od([("id", make_col("id"))])
    return InspectedSelectable(
        name=name, schema=schema, columns=cols, relationtype="r", **kwargs
    )


def make_view(name, schema="public", definition="select 1", dependent_on=None, **kwargs):
    return InspectedSelectable(
        name=name, schema=schema, columns=od(), relationtype="v",
        definition=definition, dependent_on=dependent_on or [], **kwargs
    )


def make_enum(name, elements, schema="public"):
    return InspectedEnum(name=name, schema=schema, elements=elements, pg_version=14)


def make_function(name, schema="public", identity_arguments="", definition="select 1",
                  volatility="volatile", strictness="called on null input",
                  security_type="invoker", kind="f", comment=None):
    return InspectedFunction(
        name=name, schema=schema,
        columns=od(), inputs=[],
        identity_arguments=identity_arguments,
        result_string="integer",
        language="sql",
        definition=definition,
        volatility=volatility,
        strictness=strictness,
        security_type=security_type,
        full_definition=f"create or replace function {schema}.{name}() returns integer language sql as $$ {definition} $$",
        comment=comment,
        returntype="integer",
        kind=kind,
    )


# ---------------------------------------------------------------------------
# differences()
# ---------------------------------------------------------------------------

class TestDifferences:
    def test_four_way_split(self):
        a_obj = make_table("t")
        b_obj = make_table("t", comment="changed")
        c_obj = make_table("c")
        x_obj = make_table("x")

        a = od([("a", a_obj), ("b", a_obj)])
        b = od([("b", b_obj), ("c", c_obj)])

        added, removed, modified, unmodified = differences(a, b)
        assert set(added) == {"c"}
        assert set(removed) == {"a"}
        assert set(modified) == {"b"}
        assert set(unmodified) == set()

    def test_identical_dicts_all_unmodified(self):
        t = make_table("t")
        d = od([("x", t), ("y", t)])
        added, removed, modified, unmodified = differences(d, d)
        assert not added
        assert not removed
        assert not modified
        assert set(unmodified) == {"x", "y"}

    def test_results_are_sorted(self):
        objs = {k: make_table(k) for k in ["c", "a", "b"]}
        a = od()
        b = od(objs)
        added, _, _, _ = differences(a, b)
        assert list(added.keys()) == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# ColumnInfo
# ---------------------------------------------------------------------------

class TestColumnInfo:
    def test_eq_comment_difference_makes_unequal(self):
        a = make_col(comment="hello")
        b = make_col(comment="world")
        assert a != b

    def test_eq_comment_none_vs_set(self):
        a = make_col(comment=None)
        b = make_col(comment="x")
        assert a != b

    def test_eq_is_inherited_difference(self):
        a = make_col(is_inherited=False)
        b = make_col(is_inherited=True)
        assert a != b

    def test_eq_identical(self):
        a = make_col(not_null=True, default="42")
        b = make_col(not_null=True, default="42")
        assert a == b

    def test_creation_clause_basic(self):
        c = make_col("email", "text", not_null=True)
        assert '"email" text not null' in c.creation_clause

    def test_creation_clause_default(self):
        c = make_col("n", "integer", default="0")
        assert "default 0" in c.creation_clause

    def test_creation_clause_generated(self):
        c = make_col("total", "integer", is_generated=True, default="a + b")
        clause = c.creation_clause
        assert "generated always as (a + b) stored" in clause
        assert "default" not in clause

    def test_creation_clause_identity(self):
        c = make_col("id", "integer", is_identity=True, is_identity_always=True)
        assert "generated always as identity" in c.creation_clause

    def test_alter_clauses_only_comment_changed_returns_empty(self):
        a = make_col(not_null=True)
        b = make_col(not_null=True, comment="added comment")
        # only_comment_changed should be True, alter_clauses should be empty
        assert b.only_comment_changed(a)
        assert b.alter_clauses(a) == []

    def test_alter_clauses_notnull_then_identity_order(self):
        # not_null added should appear before identity change in clause list
        old = make_col(not_null=False, is_identity=False)
        new = make_col(not_null=True, is_identity=True, is_identity_always=True)
        clauses = new.alter_clauses(old)
        notnull_idx = next(i for i, c in enumerate(clauses) if "not null" in c)
        identity_idx = next(i for i, c in enumerate(clauses) if "identity" in c)
        assert notnull_idx < identity_idx

    def test_comment_statement(self):
        c = make_col(comment="my comment")
        stmt = c.comment_statement('"public"."t"')
        assert "COMMENT ON COLUMN" in stmt
        assert "my comment" in stmt

    def test_comment_statement_none_returns_none(self):
        c = make_col(comment=None)
        assert c.comment_statement('"public"."t"') is None


# ---------------------------------------------------------------------------
# InspectedEnum
# ---------------------------------------------------------------------------

class TestInspectedEnum:
    def test_create_statement(self):
        e = make_enum("status", ["active", "inactive"])
        stmt = e.create_statement
        assert "create type" in stmt
        assert '"public"."status"' in stmt
        assert "'active'" in stmt
        assert "'inactive'" in stmt

    def test_drop_statement(self):
        e = make_enum("status", ["active"])
        assert '"public"."status"' in e.drop_statement

    def test_eq(self):
        assert make_enum("s", ["a", "b"]) == make_enum("s", ["a", "b"])
        assert make_enum("s", ["a", "b"]) != make_enum("s", ["a", "c"])

    def test_can_be_changed_to_additive(self):
        old = make_enum("s", ["a", "b"])
        new = make_enum("s", ["a", "c", "b"])
        assert old.can_be_changed_to(new)

    def test_can_be_changed_to_reorder_forbidden(self):
        old = make_enum("s", ["a", "b"])
        new = make_enum("s", ["b", "a"])
        assert not old.can_be_changed_to(new)

    def test_can_be_changed_to_removal_forbidden(self):
        old = make_enum("s", ["a", "b"])
        new = make_enum("s", ["a"])
        assert not old.can_be_changed_to(new)

    def test_change_statements_insert_middle(self):
        old = make_enum("s", ["a", "b"])
        new = make_enum("s", ["a", "c", "b"])
        stmts = old.change_statements(new)
        assert len(stmts) == 1
        assert "add value 'c' after 'a'" in stmts[0]

    def test_change_statements_insert_at_start(self):
        old = make_enum("s", ["a", "b"])
        new = make_enum("s", ["z", "a", "b"])
        stmts = old.change_statements(new)
        assert "add value 'z' before 'a'" in stmts[0]


# ---------------------------------------------------------------------------
# InspectedSelectable (tables and views)
# ---------------------------------------------------------------------------

class TestInspectedSelectable:
    @pytest.mark.parametrize("reltype,expected", [
        ("r", "drop table"),
        ("v", "drop view if exists"),
        ("m", "drop materialized view if exists"),
        ("c", "drop type"),
    ])
    def test_drop_statement_by_relationtype(self, reltype, expected):
        s = InspectedSelectable(
            name="t", schema="public", columns=od(), relationtype=reltype
        )
        assert expected in s.drop_statement

    def test_create_table_statement(self):
        cols = od([("id", make_col("id", not_null=True))])
        t = make_table("users", columns=cols)
        stmt = t.create_statement
        assert "create" in stmt
        assert "users" in stmt
        assert "id" in stmt

    def test_create_view_statement(self):
        v = make_view("v", definition="select 1")
        assert "create or replace view" in v.create_statement
        assert "select 1" in v.create_statement

    def test_can_replace_view_compatible_columns(self):
        v1 = make_view("v", definition="select 1 as x")
        v2 = make_view("v", definition="select 2 as x")
        # both have no columns — compatible
        assert v1.can_replace(v2)

    def test_can_replace_different_relationtype(self):
        v = make_view("v")
        t = make_table("v")
        assert not v.can_replace(t)

    def test_eq_comment_difference(self):
        t1 = make_table("t")
        t2 = make_table("t", comment="hi")
        assert t1 != t2

    def test_is_table(self):
        assert make_table("t").is_table
        assert not make_view("v").is_table


# ---------------------------------------------------------------------------
# InspectedConstraint
# ---------------------------------------------------------------------------

class TestInspectedConstraint:
    def _make_fk(self, **kwargs):
        c = InspectedConstraint(
            name="fk_post_user", schema="public",
            constraint_type="FOREIGN KEY", table_name="posts",
            definition=None, index=None, is_fk=True,
            is_deferrable=False, initially_deferred=False, is_not_valid=False,
        )
        c.quoted_full_foreign_table_name = '"other"."users"'
        c.fk_columns_local = ["user_id"]
        c.fk_columns_foreign = ["id"]
        c.fk_on_delete = "NO ACTION"
        c.fk_on_update = "NO ACTION"
        for k, v in kwargs.items():
            setattr(c, k, v)
        return c

    def test_fk_create_statement_schema_qualified(self):
        c = self._make_fk()
        stmt = c.create_statement
        assert '"other"."users"' in stmt, "FK must reference schema-qualified table"
        assert "FOREIGN KEY" in stmt

    def test_fk_on_delete_included(self):
        c = self._make_fk(fk_on_delete="CASCADE")
        assert "ON DELETE CASCADE" in c.create_statement

    def test_safer_create_statements_not_valid_then_validate(self):
        c = InspectedConstraint(
            name="chk", schema="public", constraint_type="CHECK",
            table_name="t", definition="CHECK (x > 0)", index=None,
            is_fk=False, is_deferrable=False, initially_deferred=False,
            is_not_valid=False,
        )
        stmts = c.safer_create_statements
        assert len(stmts) == 2
        assert "not valid" in stmts[0].lower()
        assert "validate" in stmts[1].lower()

    def test_safer_create_statements_already_not_valid(self):
        c = InspectedConstraint(
            name="chk", schema="public", constraint_type="CHECK",
            table_name="t", definition="CHECK (x > 0)", index=None,
            is_fk=False, is_deferrable=False, initially_deferred=False,
            is_not_valid=True,
        )
        stmts = c.safer_create_statements
        assert len(stmts) == 1

    def test_eq(self):
        def make():
            return InspectedConstraint(
                name="c", schema="public", constraint_type="CHECK",
                table_name="t", definition="CHECK (x > 0)", index=None,
            )
        assert make() == make()
        c = make()
        c.definition = "CHECK (x > 1)"
        assert make() != c


# ---------------------------------------------------------------------------
# InspectedFunction
# ---------------------------------------------------------------------------

class TestInspectedFunction:
    def test_eq_same(self):
        assert make_function("f") == make_function("f")

    def test_eq_volatility_differs(self):
        a = make_function("f", volatility="volatile")
        b = make_function("f", volatility="stable")
        assert a != b

    def test_eq_comment_differs(self):
        a = make_function("f", comment=None)
        b = make_function("f", comment="docs")
        assert a != b

    def test_drop_statement(self):
        f = make_function("my_func")
        assert "drop function if exists" in f.drop_statement
        assert "my_func" in f.drop_statement

    def test_signature_includes_args(self):
        f = make_function("f", identity_arguments="integer, text")
        assert "(integer, text)" in f.signature


# ---------------------------------------------------------------------------
# InspectedRowPolicy — includes bug-fix regression test
# ---------------------------------------------------------------------------

class TestInspectedRowPolicy:
    def _make(self, name="pol", **kwargs):
        defaults = dict(
            schema="public", table_name="t", commandtype="*",
            permissive=True, roles=["public"], qual=None, withcheck=None,
        )
        defaults.update(kwargs)
        return InspectedRowPolicy(name=name, **defaults)

    def test_eq_same(self):
        assert self._make() == self._make()

    def test_eq_different_name(self):
        # Regression: was `self.name == self.name` (always True)
        assert self._make("pol_a") != self._make("pol_b")

    def test_eq_different_qual(self):
        assert self._make(qual="x > 0") != self._make(qual="x > 1")

    def test_create_statement(self):
        p = self._make()
        stmt = p.create_statement
        assert "create policy" in stmt


# ---------------------------------------------------------------------------
# InspectedExtension / InspectedSequence / InspectedType
# ---------------------------------------------------------------------------

class TestMiscInspected:
    def test_extension_eq(self):
        a = InspectedExtension("pg_stat", "public", version="1.0")
        b = InspectedExtension("pg_stat", "public", version="1.0")
        assert a == b
        c = InspectedExtension("pg_stat", "public", version="2.0")
        assert a != c

    def test_extension_create_statement(self):
        e = InspectedExtension("pg_stat", "public", version="1.0")
        stmt = e.create_statement
        assert "create extension" in stmt
        assert "pg_stat" in stmt
        assert "version '1.0'" in stmt

    def test_sequence_eq(self):
        a = InspectedSequence("seq", "public", table_name="t", column_name="id")
        b = InspectedSequence("seq", "public", table_name="t", column_name="id")
        assert a == b
        c = InspectedSequence("seq", "public", table_name="other", column_name="id")
        assert a != c

    def test_type_create_statement(self):
        t = InspectedType("point", "public", {"x": "float8", "y": "float8"})
        stmt = t.create_statement
        assert "create type" in stmt
        assert "point" in stmt
        assert "float8" in stmt

    def test_type_eq_comment(self):
        a = InspectedType("t", "public", {"x": "int"}, comment=None)
        b = InspectedType("t", "public", {"x": "int"}, comment="hi")
        assert a != b


# ---------------------------------------------------------------------------
# statements_from_differences()
# ---------------------------------------------------------------------------

class TestStatementsFromDifferences:
    def _simple_obj(self, name, create="create x;", drop="drop x;"):
        """Minimal object duck-typed for the diff engine."""
        class Obj:
            dependent_on = []
            dependents = []
            def __init__(self):
                self.create_statement = create
                self.drop_statement = drop
        return Obj()

    def test_added_produces_create(self):
        obj = self._simple_obj("x", create="create x;")
        added = od([("x", obj)])
        stmts = statements_from_differences(added, od(), od())
        assert "create x;" in list(stmts)

    def test_removed_produces_drop(self):
        obj = self._simple_obj("x", drop="drop x;")
        removed = od([("x", obj)])
        stmts = statements_from_differences(od(), removed, od(), old=removed)
        assert "drop x;" in list(stmts)

    def test_creations_only_skips_drops(self):
        obj = self._simple_obj("x")
        removed = od([("x", obj)])
        stmts = statements_from_differences(od(), removed, od(), creations_only=True, old=removed)
        assert list(stmts) == []

    def test_drops_only_skips_creates(self):
        obj = self._simple_obj("x", create="create x;")
        added = od([("x", obj)])
        stmts = statements_from_differences(added, od(), od(), drops_only=True)
        assert list(stmts) == []

    def test_modifications_as_alters_calls_alter_statements(self):
        class AltObj:
            dependent_on = []
            dependents = []
            create_statement = "create x;"
            drop_statement = "drop x;"
            def alter_statements(self, other):
                return ["alter x;"]
        obj_old = AltObj()
        obj_new = AltObj()
        modified = od([("x", obj_new)])
        stmts = statements_from_differences(
            od(), od(), modified,
            modifications_as_alters=True,
            modifications_only=True,
            old=od([("x", obj_old)]),
        )
        assert "alter x;" in list(stmts)
        assert "create x;" not in list(stmts)
        assert "drop x;" not in list(stmts)

    def test_dependency_ordering_defers_drop(self):
        """B depends on A; when both are dropped, A must drop before B."""
        class Obj:
            create_statement = "create;"
            def __init__(self, name, deps=None, dependents=None):
                self.drop_statement = f"drop {name};"
                self.dependent_on = deps or []
                self.dependents = dependents or []

        a = Obj("a", dependents=["b"])
        b = Obj("b", deps=["a"])
        removed = od([("a", a), ("b", b)])
        stmts = list(statements_from_differences(
            od(), removed, od(),
            dependency_ordering=True,
            old=removed,
        ))
        assert stmts.index("drop b;") < stmts.index("drop a;")


# ---------------------------------------------------------------------------
# get_table_changes() — column add/remove
# ---------------------------------------------------------------------------

class TestGetTableChanges:
    def _run(self, tables_from, tables_target):
        empty = od()
        return get_table_changes(tables_from, tables_target, empty, empty, empty, empty)

    def test_add_column(self):
        t_before = make_table("t", columns=od([("id", make_col("id"))]))
        t_after = make_table("t", columns=od([
            ("id", make_col("id")),
            ("name", make_col("name", "text")),
        ]))
        stmts = list(self._run(od([("t", t_before)]), od([("t", t_after)])))
        assert any("add column" in s and "name" in s for s in stmts)

    def test_drop_column(self):
        t_before = make_table("t", columns=od([
            ("id", make_col("id")),
            ("old", make_col("old", "text")),
        ]))
        t_after = make_table("t", columns=od([("id", make_col("id"))]))
        stmts = list(self._run(od([("t", t_before)]), od([("t", t_after)])))
        assert any("drop column" in s and "old" in s for s in stmts)

    def test_new_table_with_comment(self):
        t = make_table("t", comment="my table")
        stmts = list(self._run(od(), od([('"public"."t"', t)])))
        assert any("COMMENT ON TABLE" in s for s in stmts)

    def test_add_column_with_comment(self):
        t_before = make_table("t", columns=od([("id", make_col("id"))]))
        t_after = make_table("t", columns=od([
            ("id", make_col("id")),
            ("desc", make_col("desc", "text", comment="a description")),
        ]))
        stmts = list(self._run(od([("t", t_before)]), od([("t", t_after)])))
        assert any("COMMENT ON COLUMN" in s for s in stmts)


# ---------------------------------------------------------------------------
# get_enum_modifications()
# ---------------------------------------------------------------------------

class TestGetEnumModifications:
    def test_enum_rename_recreate_drop_cycle(self):
        old_e = make_enum("status", ["a", "b"])
        new_e = make_enum("status", ["a", "c", "b"])
        enums_from = od([(old_e.quoted_full_name, old_e)])
        enums_target = od([(new_e.quoted_full_name, new_e)])

        stmts = list(get_enum_modifications(od(), od(), enums_from, enums_target))
        stmt_str = "\n".join(stmts)
        assert "rename to" in stmt_str
        assert "create type" in stmt_str
        assert "drop type" in stmt_str
        # rename must come before recreate
        rename_pos = stmt_str.find("rename to")
        create_pos = stmt_str.find("create type")
        assert rename_pos < create_pos

    def test_unchanged_enum_produces_no_statements(self):
        e = make_enum("status", ["a", "b"])
        enums = od([(e.quoted_full_name, e)])
        stmts = list(get_enum_modifications(od(), od(), enums, enums))
        assert stmts == []
