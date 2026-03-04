from __future__ import annotations

from collections import OrderedDict as od
from typing import TYPE_CHECKING, Any

from .misc import AutoRepr, quoted_identifier, unquoted_identifier

if TYPE_CHECKING:
    from collections.abc import Mapping


class Inspected(AutoRepr):
    name: str
    schema: str

    @property
    def quoted_full_name(self) -> str:
        return quoted_identifier(self.name, schema=self.schema)

    @property
    def signature(self) -> str:
        return self.quoted_full_name

    @property
    def unquoted_full_name(self) -> str:
        return unquoted_identifier(self.name, schema=self.schema)

    @property
    def quoted_name(self) -> str:
        return quoted_identifier(self.name)

    @property
    def quoted_schema(self) -> str:
        return quoted_identifier(self.schema)

    def __ne__(self, other: object) -> bool:
        return not self == other


class TableRelated:
    schema: str
    table_name: str

    @property
    def quoted_full_table_name(self) -> str:
        return "{}.{}".format(
            quoted_identifier(self.schema), quoted_identifier(self.table_name)
        )


class ColumnInfo(AutoRepr):
    name: str
    dbtype: str
    dbtypestr: str
    pytype: type | None
    default: str | None
    not_null: bool
    is_enum: bool
    enum: Any
    collation: str | None
    is_identity: bool
    is_identity_always: bool
    is_generated: bool
    is_inherited: bool
    can_drop_generated: bool
    can_set_expression: bool
    comment: str | None

    def __init__(
        self,
        name: str,
        dbtype: str,
        pytype: type | None,
        default: str | None = None,
        not_null: bool = False,
        is_enum: bool = False,
        enum: Any = None,
        dbtypestr: str | None = None,
        collation: str | None = None,
        is_identity: bool = False,
        is_identity_always: bool = False,
        is_generated: bool = False,
        is_inherited: bool = False,
        can_drop_generated: bool = False,
        can_set_expression: bool = False,
        comment: str | None = None,
    ) -> None:
        self.name = name or ""
        self.dbtype = dbtype
        self.dbtypestr = dbtypestr or dbtype
        self.pytype = pytype
        self.default = default or None
        self.not_null = not_null
        self.is_enum = is_enum
        self.enum = enum
        self.collation = collation
        self.is_identity = is_identity
        self.is_identity_always = is_identity_always
        self.is_generated = is_generated
        self.is_inherited = is_inherited
        self.can_drop_generated = can_drop_generated
        self.can_set_expression = can_set_expression
        self.comment = comment

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ColumnInfo):
            return NotImplemented
        return (
            self.name == other.name
            and self.dbtype == other.dbtype
            and self.dbtypestr == other.dbtypestr
            and self.default == other.default
            and self.not_null == other.not_null
            and self.enum == other.enum
            and self.collation == other.collation
            and self.is_identity == other.is_identity
            and self.is_identity_always == other.is_identity_always
            and self.is_generated == other.is_generated
            and self.is_inherited == other.is_inherited
            and self.comment == other.comment
        )

    def alter_clauses(self, other: ColumnInfo) -> list[str]:
        # ordering:
        # identify must be dropped before notnull
        # notnull must be added before identity

        clauses = []

        notnull_changed = self.not_null != other.not_null
        notnull_added = notnull_changed and self.not_null
        notnull_dropped = notnull_changed and not self.not_null

        default_changed = self.default != other.default

        # default_added = default_changed and self.default
        # default_dropped = default_changed and not self.default

        identity_changed = (
            self.is_identity != other.is_identity
            or self.is_identity_always != other.is_identity_always
        )

        type_or_collation_changed = (
            self.dbtypestr != other.dbtypestr or self.collation != other.collation
        )

        if default_changed:
            clauses.append(self.alter_default_clause_or_generated(other))

        if notnull_added:
            clauses.append(self.alter_not_null_clause)

        if identity_changed:
            clauses.append(self.alter_identity_clause(other))

        if notnull_dropped:
            clauses.append(self.alter_not_null_clause)

        if type_or_collation_changed:
            if self.is_enum and other.is_enum:
                clauses.append(self.alter_enum_type_clause)
            else:
                clauses.append(self.alter_data_type_clause)

        return clauses

    def change_enum_to_string_statement(self, table_name: str) -> str:
        if self.is_enum:
            return "alter table {} alter column {} set data type varchar using {}::varchar;".format(
                table_name, self.quoted_name, self.quoted_name
            )

        else:
            raise ValueError

    def change_string_to_enum_statement(self, table_name: str) -> str:
        if self.is_enum:
            return (
                "alter table {} alter column {} set data type {} using {}::{};".format(
                    table_name,
                    self.quoted_name,
                    self.dbtypestr,
                    self.quoted_name,
                    self.dbtypestr,
                )
            )
        else:
            raise ValueError

    def change_enum_statement(self, table_name: str) -> str:
        if self.is_enum:
            return "alter table {} alter column {} type {} using {}::text::{};".format(
                table_name,
                self.name,
                self.enum.quoted_full_name,
                self.name,
                self.enum.quoted_full_name,
            )
        else:
            raise ValueError

    def drop_default_statement(self, table_name: str) -> str:
        return "alter table {} alter column {} drop default;".format(
            table_name, self.quoted_name
        )

    def add_default_statement(self, table_name: str) -> str:
        return "alter table {} alter column {} set default {};".format(
            table_name, self.quoted_name, self.default
        )

    def alter_table_statements(self, other: ColumnInfo, table_name: str) -> list[str]:
        prefix = "alter table {}".format(table_name)
        return ["{} {};".format(prefix, c) for c in self.alter_clauses(other)]

    @property
    def quoted_name(self) -> str:
        return quoted_identifier(self.name)

    @property
    def creation_clause(self) -> str:
        x = "{} {}".format(self.quoted_name, self.dbtypestr)
        if self.is_identity:
            identity_type = "always" if self.is_identity_always else "by default"
            x += " generated {} as identity".format(identity_type)
        if self.not_null:
            x += " not null"
        if self.is_generated:
            x += " generated always as ({}) stored".format(self.default)
        elif self.default:
            x += " default {}".format(self.default)
        return x

    @property
    def add_column_clause(self) -> str:
        return "add column {}{}".format(self.creation_clause, self.collation_subclause)

    @property
    def drop_column_clause(self) -> str:
        return "drop column {k}".format(k=self.quoted_name)

    @property
    def alter_not_null_clause(self) -> str:
        keyword = "set" if self.not_null else "drop"
        return "alter column {} {} not null".format(self.quoted_name, keyword)

    @property
    def alter_default_clause(self) -> str:
        if self.default:
            alter = "alter column {} set default {}".format(
                self.quoted_name, self.default
            )
        else:
            alter = "alter column {} drop default".format(self.quoted_name)
        return alter

    def alter_default_clause_or_generated(self, other: ColumnInfo) -> str:
        if self.default:
            alter = "alter column {} set default {}".format(
                self.quoted_name, self.default
            )
        elif other.is_generated and not self.is_generated:
            alter = "alter column {} drop expression".format(self.quoted_name)
        else:
            alter = "alter column {} drop default".format(self.quoted_name)
        return alter

    @property
    def alter_set_expression_clause(self) -> str:
        """Generate ALTER COLUMN ... SET EXPRESSION for PG 17+."""
        return "alter column {} set expression as ({})".format(
            self.quoted_name, self.default
        )

    def alter_identity_clause(self, other: ColumnInfo) -> str:
        if self.is_identity:
            identity_type = "always" if self.is_identity_always else "by default"
            if other.is_identity:
                alter = "alter column {} set generated {}".format(
                    self.quoted_name, identity_type
                )
            else:
                alter = "alter column {} add generated {} as identity".format(
                    self.quoted_name, identity_type
                )
        else:
            alter = "alter column {} drop identity".format(self.quoted_name)
        return alter

    @property
    def collation_subclause(self) -> str:
        if self.collation:
            collate = " collate {}".format(quoted_identifier(self.collation))
        else:
            collate = ""
        return collate

    @property
    def alter_data_type_clause(self) -> str:
        return "alter column {} set data type {}{} using {}::{}".format(
            self.quoted_name,
            self.dbtypestr,
            self.collation_subclause,
            self.quoted_name,
            self.dbtypestr,
        )

    @property
    def alter_enum_type_clause(self) -> str:
        return "alter column {} set data type {}{} using {}::text::{}".format(
            self.quoted_name,
            self.dbtypestr,
            self.collation_subclause,
            self.quoted_name,
            self.dbtypestr,
        )

    def comment_statement(self, table_name: str) -> str | None:
        """Generate COMMENT ON COLUMN statement."""
        if self.comment is None:
            return None
        escaped = self.comment.replace("'", "''")
        return f"COMMENT ON COLUMN {table_name}.{self.quoted_name} IS '{escaped}';"

    def drop_comment_statement(self, table_name: str) -> str:
        """Generate statement to drop column comment."""
        return f"COMMENT ON COLUMN {table_name}.{self.quoted_name} IS NULL;"

    def comment_alter_statement(self, other: ColumnInfo, table_name: str) -> str | None:
        """Generate ALTER statement for comment change, or None if unchanged."""
        if self.comment == other.comment:
            return None
        if self.comment is not None:
            escaped = self.comment.replace("'", "''")
            return f"COMMENT ON COLUMN {table_name}.{self.quoted_name} IS '{escaped}';"
        return f"COMMENT ON COLUMN {table_name}.{self.quoted_name} IS NULL;"

    def only_comment_changed(self, other: ColumnInfo) -> bool:
        """Check if only comment differs (no structural change)."""
        return (
            self.name == other.name
            and self.dbtype == other.dbtype
            and self.dbtypestr == other.dbtypestr
            and self.default == other.default
            and self.not_null == other.not_null
            and self.enum == other.enum
            and self.collation == other.collation
            and self.is_identity == other.is_identity
            and self.is_identity_always == other.is_identity_always
            and self.is_generated == other.is_generated
            and self.is_inherited == other.is_inherited
            and self.comment != other.comment
        )


class InspectedSelectable(Inspected):
    name: str
    schema: str
    columns: Mapping[str, ColumnInfo]
    inputs: list[Any]
    definition: str | None
    relationtype: str
    dependent_on: list[str]
    dependents: list[str]
    dependent_on_all: list[str]
    dependents_all: list[str]
    constraints: od[str, Any]
    indexes: od[str, Any]
    comment: str | None
    parent_table: str | None
    partition_def: str | None
    rowsecurity: bool
    forcerowsecurity: bool
    persistence: str | None
    options: list[str] | None
    oid: int | None

    def __init__(
        self,
        name: str,
        schema: str,
        columns: Mapping[str, ColumnInfo],
        inputs: list[Any] | None = None,
        definition: str | None = None,
        dependent_on: list[str] | None = None,
        dependents: list[str] | None = None,
        comment: str | None = None,
        relationtype: str = "unknown",
        parent_table: str | None = None,
        partition_def: str | None = None,
        rowsecurity: bool = False,
        forcerowsecurity: bool = False,
        persistence: str | None = None,
        options: list[str] | None = None,
        oid: int | None = None,
    ) -> None:
        self.name = name
        self.schema = schema
        self.inputs = inputs or []
        self.columns = columns
        self.definition = definition
        self.relationtype = relationtype
        self.dependent_on = dependent_on or []
        self.dependents = dependents or []
        self.dependent_on_all = []
        self.dependents_all = []
        self.constraints = od()
        self.indexes = od()
        self.comment = comment
        self.parent_table = parent_table
        self.partition_def = partition_def
        self.rowsecurity = rowsecurity
        self.forcerowsecurity = forcerowsecurity
        self.persistence = persistence
        self.options = options
        self.oid = oid

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, InspectedSelectable):
            return NotImplemented
        equalities = (
            type(self) is type(other),
            self.relationtype == other.relationtype,
            self.name == other.name,
            self.schema == other.schema,
            dict(self.columns) == dict(other.columns),
            self.inputs == other.inputs,
            self.definition == other.definition,
            self.parent_table == other.parent_table,
            self.partition_def == other.partition_def,
            self.rowsecurity == other.rowsecurity,
            self.persistence == other.persistence,
            self.options == other.options,
            self.comment == other.comment,
        )
        return all(equalities)

    @property
    def comment_statement(self) -> str | None:
        """Generate a COMMENT ON statement for this object."""
        if self.comment is None:
            return None

        # Determine the object type based on relationtype
        object_type_map = {
            "r": "TABLE",
            "p": "TABLE",  # partitioned table
            "v": "VIEW",
            "m": "MATERIALIZED VIEW",
            "c": "TYPE",  # composite type
            "f": "FUNCTION",  # this is handled separately in InspectedFunction
        }

        object_type = object_type_map.get(self.relationtype, "TABLE")
        escaped_comment = self.comment.replace("'", "''")

        return (
            f"COMMENT ON {object_type} {self.quoted_full_name} IS '{escaped_comment}';"
        )

    @property
    def drop_comment_statement(self) -> str:
        """Generate a statement to drop the comment for this object."""
        object_type_map = {
            "r": "TABLE",
            "p": "TABLE",  # partitioned table
            "v": "VIEW",
            "m": "MATERIALIZED VIEW",
            "c": "TYPE",  # composite type
            "f": "FUNCTION",  # this is handled separately in InspectedFunction
        }

        object_type = object_type_map.get(self.relationtype, "TABLE")
        return f"COMMENT ON {object_type} {self.quoted_full_name} IS NULL;"

    def comment_alter_statements(self, other: InspectedSelectable) -> list[str]:
        """Generate statements to alter comments between two versions."""
        statements = []

        if self.comment != other.comment:
            if self.comment is None:
                # Adding a comment
                if other.comment is not None:
                    object_type_map = {
                        "r": "TABLE",
                        "p": "TABLE",  # partitioned table
                        "v": "VIEW",
                        "m": "MATERIALIZED VIEW",
                        "c": "TYPE",  # composite type
                        "f": "FUNCTION",  # this is handled separately in InspectedFunction
                    }

                    object_type = object_type_map.get(other.relationtype, "TABLE")
                    escaped_comment = other.comment.replace("'", "''")
                    statements.append(
                        f"COMMENT ON {object_type} {other.quoted_full_name} IS '{escaped_comment}';"
                    )
            elif other.comment is None:
                # Removing a comment
                statements.append(self.drop_comment_statement)
            else:
                # Changing a comment
                object_type_map = {
                    "r": "TABLE",
                    "p": "TABLE",  # partitioned table
                    "v": "VIEW",
                    "m": "MATERIALIZED VIEW",
                    "c": "TYPE",  # composite type
                    "f": "FUNCTION",  # this is handled separately in InspectedFunction
                }

                object_type = object_type_map.get(other.relationtype, "TABLE")
                escaped_comment = other.comment.replace("'", "''")
                statements.append(
                    f"COMMENT ON {object_type} {other.quoted_full_name} IS '{escaped_comment}';"
                )

        return statements

    def only_comment_changed(self, other: InspectedSelectable) -> bool:
        """Check if only the comment has changed between two versions of the same object."""
        # Check all attributes except comment
        equalities = (
            type(self) is type(other),
            self.relationtype == other.relationtype,
            self.name == other.name,
            self.schema == other.schema,
            dict(self.columns) == dict(other.columns),
            self.inputs == other.inputs,
            self.definition == other.definition,
            self.parent_table == other.parent_table,
            self.partition_def == other.partition_def,
            self.rowsecurity == other.rowsecurity,
            self.persistence == other.persistence,
            self.options == other.options,
        )
        return all(equalities) and self.comment != other.comment
