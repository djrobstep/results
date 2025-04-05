from collections import OrderedDict as od

from .misc import AutoRepr, quoted_identifier, unquoted_identifier


class Inspected(AutoRepr):
    @property
    def quoted_full_name(self):
        return quoted_identifier(self.name, schema=self.schema)

    @property
    def signature(self):
        return self.quoted_full_name

    @property
    def unquoted_full_name(self):
        return unquoted_identifier(self.name, schema=self.schema)

    @property
    def quoted_name(self):
        return quoted_identifier(self.name)

    @property
    def quoted_schema(self):
        return quoted_identifier(self.schema)

    def __ne__(self, other):
        return not self == other


class TableRelated(object):
    @property
    def quoted_full_table_name(self):
        return "{}.{}".format(
            quoted_identifier(self.schema), quoted_identifier(self.table_name)
        )


class ColumnInfo(AutoRepr):
    def __init__(
        self,
        name,
        dbtype,
        pytype,
        default=None,
        not_null=False,
        is_enum=False,
        enum=None,
        dbtypestr=None,
        collation=None,
        is_identity=False,
        is_identity_always=False,
        is_generated=False,
        is_inherited=False,
        can_drop_generated=False,
    ):
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

    def __eq__(self, other):
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
        )

    def alter_clauses(self, other):
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

    def change_enum_to_string_statement(self, table_name):
        if self.is_enum:
            return "alter table {} alter column {} set data type varchar using {}::varchar;".format(
                table_name, self.quoted_name, self.quoted_name
            )

        else:
            raise ValueError

    def change_string_to_enum_statement(self, table_name):
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

    def change_enum_statement(self, table_name):
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

    def drop_default_statement(self, table_name):
        return "alter table {} alter column {} drop default;".format(
            table_name, self.quoted_name
        )

    def add_default_statement(self, table_name):
        return "alter table {} alter column {} set default {};".format(
            table_name, self.quoted_name, self.default
        )

    def alter_table_statements(self, other, table_name):
        prefix = "alter table {}".format(table_name)
        return ["{} {};".format(prefix, c) for c in self.alter_clauses(other)]

    @property
    def quoted_name(self):
        return quoted_identifier(self.name)

    @property
    def creation_clause(self):
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
    def add_column_clause(self):
        return "add column {}{}".format(self.creation_clause, self.collation_subclause)

    @property
    def drop_column_clause(self):
        return "drop column {k}".format(k=self.quoted_name)

    @property
    def alter_not_null_clause(self):
        keyword = "set" if self.not_null else "drop"
        return "alter column {} {} not null".format(self.quoted_name, keyword)

    @property
    def alter_default_clause(self):
        if self.default:
            alter = "alter column {} set default {}".format(
                self.quoted_name, self.default
            )
        else:
            alter = "alter column {} drop default".format(self.quoted_name)
        return alter

    def alter_default_clause_or_generated(self, other):
        if self.default:
            alter = "alter column {} set default {}".format(
                self.quoted_name, self.default
            )
        elif other.is_generated and not self.is_generated:
            alter = "alter column {} drop expression".format(self.quoted_name)
        else:
            alter = "alter column {} drop default".format(self.quoted_name)
        return alter

    def alter_identity_clause(self, other):
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
    def collation_subclause(self):
        if self.collation:
            collate = " collate {}".format(quoted_identifier(self.collation))
        else:
            collate = ""
        return collate

    @property
    def alter_data_type_clause(self):
        return "alter column {} set data type {}{} using {}::{}".format(
            self.quoted_name,
            self.dbtypestr,
            self.collation_subclause,
            self.quoted_name,
            self.dbtypestr,
        )

    @property
    def alter_enum_type_clause(self):
        return "alter column {} set data type {}{} using {}::text::{}".format(
            self.quoted_name,
            self.dbtypestr,
            self.collation_subclause,
            self.quoted_name,
            self.dbtypestr,
        )


class InspectedSelectable(Inspected):
    def __init__(
        self,
        name,
        schema,
        columns,
        inputs=None,
        definition=None,
        dependent_on=None,
        dependents=None,
        comment=None,
        relationtype="unknown",
        parent_table=None,
        partition_def=None,
        rowsecurity=False,
        forcerowsecurity=False,
        persistence=None,
        oid=None,
    ):
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
        self.oid = oid

    def __eq__(self, other):
        equalities = (
            type(self) == type(other),
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
        )
        return all(equalities)
