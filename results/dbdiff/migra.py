from results.schemainspect import PostgreSQL, SchemaDefinition, get_inspector

from .changes import Changes
from .statements import Statements


class Migration:
    """
    The main class of migra
    """

    def __init__(
        self,
        x_from,
        x_target,
        schema=None,
        exclude_schema=None,
        ignore_extension_versions=False,
    ):
        self.statements = Statements()
        self.changes = Changes(None, None)
        if schema and exclude_schema:
            raise ValueError("You cannot have both a schema and excluded schema")
        self.schema = schema
        self.exclude_schema = exclude_schema

        def to_inspector(x):
            if isinstance(x, PostgreSQL):
                return x
            if isinstance(x, SchemaDefinition):
                return PostgreSQL.from_definition(x)
            if isinstance(x, dict):
                return PostgreSQL.from_definition(SchemaDefinition.from_dict(x))
            insp = get_inspector(x, schema=schema, exclude_schema=exclude_schema)
            return insp

        self.changes.i_from = to_inspector(x_from)
        self.changes.i_target = to_inspector(x_target)

        if not isinstance(x_from, (PostgreSQL, SchemaDefinition, dict)) and x_from:
            self.s_from = x_from
        if not isinstance(x_target, (PostgreSQL, SchemaDefinition, dict)) and x_target:
            self.s_target = x_target

        self.changes.ignore_extension_versions = ignore_extension_versions

    def inspect_from(self):
        self.changes.i_from = get_inspector(
            self.s_from, schema=self.schema, exclude_schema=self.exclude_schema
        )

    def inspect_target(self):
        self.changes.i_target = get_inspector(
            self.s_target, schema=self.schema, exclude_schema=self.exclude_schema
        )

    def clear(self):
        self.statements = Statements()

    def add(self, statements):
        self.statements += statements

    def add_sql(self, sql):
        self.statements += Statements([sql])

    def set_safety(self, safety_on):
        self.statements.safe = safety_on

    def add_extension_changes(self, creates=True, drops=True):
        if creates:
            self.add(self.changes.extensions(creations_only=True))
        if drops:
            self.add(self.changes.extensions(drops_only=True))

    def add_all_changes(self, privileges=False):
        # DEPRECATED: use add_all_changes_ordered() instead.
        # This method uses a hardcoded category sequence that does not track
        # cross-category dependencies (e.g. a table whose column default calls
        # a user-defined function). It is retained for debugging and comparison
        # purposes only.
        self.add(self.changes.schemas(creations_only=True))

        self.add(self.changes.extensions(creations_only=True, modifications=False))
        self.add(self.changes.extensions(modifications_only=True, modifications=True))
        self.add(self.changes.collations(creations_only=True))
        self.add(self.changes.enums(creations_only=True, modifications=False))
        self.add(self.changes.types(creations_only=True))
        self.add(self.changes.types(modifications_only=True))
        self.add(self.changes.sequences(creations_only=True))
        self.add(self.changes.triggers(drops_only=True))
        self.add(self.changes.rlspolicies(drops_only=True))
        if privileges:
            self.add(self.changes.privileges(drops_only=True))
        self.add(self.changes.non_pk_constraints(drops_only=True))

        self.add(self.changes.mv_indexes(drops_only=True))
        self.add(self.changes.non_table_selectable_drops())

        self.add(self.changes.pk_constraints(drops_only=True))
        self.add(self.changes.non_mv_indexes(drops_only=True))

        self.add(self.changes.tables_only_selectables())

        self.add(self.changes.sequences(drops_only=True))
        self.add(self.changes.enums(drops_only=True, modifications=False))
        self.add(self.changes.types(drops_only=True))
        self.add(self.changes.extensions(drops_only=True, modifications=False))
        self.add(self.changes.non_mv_indexes(creations_only=True))
        self.add(self.changes.pk_constraints(creations_only=True))
        self.add(self.changes.non_pk_constraints(creations_only=True))

        self.add(self.changes.non_table_selectable_creations())
        self.add(self.changes.mv_indexes(creations_only=True))

        if privileges:
            self.add(self.changes.privileges(creations_only=True))
        self.add(self.changes.rlspolicies(creations_only=True))
        self.add(self.changes.triggers(creations_only=True))
        self.add(self.changes.collations(drops_only=True))
        self.add(self.changes.schemas(drops_only=True))

    def add_all_changes_ordered(self, privileges=False):
        """
        Like add_all_changes() but uses full dependency-aware toposort ordering
        across all object categories instead of a hardcoded sequence.

        The output is guaranteed to be correctly ordered for any schema as long as
        there are no genuine circular dependencies (which Postgres itself disallows).
        """
        from .ordered import ordered_changes
        for stmt in ordered_changes(self.changes, privileges=privileges):
            self.add(stmt)

    @property
    def sql(self):
        return self.statements.sql
