"""
Dependency-aware ordered diff generation.

Replaces the hardcoded category sequence in add_all_changes() with a full
cross-category topological sort so every output statement is correctly ordered
regardless of schema complexity.

Key correctness rules encoded here:
- Functions must be created before any table/view that uses them as defaults
- Views/matviews must be dropped before tables they select from
- Tables must be dropped before functions used as their column defaults
- The overall create ordering: schemas → extensions → collations → enums →
  types → functions → tables → views/matviews → constraints/indexes → triggers
- The overall drop ordering is the reverse
"""
from __future__ import annotations

from graphlib import TopologicalSorter
from typing import TYPE_CHECKING

from .statements import Statements

if TYPE_CHECKING:
    from .changes import Changes


def ordered_changes(changes: Changes, privileges: bool = False) -> list[Statements]:
    """
    Return a list of Statements in dependency-correct order.
    The caller does: for s in ordered_changes(...): migration.add(s)
    """
    nodes: dict[str, Statements] = {}
    deps: dict[str, set[str]] = {}

    def add_node(node_id: str, stmts, prerequisites: set | None = None):
        nodes[node_id] = stmts if isinstance(stmts, Statements) else Statements(stmts)
        deps[node_id] = set(prerequisites or [])

    # ------------------------------------------------------------------
    # Gather all diff statement groups
    # ------------------------------------------------------------------

    add_node("schemas:create",   changes.schemas(creations_only=True))
    add_node("schemas:drop",     changes.schemas(drops_only=True))

    add_node("extensions:create", changes.extensions(creations_only=True, modifications=False))
    add_node("extensions:modify", changes.extensions(modifications_only=True, modifications=True))
    add_node("extensions:drop",   changes.extensions(drops_only=True, modifications=False))

    add_node("collations:create", changes.collations(creations_only=True))
    add_node("collations:drop",   changes.collations(drops_only=True))

    add_node("enums:create", changes.enums(creations_only=True, modifications=False))
    add_node("enums:modify", changes.enums(modifications=True))
    add_node("enums:drop",   changes.enums(drops_only=True, modifications=False))

    add_node("types:create", changes.types(creations_only=True))
    add_node("types:modify", changes.types(modifications_only=True))
    add_node("types:drop",   changes.types(drops_only=True))

    add_node("sequences:create", changes.sequences(creations_only=True))
    add_node("sequences:drop",   changes.sequences(drops_only=True))

    # Functions split out from other non-table selectables so they can be
    # ordered: functions before tables (column defaults), but views after tables.
    add_node("functions:drop",   changes.function_drops())
    add_node("functions:create", changes.function_creations())

    # Views, matviews, composite types (everything non-table, non-function)
    add_node("views:drop",   changes.non_function_non_table_selectable_drops())
    add_node("views:create", changes.non_function_non_table_selectable_creations())

    add_node("triggers:drop",   changes.triggers(drops_only=True))
    add_node("triggers:create", changes.triggers(creations_only=True))

    add_node("rlspolicies:drop",   changes.rlspolicies(drops_only=True))
    add_node("rlspolicies:create", changes.rlspolicies(creations_only=True))

    add_node("pk_constraints:drop",      changes.pk_constraints(drops_only=True))
    add_node("pk_constraints:create",    changes.pk_constraints(creations_only=True))
    add_node("non_pk_constraints:drop",  changes.non_pk_constraints(drops_only=True))
    add_node("non_pk_constraints:create",changes.non_pk_constraints(creations_only=True))

    add_node("mv_indexes:drop",      changes.mv_indexes(drops_only=True))
    add_node("mv_indexes:create",    changes.mv_indexes(creations_only=True))
    add_node("non_mv_indexes:drop",  changes.non_mv_indexes(drops_only=True))
    add_node("non_mv_indexes:create",changes.non_mv_indexes(creations_only=True))

    add_node("tables", changes.tables_only_selectables())

    if privileges:
        add_node("privileges:drop",   changes.privileges(drops_only=True))
        add_node("privileges:create", changes.privileges(creations_only=True))

    # ------------------------------------------------------------------
    # Dependency edges
    # ------------------------------------------------------------------

    # Everything needs schemas to exist first; schemas drop last
    for n in list(nodes):
        if n not in ("schemas:create", "schemas:drop"):
            deps[n].add("schemas:create")
    deps["schemas:drop"].update(k for k in nodes if k != "schemas:drop")

    # Extensions before anything that might use them
    for n in ("enums:create", "types:create", "functions:create",
              "sequences:create", "tables", "views:create"):
        deps[n].add("extensions:create")
        deps[n].add("extensions:modify")

    # Extensions drop after non-table selectables that might use them
    deps["extensions:drop"].add("views:drop")
    deps["extensions:drop"].add("functions:drop")
    deps["extensions:drop"].add("types:drop")
    deps["extensions:drop"].add("enums:drop")

    # Collations before anything that might collate on them
    deps["tables"].add("collations:create")
    deps["functions:create"].add("collations:create")
    deps["views:create"].add("collations:create")
    deps["collations:drop"].update(k for k in nodes
                                   if k not in ("collations:drop", "schemas:drop"))

    # Enums before tables (columns typed as enums), types, functions
    for n in ("tables", "functions:create", "views:create", "types:create"):
        deps[n].add("enums:create")
        deps[n].add("enums:modify")
    deps["enums:drop"].add("tables")
    deps["enums:drop"].add("views:drop")
    deps["enums:drop"].add("functions:drop")

    # Types before tables and views
    for n in ("tables", "functions:create", "views:create"):
        deps[n].add("types:create")
        deps[n].add("types:modify")
    deps["types:drop"].add("tables")
    deps["types:drop"].add("views:drop")
    deps["types:drop"].add("functions:drop")

    # Sequences before tables
    deps["tables"].add("sequences:create")
    deps["sequences:drop"].add("tables")

    # --- THE KEY ORDERING ---
    # Functions must be created before tables that use them as column defaults.
    # Tables must be dropped before functions used as their defaults.
    deps["tables"].add("functions:create")
    deps["functions:drop"].add("tables")

    # Views/matviews must be dropped before the tables they select from are changed
    deps["tables"].add("views:drop")

    # Non-pk constraints and non-mv indexes must be dropped before tables change
    deps["tables"].add("non_pk_constraints:drop")
    deps["tables"].add("pk_constraints:drop")
    deps["tables"].add("non_mv_indexes:drop")
    deps["tables"].add("triggers:drop")
    deps["tables"].add("rlspolicies:drop")
    deps["tables"].add("mv_indexes:drop")

    # Also drop functions before tables IF function depends on table (unusual but possible)
    # This is handled by the intra-category dependency_ordering in get_selectable_changes.

    # After tables exist: create constraints, indexes, views, triggers, rls
    for n in ("pk_constraints:create", "non_pk_constraints:create",
              "non_mv_indexes:create", "mv_indexes:create",
              "views:create", "triggers:create", "rlspolicies:create"):
        deps[n].add("tables")

    # pk before non-pk (FK may reference PK index)
    deps["non_pk_constraints:create"].add("pk_constraints:create")
    deps["non_pk_constraints:drop"].add("pk_constraints:drop")

    # Views after functions (views can call functions)
    deps["views:create"].add("functions:create")

    # Matview indexes after matviews
    deps["mv_indexes:create"].add("views:create")

    # Triggers need their trigger functions
    deps["triggers:create"].add("functions:create")
    deps["triggers:drop"].add("rlspolicies:drop")

    # RLS after tables
    deps["rlspolicies:create"].add("tables")

    # Functions drop after views that call them
    deps["functions:drop"].add("views:drop")
    deps["functions:drop"].add("triggers:drop")

    if privileges:
        deps["privileges:drop"].add("tables")
        deps["privileges:create"].add("tables")
        deps["privileges:create"].add("views:create")
        deps["privileges:create"].add("functions:create")

    # ------------------------------------------------------------------
    # Toposort and return non-empty groups in order
    # ------------------------------------------------------------------
    ts = TopologicalSorter(deps)
    result = []
    for node_id in ts.static_order():
        if node_id in nodes and nodes[node_id]:
            result.append(nodes[node_id])
    return result
