"""
SchemaDefinition: a serializable, connection-free representation of a database schema.

Produced by PostgreSQL.as_definition() and consumed by PostgreSQL.from_definition().
Can be serialized to/from YAML or JSON, enabling offline diffs without a live DB.
"""

from __future__ import annotations

import json
from collections import OrderedDict as od
from typing import Any


class SchemaDefinition:
    """
    Minimal serializable description of a PostgreSQL schema.

    Each category is a plain dict keyed by the same quoted_full_name strings
    used internally by the inspector. Values are plain dicts of constructor args.
    """

    CATEGORIES = [
        "schemas",
        "enums",
        "tables",
        "views",
        "materialized_views",
        "functions",
        "sequences",
        "constraints",
        "indexes",
        "extensions",
        "privileges",
        "triggers",
        "collations",
        "rlspolicies",
        "types",
        "domains",
        "composite_types",
    ]

    pg_version: int
    schemas: dict[str, Any]
    enums: dict[str, Any]
    tables: dict[str, Any]
    views: dict[str, Any]
    materialized_views: dict[str, Any]
    functions: dict[str, Any]
    sequences: dict[str, Any]
    constraints: dict[str, Any]
    indexes: dict[str, Any]
    extensions: dict[str, Any]
    privileges: dict[str, Any]
    triggers: dict[str, Any]
    collations: dict[str, Any]
    rlspolicies: dict[str, Any]
    types: dict[str, Any]
    domains: dict[str, Any]
    composite_types: dict[str, Any]

    def __init__(self, pg_version: int = 14, **categories: dict[str, Any]):
        self.pg_version = pg_version
        for cat in self.CATEGORIES:
            setattr(self, cat, categories.get(cat, {}))

    def to_dict(self) -> dict:
        d: dict = {"pg_version": self.pg_version}
        for cat in self.CATEGORIES:
            val = getattr(self, cat)
            if val:
                d[cat] = val
        return d

    def to_json(self, **kwargs) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    def to_yaml(self) -> str:
        import yaml

        return yaml.safe_dump(
            self.to_dict(), default_flow_style=False, allow_unicode=True
        )

    @classmethod
    def from_dict(cls, d: dict) -> SchemaDefinition:
        pg_version = d.get("pg_version", 14)
        categories = {cat: d.get(cat, {}) for cat in cls.CATEGORIES}
        return cls(pg_version=pg_version, **categories)

    @classmethod
    def from_json(cls, s: str) -> SchemaDefinition:
        return cls.from_dict(json.loads(s))

    @classmethod
    def from_yaml(cls, s: str) -> SchemaDefinition:
        import yaml

        return cls.from_dict(yaml.safe_load(s))

    def schemadiff_as_statements(self, other, **kwargs):
        """Diff this definition against another definition or live DB connection."""
        from results.dbdiff import Migration

        m = Migration(
            self,
            other,
            **{
                k: v
                for k, v in kwargs.items()
                if k in ("schema", "exclude_schema", "ignore_extension_versions")
            },
        )
        m.set_safety(False)

        if kwargs.get("create_extensions_only"):
            m.add_extension_changes(drops=False)
        else:
            m.add_all_changes_ordered(privileges=kwargs.get("with_privileges", False))
        return m.statements

    def schemadiff_as_sql(self, other, **kwargs) -> str:
        return self.schemadiff_as_statements(other, **kwargs).sql

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SchemaDefinition):
            return NotImplemented
        return self.to_dict() == other.to_dict()
