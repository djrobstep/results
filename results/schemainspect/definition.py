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
        return yaml.safe_dump(self.to_dict(), default_flow_style=False, allow_unicode=True)

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

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SchemaDefinition):
            return NotImplemented
        return self.to_dict() == other.to_dict()
