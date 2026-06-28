"""
lockinfo: analyse what locks each statement in a SQL script acquires.

For each statement, we:
  1. BEGIN a transaction
  2. Execute the statement
  3. Query pg_locks (joined with pg_class) to find all non-trivial locks
     taken by our own backend
  4. ROLLBACK

This is run against a temporary copy of a real database (or an empty one),
so the script never touches production data.

Lock modes in severity order (most permissive → most restrictive):
  AccessShareLock, RowShareLock, RowExclusiveLock, ShareUpdateExclusiveLock,
  ShareLock, ShareRowExclusiveLock, ExclusiveLock, AccessExclusiveLock

AccessExclusiveLock blocks all concurrent reads and writes and is the one
that makes migrations dangerous on busy tables.
"""
from __future__ import annotations

import textwrap
from dataclasses import dataclass, field
from typing import Optional


# Ordered from least to most disruptive
LOCK_SEVERITY = [
    "AccessShareLock",
    "RowShareLock",
    "RowExclusiveLock",
    "ShareUpdateExclusiveLock",
    "ShareLock",
    "ShareRowExclusiveLock",
    "ExclusiveLock",
    "AccessExclusiveLock",
]

LOCK_SEVERITY_INDEX = {name: i for i, name in enumerate(LOCK_SEVERITY)}

# Locks that are noteworthy (ShareUpdateExclusiveLock and above)
NOTABLE_THRESHOLD = LOCK_SEVERITY_INDEX["ShareUpdateExclusiveLock"]

# Human descriptions for each lock mode
LOCK_DESCRIPTIONS = {
    "AccessShareLock": "read (SELECT)",
    "RowShareLock": "row share (SELECT FOR UPDATE)",
    "RowExclusiveLock": "row exclusive (INSERT/UPDATE/DELETE)",
    "ShareUpdateExclusiveLock": "share update exclusive — blocks VACUUM, schema changes",
    "ShareLock": "share — blocks writes, allows reads",
    "ShareRowExclusiveLock": "share row exclusive — blocks most writes",
    "ExclusiveLock": "exclusive — blocks all but AccessShare",
    "AccessExclusiveLock": "ACCESS EXCLUSIVE — blocks all reads and writes",
}

# pg_locks query: find all non-virtual, non-trivial locks held by our own backend
# We exclude relation locks on system catalogs (relnamespace = pg_catalog)
# and the transaction-level locks that every statement takes.
_LOCKS_QUERY = """
SELECT
    l.locktype,
    l.mode,
    l.granted,
    CASE l.locktype
        WHEN 'relation' THEN c.relname
        WHEN 'extend'   THEN c.relname
        ELSE NULL
    END AS relation_name,
    CASE l.locktype
        WHEN 'relation' THEN n.nspname
        WHEN 'extend'   THEN n.nspname
        ELSE NULL
    END AS schema_name,
    CASE l.locktype
        WHEN 'relation' THEN c.relkind
        ELSE NULL
    END AS relkind,
    l.locktype,
    l.transactionid,
    l.classid,
    l.objid
FROM pg_locks l
LEFT JOIN pg_class c ON c.oid = l.relation
LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE
    l.pid = pg_backend_pid()
    AND l.granted = true
    AND l.locktype != 'virtualxid'
    AND NOT (l.locktype = 'transactionid')
    AND NOT (
        l.locktype = 'relation'
        AND n.nspname IN ('pg_catalog', 'pg_toast', 'information_schema')
    )
ORDER BY
    CASE l.locktype WHEN 'relation' THEN 0 ELSE 1 END,
    n.nspname,
    c.relname,
    l.mode
"""

RELKIND_NAMES = {
    "r": "table",
    "i": "index",
    "S": "sequence",
    "v": "view",
    "m": "materialized view",
    "p": "partitioned table",
    "I": "partitioned index",
    "t": "TOAST table",
}


@dataclass
class LockEntry:
    locktype: str
    mode: str
    relation_name: Optional[str]
    schema_name: Optional[str]
    relkind: Optional[str]

    @property
    def severity_index(self) -> int:
        return LOCK_SEVERITY_INDEX.get(self.mode, 0)

    @property
    def is_notable(self) -> bool:
        return self.severity_index >= NOTABLE_THRESHOLD

    @property
    def object_label(self) -> str:
        if self.relation_name:
            kind = RELKIND_NAMES.get(self.relkind, self.relkind or "object")
            schema = self.schema_name or "public"
            return f"{schema}.{self.relation_name} ({kind})"
        return f"{self.locktype} lock"

    @property
    def mode_description(self) -> str:
        return LOCK_DESCRIPTIONS.get(self.mode, self.mode)


@dataclass
class StatementLockInfo:
    statement: str
    locks: list[LockEntry] = field(default_factory=list)
    error: Optional[str] = None

    @property
    def notable_locks(self) -> list[LockEntry]:
        return [l for l in self.locks if l.is_notable]

    @property
    def max_severity(self) -> int:
        if not self.locks:
            return -1
        return max(l.severity_index for l in self.locks)


def split_statements(sql: str) -> list[str]:
    """Split SQL into individual statements using pglast (handles dollar-quoting etc)."""
    from pglast import split
    return list(split(sql))


def analyse_locks(db_url: str, sql: str) -> list[StatementLockInfo]:
    """
    Run each statement in `sql` against the database at `db_url` and
    record all locks acquired. Each statement is rolled back after inspection.

    Returns a list of StatementLockInfo, one per statement.
    """
    from psycopg import connect

    statements = split_statements(sql)
    results = []

    conn = connect(db_url, autocommit=False)
    try:
        for stmt in statements:
            info = StatementLockInfo(statement=stmt)
            try:
                with conn.transaction() as sp:
                    with conn.cursor() as cur:
                        cur.execute(stmt)
                        cur.execute(_LOCKS_QUERY)
                        rows = cur.fetchall()
                        cols = [d.name for d in cur.description]
                        for row in rows:
                            r = dict(zip(cols, row))
                            info.locks.append(LockEntry(
                                locktype=r["locktype"],
                                mode=r["mode"],
                                relation_name=r["relation_name"],
                                schema_name=r["schema_name"],
                                relkind=r["relkind"],
                            ))
                    # Savepoint rolls back automatically at end of `with` if we raise,
                    # but we want to always roll back so we raise to trigger it.
                    raise _Rollback()
            except _Rollback:
                pass
            except Exception as e:
                info.error = str(e)
                # Connection may be in error state; reset it
                try:
                    conn.rollback()
                except Exception:
                    pass
            results.append(info)
    finally:
        conn.close()

    return results


class _Rollback(Exception):
    """Internal sentinel to trigger savepoint rollback after lock inspection."""
    pass


def format_results(results: list[StatementLockInfo], verbose: bool = False) -> str:
    """Format lock analysis results as human-readable text."""
    lines = []

    for i, info in enumerate(results, 1):
        stmt_preview = textwrap.shorten(info.statement.strip(), width=72, placeholder="...")
        lines.append(f"[{i}] {stmt_preview}")

        if info.error:
            lines.append(f"    ERROR: {info.error}")
        elif not info.locks:
            lines.append("    (no user-object locks)")
        else:
            # Group by object
            by_object: dict[str, list[LockEntry]] = {}
            for lock in info.locks:
                key = lock.object_label
                by_object.setdefault(key, []).append(lock)

            for obj, locks in by_object.items():
                modes = sorted(set(l.mode for l in locks), key=lambda m: LOCK_SEVERITY_INDEX.get(m, 0))
                for mode in modes:
                    lock = next(l for l in locks if l.mode == mode)
                    notable_marker = " ⚠" if lock.is_notable else ""
                    if verbose:
                        lines.append(f"    {obj}: {mode}{notable_marker}")
                        lines.append(f"        ({lock.mode_description})")
                    else:
                        lines.append(f"    {obj}: {mode}{notable_marker}")

        lines.append("")

    # Summary
    notable_stmts = [r for r in results if r.notable_locks]
    errors = [r for r in results if r.error]

    if notable_stmts:
        lines.append(f"⚠  {len(notable_stmts)} statement(s) acquire notable locks (ShareUpdateExclusiveLock or stronger):")
        for r in notable_stmts:
            preview = textwrap.shorten(r.statement.strip(), width=60, placeholder="...")
            max_mode = max(r.notable_locks, key=lambda l: l.severity_index).mode
            lines.append(f"   - {preview}")
            lines.append(f"     max lock: {max_mode}")
    else:
        lines.append("✓  No notable locks (all locks are RowExclusiveLock or weaker).")

    if errors:
        lines.append(f"\n✗  {len(errors)} statement(s) failed to execute.")

    return "\n".join(lines)
