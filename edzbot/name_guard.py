"""Minimal compatibility guard: never publish a normalized EDZ name twice.

Works with both the Rust-upgraded SQLite schema (edz_name_catalog) and older
production snapshots (telegram_posts only). Does not modify or delete data.
"""
from __future__ import annotations

import unicodedata
from pathlib import Path

# Cache of normalized confirmed-post names, keyed by DB connection identity.
# Rebuilt only when telegram_posts row count changes (cheap COUNT query), so a
# new post arriving during the process lifetime is never missed.
_NAME_CACHE: dict[int, tuple[int, frozenset[str]]] = {}


def normalize_edz_name(name: str) -> str:
    """Canonical identity: basename, Unicode NFC, case-folded (spec V.2)."""
    base = Path(str(name)).name
    return unicodedata.normalize("NFC", base).lower()


def _confirmed_names(db) -> frozenset[str]:
    if not db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='telegram_posts'"
    ).fetchone():
        return frozenset()
    count = db.execute("SELECT COUNT(*) FROM telegram_posts").fetchone()[0]
    row = db.execute("PRAGMA database_list").fetchone()
    # File-backed DBs: key by path so a new connection never inherits a stale
    # cache. In-memory DBs (tests) are tiny: always rebuild, no cache.
    key = row[2] if row and row[2] else None
    if key is None:
        return frozenset(
            normalize_edz_name(stored)
            for (stored,) in db.execute("SELECT file_name FROM telegram_posts")
        )
    cached = _NAME_CACHE.get(key)
    if cached is not None and cached[0] == count:
        return cached[1]
    names = frozenset(
        normalize_edz_name(stored)
        for (stored,) in db.execute("SELECT file_name FROM telegram_posts")
    )
    _NAME_CACHE[key] = (count, names)
    return names


def name_already_published(db, name: str) -> bool:
    """True if the normalized name is already accepted/posted.

    - `edz_name_catalog` is the authoritative name catalogue written by the
      newer implementation (117k+ names), checked via indexed lookup.
    - `telegram_posts` is the confirmed destination-post mapping. Used as a
      fallback for older databases; stored names are normalized once and kept
      in a cache invalidated by row-count changes.
    Different names with the same SHA-256 are NOT considered duplicates here.
    """
    norm = normalize_edz_name(name)
    if db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='edz_name_catalog'"
    ).fetchone():
        if db.execute(
            "SELECT 1 FROM edz_name_catalog WHERE normalized_name=? AND state='accepted'",
            (norm,),
        ).fetchone():
            return True
    # Python-posted names live in artifacts.normalized_name (the Rust-only
    # telegram_posts table is not updated by Python). Check it so deleting that
    # legacy table cannot weaken duplicate protection.
    if db.execute(
        "SELECT 1 FROM pragma_table_info('artifacts') WHERE name='normalized_name'"
    ).fetchone():
        if db.execute(
            "SELECT 1 FROM artifacts WHERE normalized_name=? AND state='posted' LIMIT 1",
            (norm,),
        ).fetchone():
            return True
    if db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='telegram_posts'"
    ).fetchone():
        return norm in _confirmed_names(db)
    return False
