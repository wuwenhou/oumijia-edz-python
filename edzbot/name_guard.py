"""Minimal compatibility guard: never publish a normalized EDZ name twice.

Works with both the Rust-upgraded SQLite schema (edz_name_catalog) and older
production snapshots (telegram_posts only). Does not modify or delete data.
"""
from __future__ import annotations

import unicodedata
from pathlib import Path


def normalize_edz_name(name: str) -> str:
    """Canonical identity: basename, Unicode NFC, case-folded (spec V.2)."""
    base = Path(str(name)).name
    return unicodedata.normalize("NFC", base).lower()


def name_already_published(db, name: str) -> bool:
    """True if the normalized name is already accepted/posted.

    - `edz_name_catalog` is the authoritative name catalogue written by the
      newer implementation (117k+ names). A name accepted there is final.
    - `telegram_posts` is the confirmed destination-post mapping. Checked as a
      fallback for older databases, normalizing stored names on the fly.
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
    if db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='telegram_posts'"
    ).fetchone():
        for (stored,) in db.execute("SELECT file_name FROM telegram_posts"):
            if normalize_edz_name(stored) == norm:
                return True
    return False
