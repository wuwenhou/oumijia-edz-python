"""Conservative local cleanup of completed EDZ deliveries.

Never delete: anything in review/ready/held/posting, archive caches/work,
volume components, or any source whose derived artifacts are not ALL posted.
This is intentionally a manual, preview-first operation.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import hashlib
import time

from .db import DB


@dataclass(frozen=True)
class Candidate:
    kind: str
    key: str
    path: Path
    size: int


def _within_regular_file(data: Path, kind: str, path_text: str) -> Path | None:
    """Only regular files strictly inside a managed subdirectory; no symlinks."""
    root = (data / kind).resolve()
    supplied = Path(path_text)
    if not supplied.is_absolute():
        return None
    try:
        path = supplied.resolve(strict=True)
        if not path.is_relative_to(root) or path == root or not path.is_file():
            return None
        # A symlink in ANY path component is forbidden.
        cur = supplied
        while cur != data and cur != cur.parent:
            if cur.is_symlink():
                return None
            cur = cur.parent
        if cur != data or not path.is_file():
            return None
    except (OSError, RuntimeError):
        return None
    return path


def _hash_matches(path: Path, expected: str) -> bool:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for part in iter(lambda: stream.read(4 * 1024 * 1024), b''):
            digest.update(part)
    return digest.hexdigest() == expected


def candidates(db: DB, data: Path) -> list[Candidate]:
    """Return only entries eligible for removal, without modifying anything."""
    data = data.resolve()
    todo: list[Candidate] = []
    # Library copies are only expendable after definitive Telegram message ID.
    for row in db.execute("""SELECT sha256, path, size_bytes, file_name
                             FROM artifacts
                             WHERE state='posted' AND target_message IS NOT NULL"""):
        path = _within_regular_file(data, 'library', row['path'])
        if (path and path.name == row['file_name']
                and path.stat().st_size == row['size_bytes']
                and _hash_matches(path, row['sha256'])):
            todo.append(Candidate('posted_library', row['sha256'], path, row['size_bytes']))

    # Only complete, non-volume inputs that have produced >=1 artifact,
    # with ALL their artifacts definitively posted; never touch review inputs.
    for row in db.execute("""SELECT id, kind, file_name, local_path, size_bytes
                             FROM messages
                             WHERE state='complete' AND kind != 'volume'
                               AND local_path IS NOT NULL"""):
        stats = db.execute("""SELECT COUNT(*) AS total,
                   SUM(CASE WHEN state='posted' AND target_message IS NOT NULL
                            THEN 1 ELSE 0 END) AS posted
                   FROM artifacts WHERE source_message=?""", (row['id'],)).fetchone()
        if not stats['total'] or stats['total'] != stats['posted']:
            continue
        path = _within_regular_file(data, 'incoming', row['local_path'])
        expected_parent = (data / 'incoming' / str(row['id'])).resolve()
        if (path and path.parent == expected_parent
                and path.name == row['file_name']
                and path.stat().st_size == row['size_bytes']):
            todo.append(Candidate('complete_input', str(row['id']), path, row['size_bytes']))
    return todo


def candidate_still_safe(db: DB, data: Path, entry: Candidate) -> bool:
    """Recheck only the exact deletion target, preserving all original guards.

    Avoid calling candidates() for every entry: that rehashes all posted EDZ
    repeatedly, resulting in quadratic housekeeping cost on large libraries.
    """
    data = data.resolve()
    if entry.kind == 'posted_library':
        row = db.execute("""SELECT sha256, path, size_bytes, file_name
                            FROM artifacts WHERE sha256=? AND state='posted'
                              AND target_message IS NOT NULL""", (entry.key,)).fetchone()
        if row is None:
            return False
        path = _within_regular_file(data, 'library', row['path'])
        return bool(path and path == entry.path and path.name == row['file_name']
                    and path.stat().st_size == row['size_bytes']
                    and entry.size == row['size_bytes']
                    and _hash_matches(path, row['sha256']))
    if entry.kind == 'complete_input':
        try:
            mid = int(entry.key)
        except (ValueError, TypeError):
            return False
        row = db.execute("""SELECT id, kind, file_name, local_path, size_bytes
                            FROM messages WHERE id=? AND state='complete'
                              AND kind!='volume' AND local_path IS NOT NULL""", (mid,)).fetchone()
        if row is None:
            return False
        stats = db.execute("""SELECT COUNT(*) AS total,
                    SUM(CASE WHEN state='posted' AND target_message IS NOT NULL
                             THEN 1 ELSE 0 END) AS posted
                    FROM artifacts WHERE source_message=?""", (mid,)).fetchone()
        if not stats['total'] or stats['total'] != stats['posted']:
            return False
        path = _within_regular_file(data, 'incoming', row['local_path'])
        parent = (data / 'incoming' / str(mid)).resolve()
        return bool(path and path == entry.path and path.parent == parent
                    and path.name == row['file_name']
                    and path.stat().st_size == row['size_bytes']
                    and entry.size == row['size_bytes'])
    return False


def apply_cleanup(db: DB, data: Path, *,
                  allowed_kinds: frozenset[str] | None = None) -> tuple[int, int]:
    """Manual caller must own lock; worker already owns it.

    `allowed_kinds` is a strict deletion allowlist for automated operation.
    The automatic worker uses ONLY posted_library (never incoming or work).
    """
    db.execute("""CREATE TABLE IF NOT EXISTS cleanup_events (
        kind TEXT NOT NULL, source_key TEXT NOT NULL, path TEXT NOT NULL,
        size_bytes INTEGER NOT NULL, deleted_at REAL NOT NULL,
        PRIMARY KEY (kind, source_key, path))""")
    removed = bytes_freed = 0
    for entry in candidates(db, data):
        if allowed_kinds is not None and entry.kind not in allowed_kinds:
            continue
        # Re-evaluate eligibility in case DB was edited during iteration.
        if not candidate_still_safe(db, data, entry):
            continue
        try:
            entry.path.unlink()
        except FileNotFoundError:
            continue
        db.execute("""INSERT OR IGNORE INTO cleanup_events
                    (kind, source_key, path, size_bytes, deleted_at)
                    VALUES (?,?,?,?,?)""", (entry.kind, entry.key, str(entry.path), entry.size, time.time()))
        removed += 1
        bytes_freed += entry.size
    return removed, bytes_freed


def apply_auto_cleanup(db: DB, data: Path, *, enabled: bool, dry_run: bool) -> tuple[int, int]:
    """Opt-in, never delete during DRY_RUN, only delivered library copies.

    Source archives, multi-volumes, pending artifacts, work/cache, and
    ambiguous Telegram deliveries are deliberately excluded.
    """
    if not enabled or dry_run:
        return (0, 0)
    return apply_cleanup(db, data, allowed_kinds=frozenset({'posted_library'}))
