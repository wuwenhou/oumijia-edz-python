"""Very conservative work-cache cleanup.

Only remove a top-level split/nested work folder when:
- its original single-file Telegram source is still available and matches
  the work folder's content hash prefix;
- the message is complete;
- every output EDZ remaining in the work folder corresponds by SHA-256
  to an artifact from that source;
- all of those artifacts are either safely stored in the library or have
  a definitive posted Telegram message ID.

This DOES NOT clean source files, volume groups or untracked work folders.
"""
from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import re
import shutil
import stat
import time

from .archive import sha256_file
from .db import DB

FOLDER = re.compile(r'^(split-([a-f0-9]{20})|nested-([a-f0-9]{18}))$')


@dataclass(frozen=True)
class WorkCandidate:
    path: Path
    message_id: int
    size: int


def _regular_inside(base: Path, path: Path) -> bool:
    """Reject all path symlinks and insist on a regular file under base."""
    try:
        base = base.absolute()
        path = path.absolute()
        if not path.is_relative_to(base) or path == base:
            return False
        cursor = path
        while cursor != base:
            if cursor.is_symlink():
                return False
            cursor = cursor.parent
        return stat.S_ISREG(path.stat().st_mode)
    except (ValueError, OSError):
        return False


def _scan_tree(folder: Path) -> tuple[list[Path], int] | None:
    """Reject links, special files, and anything other than a normal tree."""
    found: list[Path] = []
    size = 0
    try:
        for root, dirs, files in os.walk(folder, topdown=True, followlinks=False):
            for name in dirs:
                p = Path(root) / name
                if p.is_symlink() or not stat.S_ISDIR(p.stat().st_mode):
                    return None
            for name in files:
                p = Path(root) / name
                if p.is_symlink() or not stat.S_ISREG(p.stat().st_mode):
                    return None
                found.append(p)
                size += p.stat().st_size
                if len(found) > 200000:
                    return None
        return found, size
    except OSError:
        return None


def work_candidates(db: DB, data: Path, *, only_folder: Path | None = None) -> list[WorkCandidate]:
    data = data.resolve()
    work = data / 'work'
    incoming = data / 'incoming'
    library = data / 'library'
    if work.is_symlink() or not work.is_dir():
        return []

    candidates: list[WorkCandidate] = []
    for folder in work.iterdir():
        if only_folder is not None and folder != only_folder:
            continue
        match = FOLDER.fullmatch(folder.name)
        if not match or folder.is_symlink() or not folder.is_dir():
            continue
        # Use only exact top-level work entries; do not resolve arbitrary paths.
        if folder.parent != work:
            continue
        prefix = match.group(2) or match.group(3)
        src_kind = 'split' if match.group(2) else 'nested'
        contents = _scan_tree(folder)
        if not contents:
            continue
        files, size = contents
        output_edzs = [f for f in files if f.suffix.lower() == '.edz']
        if not output_edzs:
            continue

        # Only source messages that finished processing; ignore volume parts.
        messages = db.execute("""SELECT id,kind,file_name,size_bytes,local_path
                                 FROM messages WHERE state='complete'
                                   AND kind IN ('edz','archive','volume') AND local_path IS NOT NULL""").fetchall()
        for msg in messages:
            if src_kind == 'split' and msg['kind'] != 'edz':
                continue
            if msg['kind'] == 'volume':
                # First part is the actual processed archive identity.
                first = db.execute('SELECT volume_index,group_name FROM messages WHERE id=?',
                                   (msg['id'],)).fetchone()
                if not first or first['volume_index'] != 1:
                    continue
                members = db.execute("SELECT state FROM messages WHERE group_name=? AND kind='volume'",
                                     (first['group_name'],)).fetchall()
                if any(member['state'] != 'complete' for member in members):
                    continue
            source = Path(msg['local_path'])
            if not _regular_inside(incoming, source):
                continue
            if source.parent != incoming / str(msg['id']) or source.name != msg['file_name']:
                continue
            if source.stat().st_size != msg['size_bytes']:
                continue
            if not sha256_file(source).startswith(prefix):
                continue

            artifacts = db.execute('SELECT * FROM artifacts WHERE source_message=?', (msg['id'],)).fetchall()
            if not artifacts:
                continue
            registered = {a['sha256'] for a in artifacts}
            if any(sha256_file(f) not in registered for f in output_edzs):
                continue
            safe = True
            for artifact in artifacts:
                if artifact['state'] == 'posted' and artifact['target_message'] is not None:
                    # The recorded delivery is final. Local EDZ may have been cleaned.
                    continue
                if artifact['state'] not in ('review','ready'):
                    safe = False
                    break
                path = Path(artifact['path'])
                if (not _regular_inside(library, path)
                        or path.stat().st_size != artifact['size_bytes']
                        or sha256_file(path) != artifact['sha256']):
                    safe = False
                    break
            if safe:
                candidates.append(WorkCandidate(folder, msg['id'], size))
            break
    return candidates


def apply_work_cleanup(db: DB, data: Path) -> tuple[int, int]:
    """Delete fully verified caches; caller MUST hold edzbot.lock."""
    db.execute("""CREATE TABLE IF NOT EXISTS cleanup_events (
        kind TEXT NOT NULL, source_key TEXT NOT NULL, path TEXT NOT NULL,
        size_bytes INTEGER NOT NULL, deleted_at REAL NOT NULL,
        PRIMARY KEY (kind, source_key, path))""")
    removed = freed = 0
    for item in work_candidates(db, data):
        # Recheck before every deletion. Never follow links.
        if item not in work_candidates(db, data, only_folder=item.path):
            continue
        shutil.rmtree(item.path)
        db.execute("""INSERT OR IGNORE INTO cleanup_events
                     (kind,source_key,path,size_bytes,deleted_at)
                     VALUES (?,?,?,?,?)""", ('complete_work', str(item.message_id), str(item.path), item.size, time.time()))
        removed += 1
        freed += item.size
    return removed, freed
