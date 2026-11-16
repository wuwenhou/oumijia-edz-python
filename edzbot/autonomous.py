"""Fail-closed, sequential automatic intake from an operator-created Telegram CSV plan.

Only 'candidate_single' and contiguous, unique 'review_multivolume' archive groups
are eligible. A source may contain multiple artifacts, but no second source is
admitted while any complete-source EDZ is still ready to publish.

Live Telegram uploads are staged, rather than allowed to bypass these gates.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
import hashlib
import os
import shutil
import sqlite3
import time

from .archive import file_kind, safe_basename, sha256_file
from .cleanup import candidates as cleanup_candidates, candidate_still_safe, _within_regular_file
from .plan_queue import _read_csv

GIB = 1024 ** 3
IN_PROGRESS = ('new', 'retry', 'downloading', 'downloaded', 'processing')


@dataclass(frozen=True)
class Part:
    message_id: int
    file_name: str
    kind: str
    size_bytes: int
    group_name: str | None
    volume_index: int | None


@dataclass(frozen=True)
class Source:
    group_key: str
    members: tuple[Part, ...]
    size_bytes: int
    multipart: bool


def _catalog_sources(cfg) -> list[Source]:
    base = cfg.data / 'catalog'
    plan = _read_csv(base / 'edz_processing_plan.csv',
                     {'name', 'kind', 'message_ids', 'size_bytes', 'decision', 'parts'})
    raw = _read_csv(base / 'telegram_edz_catalog.csv',
                    {'filename', 'message_id', 'size_bytes'})
    catalog = {}
    names = Counter()
    for row in raw:
        try:
            mid, size = int(row['message_id']), int(row['size_bytes'])
            name = row['filename']
            safe_basename(name)
            if mid <= 0 or size <= 0 or name != safe_basename(name):
                continue
        except (ValueError, KeyError, TypeError):
            continue
        if mid in catalog:  # duplicate message ID in CSV invalidates identity
            catalog[mid] = None
        else:
            catalog[mid] = (name, size)
        names[name.casefold()] += 1

    found: list[Source] = []
    for row in plan:
        kind = row.get('kind')
        decision = row.get('decision')
        if not ((kind == 'single' and decision == 'candidate_single') or
                (kind == 'multi' and decision == 'review_multivolume')):
            continue
        try:
            name = row['name']
            if safe_basename(name) != name:
                continue
            size = int(row['size_bytes'])
            mids = [int(x) for x in row['message_ids'].split(';')]
            if size <= 0 or size > cfg.max_group_bytes or len(mids) != int(row['parts']):
                continue
            if not mids or len(set(mids)) != len(mids) or min(mids) <= 0:
                continue
            parts = []
            if kind == 'single':
                if len(mids) != 1 or names[name.casefold()] != 1:
                    continue
                actual = catalog.get(mids[0])
                kind_in, _, _ = file_kind(name)
                if kind_in not in ('edz', 'archive') or actual != (name, size):
                    continue
                parts = [Part(mids[0], name, kind_in, size, None, None)]
            else:
                for mid in mids:
                    actual = catalog.get(mid)
                    if actual is None:
                        break
                    filename, part_size = actual
                    part_kind, base_name, index = file_kind(filename)
                    if (part_kind != 'volume' or base_name.casefold() != name.casefold()
                            or names[filename.casefold()] != 1):
                        break
                    parts.append(Part(mid, filename, 'volume', part_size,
                                      base_name.casefold(), index))
                if len(parts) != len(mids):
                    continue
                indexes = sorted(x.volume_index for x in parts)
                if indexes != list(range(1, len(parts)+1)):
                    continue
                if sum(x.size_bytes for x in parts) != size:
                    continue
                parts.sort(key=lambda part: part.volume_index)
            found.append(Source(f'{kind}:{name.casefold()}', tuple(parts), size,
                                kind == 'multi'))
        except (TypeError, KeyError, ValueError, AttributeError):
            continue
    return found


def _live_sources(db, cfg) -> list[Source]:
    rows = list(db.execute("""SELECT * FROM messages WHERE state='staged' AND id > 0
                              ORDER BY changed_at,id"""))
    singles, volumes = [], defaultdict(list)
    for row in rows:
        if row['size_bytes'] <= 0 or row['size_bytes'] > cfg.max_group_bytes:
            continue
        try:
            if safe_basename(row['file_name']) != row['file_name']:
                continue
            kind, base, index = file_kind(row['file_name'])
        except ValueError:
            continue
        part = Part(row['id'], row['file_name'], kind, row['size_bytes'],
                    base.casefold() if base else None, index)
        if kind in ('edz', 'archive'):
            singles.append(Source(f'live:{row["id"]}', (part,), part.size_bytes, False))
        elif kind == 'volume':
            volumes[part.group_name].append((part, row['changed_at']))
    # Live multipart is best-effort discovery only: wait a full quiet period,
    # require an unambiguous numbered series, then rely on `7z t` after download.
    for group, entries in volumes.items():
        parts = [p for p, _ in entries]
        indexes = sorted(p.volume_index for p in parts)
        if (indexes != list(range(1, len(parts)+1))
                or time.time() - max(t for _, t in entries) < cfg.volume_quiet):
            continue
        if any(p.size_bytes <= 0 for p in parts):
            continue
        size = sum(p.size_bytes for p in parts)
        if size > cfg.max_group_bytes:
            continue
        singles.append(Source('live-multi:' + group,
                              tuple(sorted(parts, key=lambda p: p.volume_index)),
                              size, True))
    return singles


def select_next_auto(db, cfg) -> tuple[Source | None, str]:
    if not cfg.auto_mode:
        return None, 'AUTO_MODE disabled'
    if cfg.backfill or cfg.dry_run or cfg.auto_plan_queue or cfg.allow_unverified:
        return None, 'Unsafe automatic configuration; refusing source intake'
    if getattr(cfg, 'continuous_pipeline', False):
        from .pipeline_parallel import admission_reason
        blocked = admission_reason(db, cfg)
        if blocked:
            return None, blocked
    else:
        active = db.execute('SELECT COUNT(*) FROM messages WHERE state IN (?,?,?,?,?)',
                            IN_PROGRESS).fetchone()[0]
        if active:
            return None, f'waiting: {active} source parts currently queued/processing'
        ready = db.execute("""SELECT COUNT(*) FROM artifacts a
              JOIN messages m ON m.id=a.source_message
              WHERE a.state IN ('ready','review','posting') AND m.state='complete'""").fetchone()[0]
        if ready:
            return None, f'unpublished: {ready} artifacts must finish before the next source'

    # The live queue does not override catalog eligibility for historical posts.
    planned = _catalog_sources(cfg)
    planned.sort(key=lambda s: (s.multipart, s.size_bytes, s.group_key))
    options = planned + sorted(_live_sources(db, cfg), key=lambda s:(s.multipart, s.size_bytes))
    disk_free = shutil.disk_usage(cfg.data).free
    no_space = 0
    # Count extra workspace reserved for all in-flight source groups. Their
    # downloaded bytes already consume disk; this is additional processing slack.
    additional_reserve = 0
    if getattr(cfg, 'continuous_pipeline', False):
        from .pipeline_parallel import active_source_groups
        for size in active_source_groups(db).values():
            additional_reserve += max(GIB, 2 * size)
    for group in options:
        if any(db.message(p.message_id) and db.message(p.message_id)['state'] != 'staged'
               for p in group.members):
            continue
        # Existing partially indexed group is not safe to adopt.
        indexed = [db.message(p.message_id) for p in group.members]
        if any(x is not None for x in indexed) and not all(x is not None and x['state']=='staged' for x in indexed):
            continue
        if not group.members or group.size_bytes > cfg.max_group_bytes:
            continue
        # Conservative compressed-source *3 headroom; actual decompression gets
        # a second hard guard in Pipeline after archive listing is known.
        workspace = max(GIB, 3 * group.size_bytes)
        if disk_free < cfg.reserve_bytes + workspace + additional_reserve:
            no_space += 1
            continue
        return group, 'catalog/live metadata verified, whole-group capacity preflight passed'
    if no_space:
        return None, f'Insufficient disk headroom for {no_space} eligible group(s); reserve preserved'
    return None, 'No remaining eligible source groups in catalog or live staging'


def enqueue_next_auto(db, cfg) -> tuple[Source | None, str]:
    source, reason = select_next_auto(db, cfg)
    if source is None:
        return None, reason
    db.execute('SAVEPOINT auto_queue')
    try:
        # One source group is registered as an atomic unit; never partially
        # register a multipart archive based on incomplete metadata.
        for part in source.members:
            old = db.message(part.message_id)
            if old and old['state'] == 'staged':
                if (old['file_name'] != part.file_name or old['size_bytes'] != part.size_bytes
                        or old['kind'] != part.kind):
                    raise ValueError('Live staged file metadata changed')
                db.change_message(part.message_id, 'new')
            elif old is None:
                db.add_message(part.message_id, part.file_name, part.kind,
                               part.size_bytes, part.group_name, part.volume_index)
                if db.execute('SELECT changes()').fetchone()[0] != 1:
                    raise ValueError('Source already queued concurrently')
            else:
                raise ValueError('Source state changed during intake')
        db.execute('RELEASE SAVEPOINT auto_queue')
    except Exception:
        db.execute('ROLLBACK TO SAVEPOINT auto_queue')
        db.execute('RELEASE SAVEPOINT auto_queue')
        raise
    return source, reason


def clean_safe_auto_sources(db, data: Path) -> tuple[int, int]:
    """Delete only complete nonvolume sources that are fully delivered.

    Work-cache must be cleaned FIRST. Keep any original source if a work folder
    still matches its digest, so unresolved temporary outputs cannot be orphaned.
    """
    data = data.resolve()
    work = data / 'work'
    removed = freed = 0
    db.execute("""CREATE TABLE IF NOT EXISTS cleanup_events (
        kind TEXT NOT NULL, source_key TEXT NOT NULL, path TEXT NOT NULL,
        size_bytes INTEGER NOT NULL, deleted_at REAL NOT NULL,
        PRIMARY KEY (kind, source_key, path))""")
    for candidate in cleanup_candidates(db, data):
        if candidate.kind != 'complete_input':
            continue
        original = _within_regular_file(data, 'incoming', str(candidate.path))
        if not original or not original.is_file():
            continue
        # Hash is an identity guard for recognizing residual work/cache.
        digest = sha256_file(original)
        if ((work / ('nested-' + digest[:18])).exists()
                or (work / ('split-' + digest[:20])).exists()):
            continue
        # Do not remove paths if their metadata changed during this pass.
        if not candidate_still_safe(db, data, candidate):
            continue
        try:
            original.unlink()
        except FileNotFoundError:
            continue
        db.execute('''INSERT OR IGNORE INTO cleanup_events
            (kind,source_key,path,size_bytes,deleted_at) VALUES (?,?,?,?,?)''',
            ('auto_complete_input', candidate.key, str(original), candidate.size, time.time()))
        removed += 1
        freed += candidate.size
    count_volume, bytes_volume = clean_safe_auto_volumes(db, data)
    return removed + count_volume, freed + bytes_volume


def clean_safe_auto_volumes(db, data: Path) -> tuple[int, int]:
    """Delete completed, fully posted multipart sources and hardlink assembly.

    Reject incomplete groups, unknown extra files, symlinks, and remaining work
    caches. Telegram originals still exist for redownload.
    """
    data = data.resolve()
    incoming, volumes, work = (data / 'incoming', data / 'volumes', data / 'work')
    from .work_cleanup import _regular_inside
    removed = freed = 0
    groups = db.execute("""SELECT DISTINCT group_name FROM messages
                        WHERE kind='volume' AND state='complete'
                          AND group_name IS NOT NULL""").fetchall()
    for rec in groups:
        group = rec['group_name']
        rows = list(db.execute("""SELECT id,file_name,local_path,size_bytes,volume_index,state
                              FROM messages WHERE kind='volume' AND group_name=?
                              ORDER BY volume_index""", (group,)))
        if not rows or any(r['state'] != 'complete' for r in rows):
            continue
        if [r['volume_index'] for r in rows] != list(range(1,len(rows)+1)):
            continue
        first_id = rows[0]['id']
        status = db.execute("""SELECT COUNT(*) total,
                       SUM(CASE WHEN state='posted' AND target_message IS NOT NULL
                           THEN 1 ELSE 0 END) delivered
                       FROM artifacts WHERE source_message=?""", (first_id,)).fetchone()
        if not status['total'] or status['total'] != status['delivered']:
            continue
        first = Path(rows[0]['local_path'] or '')
        if not _regular_inside(incoming, first):
            continue
        digest = sha256_file(first)
        if ((work / ('nested-' + digest[:18])).exists()
                or (work / ('split-' + digest[:20])).exists()):
            continue
        assembly = volumes / hashlib.sha256(group.encode()).hexdigest()[:20]
        if assembly.is_symlink() or (assembly.exists() and not assembly.is_dir()):
            continue
        # Every part is accounted for, with no hidden/untracked files in assembly.
        names = {r['file_name'] for r in rows}
        if assembly.is_dir() and set(x.name for x in assembly.iterdir()) != names:
            continue
        source_paths = []
        validated = True
        for r in rows:
            p = Path(r['local_path'] or '')
            if (not _regular_inside(incoming, p)
                    or p.parent != incoming / str(r['id'])
                    or p.name != r['file_name'] or p.stat().st_size != r['size_bytes']):
                validated = False
                break
            source_paths.append(p)
            if assembly.is_dir():
                copy = assembly / r['file_name']
                if (not _regular_inside(volumes, copy)
                        or copy.stat().st_size != r['size_bytes']):
                    validated = False
                    break
                if not os.path.samefile(p, copy) and sha256_file(p) != sha256_file(copy):
                    validated = False
                    break
        if not validated:
            continue
        if assembly.is_dir():
            for p in assembly.iterdir():
                p.unlink()
            assembly.rmdir()
        for r, p in zip(rows, source_paths):
            p.unlink()
            db.execute('''INSERT OR IGNORE INTO cleanup_events
               (kind,source_key,path,size_bytes,deleted_at) VALUES (?,?,?,?,?)''',
               ('auto_complete_volume',str(r['id']),str(p),r['size_bytes'],time.time()))
            removed += 1
            freed += r['size_bytes']
    return removed, freed
