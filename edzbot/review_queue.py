"""OUMIJIA v16.3.2: safe self-maintaining EDZ error review and repair queue.

A review export is a separate hardlink (or verified copy), NEVER a destructive
move of a still-held pipeline input. Only confirmed replacements/reconciliation
remove review links. SHA-256 identity applies throughout; send retries remain
owned by the existing Telegram publisher/reconciler.
"""
from __future__ import annotations

import csv
import errno
import logging
import os
from pathlib import Path
import re
import shutil
import sqlite3
import tempfile
import time

from .archive import safe_basename, sha256_file
from .db import DB

LOG = logging.getLogger(__name__)
ROOT_NAME = 'EDZ_ERRORS'
REPAIR_RE = re.compile(r'(?<!\w)FIX_(SOURCE|ARTIFACT):(-?[0-9]+|[0-9a-fA-F]{64})(?!\w)', re.I)
VALID_SHA = re.compile(r'[a-f0-9]{64}\Z')


class ReviewDB(DB):
    """Dedicated DB connection for the background review worker only.

    SQLite allows only one writer at a time, even in WAL mode. Publisher owns
    the critical path; review cache writes are allowed to wait and retry.
    No Publisher, scheduler, or Telegram settings are changed by this class.
    """
    _RETRIES = 8

    def __init__(self, path: Path):
        super().__init__(path)
        # The project's default connection uses 5 s. Review operations can be
        # slower without affecting sends, so allow more time to acquire a lock.
        self.conn.execute('PRAGMA busy_timeout=7000')

    def execute(self, sql: str, args: tuple = ()):
        for attempt in range(self._RETRIES + 1):
            try:
                return super().execute(sql, args)
            except sqlite3.OperationalError as exc:
                code = getattr(exc, 'sqlite_errorcode', None)
                busy_codes = (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)
                if code not in busy_codes and 'locked' not in str(exc).lower():
                    raise
                if attempt == self._RETRIES:
                    raise
                delay = min(0.10 * (2 ** attempt), 1.5)
                LOG.debug('Auto Review SQLite busy, retry %d/%d in %.2fs',
                          attempt + 1, self._RETRIES, delay)
                time.sleep(delay)


def repair_tag(message) -> tuple[str, str] | None:
    caption = str(getattr(message, 'raw_text', '') or '')
    match = REPAIR_RE.search(caption)
    if not match:
        return None
    kind = match.group(1).lower()
    key = match.group(2).lower() if kind == 'artifact' else str(int(match.group(2)))
    if kind == 'artifact' and not VALID_SHA.fullmatch(key):
        return None
    if kind == 'source' and len(key.lstrip('-')) > 12:
        return None
    return kind, key


def schema(db: DB):
    db.execute('''CREATE TABLE IF NOT EXISTS review_objects (
        sha256 TEXT PRIMARY KEY, export_path TEXT NOT NULL,
        byte_size INTEGER NOT NULL, created_at REAL NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS review_items (
        kind TEXT NOT NULL, item_key TEXT NOT NULL, sha256 TEXT NOT NULL,
        original_path TEXT NOT NULL, filename TEXT NOT NULL,
        PRIMARY KEY(kind, item_key))''')
    db.execute('''CREATE TABLE IF NOT EXISTS review_repairs (
        message_id INTEGER PRIMARY KEY, old_kind TEXT NOT NULL,
        old_key TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
        old_sha256 TEXT, old_path TEXT, created_at REAL NOT NULL,
        updated_at REAL NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS review_repair_outputs (
        message_id INTEGER NOT NULL, sha256 TEXT NOT NULL,
        PRIMARY KEY(message_id,sha256))''')
    db.execute('''CREATE TABLE IF NOT EXISTS review_hash_cache (
        kind TEXT NOT NULL, item_key TEXT NOT NULL, path TEXT NOT NULL,
        bytes INTEGER NOT NULL, mtime_ns INTEGER NOT NULL, inode INTEGER NOT NULL,
        sha256 TEXT NOT NULL, checked_at REAL NOT NULL,
        PRIMARY KEY(kind,item_key))''')
    db.execute('''CREATE TABLE IF NOT EXISTS review_events (
        event_type TEXT NOT NULL, item_key TEXT NOT NULL, details TEXT,
        created_at REAL NOT NULL)''')


def register_repair(db: DB, message, new_id: int) -> bool:
    """A user deliberately identifies the original held record in caption.

    Called only from a trusted source-channel event or verified admin inbox.
    This does not skip normal 7z/EDZ validation or authorize any send.
    """
    tag = repair_tag(message)
    if tag is None:
        return False
    kind, key = tag
    schema(db)
    if kind == 'source':
        old = db.execute("SELECT id FROM messages WHERE id=? AND state='held'", (int(key),)).fetchone()
    else:
        old = db.execute("SELECT sha256 FROM artifacts WHERE sha256=? AND state='held'", (key,)).fetchone()
    if old is None:
        LOG.warning('Rejected repair caption: original %s:%s is not held', kind, key)
        return False
    previous = db.execute('''SELECT message_id FROM review_repairs
            WHERE old_kind=? AND old_key=? AND state='pending'
            AND message_id != ?''', (kind, key, new_id)).fetchone()
    if previous:
        LOG.warning('Repair already pending for %s:%s from message %s', kind, key, previous[0])
        return False
    db.execute('''INSERT OR IGNORE INTO review_repairs
        (message_id,old_kind,old_key,state,created_at,updated_at)
        VALUES (?,?,?,'pending',?,?)''', (new_id, kind, key, time.time(), time.time()))
    LOG.info('EDZ repair registered: new #%s -> %s %s', new_id, kind, key)
    return True


def record_output(db: DB, source_id: int, sha: str):
    """Record EVERY structurally verified EDZ, even if SHA was deduped."""
    if not VALID_SHA.fullmatch(sha):
        raise ValueError('Invalid EDZ SHA256 receipt')
    if not getattr(db, '_review_schema_ready', False):
        schema(db)
        db._review_schema_ready = True
    linked = db.execute("SELECT 1 FROM review_repairs WHERE message_id=?", (source_id,)).fetchone()
    if linked:
        db.execute('INSERT OR IGNORE INTO review_repair_outputs VALUES (?,?)', (source_id,sha))


def _regular_file(path: Path, data: Path) -> bool:
    """Do not ingest symlinks or paths from outside the controlled data dir."""
    try:
        root = data.resolve()
        resolved = path.resolve(strict=True)
        if not resolved.is_relative_to(root) or not resolved.is_file():
            return False
        candidate = path
        while candidate != root:
            if candidate.is_symlink() or candidate == candidate.parent:
                return False
            candidate = candidate.parent
        return path.stat().st_size > 0
    except (OSError, RuntimeError, ValueError):
        return False


def _hash_held_cached(db: DB, record: dict, path: Path) -> str:
    # Each held source may be multi-GB. Avoid re-reading its entire contents
    # every five minutes; reverify daily and on metadata changes.
    stat = path.stat()
    row = db.execute('SELECT * FROM review_hash_cache WHERE kind=? AND item_key=?',
                     (record['kind'],record['key'])).fetchone()
    now = time.time()
    if (row and row['path']==str(path) and row['bytes']==stat.st_size
            and row['mtime_ns']==stat.st_mtime_ns and row['inode']==stat.st_ino
            and now-row['checked_at'] < 86400):
        return row['sha256']
    sha = sha256_file(path)
    db.execute('''INSERT INTO review_hash_cache
        (kind,item_key,path,bytes,mtime_ns,inode,sha256,checked_at)
        VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT(kind,item_key) DO UPDATE SET
        path=excluded.path,bytes=excluded.bytes,mtime_ns=excluded.mtime_ns,
        inode=excluded.inode,sha256=excluded.sha256,checked_at=excluded.checked_at''',
        (record['kind'],record['key'],str(path),stat.st_size,
         stat.st_mtime_ns,stat.st_ino,sha,now))
    return sha


def _resolved_path(value: str, data: Path) -> Path:
    p = Path(value)
    return p if p.is_absolute() else data.parent / p


def _unique_export_name(sha: str, file_name: str) -> str:
    # Full digest ensures uniqueness even when filenames are identical.
    name = safe_basename(file_name)
    return sha + '__' + name[:110]


def _persist_link(src: Path, target: Path, sha: str, reserve_bytes: int):
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if target.is_file() and not target.is_symlink() and sha256_file(target) == sha:
            return
        raise ValueError(f'Existing export does not match expected SHA256: {target}')
    tmp = target.with_name('.tmp_' + os.urandom(8).hex())
    try:
        try:
            os.link(src, tmp)
        except OSError as exc:
            if exc.errno not in (errno.EXDEV, errno.EOPNOTSUPP, errno.EPERM, errno.ENOSYS):
                raise
            if shutil.disk_usage(target.parent).free < src.stat().st_size + reserve_bytes:
                raise OSError('Disk reserve would be violated by review copy') from exc
            with src.open('rb') as inp, tmp.open('xb') as out:
                shutil.copyfileobj(inp, out, 4 * 1024 * 1024)
                out.flush()
                os.fsync(out.fileno())
        if sha256_file(tmp) != sha:
            raise ValueError('Review export SHA256 verification failed')
        os.replace(tmp, target)
    finally:
        if tmp.exists():
            tmp.unlink()


def _held_rows(db: DB):
    # Materialize each SELECT before yielding any record. A live SQLite cursor
    # pins a WAL read snapshot: while we hash a large EDZ, the Publisher may
    # commit a write, and the review's next INSERT will fail permanently with
    # SQLITE_BUSY_SNAPSHOT (517). Waiting/retrying cannot fix that snapshot.
    source_rows = list(db.execute("""SELECT id, file_name, local_path, error
            FROM messages WHERE state='held' ORDER BY id"""))
    for r in source_rows:
        yield {'kind': 'source', 'key': str(r['id']), 'name': r['file_name'],
               'path': r['local_path'], 'sha': None, 'error': r['error'] or ''}
    artifact_rows = list(db.execute("""SELECT sha256, source_message, file_name, path, error
            FROM artifacts WHERE state='held' ORDER BY source_message,sha256"""))
    for r in artifact_rows:
        yield {'kind': 'artifact', 'key': r['sha256'], 'name': r['file_name'],
               'path': r['path'], 'sha': r['sha256'], 'error': r['error'] or ''}


def _eligible_repair(db: DB, link) -> bool:
    msg = db.message(link['message_id'])
    if msg is None or msg['state'] != 'complete':
        return False
    # Ledger includes outputs already indexed under an older source: all must
    # have confirmed destination message IDs before old data is retired.
    outputs = list(db.execute('SELECT sha256 FROM review_repair_outputs WHERE message_id=?',
                              (link['message_id'],)))
    if not outputs:
        return False
    for output in outputs:
        delivered = db.execute('''SELECT 1 FROM artifacts WHERE sha256=?
            AND state='posted' AND target_message IS NOT NULL''', (output['sha256'],)).fetchone()
        if not delivered:
            return False
    return True


def _activate_repairs(db: DB, *, max_ready: int):
    # Do not bypass the existing source backlog admission or rate policies.
    backlog = db.execute('''SELECT COUNT(*) FROM artifacts
            WHERE state IN ('ready','review','posting')''').fetchone()[0]
    busy = db.execute('''SELECT COUNT(*) FROM messages
            WHERE state IN ('new','retry','downloading','downloaded','processing')''').fetchone()[0]
    if backlog >= max_ready or busy:
        return
    for r in list(db.execute("SELECT message_id FROM review_repairs WHERE state='pending' ORDER BY created_at LIMIT 1")):
        msg = db.message(r['message_id'])
        if msg and msg['state'] == 'staged':
            db.change_message(r['message_id'], 'new')
            LOG.info('EDZ repair #%d admitted to ordinary download queue', r['message_id'])
            return


def _safe_delete_superseded(db: DB, *, kind: str, key: str, old_path: str, sha: str, data: Path) -> bool:
    if not old_path or not sha or not VALID_SHA.fullmatch(sha):
        return False
    p = _resolved_path(old_path, data)
    # Only known library/incoming originals may be removed. Never touch work,
    # split fragments, sessions, backups, or any arbitrary external file.
    expected_root = (data / ('incoming' if kind == 'source' else 'library')).resolve()
    if not p.is_absolute() or not p.is_relative_to(expected_root):
        return False
    if kind == 'source' and p.parent != expected_root / key:
        return False
    if not p.exists():
        return True
    if not _regular_file(p, data) or sha256_file(p) != sha:
        return False
    shared_msg = db.execute('''SELECT 1 FROM messages WHERE local_path=? AND id!=? LIMIT 1''',
                            (str(p), int(key) if kind == 'source' else 0)).fetchone()
    shared_art = db.execute('''SELECT 1 FROM artifacts WHERE path=? AND sha256!=? LIMIT 1''',
                            (str(p), key if kind == 'artifact' else '')).fetchone()
    if shared_msg or shared_art:
        return False
    p.unlink()
    if kind == 'source':
        db.execute("UPDATE messages SET local_path=NULL WHERE id=? AND state='superseded'", (int(key),))
    LOG.info('Removed superseded original %s', p)
    return True


def _advance_replacements(db: DB, data: Path):
    for link in list(db.execute("SELECT * FROM review_repairs WHERE state IN ('pending','cleanup_pending') ORDER BY created_at")):
        new_id = link['message_id']
        new_msg = db.message(new_id)
        if link['state'] == 'pending':
            if new_msg and new_msg['state'] == 'held':
                db.execute("UPDATE review_repairs SET state='failed',updated_at=? WHERE message_id=?", (time.time(), new_id))
                continue
            if not _eligible_repair(db, link):
                continue
            kind, key = link['old_kind'], link['old_key']
            if kind == 'source':
                old = db.execute('SELECT * FROM messages WHERE id=?', (int(key),)).fetchone()
                if old is None or old['state'] != 'held':
                    db.execute("UPDATE review_repairs SET state='resolved_elsewhere',updated_at=? WHERE message_id=?", (time.time(), new_id))
                    continue
                path = old['local_path'] or ''
                sha = sha256_file(_resolved_path(path, data)) if path and _regular_file(_resolved_path(path, data), data) else ''
                db.change_message(int(key), 'superseded',
                    error=f'Replaced by confirmed Telegram input #{abs(new_id)}; previous error: '+str(old['error'] or '')[:900])
            else:
                old = db.execute('SELECT * FROM artifacts WHERE sha256=?', (key,)).fetchone()
                if old is None or old['state'] != 'held':
                    db.execute("UPDATE review_repairs SET state='resolved_elsewhere',updated_at=? WHERE message_id=?", (time.time(), new_id))
                    continue
                path = old['path'] or ''
                sha = old['sha256']
                db.update_artifact(key, 'superseded', error=f'Replaced by confirmed Telegram input #{abs(new_id)}; previous delivery unconfirmed')
            db.execute('''UPDATE review_repairs SET state='cleanup_pending',old_sha256=?,old_path=?,updated_at=? WHERE message_id=?''',
                       (sha, path, time.time(), new_id))
            db.execute('''INSERT INTO review_events VALUES(?,?,?,?)''',
                       ('confirmed_repair', kind + ':' + key, 'replacement_message=' + str(new_id), time.time()))
            LOG.info('Confirmed repair #%s supersedes %s:%s', new_id, kind, key)
        current = db.execute('SELECT * FROM review_repairs WHERE message_id=?', (new_id,)).fetchone()
        if _safe_delete_superseded(db, kind=current['old_kind'], key=current['old_key'],
                                   old_path=current['old_path'] or '', sha=current['old_sha256'] or '', data=data):
            db.execute("UPDATE review_repairs SET state='done',updated_at=? WHERE message_id=?", (time.time(), new_id))


def _cleanup_confirmed_duplicate_inputs(db: DB, data: Path):
    # A complete EDZ source with ZERO newly attributed artifacts may be an exact
    # duplicate. Remove only if its content is known to be delivered by SHA.
    rows = list(db.execute('''SELECT m.id,m.local_path,m.size_bytes FROM messages m
        WHERE m.state='complete' AND m.kind='edz' AND m.local_path IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM artifacts a WHERE a.source_message=m.id)
        LIMIT 100'''))
    for r in rows:
        p = _resolved_path(r['local_path'], data)
        if p.parent != (data / 'incoming' / str(r['id'])).resolve():
            continue
        if not _regular_file(p, data) or p.stat().st_size != r['size_bytes']:
            continue
        sha = sha256_file(p)
        already_posted = db.execute('''SELECT 1 FROM artifacts WHERE sha256=? AND state='posted'
                              AND target_message IS NOT NULL''', (sha,)).fetchone()
        if not already_posted:
            continue
        # Repair records may still need the file to resolve source identity.
        if db.execute("SELECT 1 FROM review_repairs WHERE message_id=? AND state='pending'", (r['id'],)).fetchone():
            continue
        other = db.execute('SELECT 1 FROM artifacts WHERE path=? LIMIT 1', (str(p),)).fetchone()
        if other:
            continue
        p.unlink()
        db.execute('UPDATE messages SET local_path=NULL WHERE id=? AND state=\'complete\'', (r['id'],))
        db.execute('INSERT INTO review_events VALUES(?,?,?,?)',
                   ('duplicate_source_cleaned', str(r['id']), sha, time.time()))
        LOG.info('Removed confirmed duplicate source #%s (%s)', r['id'], sha)


def _manifest(dest: Path, records: list[dict], db: DB):
    dest.mkdir(parents=True, exist_ok=True)
    path = dest / 'manifest.csv'
    fd, temp = tempfile.mkstemp(prefix='.manifest-', dir=str(dest))
    try:
        with os.fdopen(fd, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(('type','original_id_or_sha','filename','sha256','export_relative_path',
                             'repair_caption','error','status'))
            writer.writerows((r['kind'], r['key'], r['name'], r['sha'], r['export'],
                              r['caption'], r['error'], r['status']) for r in records)
        os.replace(temp, path)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)
    (dest / 'README.txt').write_text(
        'OUMIJIA EDZ ERROR REVIEW\n'
        'FILES: unique SHA256 EDZ source / artifact files. See manifest.csv.\n'
        'Download files to Windows and DO NOT change files on server.\n'
        'To send a repaired EDZ, upload the document to SOURCE_CHAT with the\n'
        'exact repair_caption from manifest.csv (or trusted EDZ_INBOX_CHAT).\n'
        'Normal pipeline processing and verified publication are still required.\n'
        'For ordinary source uploads without FIX_ caption, old errors cannot\n'
        'safely be identified automatically.\n'
        'FloodWait/ambiguous sends may need destination reconciliation, NOT editing.\n'
        'Do not delete this review folder: the worker maintains it.\n', encoding='utf-8')


def run_once(data: Path, *, max_ready: int = 4000, reserve_bytes: int = 10*1024**3,
             export_interval_seconds: int = 0) -> dict:
    """Maintain pending repairs every pass, and optionally refresh Held exports daily.

    The completion timestamp is persisted in SQLite settings, so service restarts
    do not trigger extra full scans. A failed export does not advance the clock.
    ``export_interval_seconds=0`` preserves on-demand/test behavior.
    """
    data = data.resolve()
    dest = data / 'review_exports' / ROOT_NAME
    files = dest / 'FILES'
    dest.mkdir(parents=True, exist_ok=True)
    db = ReviewDB(data / 'state.sqlite3')
    try:
        schema(db)
        _advance_replacements(db, data)
        _activate_repairs(db, max_ready=max_ready)
        _cleanup_confirmed_duplicate_inputs(db, data)
        # Repair reconciliation keeps running every 5 minutes. Exporting can be
        # much more expensive (hashing large Held files), so run only once/24h.
        if export_interval_seconds:
            if not isinstance(export_interval_seconds, int) or export_interval_seconds < 0:
                raise ValueError('Invalid Held export interval')
            now = time.time()
            row = db.execute(
                "SELECT val FROM settings WHERE key='edz_review_export_last_success_v023'"
            ).fetchone()
            try:
                last = float(row['val']) if row else 0.0
            except (ValueError, TypeError, OverflowError):
                last = 0.0
            if last > 0 and 0 <= now - last < export_interval_seconds:
                return {'export_skipped': True,
                        'next_export_in_seconds': int(export_interval_seconds - (now - last))}
        current = []
        for r in _held_rows(db):
            status = 'MISSING'
            sha = r['sha'] or ''
            export = ''
            original = r['path']
            if original:
                p = _resolved_path(original, data)
                if _regular_file(p, data):
                    actual = _hash_held_cached(db, r, p)
                    if not sha or actual == sha:
                        sha = actual
                        status = 'AVAILABLE'
                    else:
                        status = 'HASH_MISMATCH'
            if status == 'AVAILABLE':
                known = db.execute('SELECT export_path FROM review_objects WHERE sha256=?', (sha,)).fetchone()
                if known:
                    target = Path(known['export_path'])
                    # Never trust a mutable SQLite path outside the review vault.
                    if not target.resolve(strict=False).is_relative_to(files.resolve()):
                        status = 'EXPORT_ERROR'
                        LOG.error('Refusing review export outside FILES vault: %s', target)
                        current.append(dict(r, sha=sha, status=status, export='',
                          caption=('FIX_SOURCE:'+r['key'] if r['kind']=='source' else 'FIX_ARTIFACT:'+r['key'])))
                        continue
                    src_stat = _resolved_path(original, data).stat()
                    tgt_stat = target.stat() if target.is_file() and not target.is_symlink() else None
                    same_link = tgt_stat is not None and (src_stat.st_dev,src_stat.st_ino)==(tgt_stat.st_dev,tgt_stat.st_ino)
                    if target.is_file() and not target.is_symlink() and (same_link or sha256_file(target) == sha):
                        export = str(target.relative_to(dest))
                    elif not target.exists():
                        try:
                            _persist_link(_resolved_path(original, data), target, sha, reserve_bytes)
                            export = str(target.relative_to(dest))
                        except (OSError, ValueError) as ex:
                            status = 'EXPORT_ERROR'
                            LOG.exception('EDZ review re-export failed: %s', ex)
                    else:
                        # Fail closed on an existing corrupt/changed review export.
                        LOG.error('Review object changed: %s', target)
                        status = 'EXPORT_ERROR'
                else:
                    target = files / _unique_export_name(sha, r['name'])
                    try:
                        _persist_link(_resolved_path(original, data), target, sha, reserve_bytes)
                        db.execute('''INSERT INTO review_objects VALUES (?,?,?,?)''',
                                   (sha, str(target), target.stat().st_size, time.time()))
                        export = str(target.relative_to(dest))
                    except (OSError, ValueError) as ex:
                        status = 'EXPORT_ERROR'
                        LOG.exception('EDZ review export failed: %s', ex)
            if status == 'AVAILABLE' and export:
                db.execute('''INSERT INTO review_items(kind,item_key,sha256,original_path,filename)
                   VALUES(?,?,?,?,?) ON CONFLICT(kind,item_key) DO UPDATE SET
                   sha256=excluded.sha256,original_path=excluded.original_path,
                   filename=excluded.filename''',
                   (r['kind'], r['key'], sha, original, r['name']))
            caption = ('FIX_SOURCE:'+r['key'] if r['kind']=='source' else 'FIX_ARTIFACT:'+r['key'])
            current.append(dict(r, sha=sha, status=status, export=export, caption=caption))
        active = {(r['kind'], r['key']) for r in current}
        for row in list(db.execute('SELECT kind,item_key FROM review_items')):
            if (row['kind'], row['item_key']) not in active:
                db.execute('DELETE FROM review_items WHERE kind=? AND item_key=?',
                           (row['kind'], row['item_key']))
        for row in list(db.execute('SELECT sha256,export_path FROM review_objects')):
            linked = db.execute('SELECT 1 FROM review_items WHERE sha256=? LIMIT 1', (row['sha256'],)).fetchone()
            if linked:
                continue
            target = Path(row['export_path'])
            if target.is_file() and not target.is_symlink() and target.is_relative_to(files):
                # Review files are never published by the pipeline. The original
                # held record has now been resolved or explicitly superseded.
                if sha256_file(target) == row['sha256']:
                    target.unlink()
                else:
                    LOG.error('Refusing to unlink modified review file %s', target)
                    continue
            db.execute('DELETE FROM review_objects WHERE sha256=?', (row['sha256'],))
        _manifest(dest, current, db)
        if export_interval_seconds:
            db.execute(
                "INSERT INTO settings(key,val) VALUES('edz_review_export_last_success_v023',?) "
                "ON CONFLICT(key) DO UPDATE SET val=excluded.val",
                (str(time.time()),)
            )
        counts = {s: sum(r['status']==s for r in current) for s in ('AVAILABLE','MISSING','EXPORT_ERROR','HASH_MISMATCH')}
        counts['total'] = len(current)
        counts['unique'] = db.execute('SELECT COUNT(*) FROM review_objects').fetchone()[0]
        LOG.info('EDZ review updated: %s at %s', counts, dest)
        return counts
    finally:
        db.close()
