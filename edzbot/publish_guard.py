"""FloodWait publisher guard and fail-closed Telegram destination reconciliation.

Never re-queue an ambiguous Telegram delivery until the destination history has
been read completely and the caption SHA-256 has been compared. This module
only examines artifacts HELD for FloodWait; serious errors remain untouched.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
import re
import time

from .name_guard import normalize_edz_name

log = logging.getLogger(__name__)
_SHA_RE = re.compile(r'(?m)^SHA-256:\s*([0-9a-fA-F]{64})\s*$')
_FILE_RE = re.compile(r'(?m)^File:\s*(.+?)\s*$')
RECON_CURSOR = 'dest_reconcile_cursor'
RECON_FOUND = 'dest_reconcile_found'
RECON_DONE = 'dest_reconcile_done'
RECON_WATERMARK = 'dest_reconcile_watermark'


def flood_seconds_from_message(message: str) -> int | None:
    """Recognize only Telegram's explicit SendMediaRequest wait, never generic waits."""
    m = re.search(r'A wait of (\d{1,6}) seconds is required \(caused by SendMediaRequest\)',
                  str(message), flags=re.IGNORECASE)
    return int(m.group(1)) if m else None


def flood_error(error: str | None) -> bool:
    value = (error or '').lower()
    return ('a wait of ' in value and 'seconds' in value and
            ('sendmediarequest' in value or 'floodwait' in value)) or value.startswith('floodwait send:') or bool(re.match(r'^floodwait\s+\d+s', value))


def held_flood_rows(db):
    return [r for r in db.execute("SELECT * FROM artifacts WHERE state='held' AND target_message IS NULL")
            if flood_error(r['error'])]


def parse_destination_caption(text: str) -> tuple[str | None, str | None]:
    """Return (normalized file name, lowercase SHA-256) from an OUMIJIA caption."""
    name = _FILE_RE.search(text or '')
    sha = _SHA_RE.search(text or '')
    return (normalize_edz_name(name.group(1)) if name else None,
            sha.group(1).lower() if sha else None)


def reconcilable_rows(db) -> list[dict]:
    """Held rows from BOTH the legacy publisher and Upload Engine V2."""
    rows: list[dict] = []
    for r in held_flood_rows(db):
        rows.append({'source': 'artifact', 'key': r['sha256'], 'sha': r['sha256'].lower(),
                     'norm_name': normalize_edz_name(r['file_name']),
                     'size': int(r['size_bytes'])})
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='upload_jobs'").fetchone():
        for r in db.execute("""SELECT normalized_name,sha256,file_name,size_bytes,error
                               FROM upload_jobs WHERE state='held' AND target_message IS NULL"""):
            err = (r['error'] or '').lower()
            if not (flood_error(err) or 'reconciliation' in err or 'ambiguous' in err):
                continue
            rows.append({'source': 'v2_job', 'key': r['normalized_name'],
                         'sha': r['sha256'].lower(),
                         'norm_name': normalize_edz_name(r['file_name']),
                         'size': int(r['size_bytes'])})
    return rows


def has_held_reconcilable(db) -> bool:
    return bool(reconcilable_rows(db))


def cooldown_remaining(db, *, now: float | None = None) -> float:
    if now is None:
        now = time.time()
    try:
        deadline = float(db.value('publisher_flood_cooldown_until', '0'))
    except (ValueError, TypeError):
        return 0.0
    return max(0.0, deadline - now)


def set_cooldown(db, seconds: float, *, now: float | None = None, buffer: float = 60.0) -> float:
    if now is None:
        now = time.time()
    until = max(now + max(float(seconds), 0.0) + buffer,
                now + cooldown_remaining(db, now=now))
    db.set('publisher_flood_cooldown_until', str(until))
    return until


def next_send_remaining(db, *, now: float | None = None) -> float:
    if now is None:
        now = time.time()
    try:
        deadline = float(db.value('publisher_next_send_at', '0'))
    except (ValueError, TypeError):
        deadline = 0.0
    return max(cooldown_remaining(db, now=now), max(0, deadline - now))


def schedule_next_send(db, seconds: float):
    db.set('publisher_next_send_at', str(time.time() + max(0.0, seconds)))


def _load_checkpoint(db) -> tuple[int, dict, int]:
    try:
        cursor = int(db.value(RECON_CURSOR, '0') or 0)
    except (TypeError, ValueError):
        cursor = 0
    try:
        found = {tuple(x[0]): int(x[1]) for x in json.loads(db.value(RECON_FOUND, '[]') or '[]')}
    except (TypeError, ValueError):
        found = {}
    try:
        watermark = int(db.value(RECON_WATERMARK, '0') or 0)
    except (TypeError, ValueError):
        watermark = 0
    return cursor, found, watermark


def _save_checkpoint(db, cursor: int, found: dict) -> None:
    db.set(RECON_CURSOR, str(cursor))
    db.set(RECON_FOUND, json.dumps([[list(k), v] for k, v in found.items()]))


def _clear_checkpoint(db) -> None:
    db.execute('DELETE FROM settings WHERE key IN (?,?,?)',
               (RECON_CURSOR, RECON_FOUND, RECON_DONE))


async def reconcile_held(db, client, target, *, page_size: int = 1000,
                         max_pages: int | None = None) -> dict:
    """Resumable, fail-closed destination reconciliation for V1 + V2.

    Scans the destination in pages, persisting an offset cursor and the matches
    already seen after EVERY page. Only when the scan reaches the end (or the
    stored watermark) are matches applied -- in one place, per held row only.
    An interrupted/flooded scan applies NOTHING and can be resumed safely.
    Absent evidence never releases a held job (no 'probably not posted').
    """
    rows = reconcilable_rows(db)
    if not rows:
        return {'complete': True, 'scanned': 0, 'posted_artifact': 0,
                'posted_v2': 0, 'left_held': 0, 'pages': 0}

    targets: dict[tuple[str, str, int], list[dict]] = {}
    for r in rows:
        targets.setdefault((r['sha'], r['norm_name'], r['size']), []).append(r)

    cursor, found, watermark = _load_checkpoint(db)
    scanned = pages = highest = 0
    complete = False
    try:
        while True:
            page = []
            async for msg in client.iter_messages(target, offset_id=cursor or None,
                                                  limit=page_size):
                page.append(msg)
            if not page:
                complete = True
                break
            for msg in page:
                scanned += 1
                mid = int(msg.id)
                highest = max(highest, mid)
                if watermark and mid <= watermark:
                    complete = True
                    break
                if not getattr(msg, 'document', None):
                    continue
                norm, sha = parse_destination_caption(getattr(msg, 'message', '') or '')
                if not norm or not sha:
                    continue
                try:
                    size = int(getattr(msg.document, 'size', 0) or 0)
                except (TypeError, ValueError):
                    continue
                for t in targets.get((sha, norm, size), []):
                    key = (t['source'], t['key'])
                    prev = found.get(key)
                    if prev is not None and prev != mid:
                        raise RuntimeError(
                            f'Conflicting Telegram message ids for {t["source"]}:{t["key"]}; '
                            'no recovery performed')
                    found[key] = mid
            if complete:
                break
            oldest = min(int(m.id) for m in page)
            # No backward progress would spin forever on a broken client.
            if cursor and oldest >= cursor:
                raise RuntimeError('Destination pagination made no progress; no recovery performed')
            cursor = oldest
            _save_checkpoint(db, cursor, found)
            pages += 1
            if max_pages is not None and pages >= max_pages:
                return {'complete': False, 'scanned': scanned, 'pages': pages,
                        'posted_artifact': 0, 'posted_v2': 0, 'left_held': len(rows),
                        'paused': True}
    except Exception:
        # Keep whatever was learned so a later run resumes from this page.
        _save_checkpoint(db, cursor, found)
        raise

    if not complete:
        _save_checkpoint(db, cursor, found)
        return {'complete': False, 'scanned': scanned, 'pages': pages,
                'posted_artifact': 0, 'posted_v2': 0, 'left_held': len(rows)}

    posted_artifact = posted_v2 = 0
    for (source, key), mid in found.items():
        if source == 'artifact':
            row = db.execute("SELECT state FROM artifacts WHERE sha256=?", (key,)).fetchone()
            if row and row['state'] == 'held':
                db.update_artifact(key, 'posted', target_message=mid, error=None)
                posted_artifact += 1
        else:
            row = db.execute("SELECT state FROM upload_jobs WHERE normalized_name=?", (key,)).fetchone()
            if row and row['state'] == 'held':
                now = time.time()
                db.execute('BEGIN IMMEDIATE')
                try:
                    db.execute("UPDATE upload_jobs SET state='posted',target_message=?,error=NULL,"
                               "updated_at=? WHERE normalized_name=? AND state='held'",
                               (mid, now, key))
                    job_sha = db.execute(
                        "SELECT sha256 FROM upload_jobs WHERE normalized_name=?",
                        (key,)).fetchone()[0]
                    if db.execute("SELECT 1 FROM pragma_table_info('artifacts') "
                                  "WHERE name='normalized_name'").fetchone():
                        db.execute("UPDATE artifacts SET state='posted',target_message=?,"
                                   "error=NULL,normalized_name=?,updated_at=? "
                                   "WHERE sha256=? AND state='held'",
                                   (mid, key, now, job_sha))
                    else:
                        db.execute("UPDATE artifacts SET state='posted',target_message=?,"
                                   "error=NULL,updated_at=? WHERE sha256=? AND state='held'",
                                   (mid, now, job_sha))
                    db.execute("INSERT INTO upload_events(at,normalized_name,kind,detail)"
                               " VALUES(?,?,?,?)", (now, key, 'reconciled_ack', f'message={mid}'))
                    db.execute('COMMIT')
                except Exception:
                    db.execute('ROLLBACK')
                    raise
                posted_v2 += 1

    _clear_checkpoint(db)
    db.set(RECON_WATERMARK, str(highest or watermark))
    return {'complete': True, 'scanned': scanned, 'pages': pages,
            'posted_artifact': posted_artifact, 'posted_v2': posted_v2,
            'left_held': len(rows) - posted_artifact - posted_v2}


async def reconcile_flood_held(db, client, target, *, scan_limit: int = 25000):
    """Backwards-compatible entry point; now paginated and resumable."""
    return await reconcile_held(db, client, target, page_size=min(1000, max(1, scan_limit)))
