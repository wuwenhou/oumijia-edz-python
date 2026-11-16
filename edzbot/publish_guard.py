"""FloodWait publisher guard and fail-closed Telegram destination reconciliation.

Never re-queue an ambiguous Telegram delivery until the destination history has
been read completely and the caption SHA-256 has been compared. This module
only examines artifacts HELD for FloodWait; serious errors remain untouched.
"""
from __future__ import annotations

import logging
from pathlib import Path
import re
import time

log = logging.getLogger(__name__)
_SHA_RE = re.compile(r'(?m)^SHA-256:\s*([0-9a-fA-F]{64})\s*$')


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


async def reconcile_flood_held(db, client, target, *, scan_limit: int = 25000):
    """Read the *entire* destination history before requeuing FloodWait holds.

    If Telegram fails, or scan_limit would truncate the history, leave ALL
    pending holds untouched. A found matching caption is marked posted. An
    absent SHA is released only when its local verified-source output exists;
    SHA content is verified again by publish_one immediately before send.
    """
    rows = held_flood_rows(db)
    if not rows:
        return {'scanned': 0, 'posted': 0, 'ready': 0, 'left_held': 0}

    known = {r['sha256'].lower(): r for r in rows}
    found: dict[str, int] = {}
    scanned = 0
    async for msg in client.iter_messages(target):
        scanned += 1
        if scanned > scan_limit:
            raise RuntimeError(f'Destination history exceeds safe scan limit of {scan_limit}; no recovery performed')
        if not getattr(msg, 'document', None):
            continue
        match = _SHA_RE.search(getattr(msg, 'message', '') or '')
        if not match:
            continue
        sha = match.group(1).lower()
        if sha not in known:
            continue
        size = getattr(getattr(msg, 'document', None), 'size', None)
        if size is not None and int(size) != int(known[sha]['size_bytes']):
            log.warning('SHA caption matches but target size differs: %s', sha)
            continue
        if sha in found and found[sha] != msg.id:
            raise RuntimeError(f'Duplicate SHA found in destination history: {sha}; no recovery performed')
        found[sha] = int(msg.id)

    posted = ready = 0
    for sha, row in known.items():
        latest = db.execute('SELECT state,error FROM artifacts WHERE sha256=?', (sha,)).fetchone()
        if not latest or latest['state'] != 'held' or not flood_error(latest['error']):
            continue
        if sha in found:
            db.update_artifact(sha, 'posted', target_message=found[sha], error=None)
            posted += 1
            continue
        file = Path(row['path'])
        source = db.execute('SELECT state FROM messages WHERE id=?', (row['source_message'],)).fetchone()
        if (source and source['state'] == 'complete' and file.is_file()
                and file.stat().st_size == row['size_bytes']):
            # publish_one will verify SHA against file bytes again before send.
            db.update_artifact(sha, 'ready', error=None)
            ready += 1
    return {'scanned': scanned, 'posted': posted, 'ready': ready,
            'left_held': len(known) - posted - ready}
