"""Daily incremental source indexing AFTER historical eligible queue is exhausted.

Read-only with respect to already processed source/artifact records. Uses message-id
high-water markers, stored in SQLite; no deletion or history re-backfill.
"""
from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
import time
from zoneinfo import ZoneInfo

ZONE = ZoneInfo('Asia/Ho_Chi_Minh')
NO_MORE = 'No remaining eligible source groups in catalog or live staging'
MODE_KEY = 'edz_source_scan_mode_v15'
HIGH_KEY = 'edz_source_scan_highwater_v15'
NEXT_KEY = 'edz_source_scan_next_at_v15'
LAST_KEY = 'edz_source_scan_last_at_v15'


def is_daily(db) -> bool:
    return db.value(MODE_KEY, 'historical') == 'daily'


def next_local_two(now: float | None = None) -> float:
    """Next 02:00 in Vietnam; never run more often than the daily slot."""
    now = time.time() if now is None else now
    local = datetime.fromtimestamp(now, tz=ZONE)
    next_slot = local.replace(hour=2, minute=0, second=0, microsecond=0)
    if next_slot <= local:
        next_slot += timedelta(days=1)
    return next_slot.timestamp()


def _catalog_highwater(data: Path) -> int:
    """Metadata is the user's existing historical coverage; fail closed if missing."""
    path = data / 'catalog' / 'telegram_edz_catalog.csv'
    if not path.is_file() or path.stat().st_size > 16 * 1024**2:
        raise ValueError('Danh mục nguồn lịch sử chưa tồn tại hoặc quá lớn; chưa chuyển sang quét hằng ngày')
    maximum = 0
    with path.open(encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or 'message_id' not in reader.fieldnames:
            raise ValueError('Thiếu cột message_id trong danh mục lịch sử')
        for row in reader:
            maximum = max(maximum, int(row['message_id']))
    if maximum <= 0:
        raise ValueError('Danh mục lịch sử trống; cần kiểm tra trước khi lập lịch quét')
    return maximum


def enter_daily_if_drained(db, cfg, reason: str, *, now: float | None = None) -> bool:
    if is_daily(db) or not cfg.auto_mode or reason != NO_MORE:
        return False
    # A serious held row never blocks daily mode; active/ready/review/posting does.
    active = db.execute("SELECT COUNT(*) FROM messages WHERE state IN ('new','retry','downloading','downloaded','processing')").fetchone()[0]
    unposted = db.execute("SELECT COUNT(*) FROM artifacts WHERE state IN ('ready','review','posting')").fetchone()[0]
    if active or unposted:
        return False
    initial = _catalog_highwater(cfg.data)
    seen = db.execute('SELECT COALESCE(MAX(id),0) FROM messages').fetchone()[0]
    high = max(initial, seen, int(db.value(HIGH_KEY, '0')))
    now = time.time() if now is None else now
    db.set(HIGH_KEY, str(high))
    db.set(NEXT_KEY, str(next_local_two(now)))
    db.set(MODE_KEY, 'daily')
    return True


def due(db, *, now: float | None = None) -> bool:
    now = time.time() if now is None else now
    return is_daily(db) and now >= float(db.value(NEXT_KEY, str(next_local_two(now))))


async def scan_once(db, client, source, enqueue, *, now: float | None = None) -> dict:
    """Incremental pass. Do not commit the highwater until iteration completes.

    The overlap handles last-message race and is safe because message IDs are unique
    in SQLite. If the pass raises, a subsequent pass revisits the same span.
    """
    if not is_daily(db):
        return {'messages': 0, 'new_edz': 0, 'skipped': 'not-daily'}
    now = time.time() if now is None else now
    high = int(db.value(HIGH_KEY, '0'))
    if high <= 0:
        raise ValueError('Chưa có mốc Message ID an toàn; từ chối quét lại toàn bộ lịch sử')
    seen = high
    n = 0
    added = 0
    async for message in client.iter_messages(source, min_id=max(0, high - 20), reverse=True):
        n += 1
        if message.id > seen:
            seen = message.id
        if message.id > high:
            before = db.message(message.id) is not None
            enqueue(message)
            if not before and db.message(message.id) is not None:
                added += 1
    db.set(HIGH_KEY, str(seen))
    db.set(LAST_KEY, str(now))
    db.set(NEXT_KEY, str(next_local_two(now)))
    return {'messages': n, 'new_edz': added, 'highwater': seen}


def retry_later(db, *, now: float | None = None, delay: int = 3600):
    now = time.time() if now is None else now
    db.set(NEXT_KEY, str(now + max(300, delay)))
