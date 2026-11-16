"""Bounded OUMIJIA v16.3 source admission and read-only pipeline telemetry.

No Telegram access or file writing. Shared across admission policy and status.
"""
from __future__ import annotations

from collections import defaultdict
import time

GIB = 1024**3
DOWNLOAD_WAIT_KEY = 'pipeline_download_cooldown_until_v163'


def download_cooldown_remaining(db, *, now: float | None = None) -> float:
    now = time.time() if now is None else now
    try:
        until = float(db.value(DOWNLOAD_WAIT_KEY, '0'))
    except (ValueError, TypeError):
        return 0.0
    return max(0.0, until - now)

ACTIVE_STATES = ('new', 'retry', 'downloading', 'downloaded', 'processing')


def active_source_groups(db) -> dict[str, int]:
    """Count one multi-volume group as one source and reserve whole-group bytes."""
    groups: dict[str, int] = defaultdict(int)
    for row in db.execute("""SELECT id,kind,group_name,size_bytes FROM messages
            WHERE state IN ('new','retry','downloading','downloaded','processing')"""):
        if row['kind'] == 'volume' and row['group_name']:
            key = 'volume:' + row['group_name']
        else:
            key = 'single:' + str(row['id'])
        groups[key] += max(0, int(row['size_bytes']))
    return dict(groups)


def stats(db) -> dict[str, int]:
    messages = {r['state']: int(r['count']) for r in db.execute(
        'SELECT state,COUNT(*) AS count FROM messages GROUP BY state')}
    artifacts = {r['state']: int(r['count']) for r in db.execute(
        'SELECT state,COUNT(*) AS count FROM artifacts GROUP BY state')}
    return {
        'queued': messages.get('new', 0) + messages.get('retry', 0),
        'downloading': messages.get('downloading', 0),
        'downloaded': messages.get('downloaded', 0),
        'processing': messages.get('processing', 0),
        'ready': artifacts.get('ready', 0),
        'posting': artifacts.get('posting', 0),
        'held': artifacts.get('held', 0),
        'active_sources': len(active_source_groups(db)),
        'download_cooldown': download_cooldown_remaining(db),
    }


def admission_reason(db, cfg) -> str | None:
    """Only admit another historical source when bounded backlog has headroom.

    A new source can be downloaded while another is being split/published;
    excessive ready outputs or active source-group size applies backpressure.
    Disk usage is additionally gated by autonomous.select_next_auto.
    """
    groups = active_source_groups(db)
    limit = int(cfg.pipeline_max_active_sources)
    if len(groups) >= limit:
        return f'pipeline waiting: {len(groups)}/{limit} in-flight source groups'
    row = db.execute("""SELECT COUNT(*) AS n,COALESCE(SUM(size_bytes),0) AS bytes
        FROM artifacts WHERE state IN ('ready','review','posting')""").fetchone()
    n = int(row['n'])
    size = int(row['bytes'])
    max_n = int(cfg.pipeline_max_ready_artifacts)
    max_size = float(cfg.pipeline_max_ready_gib) * GIB
    if n >= max_n or size >= max_size:
        return (f'pipeline waiting: {n} pending EDZ, {size/GIB:.2f} GiB; '
                f'limit {max_n} EDZ / {max_size/GIB:.1f} GiB')
    return None
