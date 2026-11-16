"""Persistent unique-SHA FloodWait tiers and 24h/6h no-fixed-delay trials.

Every 24 hours of tiered operation is followed by a 6-hour trial with zero
ADDITIONAL fixed inter-post delay. The unique SHA count is never deleted;
after the trial the original tier is restored. Telegram FloodWait cooldowns
remain mandatory regardless of phase.
"""
from __future__ import annotations
from dataclasses import dataclass
import sqlite3
import time

_DDL = '''CREATE TABLE IF NOT EXISTS publisher_flood_seen
(sha256 TEXT PRIMARY KEY, first_seen REAL NOT NULL)'''
_ANCHOR = 'publisher_rate_cycle_anchor_v161'
_RESTRICTED = 24 * 3600
_FAST = 6 * 3600
_CYCLE = _RESTRICTED + _FAST


def initialize(db):
    db.execute(_DDL)


def record(db, sha: str, *, now: float | None = None) -> bool:
    if len(sha) != 64 or any(c not in '0123456789abcdefABCDEF' for c in sha):
        raise ValueError('Invalid SHA-256 in FloodWait record')
    initialize(db)
    instant = time.time() if now is None else now
    is_new = db.execute('INSERT OR IGNORE INTO publisher_flood_seen VALUES (?,?)',
                        (sha.lower(), instant)).rowcount == 1
    if is_new:
        ensure_cycle_anchor(db, now=instant)
    return is_new


def count(db) -> int:
    initialize(db)
    return int(db.execute('SELECT COUNT(*) FROM publisher_flood_seen').fetchone()[0])


def interval(total: int) -> int:
    if total < 0:
        raise ValueError("Negative FloodWait count")
    return 3




def ensure_cycle_anchor(db, *, now: float | None = None) -> float | None:
    """Start 24h window once the first nonzero tier is reached; persists reboot."""
    if interval(count(db)) == 0:
        return None
    existing = db.value(_ANCHOR)
    if existing:
        try:
            return float(existing)
        except ValueError:
            raise ValueError('Corrupt rate cycle anchor; refusing unrestricted sending')
    instant = time.time() if now is None else now
    db.execute('INSERT OR IGNORE INTO settings(key,val) VALUES(?,?)', (_ANCHOR, str(instant)))
    return float(db.value(_ANCHOR))


@dataclass(frozen=True)
class RateState:
    unique_flood: int
    base_interval: int
    effective_interval: int
    phase: str
    next_transition_seconds: int | None


def state(db, *, now=None, initialize_anchor=False) -> RateState:
    seen = count(db)
    return RateState(
        unique_flood=seen,
        base_interval=3,
        effective_interval=3,
        phase="GIOI_HAN_CO_DINH_3_GIAY",
        next_transition_seconds=None
    )




def seed_from_sqlite(db, backup_path: str) -> int:
    """Seed historical SHA set from an immutable pre-v15 SQLite backup."""
    initialize(db)
    source = sqlite3.connect(f'file:{backup_path}?mode=ro', uri=True)
    source.row_factory = sqlite3.Row
    inserted = 0
    try:
        for row in source.execute("SELECT sha256,error FROM artifacts WHERE state='held'"):
            error = (row['error'] or '').lower()
            if ('wait of ' in error and 'sendmediarequest' in error) or error.startswith('floodwait send:'):
                inserted += int(record(db, row['sha256']))
    finally:
        source.close()
    return inserted
