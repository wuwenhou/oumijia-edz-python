"""Telegram-admin-configurable publish pacing.

`SEND_DELAY_SECONDS` (env, default 3) is the default inter-post delay.
A Telegram admin can override it live from the control group:

    /limit           show the current value and its source
    /limit 3         post at most one EDZ every 3 seconds
    /limit 0         no local inter-post delay ("unlimited")
    /limit reset     clear the override, back to SEND_DELAY_SECONDS

A value of 0 removes only the LOCAL pacing. Telegram FloodWait cooldowns
always remain in force and are never bypassed by this setting; posting too
fast may trigger FloodWait or account restrictions from Telegram itself.
"""
from __future__ import annotations

import math

OVERRIDE_KEY = 'send_delay_override_seconds'
MAX_DELAY = 24 * 3600  # sanity cap; anything larger is almost certainly a typo


def override_delay(db) -> float | None:
    """Return the stored Telegram override, or None when unset/invalid."""
    raw = db.value(OVERRIDE_KEY, '')
    if raw == '':
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        # Corrupt value: fall back to the env default instead of crashing the publisher.
        return None
    if not math.isfinite(value) or not 0.0 <= value <= MAX_DELAY:
        return None
    return value


def current_delay(db, cfg) -> float:
    """Effective inter-post delay: Telegram override when present, else env default."""
    override = override_delay(db)
    if override is not None:
        return override
    try:
        value = float(cfg.send_delay)
    except (TypeError, ValueError):
        return 3.0
    return value if math.isfinite(value) and 0.0 <= value <= MAX_DELAY else 3.0


def set_delay(db, seconds) -> float:
    try:
        value = float(str(seconds).strip())
    except (TypeError, ValueError):
        raise ValueError('Số giây không hợp lệ. Ví dụ: /limit 3, /limit 0 hoặc /limit reset') from None
    if not math.isfinite(value) or not 0.0 <= value <= MAX_DELAY:
        raise ValueError(f'Khoảng nghỉ phải trong khoảng 0..{MAX_DELAY} giây (0 = không giới hạn)')
    stored = str(int(value)) if value == int(value) else str(value)
    db.set(OVERRIDE_KEY, stored)
    return value


def reset_delay(db) -> None:
    db.execute('DELETE FROM settings WHERE key=?', (OVERRIDE_KEY,))
