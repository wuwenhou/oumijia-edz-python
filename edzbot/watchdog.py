"""Out-of-process watchdog. Sends alerts through Bot API while the EDZ service is down.

Only records event transitions to avoid spam. Failed sends are queued on disk and
retried on the next timer activation, without exposing BotFather tokens in logs.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import time

from .server_monitor import issues, uptime_text


MAX_PENDING = 20
GAP_THRESHOLD = 15 * 60


def read_state(path: Path) -> dict:
    try:
        state = json.loads(path.read_text(encoding='utf-8'))
        return state if isinstance(state, dict) else {}
    except (FileNotFoundError, ValueError, OSError):
        return {}


def write_state(path: Path, state: dict):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    text = json.dumps(state, ensure_ascii=False, separators=(',', ':'))
    tmp = path.with_suffix('.tmp')
    descriptor = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(descriptor, 'w', encoding='utf-8') as stream:
            stream.write(text)
            stream.flush(); os.fsync(stream.fileno())
        os.replace(tmp, path)
        path.chmod(0o600)
    finally:
        if tmp.exists():
            tmp.unlink()


def evaluate(previous: dict, snapshot: dict, *, now: float | None = None) -> dict:
    """Calculate alerts, persist them even when outbound Telegram is offline."""
    stamp = snapshot['at'] if now is None else now
    prior_issues = set(previous.get('issues', []))
    old_percent = int(previous.get('cpu_streak', 0))
    high = snapshot['cpu_percent'] is not None and snapshot['cpu_percent'] >= 95
    streak = min(10, old_percent + 1) if high else 0
    problems = issues(snapshot, high_cpu_streak=streak)
    current_issues = set(problems)
    pending = list(previous.get('pending', []))
    last_check = float(previous.get('last_check', 0))
    old_restart = previous.get('restarts')
    new_restart = snapshot.get('service_restarts')
    if last_check and stamp - last_check > GAP_THRESHOLD:
        gap_minutes = int((stamp - last_check) / 60)
        pending.append(f'⚠️ OUMIJIA — Giám sát bị gián đoạn {gap_minutes} phút; server có thể đã tắt, mất mạng hoặc timer bị dừng. Hiện đã kiểm tra lại.')
    if old_restart is not None and new_restart is not None and new_restart > int(old_restart):
        pending.append(f'⚠️ OUMIJIA — Dịch vụ EDZ đã tự khởi động lại {new_restart - int(old_restart)} lần. Hãy xem nhật ký service.')
    for code in sorted(current_issues - prior_issues):
        pending.append('🚨 OUMIJIA — CẢNH BÁO SERVER\n' + problems[code])
    for code in sorted(prior_issues - current_issues):
        pending.append('✅ OUMIJIA — ĐÃ PHỤC HỒI\nSự cố '+code+' không còn được phát hiện.')
    # Never queue an unbounded number of stale error messages.
    return {'issues': sorted(current_issues), 'cpu_streak': streak,
            'last_check': stamp, 'restarts': new_restart,
            'pending': pending[-MAX_PENDING:]}
