"""Read-only Linux health checks for the OUMIJIA Telegram control room.

Never exposes .env, Telegram session, process arguments, usernames, or logs.
This module does not send messages or modify the bot's work queue.
"""
from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import time

GIB = 1024 ** 3


def _meminfo(path: Path = Path('/proc/meminfo')) -> dict[str, int]:
    result = {}
    for line in path.read_text().splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[1].isdigit():
            result[parts[0].removesuffix(':')] = int(parts[1]) * 1024  # kB -> bytes
    return result


def _cpu_ticks(path: Path = Path('/proc/stat')) -> tuple[int, int]:
    parts = path.read_text().splitlines()[0].split()
    if parts[0] != 'cpu':
        raise ValueError('Không đọc được /proc/stat')
    values = [int(x) for x in parts[1:]]
    # idle + iowait; guest time is already counted in user/nice
    idle = values[3] + (values[4] if len(values) > 4 else 0)
    total = sum(values[:8])
    return idle, total


def cpu_percent(*, sample_seconds: float = 0.2) -> float | None:
    try:
        before_idle, before_total = _cpu_ticks()
        time.sleep(sample_seconds)
        after_idle, after_total = _cpu_ticks()
        delta_total = after_total - before_total
        if delta_total <= 0:
            return None
        return max(0.0, min(100.0, 100.0 * (1 - (after_idle - before_idle) / delta_total)))
    except (OSError, ValueError, IndexError):
        return None


def _systemctl(*args: str) -> str:
    try:
        proc = subprocess.run(('systemctl', *args), capture_output=True, text=True,
                              check=False, timeout=5)
        return proc.stdout.strip() or ('unknown' if proc.returncode else '')
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return 'unknown'


def snapshot(data_dir: Path, reserve_bytes: int, *, sample_cpu: bool = True) -> dict:
    """Capture a point-in-time state; reserved disk bytes apply to data volume."""
    mem = _meminfo()
    disk = shutil.disk_usage(data_dir)
    loads = os.getloadavg()
    cpus = os.cpu_count() or 1
    uptime = float(Path('/proc/uptime').read_text().split()[0])
    active = _systemctl('is-active', 'edz-telegram.service')
    restarts = _systemctl('show', '-p', 'NRestarts', '--value', 'edz-telegram.service')
    try:
        restarts = int(restarts)
    except (ValueError, TypeError):
        restarts = None
    return {
        'at': time.time(), 'service': active, 'service_restarts': restarts,
        'watchdog_timer': _systemctl('is-active', 'edz-server-watchdog.timer'),
        'cpu_percent': cpu_percent() if sample_cpu else None,
        'cpus': cpus, 'load': [round(x, 2) for x in loads],
        'memory_total': mem.get('MemTotal', 0),
        'memory_available': mem.get('MemAvailable', mem.get('MemFree', 0)),
        'swap_total': mem.get('SwapTotal', 0),
        'swap_free': mem.get('SwapFree', 0),
        'disk_total': disk.total, 'disk_free': disk.free,
        'disk_used': disk.used, 'disk_reserve': int(reserve_bytes),
        'uptime_seconds': uptime,
    }


def issues(s: dict, *, high_cpu_streak: int = 0) -> dict[str, str]:
    """Failures/warnings. High CPU needs 3 consecutive checks to avoid spikes."""
    problems = {}
    if s['service'] != 'active':
        problems['service_down'] = 'Dịch vụ EDZ không hoạt động (trạng thái: '+s['service']+').'
    free = s['disk_free']
    if free < s['disk_reserve']:
        problems['disk_critical'] = 'SSD không đủ mức dự phòng 40 GiB; cần giải phóng dung lượng an toàn.'
    elif free < s['disk_reserve'] + 10 * GIB:
        problems['disk_low'] = 'SSD sắp chạm mức dự phòng; bot nên hạn chế tải file lớn.'
    total = s['memory_total']
    if total and s['memory_available'] / total < 0.10:
        problems['memory_low'] = 'RAM khả dụng dưới 10%; kiểm tra tiến trình dùng bộ nhớ.'
    if high_cpu_streak >= 3 and s['cpu_percent'] is not None and s['cpu_percent'] >= 95:
        problems['cpu_high'] = 'CPU sử dụng trên 95% trong nhiều lượt kiểm tra.'
    return problems


def _gib(n: int) -> str:
    return f'{n / GIB:.1f} GiB'


def uptime_text(seconds: float) -> str:
    secs = int(max(0, seconds)); days, secs = divmod(secs, 86400)
    hours, secs = divmod(secs, 3600); minutes = secs // 60
    return f'{days} ngày {hours} giờ {minutes} phút'


def format_snapshot(s: dict, *, mode: str = 'server') -> str:
    """Telegram-safe non-sensitive diagnostics; no shell output or credentials."""
    cpu = ('chưa đo được' if s['cpu_percent'] is None else f'{s["cpu_percent"]:.1f}%')
    usemem = max(0, s['memory_total'] - s['memory_available'])
    swapuse = max(0, s['swap_total'] - s['swap_free'])
    disk = s['disk_total']
    parts = {
        'cpu': f'🖥 CPU: {cpu} | {s["cpus"]} luồng\nLoad (1/5/15p): '+ ' / '.join(str(x) for x in s['load']),
        'ram': f'🧠 RAM: {_gib(usemem)} / {_gib(s["memory_total"])} (khả dụng {_gib(s["memory_available"])})\nSwap: {_gib(swapuse)} / {_gib(s["swap_total"])}',
        'disk': f'💽 SSD: {_gib(s["disk_used"])} / {_gib(disk)} ({100*s["disk_used"]/disk:.1f}% đã dùng)\nTrống: {_gib(s["disk_free"])} | Dự phòng yêu cầu: {_gib(s["disk_reserve"])}' if disk else '💽 Không đọc được dung lượng SSD',
        'uptime': f'⏱ Thời gian hoạt động: {uptime_text(s["uptime_seconds"])}',
    }
    if mode in parts:
        return '📊 OUMIJIA — '+mode.upper()+'\n'+parts[mode]
    return ('📊 OUMIJIA — Giám sát server\n'
            + f'Dịch vụ EDZ: {s["service"]}\n'
            + f'Giám sát độc lập: {s.get("watchdog_timer","chưa xác định")}\n'
            + parts['cpu']+'\n'+parts['ram']+'\n'+parts['disk']+'\n'+parts['uptime']
            + f'\nSố lần service tự khởi động lại: {s["service_restarts"] if s["service_restarts"] is not None else "chưa xác định"}')
