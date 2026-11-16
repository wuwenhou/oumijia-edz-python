#!/usr/bin/env python3
"""One-time, backed-up activation of OUMIJIA EDZ v12 autonomous mode.

Only run while systemd worker is stopped. Does not send any Telegram messages
and does not start the service. Configurable credentials are never printed.
"""
from __future__ import annotations
import argparse
from datetime import datetime
import fcntl
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
ENV = ROOT / '.env'
UPDATES = {
    'AUTO_MODE':'true',
    'DRY_RUN':'false',
    'BACKFILL_ON_START':'false',
    'AUTO_PLAN_QUEUE':'false',
    'ALLOW_UNVERIFIED_SPLITS':'false',
    'AUTO_CLEANUP':'true',
    'AUTO_WORK_CLEANUP':'true',
    'MAX_GROUP_GIB':'60',
    'DISK_RESERVE_GB':'40',
    'POLL_SECONDS':'10',
}


def read_pairs(lines):
    result={}
    for line in lines:
        if '=' in line and not line.lstrip().startswith('#'):
            k,v=line.split('=',1)
            if k.strip():
                result[k.strip()]=v.strip()
    return result


def configured_lines(lines, updates):
    result=[]
    seen=set()
    for line in lines:
        key=line.split('=',1)[0].strip()
        if key in updates and not line.lstrip().startswith('#'):
            if key not in seen:
                result.append(f'{key}={updates[key]}')
                seen.add(key)
        else:
            result.append(line)
    for key,value in updates.items():
        if key not in seen:
            result.append(f'{key}={value}')
    return '\n'.join(result) + '\n'


def main(argv=None):
    parser=argparse.ArgumentParser(description='Back up and activate EDZ v12 automatic mode')
    parser.add_argument('--enable',action='store_true',help='Actually update .env; default preview only')
    args=parser.parse_args(argv)
    if not ENV.exists() or ENV.is_symlink() or not ENV.is_file():
        raise SystemExit('Missing or unsafe .env file')
    lines=ENV.read_text(encoding='utf-8').splitlines()
    current=read_pairs(lines)
    if not current.get('SOURCE_CHAT') or not current.get('DEST_CHAT'):
        raise SystemExit('SOURCE_CHAT or DEST_CHAT missing')
    if current.get('SOURCE_CHAT') == current.get('DEST_CHAT'):
        raise SystemExit('Source and destination cannot be the same')
    for key in ('API_ID','API_HASH'):
        if not current.get(key):
            raise SystemExit(f'Missing {key}')
    for name in ('telegram_edz_catalog.csv','edz_processing_plan.csv'):
        if not (ROOT / 'data' / 'catalog' / name).is_file():
            raise SystemExit(f'Missing catalog file: {name}')
    db_path=ROOT/'data'/'state.sqlite3'
    if not db_path.is_file():
        raise SystemExit('SQLite database not found')
    if not (ROOT/'vendor'/'edzsplit'/'edzsplit').exists():
        raise SystemExit('edzsplit not installed')
    if not shutil.which(current.get('SEVEN_ZIP','7z')):
        raise SystemExit('7z executable not found')
    if shutil.disk_usage(ROOT).free < 41 * 1024**3:
        raise SystemExit('Insufficient disk free space for 40 GiB reserve')
    print('TELEGRAM SOURCE:',current['SOURCE_CHAT'])
    print('TELEGRAM TARGET:',current['DEST_CHAT'])
    print('ERROR CHAT:', current.get('ERROR_CHAT','me') or 'me')
    for key,value in UPDATES.items():
        print(f'{key}={value}')
    print('PREVIEW: will send 37 ready EDZ and then process catalog sources automatically.')
    if not args.enable:
        print('PREVIEW ONLY. To enable: .venv/bin/python scripts/activate-auto-v12.py --enable')
        return 0
    is_active=subprocess.run(['systemctl','is-active','--quiet','edz-telegram.service'],
                              check=False,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    if is_active.returncode == 0:
        raise SystemExit('STOP systemd service BEFORE enabling AUTO_MODE')
    lock_path=ROOT/'data'/'edzbot.lock'
    with lock_path.open('a') as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit('edzbot worker lock is still held: STOP service')
        backup=Path.home()/'edz-backups'/('auto-v12-'+datetime.now().strftime('%Y%m%d-%H%M%S'))
        backup.mkdir(parents=True,exist_ok=False,mode=0o700)
        shutil.copy2(ENV,backup/'.env')
        (backup/'.env').chmod(0o600)
        with sqlite3.connect(str(db_path)) as source:
            with sqlite3.connect(str(backup/'state.sqlite3')) as dest:
                source.backup(dest)
        (backup/'state.sqlite3').chmod(0o600)
        updates=dict(UPDATES)
        if 'ERROR_CHAT' not in current:
            updates['ERROR_CHAT']='me'
        text=configured_lines(lines,updates)
        fd, temp_path=tempfile.mkstemp(prefix='.env.auto-v12.',dir=str(ROOT))
        try:
            os.fchmod(fd,0o600)
            with os.fdopen(fd,'w',encoding='utf-8') as stream:
                stream.write(text)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp_path,ENV)
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
        print('AUTO MODE ENABLED. Backups saved to:',backup)
        print('Start service: sudo systemctl start edz-telegram.service')
    return 0


if __name__=='__main__':
    raise SystemExit(main())
