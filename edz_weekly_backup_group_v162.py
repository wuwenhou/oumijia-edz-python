#!/usr/bin/env python3
"""OUMIJIA encrypted weekly recovery backup to the private EDZ Control group.

Requires the project's venv (Telethon, python-dotenv). Use a dedicated Telegram
session, distinct from data/session/edzbot.session. Backups are AES-256 GPG
symmetric archives uploaded in 1.4 GiB segments to EDZ_INBOX_CHAT.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from contextlib import closing
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import sqlite3
import stat
import subprocess
import sys
import tarfile
import tempfile

GIB = 1024**3
PART_BYTES = 1_400_000_000  # comfortably below common Telegram upload limit
MAX_ARCHIVE_BYTES = 8 * PART_BYTES  # fail and alert rather than upload unexpectedly huge data
RESERVE_BYTES = 40 * GIB
SOURCE_SUBDIRS = ('edzbot', 'scripts', 'tests', 'docs', 'vendor')
SOURCE_FILES = (
    '.env', '.env.example', '.gitignore', 'requirements.txt',
    'requirements-resolved.txt', 'README.md', 'PLAN.md',
    'edz_catalog_plan_v5.py', 'edz_over_60gib_report.py',
    'edz_weekly_backup_group_v162.py', 'install_weekly_group_v162.sh',
    'README_BACKUP_GROUP_V162.md', 'test_weekly_backup_group_v162.py',
    'install_oumijia_v16_1.sh', 'install_oumijia_v16.sh',
)
EXCLUDED_DATA = ('data/incoming', 'data/work', 'data/volumes', 'data/logs', 'data/quarantine')


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as inp:
        for b in iter(lambda: inp.read(4 * 1024 * 1024), b''):
            h.update(b)
    return h.hexdigest()


def load_project_settings(root: Path):
    from dotenv import load_dotenv
    if not (root / '.env').is_file():
        raise RuntimeError('Missing project .env')
    load_dotenv(root / '.env', override=True)
    api_id = int(os.getenv('API_ID', '0'))
    api_hash = os.getenv('API_HASH', '').strip()
    if not api_id or not api_hash:
        raise RuntimeError('API_ID/API_HASH are missing from .env')
    data_option = Path(os.getenv('DATA_DIR', './data')).expanduser()
    data = (data_option if data_option.is_absolute() else root / data_option).resolve()
    # Project-specific recovery expects data at its default location.
    if data != (root / 'data').resolve():
        raise RuntimeError('Custom DATA_DIR is not supported by this recovery bundle')
    return api_id, api_hash, data


def strict_regular(root: Path, candidate: Path) -> Path:
    root = root.resolve()
    candidate = candidate.absolute()
    try:
        relative = candidate.relative_to(root)
    except ValueError as ex:
        raise RuntimeError(f'Backup file outside project: {candidate}') from ex
    cursor = root
    for part in relative.parts:
        cursor = cursor / part
        mode = cursor.lstat().st_mode
        if stat.S_ISLNK(mode):
            raise RuntimeError(f'Symlinks are not included in backup: {cursor}')
    if not candidate.is_file():
        raise RuntimeError(f'Not a regular file: {candidate}')
    return candidate


def sqlite_snapshot(source: Path, target: Path):
    if not source.is_file():
        raise RuntimeError(f'SQLite source missing: {source}')
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with closing(sqlite3.connect(f'file:{source.resolve()}?mode=ro', uri=True, timeout=20)) as src:
        with closing(sqlite3.connect(str(target))) as dst:
            src.backup(dst, pages=128, sleep=0.1)
            check = dst.execute('PRAGMA quick_check').fetchone()[0]
            if check != 'ok':
                raise RuntimeError(f'SQLite backup failed quick_check: {source.name}: {check}')
    target.chmod(0o600)


def pending_artifacts(state_db: Path, root: Path) -> list[tuple[Path, Path]]:
    """Only collect EDZ files that a restored worker would still need locally."""
    library = (root / 'data/library').resolve()
    records = []
    with closing(sqlite3.connect(str(state_db))) as con:
        con.row_factory = sqlite3.Row
        rows = con.execute("""SELECT sha256, path, size_bytes, state FROM artifacts
            WHERE state IN ('ready','review','held','posting') ORDER BY sha256""").fetchall()
        for r in rows:
            p = Path(r['path'])
            if not p.is_absolute():
                # Old rows may have relative paths resolved against the project.
                p = root / p
            p = strict_regular(root, p)
            try:
                p.relative_to(library)
            except ValueError as ex:
                raise RuntimeError(f'Unpublished EDZ outside library: {p}') from ex
            if p.stat().st_size != int(r['size_bytes']):
                raise RuntimeError(f'EDZ size changed: {p}')
            if sha256(p) != r['sha256']:
                raise RuntimeError(f'EDZ SHA mismatch: {p}')
            records.append((p, p.relative_to(root)))
    return records


def project_files(root: Path) -> list[tuple[Path, Path]]:
    mapping = []
    for filename in SOURCE_FILES:
        src = root / filename
        if src.exists():
            mapping.append((strict_regular(root, src), Path(filename)))
    # Keep root maintenance tools needed to rebuild/upgrade after a disaster.
    for glob_pattern in ('*.patch', 'install_*.sh', 'edz_*.py'):
        for src in sorted(root.glob(glob_pattern)):
            if src.is_file() and src.name not in SOURCE_FILES:
                mapping.append((strict_regular(root, src), Path(src.name)))
    for folder in SOURCE_SUBDIRS:
        basedir = root / folder
        if not basedir.exists():
            continue
        for src in sorted(basedir.rglob('*')):
            if '__pycache__' in src.parts or src.suffix in ('.pyc', '.pyo'):
                continue
            if src.is_symlink():
                continue  # no unsafe external references
            if src.is_file():
                mapping.append((strict_regular(root, src), src.relative_to(root)))
    basedir = root / 'data/catalog'
    if basedir.exists():
        for src in sorted(basedir.rglob('*')):
            if src.is_symlink():
                continue
            if src.is_file():
                mapping.append((strict_regular(root, src), src.relative_to(root)))
    # Watchdog state may be useful when restoring the independent monitor.
    extra = root / 'data/backup'
    if extra.is_dir():
        for src in sorted(extra.rglob('*')):
            if src.is_file() and not src.is_symlink():
                mapping.append((strict_regular(root, src), src.relative_to(root)))
    return mapping


def build_archive(root: Path, stage: Path, stamp: str) -> tuple[Path, dict]:
    if not (root / '.env').is_file():
        raise RuntimeError('Missing .env')
    snapshots = stage / 'snap'
    db_src = root / 'data/state.sqlite3'
    db_copy = snapshots / 'data/state.sqlite3'
    sqlite_snapshot(db_src, db_copy)
    files = project_files(root)
    # Pin the ACTUAL Python environment, not merely version ranges.
    pip = subprocess.run([sys.executable, '-m', 'pip', 'freeze'],
                         text=True, capture_output=True, check=True)
    resolved = snapshots / 'requirements-resolved.txt'
    resolved.write_text(pip.stdout)
    resolved.chmod(0o600)
    files = [pair for pair in files if pair[1] != Path('requirements-resolved.txt')]
    files.append((resolved, Path('requirements-resolved.txt')))
    # Make consistent online snapshots of BOTH SQLite-backed Telegram sessions.
    for name in ('edzbot.session', 'edzbackup.session'):
        original = root / 'data/session' / name
        session_copy = snapshots / 'data/session' / name
        sqlite_snapshot(original, session_copy)
        files.append((session_copy, Path('data/session') / name))
    files.append((db_copy, Path('data/state.sqlite3')))
    pending = pending_artifacts(db_copy, root)
    files.extend(pending)
    # Source input, partial archive and work caches are intentionally excluded.
    # Redownload/reprocess may be needed when recovering an interrupted source.
    report = {
        'format': 'oumijia-edz-weekly-group-v16.2',
        'timestamp_utc': stamp,
        'pending_edz_included': len(pending),
        'excluded_temporary_dirs': EXCLUDED_DATA,
        'restoration_notes': [
            'Restore under the same /home/cardanobp/edz-telegram-production path.',
            'Never run the recovered server concurrently with the original server.',
            'Cross-check Telegram destination against the restored SQLite because uploads after snapshot may not be recorded.',
            'In-progress downloads and 7z work/volume caches are NOT included; inspect and reconcile before enabling auto mode.',
        ],
    }
    name = f'oumijia-edz-weekly-{stamp}.tar.gz'
    tarpath = stage / name
    seen = set()
    with tarfile.open(tarpath, mode='w:gz', compresslevel=3) as out:
        for src, rel in sorted(files, key=lambda t: str(t[1])):
            if str(rel) in seen:
                raise RuntimeError(f'Duplicate path in backup: {rel}')
            seen.add(str(rel))
            if not src.is_file() or src.is_symlink():
                raise RuntimeError(f'File disappeared during backup: {src}')
            arc = str(Path('edz-telegram-production') / rel)
            out.add(src, arcname=arc, recursive=False)
        blob = json.dumps(report, ensure_ascii=False, indent=2).encode('utf-8')
        import io
        entry = tarfile.TarInfo('edz-telegram-production/RECOVERY_MANIFEST.json')
        entry.size = len(blob)
        entry.mode = 0o600
        entry.mtime = int(datetime.now(timezone.utc).timestamp())
        out.addfile(entry, io.BytesIO(blob))
    report['included_files'] = len(files)
    report['archive_bytes'] = tarpath.stat().st_size
    return tarpath, report


def encrypt_file(source: Path, encrypted: Path, key_path: Path, stage: Path):
    if not key_path.is_file() or (key_path.stat().st_mode & 0o077):
        raise RuntimeError('Backup key must exist with mode 0600')
    homedir = stage / 'gnupg'
    homedir.mkdir(mode=0o700)
    cmd = [
        'gpg', '--homedir', str(homedir), '--batch', '--yes',
        '--pinentry-mode', 'loopback', '--passphrase-file', str(key_path),
        '--symmetric', '--cipher-algo', 'AES256', '--s2k-digest-algo', 'SHA256',
        '--compress-algo', 'none', '--output', str(encrypted), str(source),
    ]
    subprocess.run(cmd, check=True, capture_output=True, text=True)
    if not encrypted.is_file() or encrypted.stat().st_size == 0:
        raise RuntimeError('GPG produced an empty encrypted file')


def split_encrypted(source: Path, output: Path, max_bytes: int = PART_BYTES) -> list[Path]:
    if source.stat().st_size > MAX_ARCHIVE_BYTES:
        raise RuntimeError(
            f'Backup archive {source.stat().st_size} bytes exceeds weekly Telegram safety cap '
            f'{MAX_ARCHIVE_BYTES} bytes; use external storage for large working datasets'
        )
    total = (source.stat().st_size + max_bytes - 1) // max_bytes
    parts = []
    with source.open('rb') as inp:
        for index in range(1, total + 1):
            part = output / f'{source.name}.part{index:03d}-of-{total:03d}'
            remaining = min(max_bytes, source.stat().st_size - (index - 1)*max_bytes)
            with part.open('wb') as dest:
                while remaining:
                    block = inp.read(min(4*1024*1024, remaining))
                    if not block:
                        raise RuntimeError('Encrypted backup truncated while splitting')
                    dest.write(block)
                    remaining -= len(block)
            parts.append(part)
    return parts


def key_path_default() -> Path:
    return Path.home() / '.config/edz-weekly-backup/recovery.key'


def init_key(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    token = secrets.token_urlsafe(64) + '\n'
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as stream:
        stream.write(token)
    print('CREATED RECOVERY KEY:', path)
    print('IMPORTANT: Save a second copy OFF-SERVER, separate from Telegram. Never send key to Telegram.')


def group_chat(root: Path) -> int:
    """Require a single private control-room group; never fall back to 'me'."""
    from dotenv import dotenv_values
    settings = dotenv_values(root / '.env')
    raw = (settings.get('EDZ_INBOX_CHAT') or '').strip()
    if not re.fullmatch(r'-100[0-9]{6,}', raw):
        raise RuntimeError('Thiếu EDZ_INBOX_CHAT=-100... trong .env; không gửi backup vào Saved Messages')
    return int(raw)


async def resolve_room(client, chat_id: int):
    """Resolve a joined group, even if this session has no cached peer entity."""
    try:
        return await client.get_input_entity(chat_id)
    except (ValueError, TypeError):
        pass
    async for dialog in client.iter_dialogs():
        if int(dialog.id) == chat_id:
            return dialog.input_entity
    raise RuntimeError('Phiên backup chưa tham gia nhóm EDZ Control hoặc không có quyền truy cập')


def client_factory(root: Path):
    from telethon import TelegramClient
    api_id, api_hash, data = load_project_settings(root)
    session_base = data / 'session/edzbackup'
    session_base.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    return TelegramClient(str(session_base), api_id, api_hash,
                          flood_sleep_threshold=300, request_retries=4,
                          connection_retries=5, retry_delay=5)


async def backup_login(root: Path):
    cli = client_factory(root)
    try:
        phone = os.getenv('TELEGRAM_PHONE', '').strip()
        await cli.start(phone=phone if phone else lambda: input('Telegram phone (+country code): ').strip())
        me = await cli.get_me()
        if me is None:
            raise RuntimeError('No Telegram account was authenticated')
        print(f'BACKUP SESSION AUTHORIZED: Telegram account id={me.id}')
        chat_id = group_chat(root)
        await resolve_room(cli, chat_id)
        print('NHÓM NHẬN BACKUP ĐÃ XÁC NHẬN:', chat_id)
        print('Bản backup sẽ gửi trong nhóm bằng phiên Telegram riêng của tài khoản đã đăng nhập.')
    finally:
        await cli.disconnect()
    session = root / 'data/session/edzbackup.session'
    if session.exists():
        session.chmod(0o600)


async def telegram_send(root: Path, parts: list[Path], encrypted_sha: str, report: dict):
    chat_id = group_chat(root)
    if not parts or any('.gpg.part' not in part.name for part in parts):
        raise RuntimeError('Chỉ được gửi các phần backup đã mã hóa .gpg.part...')
    client = client_factory(root)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            raise RuntimeError('Phiên Telegram riêng chưa đăng nhập; chạy lệnh login')
        peer = await resolve_room(client, chat_id)
        stamp = report['timestamp_utc']
        start_message = (
            f'🔐 OUMIJIA — BẮT ĐẦU BACKUP TUẦN {stamp}\n'
            f'Số phần mã hóa: {len(parts)}\n'
            f'SHA-256 file mã hóa: {encrypted_sha}\n'
            f'EDZ chưa đăng đã kèm: {report["pending_edz_included"]}.\n'
            'Không gồm dữ liệu tải dở/work/volumes. Khóa giải mã được giữ bên ngoài Telegram.')
        await client.send_message(peer, start_message)
        for index, part in enumerate(parts, 1):
            caption = (f'OUMIJIA — BACKUP MÃ HÓA {stamp} | PHẦN {index}/{len(parts)}\n'
                       f'SHA-256 phần: {sha256(part)}')
            sent = await client.send_file(peer, str(part), caption=caption)
            if not getattr(sent, 'id', None):
                raise RuntimeError(f'Không có xác nhận từ Telegram cho phần {index}')
            print(f'ĐÃ GỬI PHẦN {index}/{len(parts)}: tin #{sent.id}', flush=True)
        result = await client.send_message(peer,
            f'✅ OUMIJIA — SAO LƯU TUẦN HOÀN TẤT {stamp}\n'
            f'Tổng {len(parts)} phần; SHA-256 file mã hóa: {encrypted_sha}\n'
            'Cần đủ tất cả phần và khóa khôi phục đã lưu bên ngoài Telegram để giải mã.\n'
            'Không được chạy server khôi phục đồng thời với server gốc.')
        if not getattr(result, 'id', None):
            raise RuntimeError('Không xác nhận được tin nhắn hoàn tất backup')
    finally:
        await client.disconnect()


def _bot_api_failure_notice(root: Path, message: str) -> bool:
    """Independent Bot API fallback for alerts; never expose BotFather token in errors."""
    from dotenv import dotenv_values
    from urllib.parse import urlencode
    from urllib.request import Request, urlopen
    settings = dotenv_values(root / '.env')
    token = (settings.get('EDZ_NOTIFY_BOT_TOKEN') or '').strip()
    if not token:
        return False
    chat_id = group_chat(root)
    payload = urlencode({'chat_id': str(chat_id), 'text': message[:3500]}).encode('utf-8')
    try:
        req = Request(f'https://api.telegram.org/bot{token}/sendMessage',
                      data=payload, method='POST')
        with urlopen(req, timeout=20) as response:
            doc = json.loads(response.read(65536))
        return bool(doc.get('ok'))
    except Exception:
        return False


async def fail_alert(root: Path, err: str):
    message = ('⚠️ OUMIJIA — SAO LƯU TUẦN THẤT BẠI\n'
               + err[:700] + '\nXem nhật ký: journalctl -u edz-weekly-backup.service')
    # Use the independent Bot API first when possible, so a broken backup session
    # does not prevent error alerts. This only sends a small text notice.
    if await asyncio.to_thread(_bot_api_failure_notice, root, message):
        return
    try:
        client = client_factory(root)
        try:
            await client.connect()
            if await client.is_user_authorized():
                peer = await resolve_room(client, group_chat(root))
                await client.send_message(peer, message)
        finally:
            await client.disconnect()
    except Exception as alert_ex:
        print('KHÔNG GỬI ĐƯỢC CẢNH BÁO:', type(alert_ex).__name__, file=sys.stderr)


def backup_root_default() -> Path:
    return Path.home() / 'edz-backups/weekly'


def preflight(root: Path, key: Path) -> tuple[Path, int]:
    group_chat(root)  # Fail before accessing backup session if group is unconfigured.
    _, _, data = load_project_settings(root)
    for file in (data / 'state.sqlite3', data / 'session/edzbot.session', data / 'session/edzbackup.session'):
        if not file.is_file():
            raise RuntimeError(f'Missing recovery-critical file: {file}')
    if not key.is_file():
        raise RuntimeError(f'Missing encryption key (run init-key): {key}')
    if key.stat().st_mode & 0o077:
        raise RuntimeError('Encryption key permissions too open; chmod 600')
    if not shutil.which('gpg'):
        raise RuntimeError('gpg is required: sudo apt install gnupg')
    # Allow space for local staging, tar and encrypted output, plus the usual disk reserve.
    pending = 0
    with closing(sqlite3.connect(f'file:{(data / "state.sqlite3").resolve()}?mode=ro', uri=True)) as conn:
        pending = conn.execute("SELECT COALESCE(SUM(size_bytes),0) FROM artifacts "
                               "WHERE state IN ('ready','review','held','posting')").fetchone()[0]
    if int(pending) > MAX_ARCHIVE_BYTES:
        raise RuntimeError(
            f'Unpublished EDZ need {int(pending)} bytes; larger than Telegram '
            f'safety limit {MAX_ARCHIVE_BYTES}. Use external disk/NAS.'
        )
    available = shutil.disk_usage(root).free
    if available < RESERVE_BYTES + 3 * int(pending) + 256*1024*1024:
        raise RuntimeError(f'Insufficient free space for snapshot + 40 GiB reserve: {available} bytes')
    return data, int(pending)


def retain_local(directory: Path, keep: int = 4):
    backups = sorted(directory.glob('oumijia-edz-weekly-*.tar.gz.gpg'), key=lambda p: p.name)
    for older in backups[:-keep]:
        older.unlink()
        manifest = older.with_name(older.name + '.sha256')
        manifest.unlink(missing_ok=True)


async def backup_run(root: Path, key: Path, output: Path):
    _, pending = preflight(root, key)
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    lockpath = output / '.backup.lock'
    with lockpath.open('a+b') as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as ex:
            raise RuntimeError('A weekly backup is already running') from ex
        stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        print(f'BẮT ĐẦU SAO LƯU: {stamp}, pending EDZ bytes={pending}', flush=True)
        with tempfile.TemporaryDirectory(prefix='.snapshot-', dir=output) as tmp:
            stage = Path(tmp)
            tarpath, report = build_archive(root, stage, stamp)
            encrypted_name = tarpath.name + '.gpg'
            crypt = stage / encrypted_name
            if tarpath.stat().st_size > MAX_ARCHIVE_BYTES:
                raise RuntimeError('Bản nén vượt giới hạn backup lên Telegram; cần ổ lưu trữ/NAS')
            encrypt_file(tarpath, crypt, key, stage)
            encrypted_sha = sha256(crypt)
            parts = split_encrypted(crypt, stage)
            print(f'ENCRYPTED: {crypt.stat().st_size} bytes, {len(parts)} Telegram part(s)', flush=True)
            # send_file uses a distinct logged-in session, never the EDZ worker session.
            await telegram_send(root, parts, encrypted_sha, report)
            final = output / encrypted_name
            os.replace(crypt, final)
            final.with_name(final.name+'.sha256').write_text(f'{encrypted_sha}  {final.name}\n')
            final.with_name(final.name+'.sha256').chmod(0o600)
            retain_local(output)
            print(f'SAO LƯU HOÀN TẤT: {final}; Telegram đã xác nhận', flush=True)


def command_line():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('init-key', 'login', 'check', 'run'))
    parser.add_argument('--project', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--key', type=Path, default=key_path_default())
    parser.add_argument('--output', type=Path, default=backup_root_default())
    args = parser.parse_args()
    root = args.project.resolve()
    try:
        if args.command == 'init-key':
            init_key(args.key)
        elif args.command == 'login':
            asyncio.run(backup_login(root))
        elif args.command == 'check':
            data, pending = preflight(root, args.key)
            print(f'PREFLIGHT PASS: nhóm {group_chat(root)}, SQLite, hai Telegram sessions, khóa mã hóa; EDZ chờ={pending} bytes')
            print(f'Scope includes code, config, catalog, state DB, sessions, unpublished library EDZ.')
            print(f'EXCLUDES temporary working data: {", ".join(EXCLUDED_DATA)}')
            print('Chưa gửi tin nào lên Telegram. Không cần khởi động lại EDZ.')
        else:
            try:
                asyncio.run(backup_run(root, args.key, args.output))
            except Exception as ex:
                print(f'SAO LƯU THẤT BẠI: {type(ex).__name__}: {ex}', file=sys.stderr)
                asyncio.run(fail_alert(root, f'{type(ex).__name__}: {ex}'))
                raise
    except Exception as exc:
        print(f'ERROR: {type(exc).__name__}: {exc}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    command_line()
