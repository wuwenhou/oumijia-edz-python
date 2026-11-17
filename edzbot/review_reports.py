"""On-demand, admin-only EDZ held reports sent through the existing control bot.

Read-only SQLite snapshot; neither a publisher nor a repair/cleanup trigger.
No Telegram/API credentials are included in CSV output.
"""
from __future__ import annotations

import asyncio
from collections import Counter
import csv
import io
import json
from pathlib import Path
import re
import sqlite3
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import uuid

REPORT_RE = re.compile(r'^/errors(?:(csv|files))?(?:@[A-Za-z0-9_]+)?(?:\s+(.+))?$', re.I)
COLUMNS = ('type', 'source_id', 'artifact_sha256', 'file_name', 'category',
           'file_exists', 'file_path', 'repair_caption', 'error', 'recommendation')
REMEDIES = {
    'MULTI_PART': 'Kiểm tra adapter edzsplit-rs; không tự requeue hàng loạt.',
    'INCOMPLETE_EDZ': 'Kiểm tra macro/tài nguyên thiếu; giữ held đến khi hợp lệ.',
    'PROTECTED': 'Kiểm tra quyền Telegram, không tự vượt bảo vệ nội dung.',
    'FLOODWAIT': 'Tuân thủ thời gian chờ, đối chiếu kênh đích trước khi đăng lại.',
    'SEND_UNCERTAIN': 'Đối chiếu Message ID, SHA256, kích thước ở kênh đích.',
    'HASH_MISMATCH': 'Giữ file gốc, so hash và kiểm tra hỏng dữ liệu.',
    'MISSING': 'Kiểm tra file nguồn/đường dẫn, không xóa bản ghi.',
    'DISK': 'Kiểm tra dung lượng, quyền truy cập, lỗi I/O.',
    'NETWORK': 'Kiểm tra kết nối và kết quả gửi trước khi thử lại.',
    'UNKNOWN': 'Xem error/log đầy đủ, chưa tự động xử lý.',
}


def parse_request(text: str):
    """Return (mode, optional identifier) for commands owned by this add-on.

    Accepts the legacy aliases (/errorscsv, /errorsfiles) and the new spaced
    forms (/errors csv, /errors files).
    """
    raw = (text or '').strip()
    spaced = re.fullmatch(r'/errors\s+(csv|files)\s*', raw, re.I)
    if spaced:
        raw = '/errors' + spaced.group(1).lower()
    m = REPORT_RE.fullmatch(raw)
    if not m:
        return None
    suffix, arg = m.group(1), (m.group(2) or '').strip()
    if suffix:
        return (suffix.lower(), None) if not arg else None
    if not arg:
        return ('summary', None)
    if re.fullmatch(r'-?[0-9]{1,12}', arg):
        return ('detail', ('source', str(int(arg))))
    if re.fullmatch(r'[a-f0-9]{64}', arg, flags=re.I):
        return ('detail', ('artifact', arg.lower()))
    return ('invalid', None)


def classify(error: str) -> str:
    e = (error or '').casefold()
    if 'unexpected multi-part output' in e:
        return 'MULTI_PART'
    if any(x in e for x in ('partial success', 'item.missing', 'missing item', 'item.dropped', 'incomplete results')):
        return 'INCOMPLETE_EDZ'
    if any(x in e for x in ('protected chat', "can't forward messages")):
        return 'PROTECTED'
    if any(x in e for x in ('floodwait', 'flood wait', 'flood_wait', 'a wait of')):
        return 'FLOODWAIT'
    if any(x in e for x in ('ambiguous', 'uncertain', 'previous send interrupted', 'reconciliation required')):
        return 'SEND_UNCERTAIN'
    if any(x in e for x in ('hash mismatch', 'sha256 mismatch', 'size mismatch')):
        return 'HASH_MISMATCH'
    if any(x in e for x in ('no space left', 'disk full', 'permission denied', 'enospc')):
        return 'DISK'
    if any(x in e for x in ('missing local', 'file not found', 'no such file')):
        return 'MISSING'
    if any(x in e for x in ('timeout', 'network error', 'savefilepartrequest')):
        return 'NETWORK'
    return 'UNKNOWN'


def excel_safe(value: object, max_chars: int = 1000) -> str:
    """Defuse spreadsheet formula injection from untrusted filenames/errors."""
    s = str(value or '').replace('\x00', '').replace('\r', ' ').replace('\n', ' ').strip()
    s = s[:max_chars]
    if s and s[0] in ('=', '+', '-', '@', '\t'):
        s = "'" + s
    return s


def controlled_path(root: Path, value: str) -> tuple[str, bool]:
    if not value:
        return '', False
    p = Path(value)
    if not p.is_absolute():
        p = root / p
    try:
        parent = (root / 'data').resolve(strict=True)
        resolved = p.resolve(strict=False)
        if not resolved.is_relative_to(parent) or p.is_symlink():
            return '[ngoai data]', False
        return str(resolved.relative_to(root.resolve())), resolved.is_file()
    except (OSError, ValueError):
        return '[path error]', False


def query_held(root: Path, *, existing_only=False) -> list[dict]:
    root = root.resolve()
    dbfile = root / 'data/state.sqlite3'
    db = sqlite3.connect(dbfile.as_uri() + '?mode=ro', uri=True, timeout=15)
    db.row_factory = sqlite3.Row
    result = []
    try:
        db.execute('BEGIN')
        queries = (
            ('source', "SELECT id AS source_id, NULL AS sha256, file_name, local_path AS path, error FROM messages WHERE state='held' ORDER BY id"),
            ('artifact', "SELECT source_message AS source_id, sha256, file_name, path, error FROM artifacts WHERE state='held' ORDER BY source_message, sha256"),
        )
        for kind, sql in queries:
            for rec in db.execute(sql):
                rel, exists = controlled_path(root, rec['path'] or '')
                if existing_only and not exists:
                    continue
                sha = rec['sha256'] or ''
                error = rec['error'] or ''
                category = classify(error)
                caption = (f'FIX_SOURCE:{rec["source_id"]}' if kind == 'source'
                           else f'FIX_ARTIFACT:{sha}')
                result.append({
                    'type': kind,
                    'source_id': rec['source_id'],
                    'artifact_sha256': sha,
                    'file_name': rec['file_name'] or '',
                    'category': category,
                    'file_exists': 'yes' if exists else 'no',
                    'file_path': rel,
                    'repair_caption': caption,
                    'error': error,
                    'recommendation': REMEDIES[category],
                })
        db.execute('COMMIT')
    finally:
        db.close()
    return result


def csv_bytes(rows: list[dict]) -> bytes:
    output = io.StringIO(newline='')
    writer = csv.DictWriter(output, fieldnames=COLUMNS, extrasaction='ignore')
    writer.writeheader()
    for row in rows:
        writer.writerow({c: excel_safe(row.get(c, ''), max_chars=800 if c == 'error' else 280)
                         for c in COLUMNS})
    data = ('\ufeff' + output.getvalue()).encode('utf-8')
    if len(data) > 8_000_000:
        raise ValueError('CSV quá lớn (>8 MB); cần xuất theo nhóm')
    return data


def summary_text(rows: list[dict]) -> str:
    kinds = Counter(r['type'] for r in rows)
    cats = Counter(r['category'] for r in rows)
    body = '\n'.join(f'• {name}: {num}' for name, num in cats.most_common(12))
    return (f'📋 OUMIJIA — Nguồn Held: {kinds["source"]}; Artifact Held: {kinds["artifact"]}\n'
            + (body or 'Chưa có lỗi.')
            + '\n/errorscsv — CSV đầy đủ; /errorsfiles — CSV file còn trên ổ; /errors 158 — chi tiết nguồn.')


def detail_text(rows: list[dict], kind: str, key: str) -> str:
    for r in rows:
        if (kind == 'source' and r['type'] == 'source' and str(r['source_id']) == key) or (
                kind == 'artifact' and r['type'] == 'artifact' and r['artifact_sha256'] == key):
            return ('🔎 OUMIJIA — ' + r['type'].upper() + ' ' + key + '\n'
                    + 'File: ' + str(r['file_name'])[:140] + '\n'
                    + 'Nhóm: ' + r['category'] + '\n'
                    + 'Còn trên ổ: ' + r['file_exists'] + '\n'
                    + 'Đường dẫn: ' + r['file_path'][:350] + '\n'
                    + 'Lỗi: ' + re.sub(r'[\r\n]+', ' ', r['error'])[:850] + '\n'
                    + 'Đề xuất: ' + r['recommendation'] + '\n'
                    + 'Caption sửa: ' + r['repair_caption'])
    return 'Không tìm thấy bản ghi held tương ứng. Có thể file đã được xử lý.'


def _post_document(token: str, chat_id: str, filename: str, contents: bytes, caption: str) -> int:
    if not token or not chat_id:
        raise RuntimeError('Chưa cấu hình bot hoặc chat điều khiển')
    if len(contents) > 8_000_000:
        raise ValueError('Tệp CSV vượt giới hạn nội bộ')
    boundary = '----Oumijia' + uuid.uuid4().hex
    b = boundary.encode('ascii')
    def field(name, value):
        return (b'--' + b + b'\r\nContent-Disposition: form-data; name="' + name.encode('ascii') +
                b'"\r\n\r\n' + value.encode('utf-8') + b'\r\n')
    if not re.fullmatch(r'[A-Za-z0-9_.-]{1,80}', filename):
        raise ValueError('Invalid report filename')
    body = (field('chat_id', str(chat_id)) + field('caption', caption[:600]) +
            b'--' + b + b'\r\nContent-Disposition: form-data; name="document"; filename="' +
            filename.encode('ascii') + b'"\r\nContent-Type: text/csv\r\n\r\n' +
            contents + b'\r\n--' + b + b'--\r\n')
    request = Request('https://api.telegram.org/bot' + token + '/sendDocument',
                      data=body, headers={'Content-Type': 'multipart/form-data; boundary=' + boundary}, method='POST')
    try:
        with urlopen(request, timeout=45) as response:
            payload = json.load(response)
    except HTTPError as ex:
        # HTTPError's URL includes the bot token. Never log or propagate it.
        raise RuntimeError(f'Telegram API báo HTTP {ex.code}') from None
    except URLError:
        raise RuntimeError('Không kết nối được Telegram Bot API') from None
    if payload.get('ok') is not True:
        raise RuntimeError('Telegram Bot API từ chối gửi CSV')
    return int(payload['result']['message_id'])


async def send_document(token: str, chat_id: str, filename: str, contents: bytes, caption: str) -> int:
    return await asyncio.to_thread(_post_document, token, chat_id, filename, contents, caption)
