"""Manual Telegram control-room staging. ALL uploads require an admin /process.

No file contents are downloaded here. Message IDs are recorded as negative
integers because the historical source-chat IDs are positive. Group identity is
fixed by EDZ_INBOX_CHAT; the bot refuses a second group in the same SQLite DB.
"""
from __future__ import annotations

import re
from .archive import file_kind, safe_basename

_MULTIPART = re.compile(r'^(.+\.7z)\s+(\d{1,6})$', re.IGNORECASE)


def valid_ids(spec: str) -> frozenset[int]:
    ids = set()
    for item in spec.split(','):
        if item.strip():
            number = int(item.strip())
            if number <= 0:
                raise ValueError('EDZ_INBOX_USER_IDS chỉ nhận Telegram user ID dương')
            ids.add(number)
    return frozenset(ids)


def message_allowed(msg, admin_ids: frozenset[int]) -> bool:
    return bool(admin_ids and int(getattr(msg, 'sender_id', 0) or 0) in admin_ids)


def _manual_group(name: str) -> str:
    return 'manual:' + name.casefold()


def stage(db, message_id: int, name: str, size: int) -> tuple[str, str]:
    """Only metadata. Never assign 'new' here, even for a single EDZ."""
    if message_id <= 0 or size <= 0:
        raise ValueError('Thiếu Message ID hoặc dung lượng Telegram')
    name = safe_basename(name)
    kind, group, index = file_kind(name)
    if kind not in ('edz', 'archive', 'volume'):
        raise ValueError('Chỉ nhận .edz, .7z, .7z.001/.002...')
    mid = -message_id
    old = db.message(mid)
    if old:
        if old['file_name'] != name or int(old['size_bytes']) != size:
            raise ValueError('Tin nhắn đã được ghi với thông tin khác; cần kiểm tra thủ công')
        return 'exists', name
    if kind == 'volume':
        # Do not mix a late message into a group already authorized to process.
        existing = list(db.group_rows(_manual_group(group)))
        if any(r['state'] != 'staged' for r in existing):
            raise ValueError('Bộ cùng tên đã bắt đầu xử lý; hãy đổi tên bộ mới')
    db.add_message(mid, name, kind, size,
                   _manual_group(group) if kind == 'volume' else None, index)
    db.change_message(mid, 'staged')
    return 'staged', name


def staged_rows(db):
    return list(db.execute("""SELECT * FROM messages WHERE id < 0 AND state='staged'
                              ORDER BY changed_at,id"""))


def staged_summary(db) -> tuple[int, int, int]:
    rows = staged_rows(db)
    singles = sum(r['kind'] in ('edz','archive') for r in rows)
    volumes = sum(r['kind'] == 'volume' for r in rows)
    total = sum(int(r['size_bytes']) for r in rows)
    return singles, volumes, total


def _free_budget(free_bytes: int, reserve_bytes: int, compressed_bytes: int) -> bool:
    # Conservative peak storage bound for download + unpack + work copies.
    return free_bytes >= reserve_bytes + max(3 * compressed_bytes, 1024**3)


def activate_singles(db, *, limit_bytes: int, reserve_bytes: int,
                     free_bytes: int, max_jobs: int = 10000) -> tuple[int, int, int]:
    """Explicit admin authorization for standalone EDZ and 7Z only.

    Multipart pieces remain staged until /process Name.7z N. Individually too
    large and insufficient-disk singles are left staged, not silently lost.
    """
    rows = [r for r in staged_rows(db) if r['kind'] in ('edz','archive')]
    if len(rows) > max_jobs:
        raise ValueError('Quá nhiều file chờ, hãy chia thành lô nhỏ hơn')
    accepted = []
    blocked = 0
    # Reserve peak room for all newly approved singles; downloading is sequential.
    available = max(0, free_bytes - reserve_bytes - 1024**3)
    # Account for already queued manual files to avoid approving an oversized
    # second batch while the first batch has not yet downloaded any bytes.
    other = db.execute("""SELECT COALESCE(SUM(size_bytes),0) FROM messages
                         WHERE id < 0 AND state IN ('new','retry','downloading','downloaded','processing')""").fetchone()[0]
    available = max(0, available - 3 * int(other))
    for row in rows:
        size = int(row['size_bytes'])
        if size > limit_bytes or 3 * size > available:
            blocked += 1
            continue
        accepted.append(row)
        available -= 3 * size
    db.execute('SAVEPOINT manual_singles')
    try:
        for row in accepted:
            db.change_message(row['id'], 'new')
        db.execute('RELEASE SAVEPOINT manual_singles')
    except BaseException:
        db.execute('ROLLBACK TO SAVEPOINT manual_singles')
        db.execute('RELEASE SAVEPOINT manual_singles')
        raise
    return len(accepted), blocked, len(rows)


def inspect_group(db, name: str, expected: int) -> tuple[list, list[int], int]:
    name = safe_basename(name)
    if not name.lower().endswith('.7z') or not 1 <= expected <= 100000:
        raise ValueError('Dùng: /process Ten-goi.7z SO_PHAN')
    rows = list(db.group_rows(_manual_group(name)))
    if not rows:
        raise ValueError('Chưa có phần nào của bộ ' + name)
    indexes = [int(r['volume_index']) for r in rows]
    if len(indexes) != len(set(indexes)):
        raise ValueError('Bộ có các phần trùng số thứ tự, cần kiểm tra')
    missing = sorted(set(range(1, expected + 1)) - set(indexes))
    extras = sorted(set(indexes) - set(range(1, expected + 1)))
    if extras:
        raise ValueError('Bộ có phần vượt quá tổng số đã khai báo: ' + str(extras[:15]))
    return rows, missing, sum(int(r['size_bytes']) for r in rows)


def activate_group(db, name: str, expected: int, *, limit_bytes: int,
                   reserve_bytes: int, free_bytes: int) -> tuple[int, int]:
    """Requires an explicit exact part count; no guessing the last 7z segment."""
    rows, missing, total = inspect_group(db, name, expected)
    if missing or len(rows) != expected:
        raise ValueError(f'Bộ thiếu phần: {missing[:20]} (đã có {len(rows)}/{expected})')
    if any(r['state'] != 'staged' for r in rows):
        raise ValueError('Bộ đã xếp hàng hoặc đang xử lý, không chạy lặp')
    if total > limit_bytes:
        raise ValueError('Bộ vượt giới hạn 60 GiB; cần chia thành các gói nhỏ hơn')
    if not _free_budget(free_bytes, reserve_bytes, total):
        raise ValueError('Không đủ dung lượng SSD và mức dự phòng an toàn')
    db.execute('SAVEPOINT manual_group')
    try:
        for r in rows:
            db.change_message(r['id'], 'new')
        db.execute('RELEASE SAVEPOINT manual_group')
    except BaseException:
        db.execute('ROLLBACK TO SAVEPOINT manual_group')
        db.execute('RELEASE SAVEPOINT manual_group')
        raise
    return len(rows), total


def cancel_pending(db) -> int:
    """Cancel only not-yet-started manual jobs. In-flight work is unaffected."""
    return db.execute("""UPDATE messages SET state='cancelled',
                      error='Đã hủy theo lệnh quản trị',changed_at=strftime('%s','now')
                      WHERE id < 0 AND state IN ('staged','new','retry')""").rowcount


def parse_command(text: str) -> tuple[str, str | None, int | None] | None:
    """Recognize commands to @any_bot username with strict syntax."""
    command_text = (text or '').strip()
    if command_text.casefold() == 'help':
        return 'help', None, None
    m = re.fullmatch(r'/(\w+)(?:@\w+)?(?:\s+(.+))?', command_text, re.S)
    if not m:
        return None
    command = m.group(1).lower()
    arg = (m.group(2) or '').strip()
    if command == 'process':
        if not arg:
            return command, None, None
        match = _MULTIPART.fullmatch(arg)
        if match:
            return command, safe_basename(match.group(1)), int(match.group(2))
        raise ValueError('Dùng /process hoặc /process Ten-goi.7z SO_PHAN')
    # /files now owns the multipart inspection; /check stays as a hidden alias.
    if command in ('files', 'check') and arg:
        match = _MULTIPART.fullmatch(arg)
        if match:
            return command, safe_basename(match.group(1)), int(match.group(2))
        raise ValueError(f'Dùng /files Ten-goi.7z SO_PHAN (alias: /{command})')
    if command == 'limit':
        if not arg:
            return 'limit', None, None
        if re.fullmatch(r'(\d+(?:\.\d+)?|reset)', arg, re.IGNORECASE):
            return 'limit', arg.lower(), None
        raise ValueError('Dùng /limit để xem, /limit 3 đặt 3 giây, /limit 0 không giới hạn, hoặc /limit reset')
    if command in ('start','help','files','check','status','errors','cancel','server','health','cpu','ram','disk','uptime','speed') and not arg:
        return command, None, None
    return None
