"""Persistent, deduplicated Telegram notifications for held EDZ jobs.

ERROR_CHAT=me sends to Telegram Saved Messages. Reserve an alert before
networking to avoid repeat sends after uncertain delivery; failures remain
visible in systemd journal and can be investigated manually.
"""
from __future__ import annotations

import hashlib
import time

from .publish_guard import flood_error

DDL = '''
CREATE TABLE IF NOT EXISTS auto_alerts (
    error_key TEXT PRIMARY KEY,
    text TEXT NOT NULL,
    state TEXT NOT NULL,
    sent_message INTEGER,
    created_at REAL NOT NULL,
    changed_at REAL NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    retry_after REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS auto_issues (
    issue_key TEXT PRIMARY KEY,
    text TEXT NOT NULL,
    created_at REAL NOT NULL
);
'''


def _schema(db):
    db.conn.executescript(DDL)


def record_issue(db, code: str, description: str) -> None:
    _schema(db)
    description = str(description)[:900]
    key = hashlib.sha256((code + ':' + description).encode()).hexdigest()
    db.execute('INSERT OR IGNORE INTO auto_issues VALUES(?,?,?)',
               (key, f'{code}: {description}', time.time()))


def vi_reason(error: str | None) -> str:
    """Concise Vietnamese user-facing classification; original error stays in SQLite."""
    e = (error or '').lower()
    if flood_error(error):
        return 'Telegram đang giới hạn tốc độ gửi. Hệ thống sẽ chờ và đối chiếu trước khi thử lại.'
    if 'protected chat' in e or 'protected against forwarding' in e or "can't forward" in e:
        return 'Kênh nguồn bật bảo vệ nội dung. Không thể chuyển tiếp; cần xem xét thủ công.'
    if 'splitter reported partial' in e or 'missing item for' in e:
        return 'EDZ thiếu tài nguyên tham chiếu (macro, ảnh hoặc tài liệu). Không đăng bản chưa hoàn chỉnh.'
    if 'sha256' in e or 'checksum' in e or 'hash mismatch' in e:
        return 'Mã kiểm tra toàn vẹn SHA-256 không khớp. File được giữ lại để kiểm tra.'
    if 'interrupted' in e or 'ambiguous posting outcome' in e or 'timeout' in e:
        return 'Chưa xác định được kết quả đăng Telegram. Cần đối chiếu kênh đích trước khi thử lại.'
    if 'disk' in e or 'space' in e or 'headroom' in e:
        return 'Dung lượng ổ đĩa không đủ mức dự phòng an toàn. Đã ngừng tác vụ liên quan.'
    if 'archive' in e or '7z' in e or 'volume' in e or 'corrupt' in e:
        return 'Dữ liệu nén có vấn đề về cấu trúc hoặc không đủ phần; cần kiểm tra thủ công.'
    return 'Lỗi chưa phân loại, cần kiểm tra nhật ký kỹ thuật trên máy chủ.'


def _retry_message_vi(key: str, original: str, db) -> str:
    if key.startswith('artifact:'):
        sha = key.split(':', 2)[1]
        row = db.execute('SELECT file_name,source_message,error FROM artifacts WHERE sha256=?', (sha,)).fetchone()
        if row and not flood_error(row['error']):
            return (f'⚠️ OUMIJIA — LỖI FILE EDZ CẦN KIỂM TRA\n'
                    f'File: {row["file_name"]}\nNguồn: #{row["source_message"]}\n'
                    f'Nguyên nhân: {vi_reason(row["error"])}\n'
                    'Trạng thái: Giữ lại file này; các file hợp lệ khác tiếp tục được xử lý.')
    if key.startswith('src:'):
        try:
            mid = int(key.split(':', 2)[1])
        except ValueError:
            return '⚠️ OUMIJIA — Cảnh báo cũ cần kiểm tra nhật ký.'
        row = db.message(mid)
        if row:
            return (f'⚠️ OUMIJIA — LỖI NGUỒN CẦN KIỂM TRA\n'
                    f'Tin nguồn: #{mid}\nFile: {row["file_name"]}\n'
                    f'Nguyên nhân: {vi_reason(row["error"])}\n'
                    'Trạng thái: Đã giữ lại nguồn lỗi; các nguồn hợp lệ tiếp tục xử lý.')
    return '⚠️ OUMIJIA — Có cảnh báo hệ thống. Xem nhật ký kỹ thuật trên server để biết chi tiết.'


def next_alert(db) -> tuple[str, str] | None:
    _schema(db)
    # Requeue a notification whose network response went missing, rather than
    # silently dropping errors forever. Duplicate *alerts* are harmless; Telegram
    # files themselves are still never blindly re-sent.
    db.execute("""UPDATE auto_alerts SET state='pending_retry',retry_after=?
                  WHERE state='sending' AND changed_at < ?""",
               (time.time(), time.time() - 600))
    existing = db.execute("""SELECT error_key,text FROM auto_alerts
             WHERE state='pending_retry' AND retry_after <= ?
               AND text NOT LIKE '%A wait of % seconds is required%'
               AND text NOT LIKE '%FloodWait%'
             ORDER BY changed_at LIMIT 1""", (time.time(),)).fetchone()
    if existing:
        return existing['error_key'], _retry_message_vi(existing['error_key'], existing['text'], db)
    # Report prior permanent errors; ready/posted files are never called errors.
    events: list[tuple[str, str]] = []
    for row in db.execute("""SELECT id,file_name,error FROM messages
                             WHERE state='held' ORDER BY changed_at LIMIT 10000"""):
        reason = row['error'] or 'Unknown source error'
        if flood_error(reason):
            continue
        key = 'src:' + str(row['id']) + ':' + hashlib.sha256(reason.encode()).hexdigest()
        events.append((key, f'⚠️ OUMIJIA — LỖI NGUỒN CẦN KIỂM TRA\n'
                            f'Tin nguồn: #{row["id"]}\nFile: {row["file_name"]}\n'
                            f'Nguyên nhân: {vi_reason(reason)}\n'
                            'Trạng thái: Đã giữ lại nguồn lỗi; các nguồn hợp lệ tiếp tục xử lý.'))
    for row in db.execute("""SELECT sha256,file_name,error,source_message FROM artifacts
                             WHERE state='held' ORDER BY updated_at LIMIT 10000"""):
        reason = row['error'] or 'Unknown artifact error'
        if flood_error(reason):
            continue
        key = 'artifact:' + row['sha256'] + ':' + hashlib.sha256(reason.encode()).hexdigest()
        events.append((key, f'⚠️ OUMIJIA — LỖI FILE EDZ CẦN KIỂM TRA\n'
                            f'File: {row["file_name"]}\nNguồn: #{row["source_message"]}\n'
                            f'SHA256: {row["sha256"]}\nNguyên nhân: {vi_reason(reason)}\n'
                            'Trạng thái: Giữ lại file này; các file hợp lệ khác tiếp tục được xử lý.'))
    for row in db.execute('SELECT issue_key,text FROM auto_issues ORDER BY created_at LIMIT 10000'):
        events.append(('internal:' + row['issue_key'],
                       '⚠️ OUMIJIA — LỖI HỆ THỐNG\n' + vi_reason(row['text']) + '\nThông tin kỹ thuật: ' + row['text'][:250]))
    for key, body in events:
        old = db.execute('SELECT 1 FROM auto_alerts WHERE error_key=?', (key,)).fetchone()
        if old is None:
            return key, body
    return None


def mark_alert_sending(db, key: str, body: str) -> bool:
    _schema(db)
    now = time.time()
    if db.execute('SELECT state FROM auto_alerts WHERE error_key=?', (key,)).fetchone():
        return db.execute('''UPDATE auto_alerts
             SET state='sending',text=?,attempts=attempts+1,changed_at=?
             WHERE error_key=? AND state='pending_retry' AND retry_after<=?''',
             (body,now,key,now)).rowcount == 1
    return db.execute('''INSERT OR IGNORE INTO auto_alerts
           (error_key,text,state,sent_message,created_at,changed_at,attempts,retry_after)
           VALUES (?,?,'sending',NULL,?,?,1,0)''', (key,body,now,now)).rowcount == 1


def mark_alert_sent(db, key: str, message_id: int) -> None:
    _schema(db)
    db.execute("UPDATE auto_alerts SET state='sent',sent_message=?,changed_at=? WHERE error_key=? AND state='sending'",
               (message_id,time.time(),key))


def mark_alert_failed(db, key: str):
    _schema(db)
    row = db.execute('SELECT attempts FROM auto_alerts WHERE error_key=?', (key,)).fetchone()
    if row:
        delay = min(1800, 60 * 2 ** min(row['attempts']-1, 5))
        db.execute('''UPDATE auto_alerts SET state='pending_retry',retry_after=?,changed_at=?
                      WHERE error_key=? AND state='sending' ''',
                   (time.time()+delay,time.time(),key))
