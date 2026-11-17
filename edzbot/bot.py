"""Telegram MTProto collector + publisher. Interactive credentials never leave local CLI."""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
import sqlite3
import time

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.tl.types import DocumentAttributeFilename

from .archive import file_kind, safe_basename, free_space_for, test_archive
from .config import Config
from .db import DB
from .metadata import tag
from .pipeline import Pipeline
from .publish_guard import (cooldown_remaining, set_cooldown, next_send_remaining,
                            schedule_next_send, reconcile_flood_held, held_flood_rows,
                            flood_seconds_from_message)
from .storage_policy import capacity_reason
from .rate_schedule import count as rate_count, record as record_flood, ensure_cycle_anchor
from .publish_delay import current_delay, set_delay, reset_delay, override_delay
from .manual_inbox import (stage as stage_manual, activate_singles, activate_group,
                           inspect_group, staged_rows, staged_summary, cancel_pending,
                           parse_command, message_allowed)
from .bot_notifications import send as bot_send
from .server_monitor import snapshot as server_snapshot, format_snapshot
from .upload_engine import UploadEngine, EngineConfig

log = logging.getLogger(__name__)


def session_path(cfg: Config) -> str:
    session_dir = cfg.data / 'session'
    session_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    session_dir.chmod(0o700)
    return str(session_dir / 'edzbot')


def open_client(cfg: Config) -> TelegramClient:
    return TelegramClient(session_path(cfg), cfg.api_id, cfg.api_hash,
                          flood_sleep_threshold=300, request_retries=5,
                          connection_retries=5, retry_delay=5,
                          auto_reconnect=True)


def media_filename(msg) -> str | None:
    if not getattr(msg, 'document', None):
        return None
    try:
        name = msg.file.name
    except (AttributeError, ValueError):
        return None
    return safe_basename(name) if name else None


def media_size(msg) -> int:
    return int(getattr(getattr(msg, 'document', None), 'size', 0) or 0)


async def interactive_login(cfg: Config):
    cfg.setup()
    client = open_client(cfg)
    # Telethon prompts on the current TTY for SMS/app code and 2FA password.
    # Do not provide those secrets to any third party or in a chat.
    await client.start(phone=cfg.phone if cfg.phone else lambda: input('Telegram phone (+countrycode...): ').strip())
    me = await client.get_me()
    print('Login OK; account id=', me.id, 'username=', me.username or '(none)')
    await client.disconnect()
    (cfg.data / 'session' / 'edzbot.session').chmod(0o600)


async def show_dialogs(cfg: Config):
    client = open_client(cfg)
    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise RuntimeError('Not signed in. Run python -m edzbot login first.')
        async for d in client.iter_dialogs(limit=250):
            if d.is_channel:
                print(f'{d.id:>17}  {d.name}')
    finally:
        await client.disconnect()


class Service:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        cfg.setup()
        self.db = DB(cfg.data / 'state.sqlite3')
        self.client = open_client(cfg)
        self.source = None
        self.target = None
        self.inbox = None
        self.inbox_allowed = cfg.inbox_user_ids
        self.publisher_mode = cfg.publisher_mode
        self.upload_engine = None
        if cfg.upload_engine_v2:
            self.upload_engine = UploadEngine(
                self.db, self.client, None,   # resolved entities assigned in run()
                source=None,
                cfg=EngineConfig(
                    concurrency=cfg.upload_concurrency,
                    max_concurrency=cfg.upload_max_concurrency,
                    max_retries=cfg.upload_max_retries,
                    max_upload_bytes=cfg.max_upload_bytes,
                    canary_name=cfg.upload_canary_name,
                    canary_mode=(cfg.publisher_mode == 'canary')),
                get_delay=lambda: current_delay(self.db, self.cfg),
                sysinfo=lambda: server_snapshot(self.cfg.data, self.cfg.reserve_bytes),
            )
            self.upload_engine.recover_posting()

    def enqueue(self, msg):
        if getattr(msg, 'noforwards', False):
            log.warning('Skipping protected message %s', msg.id)
            return
        name = media_filename(msg)
        if not name:
            return
        kind, group, index = file_kind(name)
        if kind:
            if self.cfg.auto_mode:
                if self.db.message(msg.id) is not None:
                    return
                # Stage new uploads; do not bypass sequential source selection.
                # Multipart parts are staged until the full quiet, contiguous set is seen.
                self.db.add_message(msg.id, name, kind, media_size(msg),
                                    group.casefold() if group else None, index)
                self.db.change_message(msg.id, 'staged')
            else:
                self.db.add_message(msg.id, name, kind, media_size(msg),
                                    group.casefold() if group else None, index)
            # OUMIJIA_EDZ_REVIEW_V023: source caption repair mapping
            from .review_queue import register_repair
            register_repair(self.db, msg, int(msg.id))

    async def _manual_notice(self, text: str):
        """Write to the SAME private control group, as the BotFather bot."""
        if self.cfg.inbox_chat is None:
            return None
        try:
            return await bot_send(self.cfg.notify_bot_token, str(self.cfg.inbox_chat), text)
        except Exception:
            log.exception('Không gửi được thông báo trong nhóm điều khiển EDZ')
            return None

    def _pipeline_status_short_v163(self) -> str:
        if not self.cfg.continuous_pipeline:
            return 'Pipeline: tuần tự (v16.1)\n'
        from .pipeline_parallel import stats
        s = stats(self.db)
        return (f'Pipeline: song song (v16.3)\n'
                f'📥 Đang tải: {s["downloading"]}, nguồn chờ tải: {s["queued"]}\n'
                f'⚙️ Đang tách: {s["processing"]}, chờ tách: {s["downloaded"]}\n'
                f'📤 Đang gửi: {s["posting"]}, EDZ chờ đăng: {s["ready"]}\n'
                f'Giới hạn tải Telegram: {round(s["download_cooldown"])} giây\n')

    def _v2_status_short(self) -> str:
        if self.upload_engine is None:
            return 'Upload V2: TẮT (đang dùng Publisher V1)\n'
        s = self.upload_engine.stats.snapshot()
        q = {r['state']: r['n'] for r in self.db.execute(
            'SELECT state,COUNT(*) n FROM upload_jobs GROUP BY state').fetchall()}
        fault = ('\n⚠️ Upload V2 FAULTED — publisher đã khóa, cần operator xử lý\n'
                 if self.upload_engine.faulted else '')
        return (f'🚀 Upload V2: BẬT (concurrency '
                f'{self.upload_engine.governor.current}/{self.upload_engine.governor.maximum})\n'
                f'   Đã đăng: {s["files"]} ({s["mib"]} MiB) · '
                f'{s["mib_per_s"]} MiB/s · {s["files_per_min"]} file/phút\n'
                f'   ACK TB: {s["avg_ack_s"]}s · FloodWait: {s["floodwaits"]} · '
                f'duplicate: {s["duplicates"]}\n'
                f'   Hàng đợi: {q}{fault}')

    async def handle_manual(self, msg):
        """Single room, authorized admin commands, no implicit download on upload."""
        if not message_allowed(msg, self.inbox_allowed):
            return
        text = getattr(msg, 'raw_text', '') or ''
        # Admin-only: stream an already-staged 7z through the V2 publisher.
        if text.strip().lower().startswith('/stream7z '):
            if not self.cfg.upload_archive_stream_enabled:
                await self._manual_notice('⚠️ Streaming archive bị tắt (UPLOAD_ARCHIVE_STREAM_ENABLED=false).')
                return
            arg = text.strip().split(None, 1)[1]
            try:
                result = await self.publish_archive_v2(Path(arg))
                await self._manual_notice(
                    f'✅ V2 stream 7z: entries={result["entries"]}, '
                    f'posted={result["posted"]}, skipped={result["skipped"]}')
            except Exception as ex:
                log.exception('V2 archive stream command failed')
                await self._manual_notice('⚠️ V2 stream 7z thất bại: ' + str(ex)[:300])
            return
        # V2 stats is handled here (parse_command has no such verb).
        if text.strip().lower() in ('/v2stats', 'v2stats'):
            if self.upload_engine is None:
                await self._manual_notice('Upload Engine V2 chưa được bật (UPLOAD_ENGINE_V2=false).')
            else:
                s = self.upload_engine.stats.snapshot()
                q = {r['state']: r['n'] for r in self.db.execute(
                    'SELECT state,COUNT(*) n FROM upload_jobs GROUP BY state').fetchall()}
                await self._manual_notice(
                    '⚙️ OUMIJIA — Upload Engine V2\n'
                    f'Concurrency: {self.upload_engine.governor.current}/'
                    f'{self.upload_engine.governor.maximum}\n'
                    f'Đã đăng: {s["files"]} ({s["mib"]} MiB), {s["mib_per_s"]} MiB/s, '
                    f'{s["files_per_min"]} file/phút\n'
                    f'ACK trung bình: {s["avg_ack_s"]}s\n'
                    f'FloodWait: {s["floodwaits"]}, duplicate: {s["duplicates"]}, '
                    f'retry: {s["retries"]}\n'
                    f'Hàng đợi: {q}')
            return
        # OUMIJIA_REVIEW_CSV_V023: admin-only read-only report requests
        from .review_reports import (parse_request, query_held, csv_bytes,
                                     summary_text, detail_text, send_document)
        review_command = parse_request(text)
        if review_command is not None:
            action, target = review_command
            if action == 'invalid':
                await self._manual_notice('⚠️ Dùng /errors, /errorscsv, /errorsfiles, /errors 158 hoặc /errors <SHA256>.')
                return
            try:
                rows = await asyncio.to_thread(query_held, self.cfg.data.parent,
                                               existing_only=(action == 'files'))
                if action == 'summary':
                    await self._manual_notice(summary_text(rows))
                elif action == 'detail':
                    await self._manual_notice(detail_text(rows, target[0], target[1]))
                else:
                    import datetime
                    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d-%H%M%S')
                    filename = ('oumijia_errors_files_' if action == 'files' else 'oumijia_errors_all_') + timestamp + '.csv'
                    payload = await asyncio.to_thread(csv_bytes, rows)
                    msg_id = await send_document(self.cfg.notify_bot_token,
                        str(self.cfg.inbox_chat), filename, payload,
                        f'OUMIJIA {action}: {len(rows)} bản ghi held; chỉ quản trị viên yêu cầu')
                    log.info('Review CSV sent to authorized control chat: %d rows, message #%s',len(rows),msg_id)
            except Exception:
                log.exception('Review report command failed (no changes to Publisher)')
                await self._manual_notice('⚠️ Không tạo hoặc gửi được báo cáo CSV. Kiểm tra cấu hình bot và nhật ký.')
            return
        try:
            command = parse_command(text)
        except ValueError as ex:
            await self._manual_notice('⚠️ ' + str(ex))
            return
        if command is not None:
            verb, group_name, expected = command
            if verb in ('start','help'):
                await self._manual_notice(
                    '🤖 OUMIJIA — TRUNG TÂM ĐIỀU KHIỂN\n\n'
                    '📦 QUẢN LÝ EDZ\n\n'
                    '- "/files" — Xem file đang chờ duyệt.\n'
                    '- "/process" — Duyệt tải và xử lý file.\n'
                    '- "/cancel" — Hủy các file chưa bắt đầu.\n\n'
                    '📊 HỆ THỐNG\n\n'
                    '- "/status" — Trạng thái EDZ, Upload V2 và tốc độ đăng.\n'
                    '- "/health" — CPU, RAM, SSD và dịch vụ.\n'
                    '- "/errors" — Xem lỗi cần xử lý.\n\n'
                    '⚙️ CÀI ĐẶT\n\n'
                    '- "/limit" — Xem hoặc chỉnh khoảng nghỉ upload.\n'
                    '- "/help" — Xem hướng dẫn.\n\n'
                    'Lệnh mở rộng:\n\n'
                    '- "/files ABC.7z 3" — Kiểm tra đủ 3 phần nén.\n'
                    '- "/process ABC.7z 3" — Duyệt xử lý bộ nén.\n'
                    '- "/errors 158" — Xem chi tiết lỗi.\n'
                    '- "/errors csv" — Xuất báo cáo lỗi.\n'
                    '- "/limit 3" — Nghỉ 3 giây giữa các lượt đăng.\n'
                    '- "/limit reset" — Khôi phục mức nghỉ mặc định.\n\n'
                    '🔒 Chỉ quản trị viên được phép điều khiển. '
                    'File gửi vào nhóm chỉ được ghi nhận; cần "/process" để duyệt tải và xử lý.')
                return
            if verb == 'files':
                rows = staged_rows(self.db)
                display = '\n'.join(f'• {r["file_name"]} ({r["size_bytes"]/1024**2:.1f} MiB)'
                                    for r in rows[:15])
                await self._manual_notice(f'📥 File chờ lệnh: {len(rows)}\n'
                                          + (display or 'Không có file chờ')
                                          + ('\n...và các file khác' if len(rows)>15 else ''))
                return
            if verb == 'check':
                if group_name:
                    try:
                        rows, missing, total = inspect_group(self.db, group_name, expected)
                        await self._manual_notice(
                            f'🧩 {group_name}: đã có {len(rows)}/{expected} phần; '
                            f'tổng {total/1024**3:.2f} GiB. '
                            + (f'Thiếu: {missing[:30]}' if missing else 'Đủ số thứ tự; chưa tải/kiểm tra CRC.')
                        )
                    except ValueError as ex:
                        await self._manual_notice('⚠️ ' + str(ex))
                else:
                    n, parts, size = staged_summary(self.db)
                    await self._manual_notice(
                        f'🔎 Có {n} EDZ/7z độc lập và {parts} phần 7z đang chờ lệnh. '
                        f'Tổng metadata: {size/1024**3:.2f} GiB. Chưa tải file nào từ hộp nhận.')
                return
            if verb == 'process':
                try:
                    free = shutil.disk_usage(self.cfg.data).free
                    if group_name:
                        n, total = activate_group(self.db, group_name, expected,
                            limit_bytes=self.cfg.max_group_bytes,
                            reserve_bytes=self.cfg.reserve_bytes, free_bytes=free)
                        await self._manual_notice(
                            f'✅ Đã duyệt xử lý {group_name}: {n} phần, {total/1024**3:.2f} GiB. '
                            'Bắt đầu tải theo hàng đợi; sẽ kiểm tra CRC trước khi đăng EDZ.')
                    else:
                        n, blocked, total = activate_singles(self.db,
                            limit_bytes=self.cfg.max_group_bytes,
                            reserve_bytes=self.cfg.reserve_bytes, free_bytes=free)
                        await self._manual_notice(
                            f'✅ /process: đã duyệt {n}/{total} file EDZ/7z độc lập. '
                            f'{blocked} file chưa đủ dung lượng hoặc vượt giới hạn, vẫn chờ. '
                            'Các bộ .7z.001 cần lệnh /process Ten.7z SO_PHAN riêng.')
                except ValueError as ex:
                    await self._manual_notice('⚠️ Chưa thể xử lý: ' + str(ex))
                return
            if verb == 'cancel':
                stopped = cancel_pending(self.db)
                await self._manual_notice(f'🛑 Đã hủy {stopped} file thủ công đang chờ. '
                    'File đang tải/đang gửi sẽ kết thúc tại điểm an toàn, không tự ngắt giữa chừng.')
                return
            if verb == 'status':
                stats = self.db.stats()
                a = {x['state']: x['count'] for x in stats['artifacts']}
                staged = staged_summary(self.db)
                delay_source = 'Telegram /limit' if override_delay(self.db) is not None else 'SEND_DELAY_SECONDS'
                await self._manual_notice(
                    '📊 OUMIJIA — Trạng thái\n'
                    f'Đã đăng: {a.get("posted",0)}\n'
                    f'Chờ đăng: {a.get("ready",0)}\n'
                    f'Lỗi giữ lại: {a.get("held",0)}\n'
                    f'File thủ công chưa duyệt: {staged[0]} file + {staged[1]} phần 7z\n'
                    f'EDZ từng gặp FloodWait: {rate_count(self.db)}\n'
                    f'Khoảng nghỉ hiện tại: {current_delay(self.db, self.cfg):g} giây '
                    f'(nguồn: {delay_source}; 0 = không giới hạn)\n'
                    + self._pipeline_status_short_v163()
                    + self._v2_status_short()
                    + f'Telegram yêu cầu chờ thêm: {round(cooldown_remaining(self.db))} giây')
                return
            if verb in ('server', 'health', 'cpu', 'ram', 'disk', 'uptime'):
                try:
                    current = await asyncio.to_thread(
                        server_snapshot, self.cfg.data, self.cfg.reserve_bytes)
                    await self._manual_notice(format_snapshot(current, mode=verb))
                except Exception:
                    log.exception('Không lấy được tài nguyên server')
                    await self._manual_notice('⚠️ Không đọc được thông số server; xem nhật ký giám sát.')
                return
            if verb == 'speed':
                delay = current_delay(self.db, self.cfg)
                source = ('Telegram /limit (ghi đè)' if override_delay(self.db) is not None
                          else f'mặc định SEND_DELAY_SECONDS={self.cfg.send_delay:g}')
                await self._manual_notice(
                    '⚡ OUMIJIA — Điều tốc Telegram\n'
                    f'Khoảng nghỉ giữa 2 EDZ: {delay:g} giây'
                    + (' (không giới hạn)' if delay == 0 else '') + '\n'
                    f'Nguồn cấu hình: {source}\n'
                    f'Cooldown do Telegram yêu cầu (FloodWait): {round(cooldown_remaining(self.db))} giây\n'
                    f'EDZ từng gặp FloodWait: {rate_count(self.db)}\n'
                    'Đổi trực tiếp: /limit <giây>  (ví dụ /limit 5, /limit 0 = không giới hạn)\n'
                    'Trả về mặc định: /limit reset\n'
                    'FloodWait của Telegram luôn được tôn trọng; /limit 0 chỉ bỏ khoảng nghỉ cục bộ.')
                return
            if verb == 'limit':
                if group_name is None:
                    delay = current_delay(self.db, self.cfg)
                    source = ('Telegram /limit' if override_delay(self.db) is not None
                              else f'mặc định SEND_DELAY_SECONDS={self.cfg.send_delay:g}')
                    await self._manual_notice(
                        '⏱️ OUMIJIA — Giới hạn đăng\n'
                        f'Hiện tại: {delay:g} giây/EDZ (nguồn: {source}; 0 = không giới hạn)\n'
                        'Đặt: /limit 3 | /limit 0 | /limit reset')
                    return
                arg = group_name.strip().lower()
                if arg == 'reset':
                    reset_delay(self.db)
                    await self._manual_notice(
                        f'✅ Đã bỏ ghi đè; quay về SEND_DELAY_SECONDS={self.cfg.send_delay:g} giây/EDZ.')
                    return
                try:
                    value = set_delay(self.db, arg)
                except ValueError as ex:
                    await self._manual_notice('⚠️ ' + str(ex))
                    return
                await self._manual_notice(
                    f'✅ Khoảng nghỉ giữa 2 EDZ: {value:g} giây'
                    + (' (không giới hạn)' if value == 0 else '') + '\n'
                    'FloodWait của Telegram vẫn luôn được tôn trọng.')
                return
            if verb == 'errors':
                rows = list(self.db.execute("""SELECT file_name,error FROM artifacts
                    WHERE state='held' AND (error IS NULL OR
                    (lower(error) NOT LIKE '%floodwait%' AND lower(error) NOT LIKE '%wait of %'))
                    ORDER BY updated_at DESC LIMIT 5"""))
                from .auto_alerts import vi_reason
                body = '\n'.join('• '+r['file_name']+': '+vi_reason(r['error']) for r in rows)
                await self._manual_notice('⚠️ Lỗi cần kiểm tra gần đây:\n'+(body or 'Không có lỗi nặng mới.'))
                return
        name = media_filename(msg)
        if not name:
            return
        if getattr(msg, 'noforwards', False):
            await self._manual_notice('⚠️ Tệp bị Telegram bảo vệ nội dung, không thể tiếp nhận.')
            return
        size = media_size(msg)
        if size > self.cfg.max_group_bytes:
            await self._manual_notice('⚠️ File vượt giới hạn dữ liệu; cần chia thành gói nhỏ hơn.')
            return
        try:
            state, filename = stage_manual(self.db, int(msg.id), name, size)
        except ValueError as ex:
            await self._manual_notice('⚠️ Không ghi nhận file: ' + str(ex))
            return
        if state != 'exists':
            log.info('Manual inbox STAGED (NO download): #%s %s', msg.id, filename)
            # Trusted manual inbox repair: preserve the negative synthetic message ID.
            from .review_queue import register_repair
            register_repair(self.db, msg, -int(msg.id))

    async def backfill_manual(self):
        high = int(self.db.value('manual_inbox_highwater', '0'))
        async for msg in self.client.iter_messages(self.inbox, min_id=high, reverse=True):
            await self.handle_manual(msg)
            if msg.id > high:
                high = int(msg.id)
                self.db.set('manual_inbox_highwater', str(high))

    async def backfill(self):
        if not self.cfg.source_scan_enabled:
            log.info('Live source scan disabled (EDZ_SOURCE_SCAN_ENABLED=false); no history backfill')
            return
        if self.db.value('backfill_complete') == 'true':
            log.info('History already indexed; skipping backfill')
            return
        last = int(self.db.value('backfill_highwater', '0'))
        log.info('Backfill Telegram history from message %d', last)
        found = 0
        async for msg in self.client.iter_messages(self.source, min_id=last, reverse=True):
            self.enqueue(msg)
            if msg.id > last:
                last = msg.id
                self.db.set('backfill_highwater', str(last))
            found += 1
            if found % 100 == 0:
                log.info('Backfill scanned %d messages (latest id %d)', found, last)
        self.db.set('backfill_complete', 'true')
        log.info('Backfill index completed; scanned %d historical messages', found)

    async def download(self, row):
        msg_id = row['id']
        path = row['local_path']
        if path and Path(path).exists() and Path(path).stat().st_size == row['size_bytes']:
            self.db.change_message(msg_id, 'downloaded')
            return Path(path)
        origin_chat = self.inbox if msg_id < 0 else self.source
        if origin_chat is None:
            raise RuntimeError('Missing Telegram inbox chat for manual source')
        message = await self.client.get_messages(origin_chat, ids=-msg_id if msg_id < 0 else msg_id)
        if message is None or not getattr(message, 'document', None):
            raise RuntimeError('Source Telegram document no longer accessible')
        if getattr(message, 'noforwards', False):
            raise PermissionError('Source document protected against forwarding')
        name = safe_basename(row['file_name'])
        out = self.cfg.data / 'incoming' / str(msg_id)
        out.mkdir(parents=True, exist_ok=True)
        final = out / name
        free_space_for(out, row['size_bytes'], self.cfg.reserve_bytes)
        temp = out / (name + '.partial')
        self.db.change_message(msg_id, 'downloading')
        log.info('Downloading #%d %s (%d bytes)', msg_id, name, row['size_bytes'])
        downloaded = await self.client.download_media(message, file=str(temp))
        if not downloaded:
            raise RuntimeError('Telegram download returned no path')
        got = Path(downloaded)
        if got.stat().st_size != row['size_bytes']:
            raise ValueError('Downloaded size mismatch')
        got.replace(final)
        self.db.change_message(msg_id, 'downloaded', local_path=str(final))
        return final

    async def process_one(self, row):
        msg_id = row['id']
        try:
            # Decide from Telegram metadata BEFORE downloading any message.
            # The sum includes all indexed .7z.001/.002/... pieces in the group.
            members = (list(self.db.group_rows(row['group_name']))
                       if row['kind'] == 'volume' else [row])
            reason = capacity_reason(row, members, self.cfg.max_group_bytes)
            if reason:
                log.warning('Holding input #%d %s: %s', msg_id, row['file_name'], reason)
                for member in members:
                    if member['state'] in ('new', 'retry', 'downloaded'):
                        self.db.fail_message(member['id'], reason, retry=False)
                return
            local = await self.download(row)
            if row['kind'] == 'volume':
                return  # all parts will be grouped and processed after the quiet interval
            self.db.change_message(msg_id, 'processing')
            # Open a separate SQLite connection in worker thread; Telegram stays responsive.
            def _do():
                db = DB(self.cfg.data / 'state.sqlite3')
                try:
                    return Pipeline(self.cfg, db).process_input(local, msg_id, row['kind'], row['file_name'])
                finally:
                    db.close()
            count = await asyncio.to_thread(_do)
            self.db.change_message(msg_id, 'complete', error=None)
            log.info('Processed source #%d -> %d new EDZ artifacts', msg_id, count)
            if msg_id < 0:
                await self._manual_notice(
                    f'✅ OUMIJIA — Đã kiểm tra và xử lý {row["file_name"]}. '
                    f'Có {count} EDZ mới được đưa vào danh mục chờ đăng. '
                    'File trùng SHA-256 được bỏ qua.')
        except FloodWaitError as ex:
            # Rate-limit download too, but don't sleep inside the worker loop:
            # housekeeping and safe local jobs should remain schedulable.
            self.db.fail_message(msg_id, f'FloodWait {ex.seconds}s')
            self.db.execute("UPDATE messages SET retry_after=? WHERE id=? AND state='retry'",
                            (time.time() + ex.seconds + 60, msg_id))
            set_cooldown(self.db, ex.seconds)
            log.warning('Tạm hoãn tải nguồn #%d trong %ds theo yêu cầu Telegram',
                        msg_id, ex.seconds)
        except PermissionError as ex:
            self.db.fail_message(msg_id, str(ex), retry=False)
        except ValueError as ex:
            log.exception('Source #%d rejected by validation', msg_id)
            self.db.fail_message(msg_id, str(ex), retry=not self.cfg.auto_mode)
        except Exception as ex:
            log.exception('Input #%d failed', msg_id)
            self.db.fail_message(msg_id, str(ex))

    async def process_volume_groups(self):
        for group in self.db.groups():
            rows = list(self.db.group_rows(group['group_name']))
            # Never complete a set when parts are still queued/downloads in progress.
            if any(r['state'] not in ('downloaded','complete') for r in rows):
                continue
            active = [r for r in rows if r['state'] == 'downloaded']
            if not active:
                continue
            indexes = [r['volume_index'] for r in rows]
            if 1 not in indexes:
                continue
            # Same group name + duplicated part numbers could mix unrelated archives.
            if len(indexes) != len(set(indexes)):
                for r in active:
                    self.db.fail_message(r['id'], 'Volume name/index collision: manual review needed', retry=False)
                continue
            if sorted(indexes) != list(range(1, max(indexes)+1)):
                continue
            if (not self.cfg.auto_mode and
                    not str(group['group_name']).startswith('manual:') and
                    time.time() - max(r['changed_at'] for r in active) < self.cfg.volume_quiet):
                continue
            try:
                await self._process_group(group['group_name'], rows)
            except Exception:
                log.exception('Volume group %s failed; holding for inspection', group['group_name'])
                for r in active:
                    self.db.fail_message(r['id'], 'Volume extraction/processing failed', retry=False)

    async def _process_group(self, group: str, rows):
        safe_dir = hashlib.sha256(group.encode()).hexdigest()[:20]
        assembly = self.cfg.data / 'volumes' / safe_dir
        assembly.mkdir(parents=True, exist_ok=True)
        for r in rows:
            source = Path(r['local_path'])
            dest = assembly / safe_basename(r['file_name'])
            if not dest.exists():
                try:
                    os.link(source, dest)  # same filesystem: no 36GB extra copy
                except OSError:
                    shutil.copy2(source, dest)
        first = assembly / next(safe_basename(r['file_name']) for r in rows if r['volume_index'] == 1)
        if not await asyncio.to_thread(test_archive, first, self.cfg.sevenzip):
            if self.cfg.auto_mode:
                for member in rows:
                    self.db.fail_message(member['id'],
                        'Automatic multipart 7z integrity test failed: missing final part or corrupt archive',
                        retry=False)
                log.error('Auto multipart integrity failed: %s', group)
            else:
                log.info('Volume %s incomplete (7z t failed); awaiting missing segments', group)
            return
        first_id = next(r['id'] for r in rows if r['volume_index'] == 1)
        def _do():
            db = DB(self.cfg.data / 'state.sqlite3')
            try:
                return Pipeline(self.cfg, db).process_input(first, first_id, 'volume', first.name)
            finally:
                db.close()
        count = await asyncio.to_thread(_do)
        for r in rows:
            self.db.change_message(r['id'], 'complete', error=None)
        log.info('Completed multi-volume %s -> %d new EDZ artifacts', group, count)
        if str(group).startswith('manual:'):
            await self._manual_notice(
                f'✅ OUMIJIA — Đã ghép và kiểm tra xong {group[7:]}; '
                f'phát hiện {count} EDZ mới đã sẵn sàng để đăng. '
                'Các file trùng được bỏ qua.')

    async def publish_archive_v2(self, archive: Path) -> dict:
        """Real-pipeline integration: stream one 7z straight into the V2 queue.

        No whole-archive extraction; EDZs are enqueued and posted by the SAME
        single publisher. Restricted to archives already under DATA_DIR.
        """
        if self.upload_engine is None:
            raise RuntimeError('Upload Engine V2 is disabled')
        from .upload_engine import publish_archive
        archive = Path(archive).resolve()
        root = self.cfg.data.resolve()
        if root not in archive.parents:
            raise ValueError('Archive must already live under DATA_DIR')
        temp = self.cfg.data / 'work' / 'v2stream'
        return await publish_archive(self.upload_engine, self.cfg.sevenzip, archive,
                                     temp_dir=temp, max_files=self.cfg.max_archive_files,
                                     max_bytes=self.cfg.max_unpack_bytes)

    def caption(self, a):
        brand, part = a['brand'], a['part_number'] or 'Unknown'
        identifier = a['sha256'][:16]
        text = (f'EPLAN EDZ | {brand}\n'
                f'Part: {part}\n'
                f'File: {a["file_name"]}\n'
                f'SHA-256: {a["sha256"]}\n'
                f'#{tag(brand)} #EPLAN #EDZ #EDZID_{identifier}')
        return text[:1024]

    async def publish_one(self, a):
        if self.cfg.dry_run:
            return
        # Compatibility guard (spec): never publish a normalized EDZ name that
        # is already accepted/posted, regardless of SHA-256. Different names
        # sharing a SHA remain independent artifacts.
        from .name_guard import name_already_published
        if name_already_published(self.db, a['file_name']):
            self.db.update_artifact(a['sha256'], 'held',
                error='DUPLICATE_NAME: normalized EDZ name already accepted/posted')
            log.info('Duplicate normalized name skipped (held): %s', a['file_name'])
            return
        if a['state'] == 'review' and not self.cfg.allow_unverified:
            return
        sha = a['sha256']
        file = Path(a['path'])
        if not file.is_file() or file.stat().st_size != a['size_bytes']:
            self.db.update_artifact(sha, 'held', error='Output EDZ missing/size mismatch')
            return
        # SHA256 is verified again before publishing to protect against bad copies.
        from .archive import sha256_file
        digest = await asyncio.to_thread(sha256_file, file)
        if digest != sha:
            self.db.update_artifact(sha, 'held', error='Output EDZ SHA256 changed')
            return
        self.db.update_artifact(sha, 'posting')  # crash = ambiguous; never auto-repeat
        try:
            media = file
            if a['origin'] == 'direct' and a['source_message'] > 0:
                original = await self.client.get_messages(self.source, ids=a['source_message'])
                if original and getattr(original, 'media', None) and not getattr(original, 'noforwards', False):
                    media = original.media  # avoids re-upload of multi-GB original EDZ
            if isinstance(media, Path) and a['size_bytes'] > self.cfg.max_upload_bytes:
                raise ValueError('EDZ exceeds configured Telegram upload size limit; held')
            kwargs = {'caption': self.caption(a), 'parse_mode': None, 'force_document': True}
            if isinstance(media, Path):
                kwargs['attributes'] = [DocumentAttributeFilename(a['file_name'])]
            sent = await self.client.send_file(self.target, media, **kwargs)
            self.db.update_artifact(sha, 'posted', target_message=sent.id, error=None)
            if self.cfg.auto_mode:
                # Local inter-post pacing: Telegram override (/limit) wins over env.
                # schedule_next_send(0) clears the local gate; FloodWait cooldowns
                # in next_send_remaining() still apply.
                schedule_next_send(self.db, current_delay(self.db, self.cfg))
            else:
                await asyncio.sleep(current_delay(self.db, self.cfg))
            log.info('Posted EDZ %s as message #%d', a['file_name'], sent.id)
        except FloodWaitError as ex:
            # Telegram rejected SendMediaRequest. Do not try the next 1,500 files.
            # This artifact remains held until full destination reconciliation.
            record_flood(self.db, sha)
            until = set_cooldown(self.db, ex.seconds)
            self.db.update_artifact(sha, 'held', error=f'FloodWait send: {ex.seconds} seconds; destination reconciliation required')
            log.warning('Telegram FloodWait: publisher paused until %s; EDZ intake stays active',
                        time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(until)))
        except Exception as ex:
            # Some Telethon versions wrap FloodWait into a generic RPC exception.
            # Treat the whole publisher as rate-limited, but keep this outcome
            # unconfirmed until destination reconciliation.
            seconds = flood_seconds_from_message(str(ex))
            if seconds is not None:
                record_flood(self.db, sha)
                until = set_cooldown(self.db, seconds)
                self.db.update_artifact(sha, 'held',
                    error=f'FloodWait send: {seconds} seconds; destination reconciliation required')
                log.warning('Telegram giới hạn gửi %ds; tạm hoãn Publisher đến %s',
                            seconds, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(until)))
                return
            # API timeouts can happen AFTER successful Telegram posting. Do not retry blindly.
            self.db.update_artifact(sha, 'held', error='Ambiguous posting outcome: '+str(ex)[:1200])
            log.exception('Posting uncertain; verify destination before manual retry: %s', sha)

    async def send_control_daily_report(self):
        """At most one daily group report, persistent across restarts."""
        if self.cfg.inbox_chat is None:
            return
        from datetime import datetime
        from zoneinfo import ZoneInfo
        from .daily_scan import is_daily
        local = datetime.now(ZoneInfo('Asia/Ho_Chi_Minh'))
        if (local.hour, local.minute) < (2, 5):
            return
        today = local.strftime('%Y-%m-%d')
        if self.db.value('control_daily_report_date_v16') == today:
            return
        status = {r['state']: r['count'] for r in self.db.stats()['artifacts']}
        docs, volumes, _ = staged_summary(self.db)
        try:
            free_gib = shutil.disk_usage(self.cfg.data).free / 1024**3
            body = ('📅 OUMIJIA — Báo cáo EDZ ngày '+today+'\n'
                    f'Đã đăng: {status.get("posted",0)}\n'
                    f'Đang chờ đăng: {status.get("ready",0)}\n'
                    f'Lỗi giữ lại: {status.get("held",0)}\n'
                    f'Tệp điều khiển chờ /process: {docs} tệp + {volumes} phần 7z\n'
                    f'Đĩa còn trống: {free_gib:.1f} GiB\n'
                    'Quét nguồn: '+('02:00 hằng ngày' if is_daily(self.db) else 'Dữ liệu lịch sử'))
            delivered = await self._manual_notice(body)
            if delivered:
                self.db.set('control_daily_report_date_v16', today)
        except Exception:
            log.exception('Không gửi được báo cáo EDZ hằng ngày')

    async def auto_cleanup_once(self):
        """Run safe library-only cleanup off the event loop (service owns lock)."""
        if not self.cfg.auto_cleanup or self.cfg.dry_run:
            return (0, 0)
        from .cleanup import apply_auto_cleanup
        def _do():
            db = DB(self.cfg.data / 'state.sqlite3')
            try:
                return apply_auto_cleanup(db, self.cfg.data,
                    enabled=self.cfg.auto_cleanup, dry_run=self.cfg.dry_run)
            finally:
                db.close()
        count, freed = await asyncio.to_thread(_do)
        if count:
            log.info('Auto-cleanup removed %d posted EDZ copies (%d bytes)', count, freed)
        return count, freed

    async def auto_work_cleanup_once(self):
        """Remove only fully verified completed work caches; never sources or library artifacts.

        Uses its own SQLite connection in a worker thread. The systemd worker
        already holds the exclusive edzbot.lock, so no second producer can
        operate on the same work directory concurrently.
        """
        if not self.cfg.auto_work_cleanup:
            return (0, 0)
        from .work_cleanup import apply_work_cleanup
        def _do():
            db = DB(self.cfg.data / 'state.sqlite3')
            try:
                return apply_work_cleanup(db, self.cfg.data)
            finally:
                db.close()
        folders, freed = await asyncio.to_thread(_do)
        if folders:
            log.info('Auto work-cleanup removed %d completed cache folders (%d bytes)', folders, freed)
        return folders, freed

    async def send_auto_alerts(self):
        """Deliver errors to Telegram Saved Messages (or explicitly configured ERROR_CHAT)."""
        from .auto_alerts import next_alert, mark_alert_sending, mark_alert_sent, mark_alert_failed
        event = next_alert(self.db)
        if not event:
            return
        key, body = event
        # Reserve BEFORE calling Telegram, so a lost response cannot duplicate alerts.
        if not mark_alert_sending(self.db, key, body):
            return
        try:
            if self.cfg.inbox_chat is not None:
                msg_id = await bot_send(self.cfg.notify_bot_token, str(self.cfg.inbox_chat), body + '\nMã cảnh báo: ' + key[:16])
                sent = type('Sent', (), {'id': msg_id})()
            else:
                sent = await self.client.send_message(self.cfg.alert_chat, body + '\nMã cảnh báo — AlertID: ' + key[:16])
        except FloodWaitError as ex:
            set_cooldown(self.db, ex.seconds)
            log.warning('Error notification FloodWait %ds; postponing alerts and uploads', ex.seconds)
            mark_alert_failed(self.db, key)
            return
        except Exception as ex:
            seconds = flood_seconds_from_message(str(ex))
            if seconds is not None:
                set_cooldown(self.db, seconds)
            log.exception('Không gửi được cảnh báo Telegram: %s', key)
            mark_alert_failed(self.db, key)
            return
        mark_alert_sent(self.db, key, int(sent.id))
        log.info('Telegram error notification delivered as message #%d', sent.id)

    async def auto_housekeeping_once(self):
        """Cleanup in dependency order: verified cache, posted library, then sources."""
        from .work_cleanup import apply_work_cleanup
        from .cleanup import apply_cleanup
        from .autonomous import clean_safe_auto_sources
        def _run():
            db = DB(self.cfg.data / 'state.sqlite3')
            try:
                count_work, bytes_work = apply_work_cleanup(db, self.cfg.data)
                count_lib, bytes_lib = apply_cleanup(
                    db, self.cfg.data, allowed_kinds=frozenset({'posted_library'}))
                count_source, bytes_source = clean_safe_auto_sources(db, self.cfg.data)
                return (count_work, bytes_work, count_lib, bytes_lib,
                        count_source, bytes_source)
            finally:
                db.close()
        result = await asyncio.to_thread(_run)
        if result[0] or result[2] or result[4]:
            log.info('Auto housekeeping: cache=%d library=%d source=%d; freed %d bytes',
                     result[0], result[2], result[4], result[1] + result[3] + result[5])
        return result

    async def housekeeping_worker_v1632(self):
        """Run expensive verified cleanup outside the source admission loop."""
        log.info('PIPELINE v16.3.2 Housekeeping worker started')
        while True:
            started = time.monotonic()
            try:
                await self.auto_housekeeping_once()
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                log.exception('Automatic housekeeping failed')
                from .auto_alerts import record_issue
                record_issue(self.db, 'housekeeping', str(ex))
            finally:
                log.info('PIPELINE housekeeping pass elapsed %.1fs', time.monotonic() - started)
            await asyncio.sleep(300)

    async def reconciliation_worker_v1632(self):
        """Telegram destination reconciliation must not block new intake."""
        log.info('PIPELINE v16.3.2 Reconciliation worker started')
        while True:
            if self.cfg.auto_mode and cooldown_remaining(self.db) <= 0 and held_flood_rows(self.db):
                try:
                    recovered = await reconcile_flood_held(self.db, self.client, self.target)
                    log.warning('FloodWait destination reconciliation: %s', recovered)
                except asyncio.CancelledError:
                    raise
                except FloodWaitError as ex:
                    set_cooldown(self.db, ex.seconds)
                    log.warning('Destination reconciliation FloodWait %ds; intake continues', ex.seconds)
                except Exception:
                    log.exception('Destination reconciliation failed; artifacts remain held')
            await asyncio.sleep(1800)

    async def worker(self):
        last_cleanup = 0.0
        last_work_cleanup = 0.0
        last_plan_queue = 0.0
        last_autonomous = 0.0
        last_alerts = 0.0
        last_housekeeping = 0.0
        last_flood_reconcile = 0.0
        last_daily_scan_check = 0.0
        last_control_daily_report = 0.0
        while True:
            if self.cfg.inbox_chat is not None and time.monotonic() - last_control_daily_report >= 300:
                last_control_daily_report = time.monotonic()
                await self.send_control_daily_report()
            if self.cfg.auto_mode and time.monotonic() - last_autonomous >= 20:
                last_autonomous = time.monotonic()
                try:
                    from .autonomous import enqueue_next_auto
                    chosen, reason = enqueue_next_auto(self.db, self.cfg)
                    if chosen:
                        log.info('AUTO QUEUED source %s: %s', chosen.group_key,
                                 ','.join(str(x.message_id) for x in chosen.members))
                    elif reason.startswith('Insufficient disk headroom'):
                        from .auto_alerts import record_issue
                        record_issue(self.db, 'disk_capacity', reason)
                    elif 'waiting' not in reason and 'unpublished' not in reason:
                        log.debug('Auto intake idle: %s', reason)
                    from .daily_scan import enter_daily_if_drained
                    if enter_daily_if_drained(self.db, self.cfg, reason):
                        log.info('OUMIJIA: Đã xử lý xong nguồn đủ điều kiện; chuyển sang quét mỗi ngày lúc 02:00 VN')
                except Exception as ex:
                    log.exception('Automatic intake error')
                    from .auto_alerts import record_issue
                    record_issue(self.db, 'intake', str(ex))
            if self.cfg.auto_mode and time.monotonic() - last_daily_scan_check >= 60:
                last_daily_scan_check = time.monotonic()
                from .daily_scan import due, scan_once, retry_later
                if due(self.db) and self.cfg.source_scan_enabled:
                    try:
                        result = await scan_once(self.db, self.client, self.source, self.enqueue)
                        log.info('OUMIJIA: Quét nguồn hằng ngày xong: %s', result)
                    except FloodWaitError as ex:
                        set_cooldown(self.db, ex.seconds)
                        retry_later(self.db, delay=ex.seconds + 120)
                        log.warning('Quét nguồn bị Telegram giới hạn, sẽ thử lại sau %ds', ex.seconds + 120)
                    except Exception:
                        retry_later(self.db)
                        log.exception('Lỗi quét nguồn định kỳ; sẽ thử lại sau 1 giờ')
            if self.cfg.auto_mode and cooldown_remaining(self.db) <= 0 and time.monotonic() - last_alerts >= 300:
                last_alerts = time.monotonic()
                try:
                    await self.send_auto_alerts()
                except Exception:
                    log.exception('Could not send Telegram error alert; will not blindly repeat')
            if self.cfg.auto_mode and not self.cfg.continuous_pipeline and time.monotonic() - last_housekeeping >= 300:
                last_housekeeping = time.monotonic()
                try:
                    await self.auto_housekeeping_once()
                except Exception as ex:
                    log.exception('Automatic housekeeping failed')
                    from .auto_alerts import record_issue
                    record_issue(self.db, 'housekeeping', str(ex))
            # Persist the legacy rate-cycle anchor (kept for compatibility and
            # reporting; effective pacing comes from /limit or SEND_DELAY_SECONDS).
            if self.cfg.auto_mode:
                ensure_cycle_anchor(self.db)
            # Reconcile old/new FloodWait holds with destination BEFORE any retry.
            # Unlike Protected Chat, splitter warnings and interrupted sends,
            # successful reconciliation can return FloodWait artifacts to 'ready'.
            if (self.cfg.auto_mode and not self.cfg.continuous_pipeline
                    and cooldown_remaining(self.db) <= 0
                    and time.monotonic() - last_flood_reconcile >= 1800):
                last_flood_reconcile = time.monotonic()
                if held_flood_rows(self.db):
                    try:
                        recovered = await reconcile_flood_held(self.db, self.client, self.target)
                        log.warning('FloodWait destination reconciliation: %s', recovered)
                    except FloodWaitError as ex:
                        set_cooldown(self.db, ex.seconds)
                        log.warning('Destination reconciliation FloodWait %ds; intake continues', ex.seconds)
                    except Exception:
                        log.exception('Destination reconciliation failed; artifacts remain held')
            # Cleanup is opt-in; errors must not stop the Telegram worker.
            if (not self.cfg.auto_mode and self.cfg.auto_cleanup and not self.cfg.dry_run
                    and time.monotonic() - last_cleanup >= 300):
                last_cleanup = time.monotonic()
                try:
                    await self.auto_cleanup_once()
                except Exception:
                    log.exception('Auto-cleanup failed; worker continues')
            # Work cache cleanup is separately opt-in and safe in DRY_RUN:
            # its gate checks DB state, source SHA, output SHA and library copies.
            if (not self.cfg.auto_mode and self.cfg.auto_work_cleanup
                    and time.monotonic() - last_work_cleanup >= 300):
                last_work_cleanup = time.monotonic()
                try:
                    await self.auto_work_cleanup_once()
                except Exception:
                    log.exception('Auto work-cleanup failed; worker continues')
            if (self.cfg.auto_plan_queue and time.monotonic() - last_plan_queue >= 300):
                last_plan_queue = time.monotonic()
                try:
                    from .plan_queue import enqueue_next
                    chosen, reason = enqueue_next(self.db, self.cfg)
                    if chosen is not None:
                        log.info('Plan queued ONE EDZ #%d %s (%d bytes)',
                                 chosen.message_id, chosen.file_name, chosen.size_bytes)
                    else:
                        log.info('Plan queue idle: %s', reason)
                except Exception:
                    log.exception('Plan queue failed; worker continues')
            if self.cfg.continuous_pipeline:
                # In continuous mode these tasks have separate owners below.
                # Keeping maintenance here preserves alerts, incremental scans,
                # reconciliation, housekeeping, and cycle-state persistence.
                await asyncio.sleep(min(3, max(1, self.cfg.poll_seconds)))
                continue
            row = self.db.next_message()
            if row:
                await self.process_one(row)
            # Legacy sequential processing remains available as a rollback mode.
            await self.process_volume_groups()
            a = self.db.next_artifact(allow_unverified=self.cfg.allow_unverified,
                                      require_complete_source=self.cfg.auto_mode)
            if (a and not self.cfg.dry_run and self.publisher_mode == 'normal'
                    and not self.cfg.upload_engine_v2
                    and next_send_remaining(self.db) <= 0):
                await self.publish_one(a)
                continue
            await asyncio.sleep(self.cfg.poll_seconds)

    async def _download_one_v163(self, row):
        """Downloader owns ONLY Telegram intake and finalizes local source files.

        It never creates EDZ artifacts or posts to the destination. Existing
        fail-closed source validation/Telegram FloodWait rules are preserved.
        """
        msg_id = row['id']
        try:
            members = (list(self.db.group_rows(row['group_name']))
                       if row['kind'] == 'volume' else [row])
            reason = capacity_reason(row, members, self.cfg.max_group_bytes)
            if reason:
                for member in members:
                    if member['state'] in ('new', 'retry', 'downloaded'):
                        self.db.fail_message(member['id'], reason, retry=False)
                log.error('Pipeline intake blocked #%s: %s', msg_id, reason)
                return
            await self.download(row)
        except FloodWaitError as ex:
            self.db.fail_message(msg_id, f'FloodWait {ex.seconds}s')
            self.db.execute("UPDATE messages SET retry_after=? WHERE id=? AND state='retry'",
                            (time.time() + ex.seconds + 60, msg_id))
            # GetFile/GetMessages throttling is distinct from SendMediaRequest.
            # Keep the downloader asleep for its own Telegram wait without
            # unnecessarily pausing a healthy single publisher.
            self.db.set('pipeline_download_cooldown_until_v163',
                        str(time.time() + ex.seconds + 60))
            log.warning('Pipeline downloader FloodWait #%s: %ss', msg_id, ex.seconds)
        except PermissionError as ex:
            self.db.fail_message(msg_id, str(ex), retry=False)
        except ValueError as ex:
            self.db.fail_message(msg_id, str(ex), retry=not self.cfg.auto_mode)
            log.exception('Pipeline downloader invalid source #%s', msg_id)
        except Exception as ex:
            self.db.fail_message(msg_id, str(ex))
            log.exception('Pipeline downloader failed source #%s', msg_id)

    async def download_worker_v163(self):
        """One download at a time; asynchronous with processing and publishing."""
        log.info('PIPELINE v16.3 Downloader started')
        from .pipeline_parallel import download_cooldown_remaining
        while True:
            wait = download_cooldown_remaining(self.db)
            if wait > 0:
                await asyncio.sleep(min(5, max(0.5, wait)))
                continue
            row = self.db.next_message()
            if row:
                await self._download_one_v163(row)
            else:
                await asyncio.sleep(1)

    async def process_worker_v163(self):
        """One splitter/extractor at a time; complete source gates publishing."""
        log.info('PIPELINE v16.3 Processor started')
        while True:
            row = self.db.execute("""SELECT * FROM messages
                WHERE state='downloaded' AND kind!='volume'
                ORDER BY changed_at,id LIMIT 1""").fetchone()
            if row:
                await self.process_one(row)
            try:
                await self.process_volume_groups()
            except Exception:
                log.exception('Pipeline volume processor recovered from error')
            if row is None:
                await asyncio.sleep(1)

    async def publish_worker_v163(self):
        """SINGLE publisher; preserve SHA, FloodWait, and ambiguous-send guards."""
        log.info('PIPELINE v16.3 Publisher started (mode=%s)', self.publisher_mode)
        if self.publisher_mode == 'paused':
            log.warning('Publisher GATE=paused: no V1 or V2 post will be sent on restart')
            return
        if self.upload_engine is not None:
            log.info('Upload Engine V2 active (concurrency=%d/%d, canary=%s)',
                     self.cfg.upload_concurrency, self.cfg.upload_max_concurrency,
                     self.cfg.upload_canary_name or 'OFF')
            while True:
                try:
                    await self.upload_engine.publish_batch()
                except sqlite3.Error as ex:
                    self.upload_engine.mark_fault(ex)
                    log.error('SQLite fault: publisher locked until operator clears it')
                    await asyncio.sleep(10)
                except Exception:
                    log.exception('Upload Engine V2 batch failed; state preserved for recovery')
                    await asyncio.sleep(10)
                await asyncio.sleep(min(1, max(0.5, self.cfg.poll_seconds)))
            return
        while True:
            remaining = next_send_remaining(self.db)
            if remaining > 0:
                await asyncio.sleep(min(5, max(0.5, remaining)))
                continue
            a = self.db.next_artifact(allow_unverified=self.cfg.allow_unverified,
                                      require_complete_source=self.cfg.auto_mode)
            if a and not self.cfg.dry_run:
                try:
                    await self.publish_one(a)
                except Exception:
                    # May have entered 'posting'. Do not set to ready or retry.
                    log.exception('Unexpected publisher failure; inspect posting before restart')
                    await asyncio.sleep(10)
            else:
                await asyncio.sleep(1)

    async def review_worker_v023(self):
        # Files remain in their canonical library/incoming paths until safely resolved.
        from .review_queue import run_once
        log.info('PIPELINE v023 Auto Review worker started: export every 24h; repair check every 5m')
        while True:
            try:
                await asyncio.to_thread(run_once, self.cfg.data,
                    max_ready=self.cfg.pipeline_max_ready_artifacts,
                    reserve_bytes=self.cfg.reserve_bytes,
                    export_interval_seconds=86400)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception('EDZ Auto Review failed; other workers continue')
            await asyncio.sleep(300)

    async def run(self, *, force_history: bool = False):
        await self.client.connect()
        if not await self.client.is_user_authorized():
            raise RuntimeError('Telegram session not authorized. Run: python -m edzbot login')
        try:
            # A get_dialogs call populates entity cache for private channel integer IDs.
            await self.client.get_dialogs(limit=250)
            self.source = await self.client.get_entity(self.cfg.source)
            self.target = await self.client.get_entity(self.cfg.target)
            if self.upload_engine is not None:
                self.upload_engine.target = self.target
                self.upload_engine.source = self.source
            if self.publisher_mode == 'canary':
                # Fail-closed startup guard: exactly ONE ready artifact may exist
                # for the configured canary name, or the service must not run.
                from .name_guard import normalize_edz_name
                want = normalize_edz_name(self.cfg.upload_canary_name)
                matches = [a for a in self.db.execute(
                    "SELECT file_name FROM artifacts WHERE state='ready'")
                    if normalize_edz_name(a['file_name']) == want]
                if len(matches) != 1:
                    raise RuntimeError(
                        f'CANARY GUARD: expected exactly 1 ready artifact for '
                        f'{self.cfg.upload_canary_name}, found {len(matches)}')
                log.info('CANARY GUARD PASS: only %s may be published', matches[0]['file_name'])
            if self.cfg.inbox_chat is not None:
                self.inbox = await self.client.get_entity(self.cfg.inbox_chat)
                if self.inbox.id in (self.source.id, self.target.id):
                    raise ValueError('EDZ inbox must be separate from source and destination')
                async def manual_handler(event):
                    await self.handle_manual(event.message)
                self.client.add_event_handler(manual_handler, events.NewMessage(chats=self.inbox))
            if self.source.id == self.target.id:
                raise ValueError('Source and destination resolve to same Telegram channel')
            async def handle(event):
                # During daily maintenance we intentionally do not consume live
                # source events; the incremental 02:00 scan owns new intake.
                from .daily_scan import is_daily
                from .review_queue import repair_tag
                if not (self.cfg.auto_mode and is_daily(self.db)) or repair_tag(event.message):
                    self.enqueue(event.message)
            self.client.add_event_handler(handle, events.NewMessage(chats=self.source))
            log.info('Authenticated; SOURCE=%s DEST=%s DRY_RUN=%s ALLOW_UNVERIFIED=%s',
                     self.source.id, self.target.id, self.cfg.dry_run, self.cfg.allow_unverified)
            if self.cfg.auto_mode:
                # One startup confirmation proves the alert destination is reachable.
                # Reserve before sending; a crashed response must not spam duplicates.
                if not self.db.value('auto_mode_v12_started', ''):
                    self.db.set('auto_mode_v12_started', 'sending')
                    try:
                        notice = (f'✅ OUMIJIA EDZ AUTO MODE v12 enabled\n'
                                  f'Source: {self.source.id}\n'
                                  f'Destination: {self.target.id}\n'
                                  f'Failure alerts: this chat\n'
                                  f'Group cap: {self.cfg.max_group_bytes // 1024**3} GiB; '
                                  f'reserve: {self.cfg.reserve_bytes // 1024**3} GiB\n'
                                  'Unverified/corrupt EDZ files will NOT be published.')
                        await self.client.send_message(self.cfg.alert_chat, notice)
                        self.db.set('auto_mode_v12_started', 'sent')
                    except Exception:
                        log.exception('AUTO_MODE startup Telegram Saved Messages notice failed')
            if force_history:
                self.db.set('backfill_highwater', '0')
                self.db.set('backfill_complete', 'false')
            self.db.prepare_recovery()
            if self.inbox is not None:
                await self.backfill_manual()
                if self.db.value('control_v16_intro_room') != str(self.cfg.inbox_chat):
                    delivered = await self._manual_notice(
                        '✅ OUMIJIA v16 đã kết nối nhóm điều khiển. '
                        'Gửi .edz/.7z/.7z.001... sẽ CHỈ được ghi nhận. '
                        'Gõ /help để xem lệnh; chỉ /process mới cho phép tải và xử lý.')
                    if delivered:
                        self.db.set('control_v16_intro_room', str(self.cfg.inbox_chat))
            if self.cfg.backfill or force_history:
                await self.backfill()
            # new-message handler was attached BEFORE the history scan, so no gap.
            if self.cfg.continuous_pipeline:
                log.info('OUMIJIA v16.3 Continuous Pipeline enabled: download + process + publish')
                await asyncio.gather(self.worker(), self.download_worker_v163(),
                                     self.process_worker_v163(), self.publish_worker_v163(),
                                     self.housekeeping_worker_v1632(),
                                     self.reconciliation_worker_v1632(),
                                     self.review_worker_v023())
            else:
                await self.worker()
        finally:
            self.db.close()
            await self.client.disconnect()
