"""Configuration. A local .env is intentionally ignored by version control."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import re
from dotenv import load_dotenv
from .manual_inbox import valid_ids


def _boolean(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    if value.lower() in {"true", "yes", "1", "on"}:
        return True
    if value.lower() in {"false", "no", "0", "off"}:
        return False
    raise ValueError(f"{name} must be true/false")


def _channel(value: str) -> str | int:
    value = value.strip()
    return int(value) if value.lstrip('-').isdigit() else value


@dataclass(frozen=True)
class Config:
    api_id: int
    api_hash: str
    source: str | int
    target: str | int
    phone: str
    data: Path
    splitter: Path
    sevenzip: str
    strict_part: bool
    fix_refs: str
    dry_run: bool
    allow_unverified: bool
    auto_cleanup: bool
    auto_work_cleanup: bool
    auto_plan_queue: bool
    auto_mode: bool
    continuous_pipeline: bool
    pipeline_max_active_sources: int
    pipeline_max_ready_artifacts: int
    pipeline_max_ready_gib: float
    alert_chat: str | int
    inbox_chat: str | int | None
    inbox_user_ids: frozenset[int]
    notify_bot_token: str
    notify_chat_id: str
    plan_max_pending_artifacts: int
    plan_max_single_bytes: int
    plan_batch_id: str
    plan_batch_max_sources: int
    backfill: bool
    reserve_bytes: int
    max_group_bytes: int
    max_archive_files: int
    max_unpack_bytes: int
    max_upload_bytes: int
    volume_quiet: int
    poll_seconds: int
    send_delay: float
    source_scan_enabled: bool
    log_level: str
    # Upload Engine V2 (opt-in; default off keeps the legacy publisher intact).
    upload_engine_v2: bool
    upload_concurrency: int
    upload_max_concurrency: int
    upload_max_retries: int
    upload_temp_dir: str
    upload_canary_name: str
    publisher_mode: str
    upload_archive_stream_enabled: bool

    @classmethod
    def load(cls, *, require_telegram: bool = True) -> 'Config':
        load_dotenv(override=False)
        root = Path(__file__).resolve().parents[1]
        data_setting = Path(os.getenv('DATA_DIR', './data')).expanduser()
        data = (data_setting if data_setting.is_absolute() else root / data_setting).resolve()
        splitter_setting = Path(os.getenv('EDZSPLIT_BIN', './vendor/edzsplit/edzsplit')).expanduser()
        splitter = (splitter_setting if splitter_setting.is_absolute() else root / splitter_setting).resolve()
        # edzsplit-rs v0.2.3 supports --strict-part (exactly one part package per
        # output, which is what the artifact registry requires). The legacy C#
        # edzsplit does not; set EDZSPLIT_STRICT_PART=false for that binary.
        fix_refs = os.getenv('EDZSPLIT_FIX_REFS', 'never').strip().lower()
        if fix_refs not in ('auto', 'never'):
            raise ValueError('EDZSPLIT_FIX_REFS must be either "auto" or "never"')
        strict_part = _boolean('EDZSPLIT_STRICT_PART', True)
        api_id_raw = os.getenv('API_ID', '')
        hash_ = os.getenv('API_HASH', '').strip()
        source = os.getenv('SOURCE_CHAT', '').strip()
        target = os.getenv('DEST_CHAT', '').strip()
        if require_telegram and (not api_id_raw or not hash_ or not source or not target):
            raise ValueError('Fill API_ID, API_HASH, SOURCE_CHAT and DEST_CHAT in .env')
        try:
            api_id = int(api_id_raw) if api_id_raw else 0
        except ValueError as ex:
            raise ValueError('API_ID must be an integer') from ex
        if source and target and source == target:
            raise ValueError('SOURCE_CHAT and DEST_CHAT must differ')
        cfg = cls(
            api_id=api_id, api_hash=hash_, source=_channel(source) if source else '',
            target=_channel(target) if target else '', phone=os.getenv('TELEGRAM_PHONE', '').strip(),
            data=data, splitter=splitter, sevenzip=os.getenv('SEVEN_ZIP', '7z').strip(),
            strict_part=strict_part, fix_refs=fix_refs,
            dry_run=_boolean('DRY_RUN', True), allow_unverified=_boolean('ALLOW_UNVERIFIED_SPLITS', False),
            auto_cleanup=_boolean('AUTO_CLEANUP', False),
            auto_work_cleanup=_boolean('AUTO_WORK_CLEANUP', False),
            auto_plan_queue=_boolean('AUTO_PLAN_QUEUE', False),
            auto_mode=_boolean('AUTO_MODE', False),
            continuous_pipeline=_boolean('EDZ_CONTINUOUS_PIPELINE', False),
            pipeline_max_active_sources=int(os.getenv('EDZ_PIPELINE_MAX_ACTIVE_SOURCES', '2')),
            pipeline_max_ready_artifacts=int(os.getenv('EDZ_PIPELINE_MAX_READY_ARTIFACTS', '4000')),
            pipeline_max_ready_gib=float(os.getenv('EDZ_PIPELINE_MAX_READY_GIB', '16')),
            alert_chat=_channel(os.getenv('ERROR_CHAT', 'me')) if os.getenv('ERROR_CHAT', 'me').strip() else 'me',
            inbox_chat=_channel(os.getenv('EDZ_INBOX_CHAT', '')) if os.getenv('EDZ_INBOX_CHAT', '').strip() else None,
            inbox_user_ids=valid_ids(os.getenv('EDZ_INBOX_USER_IDS', '')),
            notify_bot_token=os.getenv('EDZ_NOTIFY_BOT_TOKEN', '').strip(),
            notify_chat_id=os.getenv('EDZ_NOTIFY_CHAT_ID', '').strip(),
            plan_max_pending_artifacts=int(os.getenv('PLAN_MAX_PENDING_ARTIFACTS', '20')),
            plan_max_single_bytes=int(os.getenv('PLAN_MAX_SINGLE_MIB', '50')) * 1024**2,
            plan_batch_id=os.getenv('PLAN_BATCH_ID', '').strip(),
            plan_batch_max_sources=int(os.getenv('PLAN_BATCH_MAX_SOURCES', '1')),
            backfill=_boolean('BACKFILL_ON_START', True),
            reserve_bytes=int(float(os.getenv('DISK_RESERVE_GB', '10')) * 1024**3),
            max_group_bytes=int(float(os.getenv('MAX_GROUP_GIB', '60')) * 1024**3),
            max_archive_files=int(os.getenv('MAX_ARCHIVE_FILES', '100000')),
            max_unpack_bytes=int(float(os.getenv('MAX_UNPACK_GB', '150')) * 1024**3),
            max_upload_bytes=int(float(os.getenv('MAX_UPLOAD_GB', '2')) * 1024**3),
            volume_quiet=int(os.getenv('VOLUME_QUIET_SECONDS', '600')),
            poll_seconds=int(os.getenv('POLL_SECONDS', '30')),
            send_delay=float(os.getenv('SEND_DELAY_SECONDS', '3')), 
            log_level=os.getenv('LOG_LEVEL', 'INFO').upper(),
            source_scan_enabled=_boolean('EDZ_SOURCE_SCAN_ENABLED', False),
            upload_engine_v2=_boolean('UPLOAD_ENGINE_V2', False),
            upload_concurrency=int(os.getenv('UPLOAD_CONCURRENCY', '1')),
            upload_max_concurrency=int(os.getenv('UPLOAD_MAX_CONCURRENCY', '2')),
            upload_max_retries=int(os.getenv('UPLOAD_MAX_RETRIES', '3')),
            upload_temp_dir=os.getenv('UPLOAD_TEMP_DIR', '').strip(),
            upload_canary_name=os.getenv('UPLOAD_CANARY_NAME', '').strip(),
            publisher_mode=os.getenv('PUBLISHER_MODE', 'paused').strip().lower(),
            upload_archive_stream_enabled=_boolean('UPLOAD_ARCHIVE_STREAM_ENABLED', False),
        )
        if (cfg.max_archive_files < 1 or cfg.max_unpack_bytes < 1 or cfg.poll_seconds < 1
                or cfg.max_group_bytes <= 0 or cfg.plan_max_pending_artifacts < 1
                or not 1024 <= cfg.plan_max_single_bytes <= cfg.max_group_bytes
                or not 1 <= cfg.plan_batch_max_sources <= 100
                or (cfg.plan_batch_id and not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', cfg.plan_batch_id))):
            raise ValueError('Invalid numeric limits in .env')
        if (not 1 <= cfg.pipeline_max_active_sources <= 4
                or not 1 <= cfg.pipeline_max_ready_artifacts <= 100000
                or not 1 <= cfg.pipeline_max_ready_gib <= 1024):
            raise ValueError('Invalid EDZ pipeline limits')
        if cfg.continuous_pipeline and not cfg.auto_mode:
            raise ValueError('EDZ_CONTINUOUS_PIPELINE=true requires AUTO_MODE=true')
        if (cfg.upload_engine_v2 and
                (not 1 <= cfg.upload_concurrency <= 2 or
                 not 1 <= cfg.upload_max_concurrency <= 2 or
                 cfg.upload_concurrency > cfg.upload_max_concurrency or
                 cfg.upload_max_retries < 0)):
            raise ValueError('UPLOAD_* limits: concurrency must be 1 or 2, retries >= 0')
        # Fail-closed publisher gate: default 'paused' means NOTHING may publish,
        # including the legacy V1 path. Canary mode is the only approved writer.
        if cfg.publisher_mode not in ('paused', 'canary', 'normal'):
            raise ValueError('PUBLISHER_MODE must be paused, canary or normal')
        if cfg.publisher_mode == 'canary' and not (
                cfg.upload_engine_v2 and cfg.upload_canary_name
                and cfg.upload_concurrency == 1 and cfg.upload_max_concurrency == 1):
            raise ValueError('Canary requires UPLOAD_ENGINE_V2=true, a canary name, '
                             'and UPLOAD_CONCURRENCY=UPLOAD_MAX_CONCURRENCY=1')
        # Full rollout runs V2 with V1 permanently retired. normal+V2=false would
        # silently reactivate the legacy publisher, so forbid it (fail closed).
        if cfg.publisher_mode == 'normal' and not cfg.upload_engine_v2:
            raise ValueError('PUBLISHER_MODE=normal requires UPLOAD_ENGINE_V2=true '
                             '(legacy V1 publisher must stay retired)')
        # Single private control group. The BotFather bot replies in that same room.
        if cfg.inbox_chat is not None:
            if not isinstance(cfg.inbox_chat, int) or cfg.inbox_chat >= 0:
                raise ValueError('EDZ_INBOX_CHAT cần là ID nhóm Telegram dạng -100...')
            if not cfg.inbox_user_ids:
                raise ValueError('Cần EDZ_INBOX_USER_IDS để chỉ quản trị viên được ra lệnh')
            if not cfg.notify_bot_token:
                raise ValueError('Cần EDZ_NOTIFY_BOT_TOKEN để bot trả lời trong cùng nhóm')
            if cfg.notify_chat_id and str(cfg.notify_chat_id) != str(cfg.inbox_chat):
                raise ValueError('EDZ_NOTIFY_CHAT_ID phải trùng EDZ_INBOX_CHAT (một nhóm duy nhất)')
        elif cfg.notify_bot_token and cfg.notify_chat_id:
            raise ValueError('Chỉ gửi bot thông báo qua EDZ_INBOX_CHAT, không cấu hình chat thứ hai')
        if cfg.auto_mode:
            if cfg.dry_run or cfg.backfill or cfg.auto_plan_queue or cfg.allow_unverified:
                raise ValueError('AUTO_MODE requires DRY_RUN=false, BACKFILL_ON_START=false, '
                                 'AUTO_PLAN_QUEUE=false, ALLOW_UNVERIFIED_SPLITS=false')
            if cfg.alert_chat != 'me' and not isinstance(cfg.alert_chat, int):
                raise ValueError('ERROR_CHAT must be me or a Telegram chat integer ID')
            if not cfg.auto_cleanup or not cfg.auto_work_cleanup:
                raise ValueError('AUTO_MODE requires AUTO_CLEANUP=true and AUTO_WORK_CLEANUP=true')
        return cfg

    def setup(self):
        for suffix in ('incoming', 'library', 'work', 'volumes', 'logs', 'quarantine'):
            (self.data / suffix).mkdir(parents=True, exist_ok=True, mode=0o700)
        self.data.mkdir(exist_ok=True, mode=0o700, parents=True)
        try:
            self.data.chmod(0o700)
        except OSError:
            pass
