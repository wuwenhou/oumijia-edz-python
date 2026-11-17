"""Upload Engine V2 -- single durable publisher for OUMIJIA Python.

Design rules (spec V2):
  * ONE publisher: this module replaces the send path inside the existing
    publish worker; it never starts a second Telegram client/process.
  * Name identity is the authority: NFC + casefold basename. Two different
    names sharing a SHA-256 are independent artifacts (handled by the
    companion tables without touching artifacts.sha256 PRIMARY KEY).
  * Atomic name ownership is claimed in SQLite BEFORE any byte is uploaded.
  * Telegram ACK (destination message id + posted + dedup index + log) is
    committed in ONE SQLite transaction.
  * If Telegram may have accepted the file but the outcome is unknown, the
    job is HELD for reconciliation and is never auto-resent.
  * Additive schema only; no historical row is edited or deleted.

Everything here is mockable: DB is the existing DB wrapper (or any object
with .execute/.value/.set), client is a Telethon-like object, and the
optional sysinfo callable supplies RAM/CPU/disk numbers for stats.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from pathlib import Path
import time
import unicodedata

from telethon.errors import FloodWaitError
from telethon.tl.types import DocumentAttributeFilename

from .archive import sha256_file
from .metadata import tag
from .name_guard import normalize_edz_name
from .publish_guard import (flood_seconds_from_message, set_cooldown,
                            cooldown_remaining)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Caption (OUMIJIA EDZ template, identical to Service.caption in bot.py)
# ---------------------------------------------------------------------------
def build_caption(name: str, sha: str, brand: str | None = None,
                  part: str | None = None, *, max_len: int = 1024) -> str:
    brand = brand or 'EPLAN EDZ'
    part = part or (name[:-4] if name.lower().endswith('.edz') else name)
    text = (f'EPLAN EDZ | {brand}\n'
            f'Part: {part}\n'
            f'File: {name}\n'
            f'SHA-256: {sha}\n'
            f'#{tag(brand)} #EPLAN #EDZ #EDZID_{sha[:16]}')
    return text[:max_len]


# ---------------------------------------------------------------------------
# Adaptive concurrency governor: 1 <-> 2 only, never above the account cap.
# ---------------------------------------------------------------------------
class AdaptiveGovernor:
    def __init__(self, *, initial: int = 1, maximum: int = 2,
                 success_streak: int = 25):
        self.minimum = 1
        self.maximum = max(1, min(2, maximum))          # hard cap per spec
        self.current = max(self.minimum, min(self.maximum, initial))
        self.success_streak = max(1, success_streak)
        self._good = 0

    def on_success(self) -> None:
        self._good += 1
        if self._good >= self.success_streak and self.current < self.maximum:
            self.current += 1
            self._good = 0

    def on_rate_limit(self) -> None:
        self.current = self.minimum      # back off immediately on any limit
        self._good = 0


# ---------------------------------------------------------------------------
# Honest, bounded statistics. System metrics are injected so tests stay pure.
# ---------------------------------------------------------------------------
@dataclass
class UploadStats:
    started: float = field(default_factory=time.monotonic)
    bytes_uploaded: int = 0
    files: int = 0
    ack_seconds_total: float = 0.0
    floodwaits: int = 0
    retries: int = 0
    duplicates: int = 0
    sysinfo: object | None = None

    def record_upload(self, size: int, ack_seconds: float) -> None:
        self.bytes_uploaded += int(size)
        self.files += 1
        self.ack_seconds_total += max(0.0, ack_seconds)

    def snapshot(self) -> dict:
        elapsed = max(1e-6, time.monotonic() - self.started)
        out = {
            'files': self.files,
            'bytes': self.bytes_uploaded,
            'mib': round(self.bytes_uploaded / 1024**2, 2),
            'mib_per_s': round(self.bytes_uploaded / 1024**2 / elapsed, 2),
            'files_per_min': round(self.files * 60 / elapsed, 2),
            'avg_ack_s': round(self.ack_seconds_total / self.files, 3) if self.files else 0.0,
            'floodwaits': self.floodwaits,
            'retries': self.retries,
            'duplicates': self.duplicates,
        }
        if self.sysinfo is not None:
            try:
                out.update(self.sysinfo())
            except Exception:
                log.debug('sysinfo unavailable', exc_info=True)
        return out


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------
@dataclass
class EngineConfig:
    concurrency: int = 1           # initial parallel uploads (1 or 2)
    max_concurrency: int = 2
    flood_budget: int = 8          # FloodWait sleeps per job, never from retry budget
    max_retries: int = 3           # bounded retries only for pre-send safe failures
    max_upload_bytes: int = 2 * 1024**3
    success_streak: int = 25


class UploadEngine:
    """The only publisher. Wrap the existing publish worker with run_batch()."""

    def __init__(self, db, client, target, *, source=None, cfg: EngineConfig | None = None,
                 get_delay=None, sysinfo=None):
        self.db = db
        self.client = client
        self.target = target
        self.source = source
        self.cfg = cfg or EngineConfig()
        self.get_delay = get_delay or (lambda: 0.0)
        self.governor = AdaptiveGovernor(initial=self.cfg.concurrency,
                                         maximum=self.cfg.max_concurrency,
                                         success_streak=self.cfg.success_streak)
        self.stats = UploadStats(sysinfo=sysinfo)

    # ---- durability / dedup ------------------------------------------------
    def _event(self, norm: str, kind: str, detail: str = '') -> None:
        self.db.execute(
            'INSERT INTO upload_events(at,normalized_name,kind,detail) VALUES(?,?,?,?)',
            (time.time(), norm, kind, str(detail)[:1500]))

    def _table(self, name: str) -> bool:
        return bool(self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())

    def claim_name(self, name: str, sha: str, size: int,
                   source_message: int | None = None) -> bool:
        """Atomically own a normalized name. False = already owned (duplicate)."""
        norm = normalize_edz_name(name)
        cur = self.db.execute(
            'INSERT OR IGNORE INTO upload_name_owners'
            '(normalized_name,sha256,file_name,size_bytes,source_message,created_at)'
            ' VALUES(?,?,?,?,?,?)',
            (norm, sha, name, int(size), source_message, time.time()))
        if cur.rowcount != 1:
            self.stats.duplicates += 1
            return False
        return True

    def enqueue(self, name: str, sha: str, path: str, size: int,
                *, source_message: int | None = None, brand: str | None = None,
                part: str | None = None) -> bool:
        """Claim the name then durably enqueue a job. No-op on duplicate name."""
        norm = normalize_edz_name(name)
        # Legacy guard: never enqueue something already accepted/posted.
        if self._table('edz_name_catalog') and self.db.execute(
                "SELECT 1 FROM edz_name_catalog WHERE normalized_name=? AND state='accepted'",
                (norm,)).fetchone():
            self.stats.duplicates += 1
            return False
        if not self.claim_name(name, sha, size, source_message):
            return False
        self.db.execute(
            'INSERT OR IGNORE INTO upload_jobs'
            '(normalized_name,sha256,file_name,path,size_bytes,source_message,state,created_at,updated_at)'
            ' VALUES(?,?,?,?,?,?,?,?,?)',
            (norm, sha, name, str(path), int(size), source_message, 'queued',
             time.time(), time.time()))
        self._event(norm, 'enqueued', f'sha={sha[:16]} size={size}')
        return True

    def enqueue_ready_artifacts(self) -> int:
        """Bridge the existing 'ready' artifacts into the durable queue."""
        n = 0
        rows = self.db.execute(
            "SELECT * FROM artifacts WHERE state='ready' ORDER BY created_at").fetchall()
        for a in rows:
            if self.enqueue(a['file_name'], a['sha256'], a['path'], a['size_bytes'],
                            source_message=a['source_message'],
                            brand=a['brand'], part=a['part_number']):
                n += 1
        return n

    def claim_next(self):
        """Atomically move one queued job to 'posting'. Safe across restarts."""
        self.db.execute('BEGIN IMMEDIATE')
        try:
            row = self.db.execute(
                "SELECT * FROM upload_jobs WHERE state='queued' ORDER BY created_at LIMIT 1").fetchone()
            if row is None:
                self.db.execute('COMMIT')
                return None
            self.db.execute(
                "UPDATE upload_jobs SET state='posting', attempts=attempts+1, updated_at=? "
                "WHERE normalized_name=?", (time.time(), row['normalized_name']))
            self.db.execute('COMMIT')
            return dict(row)
        except Exception:
            self.db.execute('ROLLBACK')
            raise

    def ack(self, job: dict, target_message: int) -> None:
        """Single transaction: message id + posted + dedup index + upload log."""
        norm = job['normalized_name']
        now = time.time()
        self.db.execute('BEGIN IMMEDIATE')
        try:
            self.db.execute(
                "UPDATE upload_jobs SET state='posted', target_message=?, error=NULL, updated_at=? "
                "WHERE normalized_name=?", (int(target_message), now, norm))
            # The artifacts row may not exist (archive-sourced name) and older
            # Python DBs lack normalized_name -- never assume the column exists.
            if self.db.execute(
                    "SELECT 1 FROM pragma_table_info('artifacts') WHERE name='normalized_name'"
            ).fetchone():
                self.db.execute(
                    "UPDATE artifacts SET state='posted', target_message=?, normalized_name=?, "
                    "error=NULL, updated_at=? WHERE sha256=?",
                    (int(target_message), norm, now, job['sha256']))
            else:
                self.db.execute(
                    "UPDATE artifacts SET state='posted', target_message=?, "
                    "error=NULL, updated_at=? WHERE sha256=?",
                    (int(target_message), now, job['sha256']))
            if self._table('edz_name_catalog'):
                self.db.execute(
                    "UPDATE edz_name_catalog SET state='accepted' WHERE normalized_name=?",
                    (norm,))
            self._event(norm, 'ack', f'message={int(target_message)}')
            self.db.execute('COMMIT')
        except Exception:
            self.db.execute('ROLLBACK')
            raise

    def hold(self, job: dict, error: str, *, ambiguous: bool = True) -> None:
        """Held jobs are NEVER auto-resent; a human/dedicated reconcile decides."""
        norm = job['normalized_name']
        self.db.execute(
            "UPDATE upload_jobs SET state='held', error=?, updated_at=? WHERE normalized_name=?",
            (str(error)[:1500], time.time(), norm))
        self._event(norm, 'ambiguous' if ambiguous else 'error', error)

    def recover_posting(self) -> int:
        """Startup: any 'posting' job is ambiguous -> hold, never auto-resend."""
        rows = self.db.execute(
            "SELECT normalized_name FROM upload_jobs WHERE state='posting'").fetchall()
        for r in rows:
            self.db.execute(
                "UPDATE upload_jobs SET state='held', "
                "error='Previous send interrupted: verify destination before retry', "
                "updated_at=? WHERE normalized_name=?", (time.time(), r['normalized_name']))
            self._event(r['normalized_name'], 'recovered_ambiguous', 'posting -> held')
        return len(rows)

    # ---- sending -----------------------------------------------------------
    def _caption(self, job: dict) -> str:
        return build_caption(job['file_name'], job['sha256'],
                             brand=job.get('brand'), part=job.get('part'))

    async def publish(self, job: dict) -> bool:
        """Validate -> send -> ACK. Returns True only on a confirmed ACK."""
        norm = job['normalized_name']
        path = Path(job['path'])
        # Pre-send failures are verifiably safe to bound-retry / hold.
        if not path.is_file():
            self.hold(job, 'Output EDZ missing', ambiguous=False)
            return False
        if path.stat().st_size != job['size_bytes']:
            self.hold(job, 'Output EDZ size mismatch', ambiguous=False)
            return False
        if job['size_bytes'] > self.cfg.max_upload_bytes:
            self.hold(job, 'EDZ exceeds configured upload size limit', ambiguous=False)
            return False
        started = time.monotonic()
        try:
            digest = await asyncio.to_thread(sha256_file, path)
            if digest != job['sha256']:
                self.hold(job, 'Output EDZ SHA256 changed', ambiguous=False)
                return False
        except Exception as ex:
            self.hold(job, f'Output EDZ unreadable: {ex}', ambiguous=False)
            return False

        media = path
        src = job.get('source_message')
        # Reuse valid source media to avoid re-uploading multi-GB originals.
        if self.source is not None and src and src > 0:
            try:
                original = await self.client.get_messages(self.source, ids=src)
                if (original and getattr(original, 'media', None)
                        and not getattr(original, 'noforwards', False)):
                    media = original.media
            except Exception:
                log.debug('Source media lookup failed; uploading local bytes', exc_info=True)
        kwargs = {'caption': self._caption(job), 'parse_mode': None, 'force_document': True}
        if isinstance(media, Path):
            kwargs['attributes'] = [DocumentAttributeFilename(job['file_name'])]
        try:
            sent = await self.client.send_file(self.target, media, **kwargs)
        except FloodWaitError as ex:
            await self._on_flood(job, ex.seconds)
            return False
        except Exception as ex:
            seconds = flood_seconds_from_message(str(ex))
            if seconds is not None:
                await self._on_flood(job, seconds)
            else:
                # Timeouts can happen AFTER Telegram accepted the post.
                self.hold(job, 'Ambiguous posting outcome: ' + str(ex)[:1200])
                log.exception('Posting uncertain; verify destination before retry: %s', norm)
            return False

        self.ack(job, sent.id)
        self.governor.on_success()
        self.stats.record_upload(job['size_bytes'], time.monotonic() - started)
        log.info('V2 Posted EDZ %s as message #%d (concurrency=%d)',
                 job['file_name'], sent.id, self.governor.current)
        return True

    async def _on_flood(self, job: dict, seconds: int) -> None:
        self.stats.floodwaits += 1
        self.governor.on_rate_limit()
        set_cooldown(self.db, seconds)         # shared gate with the control plane
        self.hold(job, f'FloodWait {seconds}s; destination reconciliation required')
        log.warning('V2 FloodWait %ss; concurrency reduced to 1; cooldown set', seconds)
        # Honor the wait exactly; never consume it from the retry budget.
        await asyncio.sleep(seconds)

    # ---- loop --------------------------------------------------------------
    async def publish_batch(self) -> int:
        """One scheduler tick: claim up to `concurrency` jobs and publish them."""
        if cooldown_remaining(self.db) > 0:
            return 0
        self.enqueue_ready_artifacts()
        jobs = []
        for _ in range(self.governor.current):
            job = self.claim_next()
            if job is None:
                break
            jobs.append(job)
        if not jobs:
            return 0
        results = await asyncio.gather(*(self.publish(j) for j in jobs),
                                       return_exceptions=True)
        for job, res in zip(jobs, results):
            if isinstance(res, BaseException):
                self.hold(job, f'Unexpected publisher failure: {res}')
                log.exception('V2 publisher batch error', exc_info=res)
        delay = float(self.get_delay() or 0.0)   # /limit override or SEND_DELAY; 0 = none
        if delay > 0:
            await asyncio.sleep(delay)
        return sum(1 for r in results if r is True)


def _accepted_names(db) -> set[str]:
    """Names already accepted anywhere we know about (skip before decoding)."""
    names: set[str] = set()
    for table, col in (('upload_name_owners', 'normalized_name'),
                       ('edz_name_catalog', 'normalized_name')):
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                      (table,)).fetchone():
            state_filter = " WHERE state='accepted'" if table == 'edz_name_catalog' else ''
            names.update(r[0] for r in db.execute(f'SELECT {col} FROM {table}{state_filter}'))
    if db.execute("SELECT 1 FROM pragma_table_info('artifacts') WHERE name='normalized_name'").fetchone():
        names.update(r[0] for r in db.execute(
            "SELECT normalized_name FROM artifacts WHERE state='posted'"))
    return names


async def publish_archive(engine: UploadEngine, sevenzip: str, archive: Path, *,
                          temp_dir: Path, max_files: int = 100000,
                          max_bytes: int = 150 * 1024**3,
                          enforce_crc: bool = True) -> dict:
    """Stream a (solid, multipart) 7z once and publish each new EDZ inline.

    Only one bounded temp file is alive at a time: enqueue -> claim -> publish
    -> cleanup before the next member is decoded. No whole-archive extraction.
    """
    from .stream_archive import list_edz_entries, stream_edz
    entries = list_edz_entries(sevenzip, archive, max_files=max_files, max_bytes=max_bytes)
    accepted = _accepted_names(engine.db)
    posted = skipped = 0
    for item in stream_edz(sevenzip, archive, entries, accepted_names=accepted,
                           temp_dir=temp_dir, max_bytes=engine.cfg.max_upload_bytes,
                           enforce_crc=enforce_crc):
        try:
            if engine.enqueue(item.name, item.sha256, str(item.path), item.size):
                job = engine.claim_next()
                if job and await engine.publish(job):
                    posted += 1
            else:
                skipped += 1
        finally:
            item.cleanup()
    return {'entries': len(entries), 'posted': posted, 'skipped': skipped}
