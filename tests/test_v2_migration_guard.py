"""Canary-2 preflight: migration, fail-closed publisher gate, media, /v2stats."""
from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from edzbot.db import DB
from edzbot.upload_engine import (UploadEngine, EngineConfig, source_media_reusable)


class FakeClient:
    def __init__(self, media=None):
        self.media = media
        self.calls = []
        self._id = 100

    async def send_file(self, target, media, **kwargs):
        self.calls.append(media)
        self._id += 1
        return type('S', (), {'id': self._id})()

    async def get_messages(self, chat, ids):
        return self.media


def msg(media=True, *, noforwards=False, chat_noforwards=False, missing=False):
    if missing:
        return type('M', (), {'media': object()})()
    return type('M', (), {
        'media': object() if media else None,
        'noforwards': noforwards,
        'chat': type('C', (), {'noforwards': chat_noforwards})(),
    })()


def eng(db, client=None, **kw):
    cfg = EngineConfig(concurrency=1, max_concurrency=1, **kw)
    return UploadEngine(db, client or FakeClient(), 'T', cfg=cfg)


# ---------------------------------------------------------------------------
# 1. Migration: old V2 schema -> 6c11caf, idempotent, preserves held job
# ---------------------------------------------------------------------------
OLD_SCHEMA = """
CREATE TABLE settings(key TEXT PRIMARY KEY, val TEXT NOT NULL);
CREATE TABLE artifacts(sha256 TEXT PRIMARY KEY, file_name TEXT NOT NULL, path TEXT NOT NULL,
 size_bytes INTEGER NOT NULL, source_message INTEGER NOT NULL, origin TEXT NOT NULL,
 brand TEXT NOT NULL, part_number TEXT NOT NULL, validation TEXT NOT NULL, state TEXT NOT NULL,
 target_message INTEGER, error TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL);
CREATE TABLE messages(id INTEGER PRIMARY KEY, file_name TEXT, kind TEXT, size_bytes INTEGER,
 group_name TEXT, volume_index INTEGER, state TEXT, local_path TEXT, attempts INTEGER,
 retry_after REAL, error TEXT, changed_at REAL);
CREATE TABLE upload_name_owners(normalized_name TEXT PRIMARY KEY, sha256 TEXT, file_name TEXT,
 size_bytes INTEGER, source_message INTEGER, created_at REAL);
CREATE TABLE upload_jobs(normalized_name TEXT PRIMARY KEY, sha256 TEXT, file_name TEXT,
 path TEXT, size_bytes INTEGER, source_message INTEGER, state TEXT, attempts INTEGER DEFAULT 0,
 target_message INTEGER, error TEXT, created_at REAL, updated_at REAL);
CREATE TABLE upload_events(id INTEGER PRIMARY KEY AUTOINCREMENT, at REAL, normalized_name TEXT,
 kind TEXT, detail TEXT);
"""


def test_migration_adds_origin_and_preserves_held(tmp_path):
    p = tmp_path / 'old.sqlite3'
    c = sqlite3.connect(p)
    c.executescript(OLD_SCHEMA)
    c.execute("INSERT INTO upload_jobs(normalized_name,sha256,file_name,path,size_bytes,"
              "source_message,state,attempts,target_message,error,created_at,updated_at)"
              " VALUES('held.edz','a','Held.edz','/tmp/h',10,1,'held',1,NULL,"
              "'Ambiguous posting outcome',0,0)")
    c.commit(); c.close()

    db = DB(p)                       # SCHEMA + idempotent migration
    cols = [r[1] for r in db.execute('PRAGMA table_info(upload_jobs)')]
    assert 'origin' in cols
    row = db.execute("SELECT state,error,target_message FROM upload_jobs WHERE normalized_name='held.edz'").fetchone()
    assert row['state'] == 'held' and 'Ambiguous' in row['error']
    assert row['target_message'] is None
    db.close()

    # second startup must be a no-op and keep the row
    db2 = DB(p)
    assert [r[1] for r in db2.execute('PRAGMA table_info(upload_jobs)')].count('origin') == 1
    assert db2.execute("SELECT state FROM upload_jobs WHERE normalized_name='held.edz'").fetchone()[0] == 'held'
    db2.close()


def test_fresh_db_has_origin_without_alter(tmp_path):
    db = DB(tmp_path / 'fresh.sqlite3')
    assert 'origin' in [r[1] for r in db.execute('PRAGMA table_info(upload_jobs)')]
    db.close()


# ---------------------------------------------------------------------------
# 2. Fail-closed publisher gate
# ---------------------------------------------------------------------------
def test_config_publisher_mode_is_fail_closed(monkeypatch):
    from edzbot.config import Config
    for k, v in dict(API_ID='1', API_HASH='h', SOURCE_CHAT='-1', DEST_CHAT='-2',
                     DATA_DIR='/tmp/cfgtest',
                     UPLOAD_ENGINE_V2='false', UPLOAD_CANARY_NAME='',
                     PUBLISHER_MODE='paused').items():
        monkeypatch.setenv(k, v)
    c = Config.load(require_telegram=False)
    assert c.publisher_mode == 'paused'
    monkeypatch.setenv('PUBLISHER_MODE', 'canary')
    with pytest.raises(ValueError):
        Config.load(require_telegram=False)       # no canary name / V2 off
    monkeypatch.setenv('UPLOAD_ENGINE_V2', 'true')
    monkeypatch.setenv('UPLOAD_CANARY_NAME', 'X.edz')
    monkeypatch.setenv('UPLOAD_CONCURRENCY', '2')
    with pytest.raises(ValueError):
        Config.load(require_telegram=False)       # concurrency must be 1
    monkeypatch.setenv('UPLOAD_CONCURRENCY', '1')
    monkeypatch.setenv('PUBLISHER_MODE', 'normal')
    assert Config.load(require_telegram=False).publisher_mode == 'normal'  # approved full rollout
    monkeypatch.setenv('UPLOAD_ENGINE_V2', 'false')
    with pytest.raises(ValueError):
        Config.load(require_telegram=False)       # normal must NOT fall back to V1


def test_governor_never_recovers_in_canary():
    from edzbot.upload_engine import AdaptiveGovernor
    g = AdaptiveGovernor(initial=1, maximum=2, success_streak=1, allow_recovery=False)
    for _ in range(10):
        g.on_success()
    assert g.current == 1


def test_restart_claim_only_canary_job(tmp_path):
    path = tmp_path / 'gate.sqlite3'
    db = DB(path)
    for i in range(4):
        db.execute("INSERT INTO artifacts(sha256,file_name,path,size_bytes,source_message,"
                   "origin,brand,part_number,validation,state,created_at,updated_at)"
                   " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                   (str(i) * 64, f'f{i}.edz', f'/tmp/f{i}', 10, 0, 'generated', '', '',
                    'ok', 'ready', 0, 0))
    db.close()

    def seed_ready():
        d = DB(path)
        for row in d.execute("SELECT * FROM artifacts WHERE state='ready'").fetchall():
            d.execute("UPDATE artifacts SET state='ready' WHERE sha256=?", (row['sha256'],))
        d.close()

    seed_ready()
    e1 = eng(DB(path), canary_name='F1.EDZ', canary_mode=True)
    assert e1.enqueue_ready_artifacts() == 1
    assert e1.claim_next()['normalized_name'] == 'f1.edz'
    e1.db.close()

    # simulated restart: new connection/process, same canary gate
    e2 = eng(DB(path), canary_name='F1.EDZ', canary_mode=True)
    assert e2.recover_posting() == 1                 # previous posting -> held
    assert e2.enqueue_ready_artifacts() == 0         # other 3 never enqueued
    assert e2.claim_next() is None                   # nothing else claimable
    assert e2.db.execute("SELECT COUNT(*) FROM upload_jobs").fetchone()[0] == 1
    e2.db.close()


# ---------------------------------------------------------------------------
# 3. Media reuse: fail-closed on every protection state
# ---------------------------------------------------------------------------
def test_source_media_reusable_matrix():
    assert source_media_reusable(msg()) is True                                  # known safe
    assert source_media_reusable(msg(noforwards=True)) is False                  # message protected
    assert source_media_reusable(msg(chat_noforwards=True)) is False             # chat protected
    assert source_media_reusable(msg(missing=True)) is False                     # unknown
    assert source_media_reusable(msg(media=False)) is False                      # no media
    assert source_media_reusable(None) is False


def test_generated_origin_uploads_local_bytes(tmp_path):
    db = DB(tmp_path / 'g.sqlite3')
    p = tmp_path / 'g.edz'
    p.write_bytes(b'g' * 10)
    digest = hashlib.sha256(p.read_bytes()).hexdigest()
    media = object()
    client = FakeClient(media=msg())
    e = eng(db, client)
    e.enqueue('Generated.edz', digest, str(p), 10, origin='generated', source_message=5)
    e.source = 'SOURCE'
    j = dict(db.execute('SELECT * FROM upload_jobs').fetchone())
    assert asyncio.run(e.publish(j)) is True
    assert client.calls[0] is not getattr(client.media, 'media', None)
    assert isinstance(client.calls[0], Path)
    db.close()


def test_bad_local_sha_is_held_without_send(tmp_path):
    db = DB(tmp_path / 'sha.sqlite3')
    p = tmp_path / 'bad.edz'
    p.write_bytes(b'wrong bytes')
    e = eng(db, FakeClient())
    e.enqueue('Bad.edz', 'f' * 64, str(p), p.stat().st_size, origin='generated')
    j = dict(db.execute('SELECT * FROM upload_jobs').fetchone())
    assert asyncio.run(e.publish(j)) is False
    assert db.execute("SELECT state FROM upload_jobs").fetchone()[0] == 'held'
    assert db.execute("SELECT COUNT(*) FROM upload_events WHERE kind='error'").fetchone()[0] == 1
    db.close()


# ---------------------------------------------------------------------------
# 4. /v2stats via control bot (no file post)
# ---------------------------------------------------------------------------
def test_ack_syncs_dedup_catalog(tmp_path):
    db = DB(tmp_path / 'cat.sqlite3')
    db.execute("CREATE TABLE edz_name_catalog(normalized_name TEXT PRIMARY KEY, "
               "winner_sha256 TEXT NOT NULL, winner_artifact_sha TEXT NOT NULL, "
               "winner_at REAL NOT NULL, state TEXT NOT NULL, updated_at REAL NOT NULL)")
    p = tmp_path / 'c.edz'
    p.write_bytes(b'c' * 10)
    digest = hashlib.sha256(p.read_bytes()).hexdigest()
    e = eng(db)
    e.enqueue('Catalog.edz', digest, str(p), 10)
    j = dict(db.execute('SELECT * FROM upload_jobs').fetchone())
    e.ack(j, 999)
    row = db.execute("SELECT normalized_name,winner_sha256,state FROM edz_name_catalog "
                     "WHERE normalized_name='catalog.edz'").fetchone()
    assert row is not None and row['state'] == 'accepted' and row['winner_sha256'] == digest
    db.close()


def test_sqlite_fault_is_fail_closed_and_survives_restart(tmp_path):
    path = tmp_path / 'fault.sqlite3'
    db = DB(path)
    p = tmp_path / 'f.edz'
    p.write_bytes(b'f' * 10)
    digest = hashlib.sha256(p.read_bytes()).hexdigest()
    e = eng(db, canary_name='F.EDZ', canary_mode=True)
    e.enqueue('F.edz', digest, str(p), 10)
    e.mark_fault(RuntimeError('database is locked'))
    e.db.close()

    # restart: a new engine must refuse to claim anything
    e2 = eng(DB(path), canary_name='F.EDZ', canary_mode=True)
    assert e2.faulted is True
    assert asyncio.run(e2.publish_batch()) == 0
    assert e2.db.execute("SELECT state FROM upload_jobs").fetchone()[0] == 'queued'
    e2.clear_fault()
    assert e2.faulted is False
    e2.db.close()


def test_help_menu_is_new_control_center(monkeypatch):
    from edzbot.bot import Service
    s = object.__new__(Service)
    s.cfg = SimpleNamespace(publisher_mode='normal', upload_engine_v2=True)
    s.db = DB(Path(':memory:'))
    s.upload_engine = None
    s.inbox_allowed = frozenset()
    out = []
    async def notice(t):
        out.append(t)
    s._manual_notice = notice
    monkeypatch.setattr('edzbot.bot.message_allowed', lambda m, a: True)
    asyncio.run(s.handle_manual(type('M', (), {'raw_text': '/help'})()))
    assert out and 'TRUNG TÂM ĐIỀU KHIỂN' in out[0]
    assert '/status' in out[0] and 'Upload V2' in out[0]
    assert 'BẢNG LỆNH' not in out[0]
    s.db.close()


def test_v2_status_block():
    from edzbot.bot import Service
    from edzbot.upload_engine import UploadStats
    s = object.__new__(Service)
    s.db = DB(Path(':memory:'))
    s.cfg = SimpleNamespace(send_delay=3.0)
    s.upload_engine = SimpleNamespace(
        stats=UploadStats(),
        governor=SimpleNamespace(current=1, maximum=1),
        faulted=False)
    s.upload_engine.stats.record_upload(1024**2, 0.4)
    text = s._v2_status_short()
    assert 'Upload V2: BẬT' in text and 'file/phút' in text and 'FloodWait' in text
    s.upload_engine.faulted = True
    assert 'FAULTED' in s._v2_status_short()
    s.db.close()


def test_v2stats_command_replies(monkeypatch):
    from edzbot.bot import Service
    s = object.__new__(Service)
    s.cfg = SimpleNamespace(publisher_mode='paused', upload_engine_v2=False)
    s.db = DB(Path(':memory:'))
    s.upload_engine = None
    s.inbox_allowed = frozenset()
    out = []
    async def notice(t):
        out.append(t)
    s._manual_notice = notice
    monkeypatch.setattr('edzbot.bot.message_allowed', lambda m, a: True)
    message = type('M', (), {'raw_text': '/v2stats'})()
    asyncio.run(s.handle_manual(message))
    assert out and 'OUMIJIA — Trạng thái' in out[0] and 'V2' in out[0]
    s.db.close()
