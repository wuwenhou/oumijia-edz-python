"""Upload Engine V2 -- unit tests. No real Telegram; DB is in-memory SQLite."""
from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path
import unicodedata

import pytest

from edzbot.db import DB
from edzbot.upload_engine import (UploadEngine, EngineConfig, AdaptiveGovernor,
                                  UploadStats, build_caption)


def fresh_db(*, legacy: bool = False):
    db = DB(Path(':memory:'))
    db.execute("CREATE TABLE IF NOT EXISTS edz_name_catalog(normalized_name TEXT, state TEXT)")
    if not legacy:   # production has this column; legacy Python DBs do not
        db.execute('ALTER TABLE artifacts ADD COLUMN normalized_name TEXT')
    return db


def make_cfg(**kw):
    base = dict(concurrency=1, max_concurrency=2)
    base.update(kw)
    return EngineConfig(**base)


class Sent:
    def __init__(self, mid):
        self.id = mid


class FakeClient:
    def __init__(self, *, fail=None, media=None):
        self.fail = fail
        self.media = media
        self.calls = []
        self._next = 1000

    async def send_file(self, target, media, **kwargs):
        self.calls.append((target, media, kwargs))
        if self.fail is not None:
            raise self.fail
        self._next += 1
        return Sent(self._next)

    async def get_messages(self, chat, ids):
        return self.media


def make_engine(db, client=None, **cfgkw):
    return UploadEngine(db, client or FakeClient(), 'TARGET', cfg=make_cfg(**cfgkw))


def job(name, sha, size=10, path='/tmp/x.edz'):
    return {'normalized_name': unicodedata.normalize('NFC', name).lower(),
            'sha256': sha, 'file_name': name, 'path': path,
            'size_bytes': size, 'source_message': None, 'brand': None, 'part': None}


def insert_art(db, sha):
    db.execute("INSERT INTO artifacts(sha256,file_name,path,size_bytes,source_message,"
               "origin,brand,part_number,validation,state,created_at,updated_at) "
               "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
               (sha, 'Ack.edz', '/tmp/ack', 10, 0, 'direct', '', '', 'ok', 'posting', 0, 0))


# --- 1. name normalization convention -------------------------------------
def test_normalize_nfc_casefold_and_caption():
    from edzbot.name_guard import normalize_edz_name
    assert normalize_edz_name('Café.EDZ') == normalize_edz_name('Cafe\u0301.EDZ')
    assert normalize_edz_name('ABC.edz') == 'abc.edz'
    assert normalize_edz_name('Straße.edz') == normalize_edz_name('STRASSE.edz')
    cap = build_caption('A.edz', '0' * 64, brand='CHINT', part='A')
    assert 'SHA-256: ' + '0' * 64 in cap
    assert '#EDZID_' + '0' * 16 in cap
    assert len(cap) <= 1024


# --- 2/3. name identity ----------------------------------------------------
def test_same_name_diff_sha_never_repost():
    db = fresh_db()
    e = make_engine(db)
    assert e.enqueue('Same.edz', 'a' * 64, '/tmp/a', 10) is True
    assert e.enqueue('SAME.edz', 'b' * 64, '/tmp/b', 10) is False
    assert db.execute('SELECT COUNT(*) FROM upload_jobs').fetchone()[0] == 1
    assert e.stats.duplicates == 1


def test_different_name_same_sha_independent():
    db = fresh_db()
    e = make_engine(db)
    sha = 'c' * 64
    assert e.enqueue('One.edz', sha, '/tmp/one', 10) is True
    assert e.enqueue('Two.edz', sha, '/tmp/two', 10) is True
    assert db.execute('SELECT COUNT(*) FROM upload_jobs').fetchone()[0] == 2
    assert db.execute('SELECT COUNT(DISTINCT sha256) FROM upload_jobs').fetchone()[0] == 1


# --- 4. ACK single transaction --------------------------------------------
def test_ack_updates_all_in_one_txn():
    db = fresh_db()
    insert_art(db, 'd' * 64)
    e = make_engine(db)
    e.enqueue('Ack.edz', 'd' * 64, '/tmp/ack', 10)
    j = dict(db.execute('SELECT * FROM upload_jobs').fetchone())
    e.ack(j, 4242)
    row = db.execute('SELECT state,target_message FROM upload_jobs').fetchone()
    assert (row['state'], row['target_message']) == ('posted', 4242)
    art = db.execute('SELECT state,target_message,normalized_name FROM artifacts').fetchone()
    assert art['state'] == 'posted' and art['target_message'] == 4242
    assert db.execute("SELECT kind FROM upload_events WHERE kind='ack'").fetchone()[0] == 'ack'


def test_ack_works_on_legacy_db_without_normalized_name():
    db = fresh_db(legacy=True)
    insert_art(db, '9' * 64)
    e = make_engine(db)
    e.enqueue('Legacy.edz', '9' * 64, '/tmp/ack', 10)
    e.ack(dict(db.execute('SELECT * FROM upload_jobs').fetchone()), 55)
    art = db.execute('SELECT state,target_message FROM artifacts').fetchone()
    assert art['state'] == 'posted' and art['target_message'] == 55


def test_ack_failure_rolls_back_everything(monkeypatch):
    db = fresh_db()
    e = make_engine(db)
    e.enqueue('Rollback.edz', 'e' * 64, '/tmp/rb', 10)
    j = e.claim_next()                    # mirror real flow: queued -> posting
    def boom(*a, **k):
        raise RuntimeError('sqlite ACK failure')
    monkeypatch.setattr(e, '_event', boom)
    with pytest.raises(RuntimeError):
        e.ack(j, 99)
    row = db.execute('SELECT state,target_message FROM upload_jobs').fetchone()
    assert row['state'] == 'posting' and row['target_message'] is None


# --- 5/9. ambiguous + restart recovery are HELD ---------------------------
def test_ambiguous_send_is_held_and_not_autoresent(tmp_path):
    db = fresh_db()
    client = FakeClient(fail=RuntimeError('Read timed out after server accepted'))
    e = make_engine(db, client)
    p = tmp_path / 'amb.edz'
    p.write_bytes(b'z' * 10)
    digest = hashlib.sha256(p.read_bytes()).hexdigest()
    e.enqueue('Ambig.edz', digest, str(p), 10)
    j = dict(db.execute('SELECT * FROM upload_jobs').fetchone())
    assert asyncio.run(e.publish(j)) is False
    row = db.execute('SELECT state,error FROM upload_jobs').fetchone()
    assert row['state'] == 'held' and 'Ambiguous' in row['error']
    assert e.claim_next() is None


def test_restart_recover_posting_to_held():
    db = fresh_db()
    e = make_engine(db)
    e.enqueue('Interrupted.edz', '1' * 64, '/tmp/i', 10)
    db.execute("UPDATE upload_jobs SET state='posting'")
    assert e.recover_posting() == 1
    row = db.execute('SELECT state,error FROM upload_jobs').fetchone()
    assert row['state'] == 'held' and 'verify destination' in row['error']


# --- 6/8. FloodWait -------------------------------------------------------
def test_floodwait_holds_and_reduces_concurrency(monkeypatch, tmp_path):
    from telethon.errors import FloodWaitError
    db = fresh_db()
    err = FloodWaitError(None, 0)
    err.seconds = 1
    e = make_engine(db, FakeClient(fail=err), concurrency=2, max_concurrency=2)
    slept = []
    async def no_sleep(s):
        slept.append(s)
    monkeypatch.setattr(asyncio, 'sleep', no_sleep)
    p = tmp_path / 'flood.edz'
    p.write_bytes(b'y' * 10)
    digest = hashlib.sha256(p.read_bytes()).hexdigest()
    e.enqueue('Flood.edz', digest, str(p), 10)
    j = dict(db.execute('SELECT * FROM upload_jobs').fetchone())
    assert asyncio.run(e.publish(j)) is False
    assert e.governor.current == 1
    assert e.stats.floodwaits == 1
    assert slept == [1]
    row = db.execute('SELECT state,error FROM upload_jobs').fetchone()
    assert row['state'] == 'held' and 'reconciliation' in row['error']
    from edzbot.publish_guard import cooldown_remaining
    assert cooldown_remaining(db) > 0


def test_governor_recovers_after_streak():
    g = AdaptiveGovernor(initial=1, maximum=2, success_streak=3)
    for _ in range(3):
        g.on_success()
    assert g.current == 2
    g.on_rate_limit()
    assert g.current == 1


# --- 7. stats --------------------------------------------------------------
def test_stats_rates():
    s = UploadStats()
    s.record_upload(10 * 1024**2, 0.5)
    snap = s.snapshot()
    assert snap['files'] == 1 and snap['mib'] == 10
    assert snap['mib_per_s'] > 0 and snap['files_per_min'] > 0
    assert snap['avg_ack_s'] == 0.5


# --- 10/11. bounded concurrency + single publisher ------------------------
def test_publish_batch_concurrency_limits(monkeypatch):
    db = fresh_db()
    e = make_engine(db, FakeClient(), concurrency=2, max_concurrency=2)
    for i in range(5):
        e.enqueue(f'c{i}.edz', str(i) * 64, f'/tmp/c{i}', 10)
    async def fake_publish(j):
        return True
    monkeypatch.setattr(e, 'publish', fake_publish)
    n = asyncio.run(e.publish_batch())
    assert n == 2
    assert db.execute("SELECT COUNT(*) FROM upload_jobs WHERE state='posting'").fetchone()[0] == 2


def test_single_publisher_no_duplicate_on_restart():
    db = fresh_db()
    e1 = make_engine(db)
    e1.enqueue('Restart.edz', '3' * 64, '/tmp/r', 10)
    e1.ack(dict(db.execute('SELECT * FROM upload_jobs').fetchone()), 7)
    e2 = make_engine(db)
    assert e2.enqueue('RESTART.edz', '4' * 64, '/tmp/r2', 10) is False
    assert db.execute('SELECT COUNT(*) FROM upload_jobs').fetchone()[0] == 1


# --- media reuse from a valid source --------------------------------------
def test_media_reuse_avoids_reupload(tmp_path):
    db = fresh_db()
    media = object()
    client = FakeClient(media=type('M', (), {'media': media, 'noforwards': False})())
    e = make_engine(db, client)
    p = tmp_path / 'reuse.edz'
    digest = hashlib.sha256(b'x' * 10).hexdigest()
    e.enqueue('Reuse.edz', digest, str(p), 10)
    j = dict(db.execute('SELECT * FROM upload_jobs').fetchone())
    j['source_message'] = 55
    e.source = 'SOURCE'
    p.write_bytes(b'x' * 10)
    assert asyncio.run(e.publish(j)) is True
    assert client.calls[0][1] is media
