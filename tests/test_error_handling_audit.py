"""Error-handling audit simulations on isolated SQLite + fake Telegram."""
from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path
import time

from edzbot.auto_alerts import next_alert, mark_alert_sending
from edzbot.db import DB
from edzbot.review_reports import query_held, csv_bytes
from edzbot.upload_engine import UploadEngine, EngineConfig


class FakeClient:
    def __init__(self, fail=None):
        self.fail = fail
        self.calls = []

    async def send_file(self, *a, **k):
        self.calls.append(a)
        if self.fail:
            raise self.fail
        return type('S', (), {'id': 900 + len(self.calls)})()


def make_engine(db, client=None, **kw):
    return UploadEngine(db, client or FakeClient(), 'DEST',
                        cfg=EngineConfig(concurrency=1, max_concurrency=1, **kw))


def seed_job(db, name='HELD.edz', error='Ambiguous posting outcome: timeout'):
    db.execute("INSERT OR IGNORE INTO upload_name_owners"
               "(normalized_name,sha256,file_name,size_bytes,created_at)"
               " VALUES(?,?,?,?,?)", (name.lower(), 'a' * 64, name, 10, time.time()))
    db.execute("INSERT INTO upload_jobs(normalized_name,sha256,file_name,path,size_bytes,"
               "source_message,origin,state,error,created_at,updated_at)"
               " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
               (name.lower(), 'a' * 64, name, f'/tmp/{name}', 10, 7, 'generated',
                'held', error, time.time(), time.time()))


def test_v2_held_job_appears_in_errors_and_csv(tmp_path):
    root = tmp_path
    (root / 'data').mkdir()
    db = DB(root / 'data/state.sqlite3')
    seed_job(db)
    rows = query_held(root)
    v2 = [r for r in rows if r['type'] == 'v2_job']
    assert len(v2) == 1
    assert v2[0]['file_name'] == 'HELD.edz'
    assert v2[0]['repair_caption'].startswith('FIX_V2JOB:')
    data = csv_bytes(rows)
    assert b'HELD.edz' in data and b'v2_job' in data
    db.close()


def test_v2_held_job_reaches_control_bot_alert(tmp_path):
    db = DB(tmp_path / 'alerts.sqlite3')
    seed_job(db, name='ALERT.edz')
    keys = []
    for _ in range(20):
        alert = next_alert(db)
        if alert is None:
            break
        key, body = alert
        keys.append(key)
        if 'UPLOAD V2' in body:
            assert 'ALERT.edz' in body and 'KHÔNG tự retry' in body
        assert mark_alert_sending(db, key, body)
    assert any(k.startswith('v2job:alert.edz') for k in keys)
    db.close()


def test_ambiguous_never_auto_resends(tmp_path):
    db = DB(tmp_path / 'amb.sqlite3')
    p = tmp_path / 'a.edz'
    p.write_bytes(b'x' * 10)
    digest = hashlib.sha256(p.read_bytes()).hexdigest()
    client = FakeClient(fail=TimeoutError('ACK lost'))
    e = make_engine(db, client)
    e.enqueue('A.edz', digest, str(p), 10)
    j = dict(db.execute('SELECT * FROM upload_jobs').fetchone())
    assert asyncio.run(e.publish(j)) is False
    assert db.execute("SELECT state FROM upload_jobs").fetchone()[0] == 'held'
    before = len(client.calls)
    asyncio.run(e.publish_batch())
    asyncio.run(e.publish_batch())
    assert len(client.calls) == before
    db.close()


def test_sqlite_fault_locks_across_restart(tmp_path):
    path = tmp_path / 'fault.sqlite3'
    db = DB(path)
    p = tmp_path / 'f.edz'
    p.write_bytes(b'f' * 10)
    digest = hashlib.sha256(p.read_bytes()).hexdigest()
    e = make_engine(db)
    e.enqueue('F.edz', digest, str(p), 10)
    e.mark_fault(RuntimeError('database is locked'))
    e.db.close()
    e2 = make_engine(DB(path))
    assert e2.faulted is True
    assert asyncio.run(e2.publish_batch()) == 0
    assert e2.db.execute("SELECT state FROM upload_jobs").fetchone()[0] == 'queued'
    e2.db.close()


def test_ack_lost_after_telegram_accepts_stays_ambiguous(tmp_path):
    """Job held, no target_message; no second job can be enqueued for the name."""
    db = DB(tmp_path / 'ack.sqlite3')
    p = tmp_path / 'n.edz'
    p.write_bytes(b'n' * 10)
    digest = hashlib.sha256(p.read_bytes()).hexdigest()
    e = make_engine(db)
    e.enqueue('N.edz', digest, str(p), 10)
    j = dict(db.execute('SELECT * FROM upload_jobs').fetchone())
    e.hold(j, 'Ambiguous posting outcome: timeout after Telegram accepted')
    assert e.enqueue('N.edz', 'b' * 64, str(p), 10) is False
    assert db.execute('SELECT COUNT(*) FROM upload_jobs').fetchone()[0] == 1
    assert db.execute('SELECT target_message FROM upload_jobs').fetchone()[0] is None
    db.close()


def test_posting_on_restart_becomes_held_not_queued(tmp_path):
    db = DB(tmp_path / 'crash.sqlite3')
    seed_job(db, name='CRASH.edz', error='x')
    db.execute("UPDATE upload_jobs SET state='posting'")
    e = make_engine(db)
    assert e.recover_posting() == 1
    row = db.execute("SELECT state,error,target_message FROM upload_jobs").fetchone()
    assert row['state'] == 'held' and row['target_message'] is None
    assert asyncio.run(e.publish_batch()) == 0
    db.close()
