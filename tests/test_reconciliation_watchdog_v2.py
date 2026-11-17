"""Resumable reconciliation (>134k history) + watchdog + /errors V2 coverage."""
from __future__ import annotations

import asyncio
from pathlib import Path
import time

import pytest

from edzbot.db import DB
from edzbot.publish_guard import (reconcile_held, reconcilable_rows,
                                  parse_destination_caption, RECON_CURSOR,
                                  RECON_FOUND, RECON_WATERMARK)
from edzbot.review_reports import query_held, summary_text, detail_text, csv_bytes
from edzbot.watchdog import evaluate
from edzbot.name_guard import normalize_edz_name


def msg(mid: int, name: str, sha: str, size: int):
    doc = type('D', (), {'size': size})()
    return type('M', (), {
        'id': mid, 'document': doc,
        'message': f'EPLAN EDZ | X\nFile: {name}\nSHA-256: {sha}\n#EPLAN',
    })()


class MockTelegram:
    """Lazy descending history of `total` messages; no 150k objects in RAM."""

    def __init__(self, total: int, hits: dict[int, tuple[str, str, int]],
                 *, fail_on_page: int | None = None):
        self.total = total
        self.hits = hits
        self.fail_on_page = fail_on_page
        self.pages = 0

    def iter_messages(self, target, offset_id=None, limit=1000):
        self.pages += 1
        failing = self.fail_on_page == self.pages
        return self._gen(offset_id or 0, limit, failing)

    async def _gen(self, offset_id, limit, failing):
        if failing:
            raise ConnectionError('network lost between pages')
        emitted = 0
        cursor = offset_id or (self.total + 1)
        for mid in range(cursor - 1, 0, -1):
            if mid in self.hits:
                name, sha, size = self.hits[mid]
            else:
                name, sha, size = f'OTHER.{mid}.edz', 'f' * 64, 999
            yield msg(mid, name, sha, size)
            emitted += 1
            if emitted >= limit:
                return


def seed(db, *, name='TARGET.edz', sha='a' * 64, size=31308,
         err='FloodWait 30s; destination reconciliation required'):
    norm = normalize_edz_name(name)
    db.execute("INSERT OR IGNORE INTO artifacts(sha256,file_name,path,size_bytes,"
               "source_message,origin,brand,part_number,validation,state,error,created_at,updated_at)"
               " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
               (sha, name, '/tmp/target.edz', size, 7, 'generated', '', '', 'ok',
                'held', err, 0, 0))
    db.execute("INSERT OR IGNORE INTO upload_name_owners(normalized_name,sha256,file_name,"
               "size_bytes,created_at) VALUES(?,?,?,?,?)", (norm, sha, name, size, 0))
    db.execute("INSERT INTO upload_jobs(normalized_name,sha256,file_name,path,size_bytes,"
               "source_message,origin,state,error,created_at,updated_at)"
               " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
               (norm, sha, name, '/tmp/target.edz', size, 7, 'generated',
                'held', err, 0, 0))
    return norm


# --- caption parse ----------------------------------------------------------
def test_parse_caption_normalizes_name_and_sha():
    m = msg(1, 'Café.EDZ', 'A' * 64, 10)
    norm, sha = parse_destination_caption(m.message)
    assert norm == normalize_edz_name('Café.EDZ') and sha == 'a' * 64


# --- 150k history, pagination, checkpoint ----------------------------------
def test_150k_history_pages_checkpoint_and_resume():
    db = DB(Path(':memory:'))
    norm = seed(db)
    client = MockTelegram(150_000, {137_500: ('TARGET.edz', 'a' * 64, 31308)})

    partial = asyncio.run(reconcile_held(db, client, 'DEST', page_size=1000, max_pages=3))
    assert partial['complete'] is False and partial['scanned'] == 3000
    assert db.value(RECON_CURSOR, '') not in ('', '0')
    assert db.execute("SELECT state FROM upload_jobs").fetchone()[0] == 'held'
    assert db.execute("SELECT target_message FROM upload_jobs").fetchone()[0] is None

    resumed = asyncio.run(reconcile_held(db, client, 'DEST', page_size=1000))
    assert resumed['complete'] is True
    assert resumed['scanned'] > 12_000          # resumed from checkpoint, not from 150k
    assert resumed['posted_artifact'] == 1 and resumed['posted_v2'] == 1
    assert resumed['left_held'] == 0
    j = db.execute("SELECT state,target_message FROM upload_jobs").fetchone()
    a = db.execute("SELECT state,target_message FROM artifacts").fetchone()
    assert (j['state'], j['target_message']) == ('posted', 137_500)
    assert (a['state'], a['target_message']) == ('posted', 137_500)
    assert db.value(RECON_WATERMARK, '') != ''
    assert db.value(RECON_CURSOR, '') == ''
    db.close()


# --- network interruption between pages ------------------------------------
def test_network_loss_mid_scan_applies_nothing_then_resumes():
    db = DB(Path(':memory:'))
    seed(db)
    failing = MockTelegram(20_000, {17_000: ('TARGET.edz', 'a' * 64, 31308)},
                           fail_on_page=2)
    with pytest.raises(ConnectionError):
        asyncio.run(reconcile_held(db, failing, 'DEST', page_size=1000))
    assert db.value(RECON_CURSOR, '') not in ('', '0')
    assert db.execute("SELECT state FROM upload_jobs").fetchone()[0] == 'held'

    ok = MockTelegram(20_000, {17_000: ('TARGET.edz', 'a' * 64, 31308)})
    result = asyncio.run(reconcile_held(db, ok, 'DEST', page_size=1000))
    assert result['complete'] is True and result['posted_v2'] == 1
    db.close()


# --- lost ACK: V2 held reconciled, never reposted ---------------------------
def test_v2_ambiguous_reconciled_without_repost():
    db = DB(Path(':memory:'))
    seed(db, err='FloodWait send: 30 seconds; destination reconciliation required; ambiguous outcome')
    client = MockTelegram(5000, {4888: ('target.edz', 'a' * 64, 31308)})
    result = asyncio.run(reconcile_held(db, client, 'DEST', page_size=500))
    assert result['posted_v2'] == 1 and result['posted_artifact'] == 1
    assert db.execute("SELECT state FROM upload_jobs").fetchone()[0] == 'posted'
    assert db.execute("SELECT kind FROM upload_events WHERE kind='reconciled_ack'").fetchone()[0]
    # No publish path is invoked by reconciliation at all.
    assert not hasattr(client, 'send_file')
    db.close()


# --- same name, different SHA must NOT match --------------------------------
def test_same_name_different_sha_is_not_reconciled():
    db = DB(Path(':memory:'))
    seed(db, name='TARGET.edz', sha='a' * 64)
    client = MockTelegram(1000, {777: ('TARGET.edz', 'b' * 64, 31308)})
    result = asyncio.run(reconcile_held(db, client, 'DEST', page_size=200))
    assert result['complete'] is True
    assert result['posted_v2'] == 0 and result['posted_artifact'] == 0
    assert db.execute("SELECT state FROM upload_jobs").fetchone()[0] == 'held'
    db.close()


# --- size mismatch must NOT match -------------------------------------------
def test_size_mismatch_is_not_reconciled():
    db = DB(Path(':memory:'))
    seed(db)
    client = MockTelegram(1000, {777: ('TARGET.edz', 'a' * 64, 99999)})
    result = asyncio.run(reconcile_held(db, client, 'DEST', page_size=200))
    assert result['posted_v2'] == 0
    db.close()


# --- unrelated held rows untouched -----------------------------------------
def test_unrelated_jobs_keep_their_state():
    db = DB(Path(':memory:'))
    seed(db)
    db.execute("INSERT INTO upload_jobs(normalized_name,sha256,file_name,path,size_bytes,"
               "state,error,created_at,updated_at)"
               " VALUES('other.edz','c'*64,'OTHER.edz','/tmp/o',10,'held','PROTECTED error',0,0)")
    client = MockTelegram(1000, {777: ('TARGET.edz', 'a' * 64, 31308)})
    asyncio.run(reconcile_held(db, client, 'DEST', page_size=200))
    assert db.execute("SELECT state FROM upload_jobs WHERE normalized_name='other.edz'").fetchone()[0] == 'held'
    db.close()


# --- /errors integration ----------------------------------------------------
def test_errors_report_and_sha_lookup_include_v2(tmp_path):
    root = tmp_path
    (root / 'data').mkdir()
    db = DB(root / 'data/state.sqlite3')
    seed(db, err='Ambiguous posting outcome: ACK lost')
    rows = query_held(root)
    text = summary_text(rows)
    assert 'V2 Job Held: 1' in text
    assert 'CẦN QUẢN TRỊ VIÊN' in text or 'chờ đối soát' in text
    detail = detail_text(rows, 'artifact', 'a' * 64)
    assert 'V2_JOB' in detail and 'CẦN QUẢN TRỊ VIÊN' in detail
    assert 'FIX_V2JOB' not in detail
    data = csv_bytes(rows)
    assert b'v2_job' in data and b'NEEDS_ADMIN' in data and b'FIX_V2JOB' not in data
    db.close()


# --- watchdog state machine (mock only, timer NOT enabled) ------------------
def test_watchdog_detects_new_issue_recovery_gap_and_restart():
    snap = {'at': 1000.0, 'service': 'active', 'service_restarts': 3,
            'cpu_percent': 10.0, 'cpus': 4, 'memory_total': 100,
            'memory_available': 50, 'disk_free': 200, 'disk_reserve': 100,
            'disk_total': 300, 'disk_used': 100}
    prev = {'issues': [], 'cpu_streak': 0, 'last_check': 0, 'restarts': 2}
    state = evaluate(prev, snap)
    assert any('tự khởi động lại' in x for x in state['pending'])
    snap2 = dict(snap, service='inactive')
    state2 = evaluate(state, snap2)
    assert any('CẢNH BÁO SERVER' in x for x in state2['pending'])
    state3 = evaluate(state2, snap)
    assert any('ĐÃ PHỤC HỒI' in x for x in state3['pending'])
    gap = evaluate({'issues': [], 'last_check': 99.0}, snap)
    assert any('gián đoạn' in x for x in gap['pending'])
