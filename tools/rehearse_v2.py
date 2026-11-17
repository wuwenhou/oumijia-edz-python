"""STAGING-ONLY clone rehearsal: crash/restart durability with a file-backed DB.

Simulates: enqueue -> posting (Telegram accepted, ACK lost) -> process restart
-> recovery holds the ambiguous job -> explicit reconcile ACK -> no re-upload.
Run: .venv/bin/python tools/rehearse_v2.py
"""
from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from edzbot.db import DB
from edzbot.upload_engine import UploadEngine, EngineConfig


class NoopClient:
    async def send_file(self, *a, **k):
        raise AssertionError('rehearsal must not send anything')


def seed(db, files):
    for p, sha in files:
        db.execute("INSERT INTO artifacts(sha256,file_name,path,size_bytes,source_message,"
                   "origin,brand,part_number,validation,state,created_at,updated_at)"
                   " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                   (sha, p.name, str(p), p.stat().st_size, 0, 'direct', '', '', 'ok',
                    'ready', time.time(), time.time()))


def main():
    with tempfile.TemporaryDirectory(prefix='v2rehearse-') as d:
        dbpath = Path(d) / 'state.sqlite3'
        files = []
        for i in range(3):
            p = Path(d) / f'REH.{i}.edz'
            p.write_bytes(bytes([i]) * 128)
            files.append((p, hashlib.sha256(p.read_bytes()).hexdigest()))

        # --- process 1: enqueue all, claim two as 'posting' (simulated crash) --
        db = DB(dbpath)
        seed(db, files)
        e = UploadEngine(db, NoopClient(), 'TARGET', cfg=EngineConfig())
        for p, sha in files:
            assert e.enqueue(p.name, sha, str(p), p.stat().st_size)
        claimed = {e.claim_next()['normalized_name'] for _ in range(2)}
        db.close()

        # --- process 2: restart, recover ambiguous, reconcile one by hand -----
        db = DB(dbpath)
        e2 = UploadEngine(db, NoopClient(), 'TARGET', cfg=EngineConfig())
        recovered = e2.recover_posting()
        held_before = db.execute("SELECT COUNT(*) FROM upload_jobs WHERE state='held'").fetchone()[0]
        # Operator reconciled ONE held job against Telegram (found message #777).
        one = dict(db.execute("SELECT * FROM upload_jobs WHERE state='held' LIMIT 1").fetchone())
        e2.ack(one, 777)
        held_after = db.execute("SELECT COUNT(*) FROM upload_jobs WHERE state='held'").fetchone()[0]
        queued = db.execute("SELECT COUNT(*) FROM upload_jobs WHERE state='queued'").fetchone()[0]
        posted = db.execute("SELECT COUNT(*) FROM upload_jobs WHERE state='posted'").fetchone()[0]
        requeued_ambiguous = [r[0] for r in db.execute(
            "SELECT normalized_name FROM upload_jobs WHERE state='queued'")
            if r[0] in claimed]
        dup = e2.enqueue(files[0][0].name, 'f' * 64, '/tmp/x', 128)  # same name must not re-enqueue
        result = {
            'recovered_posting_to_held': recovered,
            'held_after_restart': held_before,
            'held_after_manual_reconcile': held_after,
            'queued_never_sent': queued, 'posted': posted,
            'same_name_re_enqueue_blocked': not dup,
            'ambiguous_jobs_auto_resent': requeued_ambiguous,
            'no_auto_resend': not requeued_ambiguous,
        }
        db.close()
        print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
