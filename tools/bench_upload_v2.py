"""STAGING-ONLY benchmark: legacy sequential publisher vs Upload Engine V2.

No Telegram, no production DB. Mock client simulates a fixed ACK latency and
file hashing is real. Reports throughput for concurrency 1 and 2 so the
adaptive default is chosen from measured data, not assumption.

Run: .venv/bin/python tools/bench_upload_v2.py
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import resource
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from edzbot.db import DB
from edzbot.upload_engine import UploadEngine, EngineConfig

N_FILES = 60
FILE_MIB = 1
ACK_LATENCY = 0.08          # simulated Telegram ACK latency (seconds)


class MockClient:
    def __init__(self):
        self._id = 5000

    async def send_file(self, target, media, **kwargs):
        await asyncio.sleep(ACK_LATENCY)
        self._id += 1
        return type('S', (), {'id': self._id})()


def make_files(tmp: Path):
    out = []
    for i in range(N_FILES):
        # Unique bytes -> unique SHA (mirrors real distinct EDZ artifacts).
        payload = (b'\x5a' * (FILE_MIB * 1024 * 1024 - 16)) + f'{i:016d}'.encode()
        p = tmp / f'EDZ.BENCH.{i:04d}.edz'
        p.write_bytes(payload)
        out.append((p, hashlib.sha256(payload).hexdigest()))
    return out


def seed_artifacts(db, files):
    for p, sha in files:
        db.execute("INSERT OR IGNORE INTO artifacts(sha256,file_name,path,size_bytes,"
                   "source_message,origin,brand,part_number,validation,state,created_at,updated_at)"
                   " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                   (sha, p.name, str(p), p.stat().st_size, 0, 'direct', '', '', 'ok',
                    'ready', time.time(), time.time()))


async def run_legacy(db, files):
    """Emulates the current publish_one send path: sequential, one publisher."""
    client = MockClient()
    from edzbot.archive import sha256_file
    t0 = time.monotonic()
    for p, sha in files:
        got = await asyncio.to_thread(sha256_file, p)
        assert got == sha
        await client.send_file('TARGET', p, caption='x', force_document=True)
        db.execute("UPDATE artifacts SET state='posted' WHERE sha256=?", (sha,))
    return time.monotonic() - t0


async def run_v2(db, files, concurrency):
    client = MockClient()
    eng = UploadEngine(db, client, 'TARGET',
                       cfg=EngineConfig(concurrency=concurrency, max_concurrency=concurrency))
    total = 0
    t0 = time.monotonic()
    while total < N_FILES:
        made = await eng.publish_batch()
        if made == 0:
            raise RuntimeError(f'V2 stalled at {total}/{N_FILES} posted')
        total += made
    return time.monotonic() - t0, eng


def main():
    with tempfile.TemporaryDirectory(prefix='v2bench-') as d:
        tmp = Path(d)
        files = make_files(tmp)
        results = {}
        for label, fn in (
            ('legacy_sequential_c1', lambda db: run_legacy(db, files)),
            ('v2_concurrency_1', lambda db: run_v2(db, files, 1)),
            ('v2_concurrency_2', lambda db: run_v2(db, files, 2)),
        ):
            db = DB(tmp / f'{label}.sqlite3')
            seed_artifacts(db, files)
            out = asyncio.run(fn(db))
            elapsed = out[0] if isinstance(out, tuple) else out
            total_mib = N_FILES * FILE_MIB
            results[label] = {
                'files': N_FILES,
                'mib': total_mib,
                'elapsed_s': round(elapsed, 3),
                'mib_per_s': round(total_mib / elapsed, 2),
                'files_per_min': round(N_FILES * 60 / elapsed, 1),
            }
            db.close()
        results['bytes_total'] = N_FILES * FILE_MIB * 1024 * 1024
        results['ack_latency_s'] = ACK_LATENCY
        results['max_rss_mib'] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)
        print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()
