"""No Telegram connection: deterministic FloodWait recovery and idempotency tests."""
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

from edzbot.db import DB
from edzbot.publish_guard import (flood_error, held_flood_rows,
                                  cooldown_remaining, set_cooldown,
                                  next_send_remaining, schedule_next_send,
                                  reconcile_flood_held)


OLD = 'Ambiguous posting outcome: A wait of 998 seconds is required (caused by SendMediaRequest)'
NEW = 'FloodWait send: 743 seconds; destination reconciliation required'
SERIOUS = 'Ambiguous posting outcome: You can\'t forward messages from a protected chat (caused by SendMediaRequest)'


class FakeClient:
    def __init__(self, msgs=None, fail=False):
        self.msgs = list(msgs or [])
        self.fail = fail

    async def iter_messages(self, target):
        for m in self.msgs:
            yield m
        if self.fail:
            raise RuntimeError('API disconnected')


def message(sha, n, size, *, media=True):
    return SimpleNamespace(id=n, document=SimpleNamespace(size=size) if media else None,
                           message=f'EPLAN EDZ\nSHA-256: {sha}\n#EDZ')


class GuardTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.db = DB(self.root / 'db.sqlite3')
        self.addCleanup(self.db.close)
        self.db.add_message(42, 'batch.7z', 'archive', 9)
        self.db.change_message(42, 'complete')

    def artifact(self, content, *, error=OLD, filename='x.edz', src=42):
        import hashlib
        sha = hashlib.sha256(content).hexdigest()
        path = self.root / filename
        path.write_bytes(content)
        self.db.put_artifact(sha=sha, file_name=filename, path=str(path), size=len(content),
                             source_id=src, origin='generated', brand='X', part_number='X',
                             validation='verified', state='held')
        self.db.update_artifact(sha, 'held', error=error)
        return sha

    def row(self, sha):
        return self.db.execute('SELECT * FROM artifacts WHERE sha256=?', (sha,)).fetchone()

    def test_classify(self):
        self.assertTrue(flood_error(OLD))
        self.assertTrue(flood_error(NEW))
        self.assertFalse(flood_error(SERIOUS))
        self.assertFalse(flood_error('Previous send interrupted'))

    def test_cooldown_persisted_and_extended(self):
        self.assertEqual(cooldown_remaining(self.db, now=100), 0)
        first = set_cooldown(self.db, 998, now=100)
        self.assertEqual(first, 1158)
        self.assertEqual(cooldown_remaining(self.db, now=200), 958)
        self.assertEqual(set_cooldown(self.db, 10, now=200), first)
        self.assertEqual(next_send_remaining(self.db, now=200), 958)

    def test_history_match_marks_posted(self):
        sha = self.artifact(b'a')
        out = asyncio.run(reconcile_flood_held(self.db, FakeClient([message(sha, 2001, 1)]), -1))
        self.assertEqual((out['posted'], out['ready']), (1, 0))
        self.assertEqual(self.row(sha)['target_message'], 2001)
        self.assertEqual(self.row(sha)['state'], 'posted')

    def test_no_match_releases_verified_source(self):
        sha = self.artifact(b'b')
        out = asyncio.run(reconcile_flood_held(self.db, FakeClient([]), -1))
        self.assertEqual(out['ready'], 1)
        self.assertEqual(self.row(sha)['state'], 'ready')
        self.assertEqual(self.row(sha)['target_message'], None)

    def test_serious_held_untouched(self):
        sha = self.artifact(b'c', error=SERIOUS)
        self.assertEqual(len(held_flood_rows(self.db)), 0)
        asyncio.run(reconcile_flood_held(self.db, FakeClient([]), -1))
        self.assertEqual(self.row(sha)['state'], 'held')

    def test_failure_no_mutation(self):
        sha = self.artifact(b'd')
        with self.assertRaises(RuntimeError):
            asyncio.run(reconcile_flood_held(self.db, FakeClient([message(sha, 42, 1)], fail=True), -1))
        self.assertEqual(self.row(sha)['state'], 'held')

    def test_history_truncation_no_mutation(self):
        sha = self.artifact(b'e')
        msgs = [message(sha, n, 1) for n in range(4)]
        with self.assertRaises(RuntimeError):
            asyncio.run(reconcile_flood_held(self.db, FakeClient(msgs), -1, scan_limit=2))
        self.assertEqual(self.row(sha)['state'], 'held')

    def test_duplicate_sha_no_mutation(self):
        sha = self.artifact(b'f')
        with self.assertRaises(RuntimeError):
            asyncio.run(reconcile_flood_held(self.db, FakeClient([message(sha, 1, 1), message(sha, 2, 1)]), -1))
        self.assertEqual(self.row(sha)['state'], 'held')

    def test_wrong_size_not_match(self):
        sha = self.artifact(b'g')
        out = asyncio.run(reconcile_flood_held(self.db, FakeClient([message(sha, 1, 9999)]), -1))
        self.assertEqual(out['posted'], 0)
        self.assertEqual(self.row(sha)['state'], 'ready')

    def test_missing_local_remains_held(self):
        sha = self.artifact(b'h')
        (self.root / 'x.edz').unlink()
        out = asyncio.run(reconcile_flood_held(self.db, FakeClient([]), -1))
        self.assertEqual(out['left_held'], 1)
        self.assertEqual(self.row(sha)['state'], 'held')

    def test_incomplete_source_remains_held(self):
        sha = self.artifact(b'i')
        self.db.change_message(42, 'held')
        asyncio.run(reconcile_flood_held(self.db, FakeClient([]), -1))
        self.assertEqual(self.row(sha)['state'], 'held')

    def test_idempotent(self):
        sha = self.artifact(b'j')
        asyncio.run(reconcile_flood_held(self.db, FakeClient([]), -1))
        out = asyncio.run(reconcile_flood_held(self.db, FakeClient([]), -1))
        self.assertEqual(out['ready'], 0)
        self.assertEqual(self.row(sha)['state'], 'ready')


if __name__ == '__main__':
    unittest.main()
