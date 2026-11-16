"""Tests for the worker's optional finished-work cleanup hook."""
import asyncio
import hashlib
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace

from edzbot.bot import Service
from edzbot.db import DB


class AutoWorkCleanupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.data = Path(self.temp.name).resolve()
        self.db = DB(self.data / 'state.sqlite3')
        self.addCleanup(self.db.close)
        self.source = self.data / 'incoming' / '272' / 'Reliance.edz'
        self.source.parent.mkdir(parents=True)
        self.source.write_bytes(b'source-data')
        digest = hashlib.sha256(self.source.read_bytes()).hexdigest()
        self.work = self.data / 'work' / ('split-' + digest[:20])
        self.work.mkdir(parents=True)
        content = b'output EDZ'
        (self.work / 'REL.RNK10.edz').write_bytes(content)
        sha = hashlib.sha256(content).hexdigest()
        library = self.data / 'library' / 'REL' / 'x' / 'REL.RNK10.edz'
        library.parent.mkdir(parents=True)
        library.write_bytes(content)
        self.db.add_message(272, 'Reliance.edz', 'edz', self.source.stat().st_size)
        self.db.change_message(272, 'complete', local_path=str(self.source))
        self.db.put_artifact(sha=sha, file_name='REL.RNK10.edz', path=str(library),
                             size=len(content), source_id=272, origin='generated',
                             brand='REL', part_number='REL.RNK10',
                             validation='structural_unproven_import', state='review')
        self.library = library

    def _service(self, enabled):
        service = Service.__new__(Service)  # no Telegram client needed
        service.cfg = SimpleNamespace(data=self.data, auto_work_cleanup=enabled)
        return service

    def test_disabled_does_not_delete_work(self):
        self.assertEqual((0, 0), asyncio.run(self._service(False).auto_work_cleanup_once()))
        self.assertTrue(self.work.exists())
        self.assertTrue(self.library.exists())

    def test_enabled_removes_verified_work_but_keeps_review(self):
        self.assertEqual((1, len(b'output EDZ')),
                         asyncio.run(self._service(True).auto_work_cleanup_once()))
        self.assertFalse(self.work.exists())
        self.assertTrue(self.library.exists())
        self.assertTrue(self.source.exists())
        self.assertEqual('review', self.db.execute('SELECT state FROM artifacts').fetchone()[0])


if __name__ == '__main__':
    unittest.main()
