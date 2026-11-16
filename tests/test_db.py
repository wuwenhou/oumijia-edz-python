import tempfile
import unittest
from pathlib import Path
from edzbot.db import DB


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = DB(Path(self.tmp.name) / 'state.sqlite3')

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def test_dedup_messages_and_recovery(self):
        self.db.add_message(123, 'Theben.7z', 'archive', 8)
        self.db.add_message(123, 'Theben.7z', 'archive', 8)
        self.assertEqual(self.db.execute('select count(*) from messages').fetchone()[0], 1)
        self.db.change_message(123, 'downloading')
        self.db.prepare_recovery()
        self.assertEqual(self.db.message(123)['state'], 'retry')

    def test_generated_held_and_explicit_approval(self):
        kw = dict(sha='ab'*32, file_name='A.edz', path='/tmp/A.edz', size=100,
                  source_id=123, origin='generated', brand='Test', part_number='TEST.001',
                  validation='structural_unproven_import', state='review')
        self.assertTrue(self.db.put_artifact(**kw))
        self.assertFalse(self.db.put_artifact(**kw))
        self.assertIsNone(self.db.next_artifact())
        self.assertIsNotNone(self.db.next_artifact(allow_unverified=True))
        self.db.update_artifact(kw['sha'], 'ready')
        self.assertEqual(self.db.next_artifact()['file_name'], 'A.edz')
        self.db.update_artifact(kw['sha'], 'posting')
        self.db.prepare_recovery()
        self.assertEqual(self.db.execute('SELECT state FROM artifacts').fetchone()[0], 'held')


if __name__ == '__main__': unittest.main()
