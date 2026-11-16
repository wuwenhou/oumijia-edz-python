import hashlib
from pathlib import Path
import tempfile
import unittest
from edzbot.db import DB
from edzbot.work_cleanup import work_candidates, apply_work_cleanup


class WorkCleanupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.data = Path(self.temp.name).resolve()
        self.db = DB(self.data / 'state.sqlite3')
        self.source = self.data / 'incoming' / '272' / 'Reliance.edz'
        self.source.parent.mkdir(parents=True)
        self.source.write_bytes(b'source-data')
        digest = hashlib.sha256(self.source.read_bytes()).hexdigest()
        self.work = self.data / 'work' / ('split-' + digest[:20])
        self.work.mkdir(parents=True)
        self.payload = b'output EDZ'
        self.out = self.work / 'REL.RNK10.edz'
        self.out.write_bytes(self.payload)
        self.sha = hashlib.sha256(self.payload).hexdigest()
        self.library = self.data / 'library' / 'REL' / 'x' / 'REL.RNK10.edz'
        self.library.parent.mkdir(parents=True)
        self.library.write_bytes(self.payload)
        self.db.add_message(272, 'Reliance.edz', 'edz', self.source.stat().st_size)
        self.db.change_message(272, 'complete', local_path=str(self.source))
        self.db.put_artifact(sha=self.sha, file_name=self.out.name,
                             path=str(self.library), size=len(self.payload), source_id=272,
                             origin='generated', brand='REL', part_number='REL.RNK10',
                             validation='structural_unproven_import', state='review')

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def test_review_artifact_in_library_allows_cache_cleanup(self):
        self.assertEqual(1, len(work_candidates(self.db, self.data)))
        self.assertEqual((1, len(self.payload)), apply_work_cleanup(self.db, self.data))
        self.assertFalse(self.work.exists())
        self.assertTrue(self.library.exists())
        self.assertTrue(self.source.exists())
        self.assertEqual('review', self.db.execute('SELECT state FROM artifacts').fetchone()[0])

    def test_original_source_missing_blocks_cache_cleanup(self):
        self.source.unlink()
        self.assertEqual([], work_candidates(self.db, self.data))

    def test_incomplete_message_blocks_cache_cleanup(self):
        self.db.change_message(272, 'retry')
        self.assertEqual([], work_candidates(self.db, self.data))

    def test_library_tampered_blocks_cache_cleanup(self):
        self.library.write_bytes(b'bad')
        self.assertEqual([], work_candidates(self.db, self.data))

    def test_unknown_output_blocks_cache_cleanup(self):
        (self.work / 'unknown.edz').write_bytes(b'untracked')
        self.assertEqual([], work_candidates(self.db, self.data))

    def test_held_artifact_blocks_cache_cleanup(self):
        self.db.update_artifact(self.sha, 'held')
        self.assertEqual([], work_candidates(self.db, self.data))

    def test_symlink_in_cache_blocks_cleanup(self):
        outside = self.data / 'outside.txt'
        outside.write_text('keep')
        (self.work / 'link').symlink_to(outside)
        self.assertEqual([], work_candidates(self.db, self.data))
        self.assertTrue(outside.exists())

    def test_bad_folder_name_blocks_cleanup(self):
        other = self.work.parent / 'split-unsafe'
        self.work.rename(other)
        self.assertEqual([], work_candidates(self.db, self.data))

    def test_posted_after_local_cleanup_allows_work_cleanup(self):
        self.db.update_artifact(self.sha, 'posted', target_message=4)
        self.library.unlink()
        self.assertEqual(1, len(work_candidates(self.db, self.data)))

    def test_source_tampered_blocks_cache_cleanup(self):
        self.source.write_bytes(b'other-content')
        self.assertEqual([], work_candidates(self.db, self.data))

if __name__ == '__main__':
    unittest.main()
