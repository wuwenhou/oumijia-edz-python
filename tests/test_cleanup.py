import hashlib
from pathlib import Path
import tempfile
import unittest
from edzbot.db import DB
from edzbot.cleanup import candidates, apply_cleanup, apply_auto_cleanup


class CleanupTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name).resolve()
        self.db = DB(self.root / 'state.sqlite3')
        self.library = self.root / 'library' / 'REL'
        self.library.mkdir(parents=True)
        self.incoming = self.root / 'incoming' / '272'
        self.incoming.mkdir(parents=True)
        self.payload = b'example edz test contents'
        self.sha = hashlib.sha256(self.payload).hexdigest()
        self.out = self.library / 'REL.RNK10.edz'
        self.out.write_bytes(self.payload)
        self.src = self.incoming / 'Reliance.edz'
        self.src.write_bytes(b'SRC')
        self.db.add_message(272, 'Reliance.edz', 'edz', 3)
        self.db.change_message(272, 'complete', local_path=str(self.src))
        self.db.put_artifact(sha=self.sha, file_name=self.out.name,
            path=str(self.out), size=len(self.payload), source_id=272,
            origin='generated', brand='REL', part_number='REL.RNK10',
            validation='structural_unproven_import', state='review')

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def test_review_is_preserved(self):
        self.assertEqual([], candidates(self.db, self.root))

    def test_ready_not_deleted(self):
        self.db.update_artifact(self.sha, 'ready')
        self.assertEqual([], candidates(self.db, self.root))

    def test_posted_requires_target_message(self):
        self.db.update_artifact(self.sha, 'posted')
        self.assertEqual([], candidates(self.db, self.root))

    def test_posted_selects_two_files_and_records(self):
        self.db.update_artifact(self.sha, 'posted', target_message=4)
        self.assertEqual(2, len(candidates(self.db, self.root)))
        count, freed = apply_cleanup(self.db, self.root)
        self.assertEqual(2, count)
        self.assertEqual(len(self.payload) + 3, freed)
        self.assertFalse(self.out.exists())
        self.assertFalse(self.src.exists())
        self.assertEqual('posted', self.db.execute('SELECT state FROM artifacts').fetchone()[0])
        self.assertEqual(2, self.db.execute('SELECT count(*) FROM cleanup_events').fetchone()[0])
        self.assertEqual((0, 0), apply_cleanup(self.db, self.root))

    def test_auto_cleanup_disabled_keeps_everything(self):
        self.db.update_artifact(self.sha, 'posted', target_message=4)
        self.assertEqual((0, 0), apply_auto_cleanup(self.db, self.root,
                                                     enabled=False, dry_run=False))
        self.assertTrue(self.out.exists())
        self.assertTrue(self.src.exists())

    def test_auto_cleanup_respects_dry_run(self):
        self.db.update_artifact(self.sha, 'posted', target_message=4)
        self.assertEqual((0, 0), apply_auto_cleanup(self.db, self.root,
                                                     enabled=True, dry_run=True))
        self.assertTrue(self.out.exists())
        self.assertTrue(self.src.exists())

    def test_auto_cleanup_only_posted_library_not_input(self):
        self.db.update_artifact(self.sha, 'posted', target_message=4)
        count, freed = apply_auto_cleanup(self.db, self.root,
                                           enabled=True, dry_run=False)
        self.assertEqual((1, len(self.payload)), (count, freed))
        self.assertFalse(self.out.exists())
        self.assertTrue(self.src.exists())
        self.assertEqual((0, 0), apply_auto_cleanup(self.db, self.root,
                                                     enabled=True, dry_run=False))

    def test_auto_cleanup_preserves_review_and_input(self):
        self.assertEqual((0, 0), apply_auto_cleanup(self.db, self.root,
                                                     enabled=True, dry_run=False))
        self.assertTrue(self.out.exists())
        self.assertTrue(self.src.exists())

    def test_partial_review_blocks_input_cleanup(self):
        sha2 = hashlib.sha256(b'other').hexdigest()
        out2 = self.library / 'other.edz'
        out2.write_bytes(b'other')
        self.db.put_artifact(sha=sha2, file_name=out2.name,
            path=str(out2), size=5, source_id=272, origin='generated',
            brand='REL', part_number='other', validation='test', state='review')
        self.db.update_artifact(self.sha, 'posted', target_message=4)
        self.assertEqual(['posted_library'], [c.kind for c in candidates(self.db, self.root)])
        self.assertTrue(self.src.exists())

    def test_mismatch_sha_not_deleted(self):
        self.db.update_artifact(self.sha, 'posted', target_message=4)
        self.out.write_bytes(b'changed bytes but same-ish')
        self.assertEqual(['complete_input'], [c.kind for c in candidates(self.db, self.root)])

    def test_symlink_not_deleted(self):
        self.db.update_artifact(self.sha, 'posted', target_message=4)
        original = self.out.read_bytes()
        external = self.root / 'outside.edz'
        external.write_bytes(original)
        self.out.unlink()
        self.out.symlink_to(external)
        self.assertEqual(['complete_input'], [c.kind for c in candidates(self.db, self.root)])

    def test_volume_not_deleted(self):
        self.db.add_message(273, 'ABB.7z.001', 'volume', 3, 'ABB.7z', 1)
        self.db.change_message(273, 'complete', local_path=str(self.src))
        self.db.update_artifact(self.sha, 'posted', target_message=4)
        self.assertEqual(2, len(candidates(self.db, self.root)))


if __name__ == '__main__':
    unittest.main()
