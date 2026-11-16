"""Safety gates for opt-in single-file queue planner v6."""
import csv
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from edzbot.db import DB
from edzbot.plan_queue import MIB, GIB, select_next, enqueue_next


class PlanQueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.data = Path(self.tmp.name)
        (self.data / 'catalog').mkdir()
        self.db = DB(self.data / 'state.sqlite3')
        self.addCleanup(self.db.close)
        self.cfg = SimpleNamespace(data=self.data, backfill=False,
            max_group_bytes=60*GIB, reserve_bytes=40*GIB,
            plan_max_pending_artifacts=20, plan_max_single_bytes=50*MIB,
            plan_batch_id="pilot-unit-test", plan_batch_max_sources=3)
        self.write_catalog([{'filename':'IFSYS.edz','message_id':250,'size_bytes':MIB},
                            {'filename':'Another.edz','message_id':251,'size_bytes':2*MIB}])
        self.write_plan([{'name':'IFSYS.edz','message_ids':250,'kind':'single','parts':1,
                          'size_bytes':MIB,'decision':'candidate_single'},
                         {'name':'Another.edz','message_ids':251,'kind':'single','parts':1,
                          'size_bytes':2*MIB,'decision':'candidate_single'}])
        self.space = patch('edzbot.plan_queue.shutil.disk_usage',
                           return_value=SimpleNamespace(free=203*GIB))
        self.space.start()
        self.addCleanup(self.space.stop)

    def write_catalog(self, rows):
        with (self.data/'catalog'/'telegram_edz_catalog.csv').open('w', newline='') as out:
            w=csv.DictWriter(out, fieldnames=('filename','message_id','size_bytes'))
            w.writeheader(); w.writerows(rows)

    def write_plan(self, rows):
        with (self.data/'catalog'/'edz_processing_plan.csv').open('w', newline='') as out:
            w=csv.DictWriter(out, fieldnames=('name','message_ids','kind','parts','size_bytes','decision'))
            w.writeheader(); w.writerows(rows)

    def test_preview_no_sqlite_write(self):
        candidate, why = select_next(self.db, self.cfg)
        self.assertEqual(250, candidate.message_id)
        self.assertIsNone(self.db.message(250))

    def test_queue_one_then_pick_next(self):
        candidate, _ = enqueue_next(self.db, self.cfg)
        self.assertEqual(250, candidate.message_id)
        self.assertEqual('new', self.db.message(250)['state'])
        second, why = select_next(self.db, self.cfg)
        self.assertIsNone(second)
        self.assertIn('pending', why)
        self.db.change_message(250, 'complete')
        candidate, _ = enqueue_next(self.db, self.cfg)
        self.assertEqual(251, candidate.message_id)

    def test_review_backlog_blocks(self):
        for i in range(20):
            self.db.put_artifact(sha=f'{i:064x}', file_name=f'part{i}.edz',
                path='/unused', size=10, source_id=10, origin='generated',
                brand='TEST', part_number=str(i), validation='structural', state='review')
        candidate, why = select_next(self.db, self.cfg)
        self.assertIsNone(candidate)
        self.assertIn('20 unpublished', why)

    def test_never_queue_multipart_or_7z(self):
        self.write_plan([{'name':'archive.7z.001','message_ids':'250;251','kind':'multi',
                          'parts':2,'size_bytes':MIB,'decision':'review_multivolume'}])
        candidate, _ = select_next(self.db, self.cfg)
        self.assertIsNone(candidate)

    def test_duplicate_names_block(self):
        self.write_catalog([{'filename':'IFSYS.edz','message_id':250,'size_bytes':MIB},
                            {'filename':'IFSYS.edz','message_id':500,'size_bytes':MIB}])
        self.write_plan([{'name':'IFSYS.edz','message_ids':250,'kind':'single',
                          'parts':1,'size_bytes':MIB,'decision':'candidate_single'}])
        candidate, _ = select_next(self.db, self.cfg)
        self.assertIsNone(candidate)

    def test_mismatched_metadata_blocks(self):
        self.write_catalog([{'filename':'IFSYS.edz','message_id':250,'size_bytes':MIB+1}])
        self.write_plan([{'name':'IFSYS.edz','message_ids':250,'kind':'single',
                          'parts':1,'size_bytes':MIB,'decision':'candidate_single'}])
        candidate, _ = select_next(self.db, self.cfg)
        self.assertIsNone(candidate)

    def test_backfill_blocks(self):
        self.cfg.backfill = True
        candidate, why = select_next(self.db, self.cfg)
        self.assertIsNone(candidate)
        self.assertIn('BACKFILL', why)

    def test_no_disk_headroom(self):
        self.space.stop()
        with patch('edzbot.plan_queue.shutil.disk_usage',
                   return_value=SimpleNamespace(free=40*GIB)):
            candidate, why = select_next(self.db, self.cfg)
        self.assertIsNone(candidate)
        self.assertIn('Insufficient', why)

    def test_source_already_queued_is_skipped(self):
        self.db.add_message(250, 'IFSYS.edz', 'edz', MIB)
        self.db.change_message(250, 'complete')
        chosen, _ = select_next(self.db, self.cfg)
        self.assertEqual(251, chosen.message_id)

    def test_missing_plan_is_error_not_queued(self):
        (self.data/'catalog'/'edz_processing_plan.csv').unlink()
        with self.assertRaises(ValueError):
            enqueue_next(self.db, self.cfg)
        self.assertEqual(0, self.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0])

    def test_over_single_limit_blocks(self):
        self.cfg.plan_max_single_bytes = 1024
        candidate, _ = select_next(self.db, self.cfg)
        self.assertIsNone(candidate)


if __name__=='__main__':
    unittest.main()
