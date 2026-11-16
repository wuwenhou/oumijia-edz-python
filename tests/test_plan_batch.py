"""v7: permanent one-source-per-batch cap even when an EDZ expands to many parts."""
import tempfile
from pathlib import Path
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from edzbot.db import DB
from edzbot.plan_queue import enqueue_next, select_next, batch_usage, GIB, MIB
import test_plan_queue as fixtures


class BatchSafetyTests(unittest.TestCase):
    def setUp(self):
        fixtures.PlanQueueTests.setUp(self)
        self.cfg.plan_batch_max_sources = 1
    write_catalog = fixtures.PlanQueueTests.write_catalog
    write_plan = fixtures.PlanQueueTests.write_plan

    def test_single_source_per_batch_even_if_complete(self):
        first, _ = enqueue_next(self.db, self.cfg)
        self.assertEqual(250, first.message_id)
        self.db.change_message(250, 'complete')
        selected, why = enqueue_next(self.db, self.cfg)
        self.assertIsNone(selected)
        self.assertIn('1/1', why)
        self.assertIsNone(self.db.message(251))

    def test_quota_survives_database_restart(self):
        enqueue_next(self.db, self.cfg)
        self.db.close()
        self.db = DB(self.data / 'state.sqlite3')
        self.addCleanup(self.db.close)
        self.db.change_message(250, 'complete')
        self.assertEqual((1, 1), batch_usage(self.db, self.cfg))
        chosen, why = enqueue_next(self.db, self.cfg)
        self.assertIsNone(chosen)
        self.assertIn('1/1', why)

    def test_many_review_parts_do_not_reopen_batch(self):
        enqueue_next(self.db, self.cfg)
        self.db.change_message(250, 'complete')
        for n in range(33):
            self.db.put_artifact(sha=f'{n:064x}', file_name=f'P{n}.edz', path='/nonexistent',
                size=999, source_id=250, origin='generated', brand='TEST', part_number=str(n),
                validation='structural', state='review')
        # Even if operator raises backlog threshold, batch cap remains independent.
        self.cfg.plan_max_pending_artifacts = 100
        chosen, why = select_next(self.db, self.cfg)
        self.assertIsNone(chosen)
        self.assertIn('1/1', why)

    def test_missing_id_fail_closed(self):
        self.cfg.plan_batch_id = ''
        result, reason = enqueue_next(self.db, self.cfg)
        self.assertIsNone(result)
        self.assertIn('PLAN_BATCH_ID', reason)
        self.assertEqual(0, self.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0])

    def test_new_explicit_batch_can_take_next_source(self):
        enqueue_next(self.db, self.cfg)
        self.db.change_message(250, 'complete')
        self.cfg.plan_batch_id = 'pilot-next-explicit'
        selected, why = enqueue_next(self.db, self.cfg)
        self.assertEqual(251, selected.message_id)
        self.assertEqual((1, 1), batch_usage(self.db, self.cfg))
        old = self.cfg.plan_batch_id
        self.cfg.plan_batch_id = 'pilot-unit-test'
        self.assertEqual((1, 1), batch_usage(self.db, self.cfg))
        self.cfg.plan_batch_id = old

    def test_duplicate_source_wont_consume_quota(self):
        self.db.add_message(250, 'IFSYS.edz', 'edz', MIB)
        self.db.change_message(250, 'complete')
        chosen, _ = enqueue_next(self.db, self.cfg)
        self.assertEqual(251, chosen.message_id)
        self.assertEqual((1, 1), batch_usage(self.db, self.cfg))

    def test_batch_state_no_side_effect_of_preview(self):
        chosen, why = select_next(self.db, self.cfg)
        self.assertIsNotNone(chosen)
        self.assertEqual((0,1), batch_usage(self.db, self.cfg))


class BatchConfigTests(unittest.TestCase):
    def test_safe_defaults(self):
        from edzbot.config import Config
        with patch('edzbot.config.load_dotenv', return_value=None), patch.dict('os.environ', {}, clear=True):
            cfg=Config.load(require_telegram=False)
            self.assertEqual('',cfg.plan_batch_id)
            self.assertEqual(1,cfg.plan_batch_max_sources)

    def test_invalid_batch_id(self):
        from edzbot.config import Config
        with patch('edzbot.config.load_dotenv', return_value=None), patch.dict('os.environ', {'PLAN_BATCH_ID':'bad id with spaces'}, clear=True):
            with self.assertRaises(ValueError): Config.load(require_telegram=False)

    def test_invalid_batch_max(self):
        from edzbot.config import Config
        with patch('edzbot.config.load_dotenv', return_value=None), patch.dict('os.environ', {'PLAN_BATCH_MAX_SOURCES':'0'}, clear=True):
            with self.assertRaises(ValueError): Config.load(require_telegram=False)
