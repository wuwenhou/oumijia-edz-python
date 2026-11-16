"""V15: unit checks for daily scheduling and safe incremental Telegram indexing."""
import asyncio
import csv
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from zoneinfo import ZoneInfo

from edzbot.db import DB
from edzbot.daily_scan import (enter_daily_if_drained, is_daily, due, scan_once,
                                next_local_two, NO_MORE, HIGH_KEY, LAST_KEY)
from edzbot.auto_alerts import next_alert
from edzbot.publish_guard import flood_seconds_from_message


class FakeTelegram:
    def __init__(self, msgs, *, fail=False):
        self.msgs = msgs
        self.fail = fail
        self.mins = []

    async def iter_messages(self, source, *, min_id, reverse):
        self.mins.append(min_id)
        for m in self.msgs:
            if m.id > min_id:
                yield m
        if self.fail:
            raise ConnectionError('Telegram temporarily unavailable')


class DailyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / 'catalog').mkdir()
        with (self.root / 'catalog' / 'telegram_edz_catalog.csv').open('w', newline='') as f:
            w = csv.writer(f)
            w.writerow(['message_id', 'filename', 'size_bytes'])
            w.writerow([100, 'old.edz', 10])
        self.db = DB(self.root / 'state.sqlite3')
        self.addCleanup(self.db.close)
        self.cfg = SimpleNamespace(auto_mode=True, data=self.root)
        self.now = datetime(2026, 11, 10, 9, 10, tzinfo=ZoneInfo('Asia/Ho_Chi_Minh')).timestamp()

    def enter(self):
        return enter_daily_if_drained(self.db, self.cfg, NO_MORE, now=self.now)

    def test_transition_only_after_drained(self):
        self.db.add_message(91, 'work.edz', 'edz', 20)
        self.assertFalse(self.enter())
        self.db.change_message(91, 'complete')
        self.db.put_artifact(sha='a'*64, file_name='x.edz', path='/unused', size=4,
            source_id=91, origin='direct', brand='A', part_number='1', validation='ok', state='ready')
        self.assertFalse(self.enter())
        self.db.update_artifact('a'*64, 'held', error='protected chat')
        self.assertTrue(self.enter())
        self.assertTrue(is_daily(self.db))
        self.assertEqual(self.db.value(HIGH_KEY), '100')
        self.assertFalse(due(self.db, now=self.now))
        self.assertEqual(datetime.fromtimestamp(next_local_two(self.now),
            ZoneInfo('Asia/Ho_Chi_Minh')).hour, 2)

    def test_nonempty_reason_disallows_transition(self):
        self.assertFalse(enter_daily_if_drained(self.db, self.cfg, 'Insufficient disk headroom', now=self.now))
        self.assertFalse(is_daily(self.db))

    def test_missing_catalog_fails_closed(self):
        (self.root / 'catalog' / 'telegram_edz_catalog.csv').unlink()
        with self.assertRaises(ValueError):
            self.enter()
        self.assertFalse(is_daily(self.db))

    def test_scan_adds_new_only_and_persists_marker(self):
        self.assertTrue(self.enter())
        msgs = [SimpleNamespace(id=95), SimpleNamespace(id=101), SimpleNamespace(id=102)]
        client = FakeTelegram(msgs)
        def enqueue(m):
            self.db.add_message(m.id, f'{m.id}.edz', 'edz', 4)
            self.db.change_message(m.id, 'staged')
        result = asyncio.run(scan_once(self.db, client, 'source', enqueue, now=self.now+86400))
        self.assertEqual(result['new_edz'], 2)
        self.assertEqual(self.db.value(HIGH_KEY), '102')
        self.assertEqual(client.mins, [80])
        self.assertEqual(self.db.value(LAST_KEY), str(self.now+86400))
        self.assertEqual(self.db.message(101)['state'], 'staged')
        result = asyncio.run(scan_once(self.db, client, 'source', enqueue, now=self.now+2*86400))
        self.assertEqual(result['new_edz'], 0)

    def test_scan_error_does_not_advance_marker(self):
        self.enter()
        client = FakeTelegram([SimpleNamespace(id=101)], fail=True)
        with self.assertRaises(ConnectionError):
            asyncio.run(scan_once(self.db, client, 'source',
                lambda msg: self.db.add_message(msg.id, 'x.edz', 'edz', 1), now=self.now+86400))
        self.assertEqual(self.db.value(HIGH_KEY), '100')

    def test_localization_and_skip_mass_floodwait(self):
        self.db.add_message(51, 'protected.edz', 'edz', 1)
        self.db.fail_message(51, 'You can\'t forward messages from a protected chat', retry=False)
        self.db.add_message(52, 'flood.edz', 'edz', 1)
        self.db.fail_message(52, 'FloodWait send: 743 seconds; destination reconciliation required', retry=False)
        key, body = next_alert(self.db)
        self.assertIn('LỖI NGUỒN', body)
        self.assertIn('bảo vệ nội dung', body)
        self.assertIn('#51', body)
        from edzbot.auto_alerts import mark_alert_sending
        self.assertTrue(mark_alert_sending(self.db, key, body))
        self.assertIsNone(next_alert(self.db))

    def test_parse_wrapped_send_media_wait(self):
        self.assertEqual(flood_seconds_from_message(
            'A wait of 482 seconds is required (caused by SendMediaRequest)'), 482)
        self.assertIsNone(flood_seconds_from_message('Please wait for 300 seconds on login'))


if __name__ == '__main__':
    unittest.main()
