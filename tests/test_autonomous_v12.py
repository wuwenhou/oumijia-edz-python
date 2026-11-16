"""v12 opt-in automatic intake, structural publication gate, cleanup and alerts."""
from dataclasses import replace
from pathlib import Path
import csv
import hashlib
import os
import tempfile
import unittest
from unittest.mock import patch

from edzbot.config import Config
from edzbot.db import DB
from edzbot.autonomous import select_next_auto, enqueue_next_auto, clean_safe_auto_sources
from edzbot.auto_alerts import next_alert, mark_alert_sending, mark_alert_sent, record_issue


class AutoBase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.data = Path(self.temp.name)
        (self.data / 'catalog').mkdir()
        (self.data / 'incoming').mkdir()
        (self.data / 'work').mkdir()
        (self.data / 'volumes').mkdir()
        (self.data / 'library').mkdir()
        self.db = DB(self.data / 'state.sqlite3')
        self.addCleanup(self.db.close)
        with patch('edzbot.config.load_dotenv', return_value=None), patch.dict(os.environ, {}, clear=True):
            config = Config.load(require_telegram=False)
        self.cfg = replace(config, data=self.data, auto_mode=True, dry_run=False,
                           auto_cleanup=True, auto_work_cleanup=True,
                           backfill=False, auto_plan_queue=False, allow_unverified=False,
                           reserve_bytes=512, max_group_bytes=20000000)

    def write_csv(self, name, fields, rows):
        with (self.data / 'catalog' / name).open('w', newline='') as f:
            out = csv.DictWriter(f, fieldnames=fields)
            out.writeheader()
            out.writerows(rows)

    def plan(self):
        self.write_csv('telegram_edz_catalog.csv', ['message_id','filename','size_bytes'], [
            dict(message_id=250, filename='A.edz', size_bytes=2000),
            dict(message_id=251, filename='B.7z', size_bytes=5000),
            dict(message_id=300, filename='X.7z.001', size_bytes=3500),
            dict(message_id=301, filename='X.7z.002', size_bytes=3400),
        ])
        self.write_csv('edz_processing_plan.csv',
                       ['name','kind','message_ids','size_bytes','decision','parts'], [
            dict(name='A.edz',kind='single',message_ids='250',size_bytes=2000,decision='candidate_single',parts=1),
            dict(name='B.7z',kind='single',message_ids='251',size_bytes=5000,decision='candidate_single',parts=1),
            dict(name='X.7z',kind='multi',message_ids='300;301',size_bytes=6900,decision='review_multivolume',parts=2),
        ])


class AutoIntakeTests(AutoBase):
    def test_auto_config_gated(self):
        from unittest.mock import patch
        combinations = [
            {'AUTO_MODE':'true','DRY_RUN':'true','BACKFILL_ON_START':'false','AUTO_CLEANUP':'true','AUTO_WORK_CLEANUP':'true'},
            {'AUTO_MODE':'true','DRY_RUN':'false','BACKFILL_ON_START':'true','AUTO_CLEANUP':'true','AUTO_WORK_CLEANUP':'true'},
            {'AUTO_MODE':'true','DRY_RUN':'false','BACKFILL_ON_START':'false','AUTO_CLEANUP':'true','AUTO_WORK_CLEANUP':'false'},
        ]
        for combination in combinations:
            with patch('edzbot.config.load_dotenv', return_value=None), patch.dict(os.environ, combination, clear=True):
                with self.assertRaises(ValueError):
                    Config.load(require_telegram=False)
        allowed = dict(AUTO_MODE='true',DRY_RUN='false',BACKFILL_ON_START='false',
                       AUTO_CLEANUP='true',AUTO_WORK_CLEANUP='true',ERROR_CHAT='me')
        with patch('edzbot.config.load_dotenv', return_value=None), patch.dict(os.environ, allowed, clear=True):
            c = Config.load(require_telegram=False)
        self.assertTrue(c.auto_mode)

    def test_auto_select_single_first_and_wait_on_published(self):
        self.plan()
        chosen, msg = enqueue_next_auto(self.db, self.cfg)
        self.assertEqual('A.edz', chosen.members[0].file_name)
        self.assertEqual('new',self.db.message(250)['state'])
        self.assertIsNone(select_next_auto(self.db,self.cfg)[0])
        self.db.change_message(250,'complete')
        self.db.put_artifact(sha='1'*64, file_name='A.edz', path='/none',size=100,
                source_id=250,origin='direct',brand='A',part_number='1',validation='auto',state='ready')
        self.assertIn('unpublished',select_next_auto(self.db,self.cfg)[1])
        self.db.update_artifact('1'*64,'posted',target_message=15)
        next_one, _ = enqueue_next_auto(self.db,self.cfg)
        self.assertEqual('B.7z', next_one.members[0].file_name)

    def test_multipart_atomic_and_restarts_not_repeat(self):
        self.plan()
        for mid, name, kind, size in ((250,'A.edz','edz',2000),(251,'B.7z','archive',5000)):
            self.db.add_message(mid,name,kind,size)
            self.db.change_message(mid,'complete')
        chosen, _ = enqueue_next_auto(self.db,self.cfg)
        self.assertTrue(chosen.multipart)
        self.assertEqual((300,301),tuple(x.message_id for x in chosen.members))
        self.assertEqual('new',self.db.message(300)['state'])
        self.assertEqual('new',self.db.message(301)['state'])
        self.db.close()
        self.db = DB(self.data/'state.sqlite3')
        self.assertIsNone(select_next_auto(self.db,self.cfg)[0])

    def test_missing_multipart_segment_skipped(self):
        self.plan()
        plan_path=self.data/'catalog'/'edz_processing_plan.csv'
        text=plan_path.read_text().replace('300;301','300;999')
        plan_path.write_text(text)
        for mid,name,kind,size in ((250,'A.edz','edz',2000),(251,'B.7z','archive',5000)):
            self.db.add_message(mid,name,kind,size)
            self.db.change_message(mid,'complete')
        self.assertIsNone(select_next_auto(self.db,self.cfg)[0])

    def test_headroom_prevents_intake(self):
        self.plan()
        self.cfg=replace(self.cfg,reserve_bytes=10**16)
        chosen,msg=select_next_auto(self.db,self.cfg)
        self.assertIsNone(chosen)
        self.assertIn('headroom',msg)

    def test_staged_live_single(self):
        self.plan()
        self.db.add_message(800,'LIVE.edz','edz',1100)
        self.db.change_message(800,'staged')
        choice,_=enqueue_next_auto(self.db,self.cfg)
        self.assertEqual('A.edz',choice.members[0].file_name)  # catalog first
        self.db.change_message(250,'complete')
        self.db.add_message(251,'B.7z','archive',5000)
        self.db.change_message(251,'complete')
        for id,name,idx in ((300,'X.7z.001',1),(301,'X.7z.002',2)):
            self.db.add_message(id,name,'volume',3500 if idx==1 else 3400,'x.7z',idx)
            self.db.change_message(id,'complete')
        choice,_=enqueue_next_auto(self.db,self.cfg)
        self.assertEqual(800,choice.members[0].message_id)
        self.assertEqual('new',self.db.message(800)['state'])

    def test_incomplete_parent_not_published(self):
        self.db.add_message(250,'A.edz','edz',2000)
        self.db.change_message(250,'retry')
        self.db.put_artifact(sha='a'*64, file_name='part.edz',path='/none',size=20,
            source_id=250,origin='generated',brand='A',part_number='1',validation='auto',state='ready')
        self.assertIsNone(self.db.next_artifact(require_complete_source=True))
        self.db.change_message(250,'complete')
        self.assertIsNotNone(self.db.next_artifact(require_complete_source=True))


class AlertTests(AutoBase):
    def test_held_source_message_dedup(self):
        self.db.add_message(123,'broken.edz','edz',10000)
        self.db.fail_message(123,'invalid structure',retry=False)
        key,body=next_alert(self.db)
        self.assertIn('#123',body)
        self.assertTrue(mark_alert_sending(self.db,key,body))
        self.assertFalse(mark_alert_sending(self.db,key,body))
        self.assertIsNone(next_alert(self.db))
        mark_alert_sent(self.db,key,456)
        row=self.db.execute('SELECT sent_message,state FROM auto_alerts WHERE error_key=?',(key,)).fetchone()
        self.assertEqual((456,'sent'),(row['sent_message'],row['state']))

    def test_internal_error_reported_once(self):
        record_issue(self.db,'planner','missing csv')
        record_issue(self.db,'planner','missing csv')
        key,body=next_alert(self.db)
        self.assertIn('missing csv',body)
        self.assertTrue(mark_alert_sending(self.db,key,body))
        self.assertIsNone(next_alert(self.db))


class CleanupTests(AutoBase):
    def test_complete_delivered_input_deleted(self):
        self.db.add_message(23,'test.edz','edz',3)
        file=self.data/'incoming'/'23'/'test.edz'
        file.parent.mkdir()
        file.write_bytes(b'ABC')
        self.db.change_message(23,'complete',local_path=str(file))
        self.db.put_artifact(sha='a'*64,file_name='out.edz',path='/unused',size=3,
           source_id=23,origin='generated',brand='a',part_number='1',validation='v',state='posted')
        self.db.update_artifact('a'*64,'posted',target_message=1)
        count,size=clean_safe_auto_sources(self.db,self.data)
        self.assertEqual((1,3),(count,size))
        self.assertFalse(file.exists())

    def test_pending_artifact_prevents_delete(self):
        self.db.add_message(23,'test.edz','edz',3)
        file=self.data/'incoming'/'23'/'test.edz'
        file.parent.mkdir()
        file.write_bytes(b'ABC')
        self.db.change_message(23,'complete',local_path=str(file))
        self.db.put_artifact(sha='a'*64,file_name='out.edz',path='/unused',size=3,
           source_id=23,origin='generated',brand='a',part_number='1',validation='v',state='ready')
        self.assertEqual((0,0),clean_safe_auto_sources(self.db,self.data))
        self.assertTrue(file.exists())

    def test_work_cache_blocks_auto_input_delete(self):
        self.db.add_message(23,'test.edz','edz',3)
        file=self.data/'incoming'/'23'/'test.edz'
        file.parent.mkdir()
        file.write_bytes(b'ABC')
        self.db.change_message(23,'complete',local_path=str(file))
        self.db.put_artifact(sha='a'*64,file_name='out.edz',path='/unused',size=3,
           source_id=23,origin='generated',brand='a',part_number='1',validation='v',state='posted')
        self.db.update_artifact('a'*64,'posted',target_message=1)
        digest=hashlib.sha256(b'ABC').hexdigest()
        (self.data/'work'/('split-'+digest[:20])).mkdir()
        self.assertEqual((0,0),clean_safe_auto_sources(self.db,self.data))
        self.assertTrue(file.exists())

    def test_completed_volume_hardlinks_deleted(self):
        group='parts.7z'
        root=self.data/'volumes'/hashlib.sha256(group.encode()).hexdigest()[:20]
        root.mkdir()
        for mid,number,payload in ((100,1,b'abc'),(101,2,b'xy')):
            name=f'parts.7z.{number:03}'
            inp=self.data/'incoming'/str(mid)/name
            inp.parent.mkdir()
            inp.write_bytes(payload)
            os.link(inp,root/name)
            self.db.add_message(mid,name,'volume',len(payload),group,number)
            self.db.change_message(mid,'complete',local_path=str(inp))
        self.db.put_artifact(sha='a'*64,file_name='part.edz',path='/unused',size=10,
           source_id=100,origin='nested',brand='A',part_number='1',validation='auto',state='posted')
        self.db.update_artifact('a'*64,'posted',target_message=55)
        count,bytes_=clean_safe_auto_sources(self.db,self.data)
        self.assertEqual((2,5),(count,bytes_))
        self.assertFalse(root.exists())
        self.assertFalse((self.data/'incoming'/'100'/'parts.7z.001').exists())

    def test_pending_volume_kept(self):
        group='parts.7z'
        for mid,idx in ((100,1),(101,2)):
            name=f'parts.7z.{idx:03}'
            inp=self.data/'incoming'/str(mid)/name
            inp.parent.mkdir()
            inp.write_bytes(b'abc')
            self.db.add_message(mid,name,'volume',3,group,idx)
            self.db.change_message(mid,'complete',local_path=str(inp))
        self.db.put_artifact(sha='a'*64,file_name='part.edz',path='/none',size=10,
           source_id=100,origin='nested',brand='a',part_number='1',validation='auto',state='ready')
        self.assertEqual((0,0),clean_safe_auto_sources(self.db,self.data))
        self.assertTrue((self.data/'incoming'/'101'/'parts.7z.002').exists())


if __name__=='__main__':
    unittest.main()

class PipelineStructuralTests(AutoBase):
    def test_generated_auto_ready_only_after_splitter_verification(self):
        from edzbot.pipeline import Pipeline
        from types import SimpleNamespace
        src=self.data/'test.edz'
        src.write_bytes(b'good-edz')
        meta=SimpleNamespace(brand='MUL',group='g',part='p123',part_count=1)
        with (patch('edzbot.pipeline.list_members', return_value=object()),
              patch('edzbot.pipeline.test_archive', return_value=True),
              patch('edzbot.pipeline.read_metadata', return_value=meta),
              patch('edzbot.pipeline.subprocess.run', return_value=SimpleNamespace(returncode=0))):
            count=Pipeline(self.cfg,self.db).add_edz(src,250,'generated')
        self.assertEqual(1,count)
        row=self.db.execute('SELECT state,validation FROM artifacts').fetchone()
        self.assertEqual('ready',row['state'])
        self.assertEqual('structural_auto_no_eplan_test',row['validation'])

    def test_structural_fail_never_creates_ready_artifact(self):
        from edzbot.pipeline import Pipeline
        from types import SimpleNamespace
        src=self.data/'bad.edz'
        src.write_bytes(b'bad-edz')
        meta=SimpleNamespace(brand='MUL',group='g',part='p123',part_count=1)
        with (patch('edzbot.pipeline.list_members', return_value=object()),
              patch('edzbot.pipeline.test_archive', return_value=True),
              patch('edzbot.pipeline.read_metadata', return_value=meta),
              patch('edzbot.pipeline.subprocess.run',
                    return_value=SimpleNamespace(returncode=2,stdout='missing refs',stderr=''))):
            with self.assertRaisesRegex(ValueError,'structural verification failed'):
                Pipeline(self.cfg,self.db).add_edz(src,250,'generated')
        self.assertEqual(0,self.db.execute('SELECT COUNT(*) FROM artifacts').fetchone()[0])

    def test_volume_work_cache_can_clean_after_posting(self):
        from edzbot.work_cleanup import work_candidates, apply_work_cleanup
        group='parts.7z'
        for mid,idx,payload in ((100,1,b'abc'),(101,2,b'xy')):
            name=f'parts.7z.{idx:03}'
            inp=self.data/'incoming'/str(mid)/name
            inp.parent.mkdir()
            inp.write_bytes(payload)
            self.db.add_message(mid,name,'volume',len(payload),group,idx)
            self.db.change_message(mid,'complete',local_path=str(inp))
        digest=hashlib.sha256(b'abc').hexdigest()
        work=self.data/'work'/('nested-'+digest[:18])
        work.mkdir()
        blob=b'EDZ DATA'
        (work/'TEST.edz').write_bytes(blob)
        sha=hashlib.sha256(blob).hexdigest()
        self.db.put_artifact(sha=sha,file_name='TEST.edz',path='/unused',size=len(blob),
            source_id=100,origin='nested',brand='A',part_number='1',validation='auto',state='posted')
        self.db.update_artifact(sha,'posted',target_message=77)
        self.assertEqual(1,len(work_candidates(self.db,self.data)))
        self.assertEqual((1,len(blob)),apply_work_cleanup(self.db,self.data))
        self.assertFalse(work.exists())

class TelegramAlertTests(AutoBase):
    def test_worker_alert_sent_to_saved_messages(self):
        import asyncio
        from types import SimpleNamespace
        from edzbot.bot import Service
        self.db.add_message(999,'broken.edz','edz',123)
        self.db.fail_message(999,'Structural failure',retry=False)
        class Client:
            sent=[]
            async def send_message(self,where,what):
                self.sent.append((where,what))
                return SimpleNamespace(id=345)
        client=Client()
        svc=Service.__new__(Service)
        svc.client=client
        svc.cfg=self.cfg
        svc.db=self.db
        asyncio.run(svc.send_auto_alerts())
        self.assertEqual('me',client.sent[0][0])
        self.assertIn('#999',client.sent[0][1])
        self.assertIn('AlertID:',client.sent[0][1])
        self.assertEqual(345,self.db.execute('SELECT sent_message FROM auto_alerts').fetchone()[0])

    def test_alert_send_failure_stored_for_retry(self):
        import asyncio
        from edzbot.bot import Service
        self.db.add_message(999,'broken.edz','edz',123)
        self.db.fail_message(999,'Structural failure',retry=False)
        class Client:
            async def send_message(self,where,what):
                raise ConnectionError('offline')
        svc=Service.__new__(Service)
        svc.client=Client()
        svc.cfg=self.cfg
        svc.db=self.db
        asyncio.run(svc.send_auto_alerts())
        row=self.db.execute('SELECT state,retry_after FROM auto_alerts').fetchone()
        self.assertEqual('pending_retry',row['state'])
        self.assertGreater(row['retry_after'],0)
