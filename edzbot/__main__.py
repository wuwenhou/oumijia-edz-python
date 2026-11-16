"""CLI: login, dialogs, run, inspect, and staged approvals."""
from __future__ import annotations
import argparse
import asyncio
import fcntl
import json
import logging
from pathlib import Path
from .config import Config
from .db import DB
from .bot import Service, interactive_login, show_dialogs


def main():
    parser = argparse.ArgumentParser(description='Telegram -> EDZ -> one channel (Telethon)')
    cmd = parser.add_subparsers(dest='cmd', required=True)
    cmd.add_parser('login', help='Interactive Telegram login on this computer')
    cmd.add_parser('dialogs', help='List channel IDs visible to the logged-in account')
    run = cmd.add_parser('run', help='Backfill once and follow new Telegram files indefinitely')
    run.add_argument('--backfill', action='store_true', help='Index entire channel history again')
    cmd.add_parser('status', help='Inspect SQLite pipeline state')
    cmd.add_parser('auto-status', help='Read-only autonomous mode and alert status')
    cleanup = cmd.add_parser('cleanup', help='Preview safe local cleanup (does not delete by default)')
    cleanup.add_argument('--apply', action='store_true', help='Delete eligible files; requires the service to be stopped')
    cleanup_work = cmd.add_parser('cleanup-work', help='Preview safe work-cache cleanup')
    cleanup_work.add_argument('--apply', action='store_true', help='Delete eligible work caches; stop service first')
    plan_next = cmd.add_parser('plan-next', help='Preview next unique standalone EDZ from local catalog')
    plan_next.add_argument('--apply', action='store_true', help='Queue one EDZ; requires stopping systemd worker')
    review = cmd.add_parser('review', help='List held/review/ambiguous files')
    review.add_argument('--limit', type=int, default=30)
    approve = cmd.add_parser('approve', help='Approve one SHA-256 generated EDZ for publishing after real EPLAN validation')
    approve.add_argument('sha256')
    args = parser.parse_args()
    cfg = Config.load(require_telegram=args.cmd in ('login','dialogs','run'))
    cfg.setup()
    logging.basicConfig(level=cfg.log_level,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    if args.cmd == 'login':
        asyncio.run(interactive_login(cfg))
    elif args.cmd == 'dialogs':
        asyncio.run(show_dialogs(cfg))
    elif args.cmd == 'run':
        # Ensure one producer per data directory (no double posting due to two services).
        lock = cfg.data / 'edzbot.lock'
        with lock.open('w') as stream:
            try:
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise SystemExit('Another edzbot process already owns data directory')
            asyncio.run(Service(cfg).run(force_history=args.backfill))
    else:
        db = DB(cfg.data / 'state.sqlite3')
        try:
            if args.cmd == 'status':
                print(json.dumps(db.stats(), ensure_ascii=False, indent=2))
            elif args.cmd == 'auto-status':
                from .auto_alerts import _schema
                _schema(db)
                summary = db.stats()
                print('AUTO MODE:', 'ENABLED' if cfg.auto_mode else 'DISABLED')
                print('PIPELINE:', 'CONTINUOUS v16.3.2' if cfg.continuous_pipeline else 'SEQUENTIAL v16.1')
                if cfg.continuous_pipeline:
                    from .pipeline_parallel import stats as pipeline_stats
                    print('PIPELINE STAGES:', pipeline_stats(db))
                print('ERROR CHAT:', cfg.alert_chat)
                print('DRY_RUN:', cfg.dry_run)
                print('AUTO CLEANUP:', cfg.auto_cleanup)
                print('AUTO WORK CLEANUP:', cfg.auto_work_cleanup)
                print('GROUP LIMIT GiB:', round(cfg.max_group_bytes/1024**3, 2))
                print('DISK RESERVE GiB:', round(cfg.reserve_bytes/1024**3, 2))
                print('MESSAGES:', {x['state']:x['count'] for x in summary['messages']})
                print('ARTIFACTS:', {x['state']:x['count'] for x in summary['artifacts']})
                alerts = [dict(x) for x in db.execute(
                    'SELECT state,COUNT(*) count FROM auto_alerts GROUP BY state')]
                print('TELEGRAM ERROR ALERTS:', {x['state']:x['count'] for x in alerts})
                from .publish_guard import flood_error, cooldown_remaining, next_send_remaining
                flood_count = sum(1 for r in db.execute("SELECT error FROM artifacts WHERE state='held'")
                                  if flood_error(r['error']))
                print('FLOODWAIT HELD:', flood_count)
                from .rate_schedule import state as rate_state
                rate = rate_state(db)
                print('FLOODWAIT UNIQUE HISTORICAL:', rate.unique_flood)
                print('PUBLISH INTERVAL SECONDS:', rate.effective_interval)
                print('RATE BASE INTERVAL SECONDS:', rate.base_interval)
                print('RATE CYCLE MODE:', rate.phase)
                print('RATE NEXT TRANSITION SECONDS:', rate.next_transition_seconds)
                from .publish_delay import current_delay, override_delay
                print('PUBLISH DELAY SECONDS (EFFECTIVE):', current_delay(db, cfg))
                print('PUBLISH DELAY SOURCE:', 'telegram-override'
                      if override_delay(db) is not None else 'env SEND_DELAY_SECONDS')
                print('MANUAL INBOX:', cfg.inbox_chat if cfg.inbox_chat else 'DISABLED')
                print('PUBLISH COOLDOWN REMAINING SECONDS:', round(cooldown_remaining(db)))
                print('NEXT PUBLISH ELIGIBLE IN SECONDS:', round(next_send_remaining(db)))
                from .daily_scan import is_daily, HIGH_KEY, NEXT_KEY
                print('SOURCE SCAN MODE:', 'DAILY 02:00 VN' if is_daily(db) else 'HISTORICAL')
                if is_daily(db):
                    from datetime import datetime
                    from zoneinfo import ZoneInfo
                    print('SOURCE LAST SCANNED MESSAGE ID:', db.value(HIGH_KEY))
                    next_at = float(db.value(NEXT_KEY, '0'))
                    if next_at:
                        print('SOURCE NEXT SCAN:', datetime.fromtimestamp(next_at,
                              ZoneInfo('Asia/Ho_Chi_Minh')).isoformat())
                recent = db.execute('''SELECT id,file_name,error FROM messages
                          WHERE state='held' ORDER BY changed_at DESC LIMIT 5''').fetchall()
                for item in recent:
                    print(f'HELD SOURCE #{item["id"]}: {item["file_name"]} - {item["error"]}')
            elif args.cmd == 'cleanup':
                from .cleanup import candidates, apply_cleanup
                if args.apply:
                    lock = cfg.data / 'edzbot.lock'
                    with lock.open('w') as stream:
                        try:
                            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        except BlockingIOError:
                            raise SystemExit('Worker holds the data lock; stop systemd service before --apply')
                        count, size = apply_cleanup(db, cfg.data)
                        print(f'CLEANUP APPLIED: {count} files; {size} bytes removed')
                else:
                    todo = candidates(db, cfg.data)
                    for item in todo:
                        print(f'ELIGIBLE {item.kind}: {item.size} bytes | {item.path}')
                    print(f'PREVIEW ONLY: {len(todo)} files, {sum(x.size for x in todo)} bytes')
            elif args.cmd == 'cleanup-work':
                from .work_cleanup import work_candidates, apply_work_cleanup
                if args.apply:
                    lock = cfg.data / 'edzbot.lock'
                    with lock.open('w') as stream:
                        try:
                            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        except BlockingIOError:
                            raise SystemExit('Worker holds the data lock; stop systemd service before --apply')
                        count, size = apply_work_cleanup(db, cfg.data)
                        print(f'WORK CLEANUP APPLIED: {count} folders; {size} bytes removed')
                else:
                    todo = work_candidates(db, cfg.data)
                    for item in todo:
                        print(f'ELIGIBLE completed_work: {item.size} bytes | {item.path} | message #{item.message_id}')
                    print(f'WORK PREVIEW ONLY: {len(todo)} folders, {sum(x.size for x in todo)} bytes')
            elif args.cmd == 'plan-next':
                from .plan_queue import select_next, enqueue_next
                if args.apply:
                    lock = cfg.data / 'edzbot.lock'
                    with lock.open('w') as stream:
                        try:
                            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        except BlockingIOError:
                            raise SystemExit('Worker holds the data lock; stop systemd before plan-next --apply')
                        chosen, reason = enqueue_next(db, cfg)
                else:
                    chosen, reason = select_next(db, cfg)
                if chosen is None:
                    print('PLAN BLOCKED:', reason)
                else:
                    action = 'PLAN QUEUED' if args.apply else 'PLAN PREVIEW ONLY'
                    print(f'{action}: #{chosen.message_id} {chosen.file_name} ({chosen.size_bytes} bytes)')
                    print('CHECK:', reason)
            elif args.cmd == 'review':
                for row in db.execute("""SELECT sha256,file_name,brand,part_number,state,error FROM artifacts
                                         WHERE state IN ('review','held','posting') ORDER BY created_at LIMIT ?""",
                                      (args.limit,)):
                    print(json.dumps(dict(row), ensure_ascii=False))
            elif args.cmd == 'approve':
                row = db.execute('SELECT sha256,origin,state FROM artifacts WHERE sha256=?', (args.sha256,)).fetchone()
                if not row:
                    raise SystemExit('SHA256 not found in artifacts database')
                if row['state'] not in ('review','held'):
                    raise SystemExit('Artifact is not currently in review/held state')
                if row['origin'] != 'generated':
                    raise SystemExit('approve only supports generated EDZ; review direct failures manually')
                db.update_artifact(row['sha256'], 'ready', error='Explicitly approved after EPLAN test')
                print('Approved for sending when DRY_RUN=false:', row['sha256'])
        finally:
            db.close()


if __name__ == '__main__':
    main()
