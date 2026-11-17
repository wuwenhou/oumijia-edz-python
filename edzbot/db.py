"""SQLite write-ahead journal with idempotent Telegram messages and SHA-256 artifacts."""
from __future__ import annotations
import sqlite3
import time
from pathlib import Path

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA busy_timeout=5000;
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY, val TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY,
    file_name TEXT NOT NULL,
    kind TEXT NOT NULL,
    size_bytes INTEGER NOT NULL DEFAULT 0,
    group_name TEXT,
    volume_index INTEGER,
    state TEXT NOT NULL DEFAULT 'new',
    local_path TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    retry_after REAL NOT NULL DEFAULT 0,
    error TEXT,
    changed_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_msg_state ON messages(state, retry_after);
CREATE INDEX IF NOT EXISTS idx_msg_group ON messages(group_name, volume_index);
CREATE TABLE IF NOT EXISTS artifacts (
    sha256 TEXT PRIMARY KEY,
    file_name TEXT NOT NULL,
    path TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    source_message INTEGER NOT NULL,
    origin TEXT NOT NULL,
    brand TEXT NOT NULL,
    part_number TEXT NOT NULL,
    validation TEXT NOT NULL,
    state TEXT NOT NULL,
    target_message INTEGER,
    error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_art_state ON artifacts(state, created_at);
CREATE TABLE IF NOT EXISTS plan_batch_sources (
    batch_id TEXT NOT NULL,
    message_id INTEGER NOT NULL UNIQUE,
    queued_at REAL NOT NULL,
    PRIMARY KEY (batch_id, message_id)
);
CREATE INDEX IF NOT EXISTS idx_plan_batch_id ON plan_batch_sources(batch_id);
-- V2 upload engine: name-owned artifacts, durable queue and audit.
-- Additive only; artifacts.sha256 PRIMARY KEY is NOT changed.
CREATE TABLE IF NOT EXISTS upload_name_owners (
    normalized_name TEXT PRIMARY KEY,
    sha256 TEXT NOT NULL,
    file_name TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    source_message INTEGER,
    created_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS upload_jobs (
    normalized_name TEXT PRIMARY KEY,
    sha256 TEXT NOT NULL,
    file_name TEXT NOT NULL,
    path TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    source_message INTEGER,
    state TEXT NOT NULL DEFAULT 'queued',
    attempts INTEGER NOT NULL DEFAULT 0,
    origin TEXT,
    target_message INTEGER,
    error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL);
CREATE INDEX IF NOT EXISTS idx_upload_jobs_state ON upload_jobs(state, created_at);
CREATE TABLE IF NOT EXISTS upload_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at REAL NOT NULL,
    normalized_name TEXT NOT NULL,
    kind TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '');
"""


def migrate_upload_jobs(conn):
    """Idempotent, additive migration for the pre-canary V2 schema.

    Production already has upload_jobs WITHOUT `origin`. CREATE TABLE IF NOT
    EXISTS never adds a column to an existing table, so add it explicitly.
    ALTER preserves every row (including the held canary-1 job). No DROP, no
    CREATE replacement, no data rewrite. Safe to run on every startup.
    """
    if not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='upload_jobs'"
    ).fetchone():
        return                      # fresh DB: SCHEMA already created the column
    cols = {r[1] for r in conn.execute('PRAGMA table_info(upload_jobs)')}
    if 'origin' not in cols:
        conn.execute('ALTER TABLE upload_jobs ADD COLUMN origin TEXT')


class DB:
    def __init__(self, path: Path):
        path.parent.mkdir(exist_ok=True, parents=True)
        self.conn = sqlite3.connect(str(path), isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        migrate_upload_jobs(self.conn)

    def close(self): self.conn.close()

    def execute(self, sql: str, args: tuple = ()): return self.conn.execute(sql, args)

    def value(self, key: str, default: str = '') -> str:
        row = self.execute('SELECT val FROM settings WHERE key=?', (key,)).fetchone()
        return row['val'] if row else default

    def set(self, key: str, val: str):
        self.execute('INSERT INTO settings(key,val) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET val=excluded.val', (key, val))

    def add_message(self, msg_id: int, filename: str, kind: str, size: int, group: str | None = None, index: int | None = None):
        self.execute('''INSERT OR IGNORE INTO messages
            (id,file_name,kind,size_bytes,group_name,volume_index,changed_at)
            VALUES(?,?,?,?,?,?,?)''', (msg_id, filename, kind, size, group, index, time.time()))

    def message(self, msg_id: int):
        return self.execute('SELECT * FROM messages WHERE id=?', (msg_id,)).fetchone()

    def next_message(self):
        return self.execute("""SELECT * FROM messages WHERE state IN ('new','retry') AND retry_after<=?
                            ORDER BY id LIMIT 1""", (time.time(),)).fetchone()

    def change_message(self, msg_id: int, state: str, **extra):
        extra = dict(extra, state=state, changed_at=time.time())
        sql = 'UPDATE messages SET ' + ', '.join(f'{k}=?' for k in extra) + ' WHERE id=?'
        self.execute(sql, tuple(extra.values()) + (msg_id,))

    def fail_message(self, msg_id: int, error: str, *, retry: bool = True):
        row = self.message(msg_id)
        attempts = (row['attempts'] if row else 0) + 1
        state = 'retry' if retry and attempts <= 5 else 'held'
        delay = min(3600, 2 ** attempts * 30)
        self.change_message(msg_id, state, attempts=attempts, error=error[:1500], retry_after=time.time()+delay)

    def groups(self):
        return self.execute('''SELECT group_name, MAX(changed_at) last_change, COUNT(*) count
                               FROM messages WHERE kind='volume' AND state='downloaded'
                               GROUP BY group_name''').fetchall()

    def group_rows(self, group: str):
        return self.execute("SELECT * FROM messages WHERE group_name=? ORDER BY volume_index,id", (group,)).fetchall()

    def put_artifact(self, *, sha: str, file_name: str, path: str, size: int, source_id: int,
                     origin: str, brand: str, part_number: str, validation: str, state: str) -> bool:
        now = time.time()
        cursor = self.execute('''INSERT OR IGNORE INTO artifacts
            (sha256,file_name,path,size_bytes,source_message,origin,brand,part_number,validation,state,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',
            (sha,file_name,path,size,source_id,origin,brand,part_number,validation,state,now,now))
        return cursor.rowcount > 0

    def next_artifact(self, *, allow_unverified: bool = False, require_complete_source: bool = False):
        states = "('ready','review')" if allow_unverified else "('ready')"
        source_gate = (" AND EXISTS (SELECT 1 FROM messages m WHERE m.id=artifacts.source_message "
                       "AND m.state='complete')") if require_complete_source else ""
        return self.execute(f'''SELECT * FROM artifacts WHERE state IN {states}{source_gate}
                                ORDER BY created_at LIMIT 1''').fetchone()

    def update_artifact(self, sha: str, state: str, **extra):
        extra = dict(extra, state=state, updated_at=time.time())
        self.execute('UPDATE artifacts SET ' + ', '.join(f'{k}=?' for k in extra) + ' WHERE sha256=?',
                     tuple(extra.values()) + (sha,))

    def prepare_recovery(self):
        # After a process crash, retry interrupted downloads/processing. Never
        # replay an ambiguous Telegram send without a human destination check.
        self.execute("""UPDATE messages SET state='retry', retry_after=0
                        WHERE state IN ('downloading','processing')
                        OR (state='downloaded' AND kind!='volume')""")
        self.execute("""UPDATE artifacts SET state='held',
                        error='Previous send interrupted: verify destination before retry'
                        WHERE state='posting'""")

    def stats(self):
        return {
            'messages': [dict(r) for r in self.execute('SELECT state,COUNT(*) count FROM messages GROUP BY state')],
            'artifacts': [dict(r) for r in self.execute('SELECT state,COUNT(*) count FROM artifacts GROUP BY state')],
            'backfill_complete': self.value('backfill_complete', 'false'),
            'backfill_highwater': self.value('backfill_highwater', '0'),
        }
