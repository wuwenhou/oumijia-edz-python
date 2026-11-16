import sqlite3
import pytest
from edzbot.name_guard import normalize_edz_name, name_already_published


def _db():
    db = sqlite3.connect(':memory:')
    db.executescript("""
      CREATE TABLE artifacts(sha256 TEXT PRIMARY KEY, file_name TEXT, state TEXT, path TEXT, size_bytes INTEGER, source_message INTEGER, origin TEXT, brand TEXT, part_number TEXT, validation TEXT, created_at REAL, updated_at REAL);
      CREATE TABLE telegram_posts(destination_chat_id INTEGER, destination_message_id INTEGER, artifact_sha256 TEXT, file_name TEXT, created_at REAL);
      CREATE TABLE edz_name_catalog(normalized_name TEXT PRIMARY KEY, winner_sha256 TEXT, state TEXT);
    """)
    return db


def test_normalize_nfc_case_path():
    assert normalize_edz_name('Folder/A.edz') == 'a.edz'
    assert normalize_edz_name('a.EDZ') == 'a.edz'
    assert normalize_edz_name('e\u0301.edz') == normalize_edz_name('é.edz')


def test_same_name_different_sha_is_rejected():
    db = _db()
    db.execute("INSERT INTO edz_name_catalog VALUES('a.edz','sha111','accepted')")
    assert name_already_published(db, 'A.edz')
    assert not name_already_published(db, 'b.edz')


def test_different_name_same_sha_is_kept():
    db = _db()
    db.execute("INSERT INTO edz_name_catalog VALUES('a.edz','sha111','accepted')")
    # sha equality is irrelevant to the name guard
    assert not name_already_published(db, 'b.edz')


def test_confirmed_post_mapping_fallback():
    db = _db()
    db.execute("INSERT INTO telegram_posts VALUES(-100,122538,'sha999','Wöhrle.EDZ',1)")
    assert name_already_published(db, 'wöhrle.edz')
    assert not name_already_published(db, 'other.edz')


def test_missing_telegram_posts_table_and_artifacts_fallback():
    db = sqlite3.connect(':memory:')
    db.execute("CREATE TABLE artifacts(sha256 TEXT PRIMARY KEY, file_name TEXT, state TEXT, path TEXT, size_bytes INTEGER, source_message INTEGER, origin TEXT, brand TEXT, part_number TEXT, validation TEXT, created_at REAL, updated_at REAL, normalized_name TEXT)")
    db.execute("INSERT INTO artifacts(sha256,file_name,state,normalized_name) VALUES('s1','PythonPosted.edz','posted','pythonposted.edz')")
    # no telegram_posts table at all -> must not raise, and artifact state wins
    assert name_already_published(db, 'PYTHONPOSTED.edz')
    assert not name_already_published(db, 'never-seen.edz')


def test_cache_invalidates_when_new_post_arrives():
    db = _db()
    assert not name_already_published(db, 'fresh.edz')
    db.execute("INSERT INTO telegram_posts VALUES(-100,122539,'sha777','FRESH.edz',2)")
    # in-memory DBs are never cached; a new post is visible immediately
    assert name_already_published(db, 'fresh.edz')

