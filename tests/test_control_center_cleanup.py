"""Control Bot cleanup: parser, admin gate, 7-command help, files/errors merge."""
from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from edzbot.db import DB
from edzbot.manual_inbox import (parse_command, message_allowed, stage,
                                  valid_ids, _manual_group)
from edzbot.review_reports import parse_request
from edzbot.upload_engine import UploadStats
from edzbot.bot import Service


# ---- 1. parser -------------------------------------------------------------
def test_parse_command_files_and_hidden_check():
    assert parse_command('/files') == ('files', None, None)
    assert parse_command('/files ABC.7z 3') == ('files', 'ABC.7z', 3)
    assert parse_command('/check ABC.7z 3') == ('check', 'ABC.7z', 3)   # alias kept
    with pytest.raises(ValueError):
        parse_command('/files ABC.7z xyz')


def test_hidden_admin_commands_still_parse():
    assert parse_command('/limit') == ('limit', None, None)
    assert parse_command('/limit 3') == ('limit', '3', None)
    assert parse_command('/limit reset') == ('limit', 'reset', None)
    assert parse_command('/speed') == ('speed', None, None)
    assert parse_command('/process ABC.7z 2') == ('process', 'ABC.7z', 2)


def test_errors_aliases_and_new_forms():
    assert parse_request('/errors') == ('summary', None)
    assert parse_request('/errors csv') == ('csv', None)
    assert parse_request('/errors files') == ('files', None)
    assert parse_request('/errorscsv') == ('csv', None)          # legacy alias
    assert parse_request('/errorsfiles') == ('files', None)      # legacy alias
    assert parse_request('/errorscsv@mybot') == ('csv', None)
    assert parse_request('/errors 158') == ('detail', ('source', '158'))
    assert parse_request('/errors ' + 'a' * 64) == ('detail', ('artifact', 'a' * 64))
    assert parse_request('/errors nonsense') == ('invalid', None)


# ---- 2. admin permission ---------------------------------------------------
def test_message_allowed_admin_gate():
    admins = valid_ids('8269169497')
    assert message_allowed(type('M', (), {'sender_id': 8269169497})(), admins) is True
    assert message_allowed(type('M', (), {'sender_id': 123})(), admins) is False
    assert message_allowed(type('M', (), {'sender_id': None})(), frozenset()) is False


def test_non_admin_command_is_ignored(monkeypatch):
    s = object.__new__(Service)
    s.cfg = SimpleNamespace(publisher_mode='normal', upload_engine_v2=True)
    s.db = DB(Path(':memory:'))
    s.upload_engine = None
    s.inbox_allowed = frozenset({1})
    out = []
    async def notice(t):
        out.append(t)
    s._manual_notice = notice
    monkeypatch.setattr('edzbot.bot.message_allowed', message_allowed)
    msg = type('M', (), {'sender_id': 999, 'raw_text': '/help'})()
    asyncio.run(s.handle_manual(msg))
    assert out == []                       # no reply, no action for non-admin
    s.db.close()


# ---- 3. help: exactly 7 main commands, no /limit or /speed ----------------
def test_help_shows_only_seven_commands(monkeypatch):
    s = object.__new__(Service)
    s.cfg = SimpleNamespace(publisher_mode='normal', upload_engine_v2=True)
    s.db = DB(Path(':memory:'))
    s.upload_engine = None
    s.inbox_allowed = frozenset({1})
    out = []
    async def notice(t):
        out.append(t)
    s._manual_notice = notice
    monkeypatch.setattr('edzbot.bot.message_allowed', lambda m, a: True)
    asyncio.run(s.handle_manual(type('M', (), {'sender_id': 1, 'raw_text': '/help'})()))
    text = out[0]
    commands = [line.strip().split('"')[1] for line in text.splitlines()
                if line.strip().startswith('- "/')]
    assert commands == ['/help', '/files', '/process', '/cancel',
                        '/status', '/health', '/errors']
    assert '/limit' not in text and '/speed' not in text
    assert '/check' not in text
    s.db.close()


# ---- 4. /files group inspection (merged /check) ----------------------------
def test_files_group_inspection(tmp_path):
    db = DB(tmp_path / 'files.sqlite3')
    for i in (1, 2):
        mid = -1000 - i
        db.add_message(mid, f'ABC.7z.{i:03d}', 'volume', 1024,
                       _manual_group('ABC.7z'), i)
        db.change_message(mid, 'staged')
    s = object.__new__(Service)
    s.db = db
    out = []
    async def notice(t):
        out.append(t)
    s._manual_notice = notice
    asyncio.run(s._files_notice('ABC.7z', 3))
    assert '2/3' in out[0] and 'Thiếu' in out[0]
    asyncio.run(s._files_notice('ABC.7z', 2))
    assert 'Đủ số thứ tự' in out[-1]
    db.close()


# ---- 5. /status: session vs historical + pacing honesty -------------------
def test_status_distinguishes_session_and_history():
    s = object.__new__(Service)
    s.db = DB(Path(':memory:'))
    s.cfg = SimpleNamespace(send_delay=3.0)
    s.upload_engine = SimpleNamespace(
        stats=UploadStats(),
        governor=SimpleNamespace(current=1, maximum=1),
        faulted=False)
    s.upload_engine.stats.record_upload(1024**2, 0.4)
    text = s._v2_status_short()
    assert 'Đã đăng (phiên này)' in text
    assert 'SEND_DELAY_SECONDS=3s' in text
    assert 'không tự đổi khoảng nghỉ' in text
    assert 'PHIÊN NÀY' in text
    s.db.close()
