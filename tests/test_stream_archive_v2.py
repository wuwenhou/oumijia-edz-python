"""Streaming 7z tests: single-pass demux, CRC guard, bounded temp, name skip."""
from __future__ import annotations

import hashlib
import io
import subprocess
from pathlib import Path
import zlib

import pytest

from edzbot import stream_archive as sa


def listing_block(path, size, crc=None):
    lines = [f'Path = {path}', f'Size = {size}']
    if crc is not None:
        lines.append(f'CRC = {crc:X}')
    lines.append('Attributes = ....A')
    return '\n'.join(lines)


def make_run(payload_blocks):
    def fake_run(args, **kw):
        return subprocess.CompletedProcess(args, 0, '\n\n'.join(payload_blocks).encode(), b'')
    return fake_run


def make_popen(data):
    class FakeProc:
        def __init__(self, *a, **kw):
            self.stdout = io.BytesIO(data)
            self.stderr = io.BytesIO(b'')
        def wait(self, timeout=None):
            return 0
        def poll(self):
            return 0
        def kill(self):
            pass
    return FakeProc


def test_list_entries_parses_size_crc_and_skips_nested(monkeypatch):
    blocks = [
        listing_block('CHINT/CHT.101041.edz', 8, zlib.crc32(b'AAAAAAAA')),
        listing_block('CHINT/NOCRC.edz', 3),                 # no CRC -> None
        listing_block('outer.edz/inner.edz', 5),             # nested -> skipped
        listing_block('CHINT/readme.txt', 4),                # not edz -> skipped
        'Path = CHINT\nFolder = +',                          # folder -> skipped
    ]
    monkeypatch.setattr(sa.subprocess, 'run', make_run(blocks))
    entries = sa.list_edz_entries('7z', Path('a.7z'), max_files=100, max_bytes=1 << 20)
    assert [e.file_name for e in entries] == ['CHT.101041.edz', 'NOCRC.edz']
    assert entries[0].crc32 == zlib.crc32(b'AAAAAAAA')
    assert entries[1].crc32 is None


def test_stream_demux_crc_sha_and_temp_cleanup(monkeypatch, tmp_path):
    a, b = b'AAAAAAAA', b'BBBBB'
    monkeypatch.setattr(sa.subprocess, 'run', make_run([
        listing_block('A.edz', len(a), zlib.crc32(a)),
        listing_block('B.edz', len(b), zlib.crc32(b)),
    ]))
    monkeypatch.setattr(sa.subprocess, 'Popen', make_popen(a + b))
    entries = sa.list_edz_entries('7z', Path('a.7z'), max_files=100, max_bytes=1 << 20)
    items = list(sa.stream_edz('7z', Path('a.7z'), entries,
                               accepted_names=set(), temp_dir=tmp_path,
                               max_bytes=1 << 20))
    assert [i.name for i in items] == ['A.edz', 'B.edz']
    assert items[0].path.read_bytes() == a
    assert items[0].sha256 == hashlib.sha256(a).hexdigest()
    assert items[0].crc32 == zlib.crc32(a)
    for i in items:
        i.cleanup()
        assert not i.path.exists()


def test_stream_skips_accepted_name_but_stays_aligned(monkeypatch, tmp_path):
    a, b = b'AAAAAAAA', b'BBBBB'
    monkeypatch.setattr(sa.subprocess, 'run', make_run([
        listing_block('A.edz', len(a), zlib.crc32(a)),
        listing_block('B.edz', len(b), zlib.crc32(b)),
    ]))
    monkeypatch.setattr(sa.subprocess, 'Popen', make_popen(a + b))
    entries = sa.list_edz_entries('7z', Path('a.7z'), max_files=100, max_bytes=1 << 20)
    items = list(sa.stream_edz('7z', Path('a.7z'), entries,
                               accepted_names={'a.edz'}, temp_dir=tmp_path,
                               max_bytes=1 << 20))
    assert [i.name for i in items] == ['B.edz']    # A skipped, B still aligned
    assert items[0].path.read_bytes() == b
    items[0].cleanup()


def test_stream_crc_mismatch_aborts_before_publish(monkeypatch, tmp_path):
    a = b'AAAAAAAA'
    monkeypatch.setattr(sa.subprocess, 'run', make_run([
        listing_block('A.edz', len(a), zlib.crc32(b'XXXXXXXX')),  # wrong CRC
    ]))
    monkeypatch.setattr(sa.subprocess, 'Popen', make_popen(a))
    entries = sa.list_edz_entries('7z', Path('a.7z'), max_files=100, max_bytes=1 << 20)
    with pytest.raises(ValueError, match='CRC-32 mismatch'):
        list(sa.stream_edz('7z', Path('a.7z'), entries, accepted_names=set(),
                           temp_dir=tmp_path, max_bytes=1 << 20))


def test_stream_truncated_data_raises(monkeypatch, tmp_path):
    monkeypatch.setattr(sa.subprocess, 'run', make_run([
        listing_block('A.edz', 10, zlib.crc32(b'AAAAAAAA')),
    ]))
    monkeypatch.setattr(sa.subprocess, 'Popen', make_popen(b'AAAA'))  # too short
    entries = sa.list_edz_entries('7z', Path('a.7z'), max_files=100, max_bytes=1 << 20)
    with pytest.raises(ValueError, match='ended early'):
        list(sa.stream_edz('7z', Path('a.7z'), entries, accepted_names=set(),
                           temp_dir=tmp_path, max_bytes=1 << 20))
