"""Bounded, single-pass 7z -> EDZ streaming for Upload Engine V2.

Ported ideas from telegram-downloader-rs archive.rs/upload.rs, adapted to the
Python platform and the OUMIJIA publish rules:

  * Read the 7z listing FIRST (name + size + CRC-32 from `7z l -slt`).
  * Decode the archive ONCE with `7z x -so` and de-multiplex stdout by the
    listed sizes. A solid archive is a single LZMA2 stream: we never claim
    random access -- bytes are consumed strictly in listing order.
  * Skip names already accepted (normalized NFC + casefold) but still consume
    their bytes to stay aligned with the solid stream.
  * CRC-32 (IEEE) is checked ONLY when a trustworthy CRC is in the listing.
    SHA-256 is always computed over the full member bytes.
  * Bytes go to a temp file (bounded RAM: fixed chunk reads), never an
    unbounded in-memory buffer. One temp file is alive at a time.
  * No edzsplit is ever invoked here; extracted EDZs go straight to the queue.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import re
import subprocess
import tempfile
import zlib

from .archive import safe_member
from .name_guard import normalize_edz_name

CHUNK = 256 * 1024
_CRC_RE = re.compile(r'^[0-9a-fA-F]{1,8}$')


@dataclass(frozen=True)
class ArchiveEntry:
    path: str
    file_name: str
    size: int
    crc32: int | None          # None when the listing has no trustworthy CRC
    is_edz: bool = True        # False for non-EDZ / nested members: consume, don't publish


@dataclass
class StreamedEDZ:
    name: str
    size: int
    sha256: str
    crc32: int
    path: Path
    temp: object | None = None

    def cleanup(self) -> None:
        if self.temp is not None:
            try:
                self.temp.close()
            finally:
                Path(self.path).unlink(missing_ok=True)
                self.temp = None


def _fields_from_block(block: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in block.splitlines():
        if ' = ' in line:
            k, v = line.split(' = ', 1)
            fields[k.strip()] = v.strip()
    return fields


def list_entries(sevenzip: str, archive: Path, *, max_files: int,
                 max_bytes: int) -> list[ArchiveEntry]:
    """`7z l -slt`: EVERY file member in archive order (size + CRC if present).

    Non-EDZ and nested-EDZ members MUST stay in this list: `7z x -so` emits their
    bytes too, and dropping them from the demux stride shifts every later file.
    """
    p = subprocess.run([sevenzip, 'l', '-slt', '-sccUTF-8', str(archive)],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=1200)
    if p.returncode != 0:
        raise ValueError(f'7z list failed ({p.returncode}): {p.stderr[-1200:]!r}')
    blocks = re.split(r'\r?\n\s*\r?\n', p.stdout.decode('utf-8', errors='replace'))
    entries: list[ArchiveEntry] = []
    for block in blocks:
        f = _fields_from_block(block)
        name = f.get('Path')
        if not name or 'Size' not in f or f.get('Folder') == '+':
            continue
        if not safe_member(name):
            raise ValueError('Unsafe path inside archive: ' + repr(name))
        size = int(f['Size'])
        if size < 0 or size > max_bytes:
            raise ValueError('Invalid/oversized entry: ' + name)
        crc_raw = f.get('CRC', '').strip()
        crc = int(crc_raw, 16) if _CRC_RE.fullmatch(crc_raw) else None
        parts = name.replace('\\', '/').split('/')
        is_edz = (parts[-1].lower().endswith('.edz')
                  and not any(x.lower().endswith('.edz') for x in parts[:-1]))
        entries.append(ArchiveEntry(name, parts[-1], size, crc, is_edz))
        if len(entries) > max_files:
            raise ValueError('Archive exceeds MAX_ARCHIVE_FILES')
    return entries


def list_edz_entries(sevenzip: str, archive: Path, *, max_files: int,
                     max_bytes: int) -> list[ArchiveEntry]:
    """Convenience filter; do NOT use as the demux stride (see list_entries)."""
    return [e for e in list_entries(sevenzip, archive, max_files=max_files,
                                    max_bytes=max_bytes) if e.is_edz]


def stream_edz(sevenzip: str, archive: Path, entries: list[ArchiveEntry], *,
               accepted_names, temp_dir: Path, max_bytes: int,
               enforce_crc: bool = True):
    """Yield validated StreamedEDZ objects; decode once, demux by size.

    `accepted_names` is a collection of normalized names to skip. Their bytes
    are still consumed (and CRC/size checked) so the solid stream stays aligned.
    """
    temp_dir = Path(temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    args = [sevenzip, 'x', '-so', '-y', '-spd', '-sccUTF-8', str(archive)]
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert proc.stdout is not None
    try:
        for entry in entries:
            if entry.size > max_bytes:
                raise ValueError('Entry exceeds streaming size cap: ' + entry.path)
            norm = normalize_edz_name(entry.file_name)
            # Every member is consumed in listing order; only NEW top-level
            # EDZs are kept. Non-EDZ/nested bytes are read and discarded so
            # the solid stream stays aligned (preflight condition 3).
            skip = (not entry.is_edz) or (norm in accepted_names)
            sha = hashlib.sha256()
            crc = 0
            total = 0
            tmp = None
            out_path: Path | None = None
            if not skip:
                tmp = tempfile.NamedTemporaryFile(prefix='v2edz-', suffix='.edz',
                                                  dir=temp_dir, delete=False)
                out_path = Path(tmp.name)
            try:
                remaining = entry.size
                while remaining:
                    chunk = proc.stdout.read(min(CHUNK, remaining))
                    if not chunk:
                        raise ValueError('7z stream ended early at ' + entry.path)
                    remaining -= len(chunk)
                    total += len(chunk)
                    sha.update(chunk)
                    crc = zlib.crc32(chunk, crc)
                    if not skip:
                        tmp.write(chunk)
            except Exception:
                if tmp is not None:
                    tmp.close()
                    out_path.unlink(missing_ok=True)
                raise
            finally:
                if tmp is not None:
                    tmp.close()
            if total != entry.size:
                out_path and out_path.unlink(missing_ok=True)
                raise ValueError(f'Streamed size mismatch: {entry.path} {total}!={entry.size}')
            if enforce_crc and entry.crc32 is not None and (crc & 0xffffffff) != entry.crc32:
                out_path and out_path.unlink(missing_ok=True)
                raise ValueError(f'CRC-32 mismatch before publish: {entry.path} '
                                 f'{crc & 0xffffffff:08x}!={entry.crc32:08x}')
            if skip:
                continue
            yield StreamedEDZ(entry.file_name, entry.size, sha.hexdigest(),
                              crc & 0xffffffff, out_path, tmp)
        # Drain any trailing bytes so 7z can exit cleanly.
        while proc.stdout.read(CHUNK):
            pass
        if proc.wait(timeout=300) != 0:
            raise ValueError('7z stream process failed: ' + str(proc.stderr.read()[-800:]))
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
