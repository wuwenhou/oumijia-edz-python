"""Read-only archive discovery and guarded extraction of selected members.

Security: reject unsafe archive entries before calling 7z. This is a preflight,
not an OS sandbox. The service also runs as a non-privileged systemd user.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
import os
import re
import shutil
import subprocess


@dataclass(frozen=True)
class Member:
    path: str
    size: int
    attributes: str = ''


@dataclass(frozen=True)
class Inspection:
    members: list[Member]
    total_bytes: int
    nested_edz: list[Member]


VOLUME_RE = re.compile(r'(?i)^(.+\.7z)\.(\d{3,})$')


def file_kind(name: str) -> tuple[str | None, str | None, int | None]:
    name = Path(name).name
    m = VOLUME_RE.fullmatch(name)
    if m:
        return 'volume', m.group(1), int(m.group(2))
    if name.lower().endswith('.edz'):
        return 'edz', None, None
    if name.lower().endswith('.7z'):
        return 'archive', None, None
    return None, None, None


def safe_member(path: str) -> bool:
    if not path or any(ord(c) < 32 or ord(c) == 127 for c in path) or len(path) > 600:
        return False
    path = path.replace('\\', '/')
    if path.startswith('/') or path.startswith('~/') or re.match(r'^[A-Za-z]:', path):
        return False
    parts = path.split('/')
    return all(p not in ('', '.', '..') and len(p) <= 255 for p in parts)


def safe_basename(name: str) -> str:
    name = Path(name.replace('\\', '/')).name
    if not name or name in ('.', '..') or not safe_member(name):
        raise ValueError('Unsafe file name: ' + repr(name))
    return name


def _run(args: list[str], *, timeout: int = 3600) -> subprocess.CompletedProcess:
    return subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          timeout=timeout, check=False)


def list_members(archive: Path, sevenzip: str, *, max_files: int, max_bytes: int) -> Inspection:
    p = _run([sevenzip, 'l', '-slt', '-sccUTF-8', str(archive)], timeout=1200)
    if p.returncode != 0:
        raise ValueError(f'7z list failed ({p.returncode}): {p.stderr[-1200:]!r}')
    blocks = re.split(r'\r?\n\s*\r?\n', p.stdout.decode('utf-8', errors='replace'))
    entries = []
    for block in blocks:
        fields = {}
        for line in block.splitlines():
            if ' = ' in line:
                k, val = line.split(' = ', 1)
                fields[k.strip()] = val.strip()
        name = fields.get('Path')
        if not name or 'Size' not in fields:  # archive-level block has no Size
            continue
        if fields.get('Folder') == '+':
            continue
        if not safe_member(name):
            raise ValueError('Unsafe path inside archive: ' + repr(name))
        attr = fields.get('Attributes', '')
        # Symbolic links are not acceptable as data in our extraction workdir.
        if ('Symbolic Link' in block or 'Hard Link' in block or
                re.search(r'(^|\s)l[rwx-]{6,}', attr)):
            raise ValueError('Link entry disallowed: ' + name)
        size = int(fields['Size'])
        if size < 0 or size > max_bytes:
            raise ValueError('Invalid/oversized entry: ' + name)
        entries.append(Member(name, size, attr))
        if len(entries) > max_files:
            raise ValueError('Archive exceeds MAX_ARCHIVE_FILES')
    if not entries:
        raise ValueError('Archive contains no listed files')
    total = sum(e.size for e in entries)
    if total > max_bytes:
        raise ValueError('Archive would decompress more than MAX_UNPACK_GB')
    return Inspection(entries, total, [m for m in entries if m.path.lower().endswith('.edz')])


def test_archive(archive: Path, sevenzip: str) -> bool:
    return _run([sevenzip, 't', '-sccUTF-8', str(archive)], timeout=7200).returncode == 0


def extract_member(archive: Path, sevenzip: str, member: Member, destination: Path, *, max_bytes: int):
    if not safe_member(member.path) or member.size > max_bytes:
        raise ValueError('Unsafe extraction request')
    destination.parent.mkdir(parents=True, exist_ok=True)
    part = destination.with_name(destination.name + '.partial')
    args = [sevenzip, 'x', '-so', '-spd', '-sccUTF-8', str(archive), member.path]
    with part.open('wb') as out:
        process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        total = 0
        try:
            assert process.stdout is not None
            while True:
                chunk = process.stdout.read(256 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > max_bytes:
                    raise ValueError('Extracted member exceeded size cap')
                out.write(chunk)
            if process.wait(timeout=900) != 0:
                raise ValueError('7z extract failed for ' + member.path)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
    if total != member.size:
        part.unlink(missing_ok=True)
        raise ValueError(f'Extracted size differs from listing: {member.path} {total}!={member.size}')
    part.replace(destination)


def read_member(archive: Path, sevenzip: str, member: Member, *, max_bytes: int = 32*1024*1024) -> bytes:
    if member.size > max_bytes:
        raise ValueError('XML entry exceeds read limit')
    # Reading to memory is acceptable for the bounded XML metadata only.
    process = subprocess.Popen([sevenzip, 'x', '-so', '-spd', '-sccUTF-8', str(archive), member.path],
                               stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    try:
        assert process.stdout is not None
        output = process.stdout.read(max_bytes + 1)
        if len(output) > max_bytes:
            raise ValueError('7z metadata output exceeds limit')
        if process.wait(timeout=120) != 0:
            raise ValueError('7z metadata extraction failed')
        return output
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


def free_space_for(path: Path, needed: int, reserve: int):
    root = path if path.exists() else path.parent
    free = shutil.disk_usage(root).free
    if free < needed + reserve:
        raise OSError(f'Not enough disk space: free={free} bytes, required={needed+reserve} bytes')


def sha256_file(path: Path) -> str:
    import hashlib
    digest = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(4*1024*1024), b''):
            digest.update(chunk)
    return digest.hexdigest()
