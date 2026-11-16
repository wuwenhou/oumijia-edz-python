"""Local processing pipeline. No Telegram API calls in this module."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import logging
import os
import subprocess
import shutil
import tempfile

from .archive import file_kind, list_members, extract_member, test_archive, free_space_for, sha256_file, safe_basename
from .metadata import read_metadata, folder_segment
from .config import Config
from .db import DB

log = logging.getLogger(__name__)


@dataclass
class Pipeline:
    cfg: Config
    db: DB

    def add_edz(self, file: Path, source_id: int, origin: str, source_filename: str | None = None) -> int:
        info = list_members(file, self.cfg.sevenzip, max_files=self.cfg.max_archive_files, max_bytes=self.cfg.max_unpack_bytes)
        if not test_archive(file, self.cfg.sevenzip):
            raise ValueError('7z integrity test failed on EDZ: ' + file.name)
        metadata = read_metadata(file, info, self.cfg.sevenzip)
        if metadata.part_count > 1:
            return self.split_edz(file, source_id)
        if self.cfg.auto_mode:
            verify = subprocess.run([str(self.cfg.splitter), 'verify', str(file)],
                                    capture_output=True, text=True, timeout=3600)
            if verify.returncode != 0:
                raise ValueError('Automated EDZ structural verification failed: '
                                 + file.name + ': ' + (verify.stdout + verify.stderr)[-700:])
        sha = sha256_file(file)
        # SHA256 is the artifact identity: never create an untracked second copy
        # before INSERT OR IGNORE. An existing held/posting artifact must not be
        # implicitly moved or requeued by a duplicate source.
        existing = self.db.execute('SELECT path,state FROM artifacts WHERE sha256=?', (sha,)).fetchone()
        if existing is not None:
            # OUMIJIA_EDZ_REVIEW_V023: receipt for already-indexed SHA, never re-post.
            from .review_queue import record_output
            record_output(self.db, source_id, sha)
            log.info('Skipping already indexed SHA256 %s (%s); no library copy created', sha, existing['state'])
            return 0
        original_name = safe_basename(source_filename or file.name)
        folder = self.cfg.data / 'library' / folder_segment(metadata.brand) / folder_segment(metadata.group) / sha[:16]
        folder.mkdir(parents=True, exist_ok=True)
        dest = folder / original_name
        copied = False
        if not dest.exists():
            shutil.copy2(file, dest)
            copied = True
        if sha256_file(dest) != sha:
            raise ValueError('SHA256 mismatch on library copy')
        generated = origin == 'generated'
        validation = ('structural_auto_no_eplan_test' if self.cfg.auto_mode
                      else ('structural_unproven_import' if generated else 'source_original'))
        status = 'ready' if self.cfg.auto_mode else ('review' if generated else 'ready')
        inserted = self.db.put_artifact(sha=sha, file_name=original_name, path=str(dest),
             size=dest.stat().st_size, source_id=source_id, origin=origin,
             brand=metadata.brand, part_number=metadata.part, validation=validation, state=status)
        if not inserted and copied:
            # A concurrent insert won. Only remove a library file if no DB row
            # references this exact path; never delete another producer's copy.
            referenced = self.db.execute('SELECT 1 FROM artifacts WHERE path=?', (str(dest),)).fetchone()
            if referenced is None and dest.is_file() and sha256_file(dest) == sha:
                dest.unlink()
        # OUMIJIA_EDZ_REVIEW_V023: record only verified outputs, including dedupe.
        from .review_queue import record_output
        record_output(self.db, source_id, sha)
        return int(bool(inserted))

    def split_edz(self, file: Path, source_id: int) -> int:
        if not self.cfg.splitter.exists():
            raise RuntimeError(f'edzsplit not installed: {self.cfg.splitter}; run scripts/install-edzsplit.sh')
        sha = sha256_file(file)
        work_root = self.cfg.data / 'work'
        work_root.mkdir(parents=True, exist_ok=True)
        # preflight first, and reserve space for archive decompression + output.
        inspected = list_members(file, self.cfg.sevenzip,
                    max_files=self.cfg.max_archive_files, max_bytes=self.cfg.max_unpack_bytes)
        free_space_for(work_root, inspected.total_bytes + max(file.stat().st_size, inspected.total_bytes),
                       self.cfg.reserve_bytes)
        # OUMIJIA publishes one EDZ per part, so each split output must contain
        # exactly one `part` package. edzsplit-rs v0.2.3 guarantees this with
        # --strict-part; the legacy C# binary has no such flag (it keeps the
        # full accessory-closure, i.e. possibly >1 part package, which used to
        # hold every multi-part source). EDZSPLIT_STRICT_PART=false restores
        # the old behaviour for that binary, fail-closed.
        log_path = self.cfg.data / 'logs' / f'split-{sha[:16]}.jsonl'
        # A fresh run-owned workspace prevents old partial .edz outputs from
        # being globbed into a new success. Temporary files are removed after
        # a complete copy and registration; evidence JSONL stays in logs.
        with tempfile.TemporaryDirectory(prefix=f'split-{sha[:16]}-', dir=work_root) as workspace:
            target = Path(workspace)
            command = [str(self.cfg.splitter), 'split', str(file), '--out', str(target),
                       '--closure', 'full', '--fix-refs', self.cfg.fix_refs]
            if self.cfg.strict_part:
                command.append('--strict-part')
            command += ['--log', str(log_path)]
            proc = subprocess.run(command, capture_output=True, text=True, timeout=3600*12)
            if proc.returncode == 2 and self.cfg.strict_part:
                raise RuntimeError('edzsplit rejected its arguments (exit 2), most likely because this build '
                                   'does not support --strict-part. Install edzsplit-rs v0.2.3 '
                                   '(vendor/edzsplit-rs/v0.2.3/edzsplit, SHA256-verified) and point EDZSPLIT_BIN '
                                   'at it, or set EDZSPLIT_STRICT_PART=false to keep the legacy splitter. '
                                   f'Output: {(proc.stdout + proc.stderr)[-900:]}')
            if proc.returncode not in (0, 3):
                raise RuntimeError(f'edzsplit failure ({proc.returncode}), evidence log {log_path}: '
                                   f'{(proc.stdout+proc.stderr)[-1800:]}')
            if self.cfg.auto_mode and proc.returncode != 0:
                raise RuntimeError('Splitter reported partial success; do not publish incomplete results '
                                   f'(full evidence in {log_path}): '
                                   + (proc.stdout + proc.stderr)[-900:])
            outputs = sorted(target.glob('*.edz'))
            if self.cfg.auto_mode and not outputs:
                raise RuntimeError('Splitter produced zero EDZ files')
            registered = 0
            for child in outputs:
                # edzsplit-rs v0.2.3 enforces --verify-structural on every output; the
                # legacy C# binary did not. Either way, verify each EDZ independently and
                # never mark as ready for posting by default.
                inspected_child = list_members(child, self.cfg.sevenzip,
                           max_files=self.cfg.max_archive_files, max_bytes=self.cfg.max_unpack_bytes)
                if not test_archive(child, self.cfg.sevenzip):
                    if self.cfg.auto_mode:
                        raise ValueError('Corrupt split output: ' + str(child))
                    log.error('Corrupt split output: %s', child)
                    continue
                verify = subprocess.run([str(self.cfg.splitter), 'verify', str(child)],
                                        capture_output=True, text=True, timeout=3600)
                if verify.returncode != 0:
                    if self.cfg.auto_mode:
                        raise ValueError('Generated EDZ structural verification failed: ' + str(child)
                                         + ' ' + (verify.stdout + verify.stderr)[-500:])
                    log.error('Generated EDZ structural verification failed %s: %s', child, verify.stdout[-500:])
                    continue
                meta = read_metadata(child, inspected_child, self.cfg.sevenzip)
                if meta.part_count != 1:
                    # Keep the exact prefix: the review vault classifies this as
                    # SPLITTER_UNEXPECTED_MULTIPART. With --strict-part this indicates
                    # a splitter/config mismatch, not just a large source.
                    detail = (f'Unexpected multi-part output: {child} '
                              f'(part_count={meta.part_count}, strict_part={self.cfg.strict_part}, '
                              f'evidence={log_path})')
                    if self.cfg.auto_mode:
                        raise ValueError(detail)
                    log.error('%s', detail)
                    continue
                registered += self.add_edz(child, source_id, 'generated')
            log.info('Splitter registered %d EDZ outputs from %s', registered, file.name)
            return registered

    def process_input(self, file: Path, source_id: int, kind: str, original_name: str):
        if kind == 'edz':
            inspection = list_members(file, self.cfg.sevenzip,
                max_files=self.cfg.max_archive_files, max_bytes=self.cfg.max_unpack_bytes)
            # A file ending in .edz can itself be a bundle of pre-existing EDZ files.
            if inspection.nested_edz:
                return self.process_input(file, source_id, 'archive', original_name)
            return self.add_edz(file, source_id, 'direct', original_name)
        if kind not in ('archive', 'volume'):
            raise ValueError('Unsupported file type: ' + kind)
        listing = list_members(file, self.cfg.sevenzip,
                    max_files=self.cfg.max_archive_files, max_bytes=self.cfg.max_unpack_bytes)
        if not test_archive(file, self.cfg.sevenzip):
            raise ValueError('Source 7z failed integrity test or volume set incomplete')
        if not listing.nested_edz:
            raise ValueError('7z contains no ready-made EDZ; requires manual inspection')
        # Archive only contains ready-made EDZ. Extract each entry to a sandboxed name,
        # NOT via 7z x into an untrusted path tree.
        output_dir = self.cfg.data / 'work' / ('nested-' + sha256_file(file)[:18])
        output_dir.mkdir(parents=True, exist_ok=True)
        free_space_for(output_dir, sum(m.size for m in listing.nested_edz), self.cfg.reserve_bytes)
        success = 0
        for n, member in enumerate(listing.nested_edz):
            file_name = safe_basename(member.path)
            dest = output_dir / f'{n:06d}' / file_name
            if not dest.exists() or dest.stat().st_size != member.size:
                extract_member(file, self.cfg.sevenzip, member, dest, max_bytes=self.cfg.max_unpack_bytes)
            try:
                success += self.add_edz(dest, source_id, 'nested', file_name)
            except Exception:
                log.exception('Failed nested EDZ: %s', member.path)
                if self.cfg.auto_mode:
                    raise  # fail closed: no partial publication of the source
        if success == 0:
            log.info('Archive contains only already-indexed EDZ files: %s', file.name)
        return success
