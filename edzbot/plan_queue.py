"""Conservative opt-in queue intake from a verified local Telegram catalog.

Only standalone original .edz files are eligible in v6. No multipart sets,
archive extraction, Telegram access, publication, or deletion happens here.
This selects ONE row; the service lock serializes producer modifications.
"""
from __future__ import annotations

import csv
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
import shutil
import time

from .archive import file_kind, safe_basename

MIB = 1024 ** 2
GIB = 1024 ** 3
IN_PROGRESS = ("new", "retry", "downloading", "downloaded", "processing")
UNRESOLVED = ("ready", "review", "held", "posting")


@dataclass(frozen=True)
class Candidate:
    message_id: int
    file_name: str
    size_bytes: int


def _read_csv(path: Path, required: set[str]) -> list[dict]:
    # Local metadata from a read-only historical scan, not arbitrary user uploads.
    if not path.is_file() or path.stat().st_size > 16 * MIB:
        raise ValueError(f"Catalog/plan missing or exceeds 16 MiB: {path}")
    with path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames or not required.issubset(reader.fieldnames):
            raise ValueError(f"Missing CSV columns in {path.name}: {sorted(required)}")
        return list(reader)


def batch_usage(db, cfg) -> tuple[int, int]:
    """Count persistently reserved plan sources, including completed/failed ones."""
    used = db.execute(
        'SELECT COUNT(*) FROM plan_batch_sources WHERE batch_id=?',
        (cfg.plan_batch_id,),
    ).fetchone()[0]
    return used, cfg.plan_batch_max_sources


def batch_gate(db, cfg) -> str | None:
    """Require an explicit fresh batch ID; cap survives restart and failed imports."""
    if not cfg.plan_batch_id:
        return 'PLAN_BATCH_ID missing: new plan batch must be explicitly authorized'
    used, limit = batch_usage(db, cfg)
    if used >= limit:
        return f'Plan batch {cfg.plan_batch_id}: {used}/{limit} sources used; no more intake'
    return None


def select_next(db, cfg) -> tuple[Candidate | None, str]:
    """Plan one file, no writes. Return (candidate, reason) or (None, block reason)."""
    if cfg.backfill:
        return None, "BACKFILL_ON_START must remain false for controlled plan intake"
    reason = batch_gate(db, cfg)
    if reason:
        return None, reason

    active = db.execute(
        "SELECT COUNT(*) FROM messages WHERE state IN (?,?,?,?,?)", IN_PROGRESS
    ).fetchone()[0]
    if active:
        return None, f"{active} source message(s) are already pending"

    backlog = db.execute(
        "SELECT COUNT(*) FROM artifacts WHERE state IN (?,?,?,?)", UNRESOLVED
    ).fetchone()[0]
    if backlog >= cfg.plan_max_pending_artifacts:
        return None, (f"{backlog} unpublished/review/held artifacts; "
                      f"limit {cfg.plan_max_pending_artifacts}")

    base = cfg.data / "catalog"
    plan = _read_csv(base / "edz_processing_plan.csv",
                     {"name", "kind", "message_ids", "size_bytes", "decision", "parts"})
    catalog = _read_csv(base / "telegram_edz_catalog.csv",
                        {"filename", "message_id", "size_bytes"})
    by_id: dict[int, dict] = {}
    name_count: Counter[str] = Counter()
    for item in catalog:
        try:
            mid = int(item["message_id"])
            size = int(item["size_bytes"])
            name = item["filename"]
        except (TypeError, ValueError, KeyError):
            continue
        if mid <= 0 or size <= 0 or not isinstance(name, str):
            continue
        # A duplicate row or name is not approved implicitly.
        if mid in by_id:
            by_id[mid] = None
        else:
            by_id[mid] = {"filename": name, "size_bytes": size}
        name_count[name.casefold()] += 1

    options: list[Candidate] = []
    for item in plan:
        if item.get("decision") != "candidate_single" or item.get("kind") != "single":
            continue
        try:
            if int(item["parts"]) != 1:
                continue
            mid = int(item["message_ids"])
            size = int(item["size_bytes"])
            name = item["name"]
            if not (1024 <= size <= cfg.plan_max_single_bytes and size <= cfg.max_group_bytes):
                continue
            if safe_basename(name) != name or file_kind(name)[0] != "edz":
                continue
        except (TypeError, ValueError, KeyError):
            continue
        if name_count[name.casefold()] != 1:
            continue
        row = by_id.get(mid)
        if row is None or row["filename"] != name or row["size_bytes"] != size:
            continue
        if db.message(mid) is not None:
            continue
        options.append(Candidate(mid, name, size))

    if not options:
        return None, "No eligible unqueued standalone EDZ in the local plan"

    # Pilot order prefers manageable ~1 MiB samples, stable tie-break by ID.
    options.sort(key=lambda c: (abs(c.size_bytes - MIB), c.message_id))
    free = shutil.disk_usage(cfg.data).free
    for option in options:
        # Preflight headroom is not a guarantee about archive expansion.
        scratch_budget = max(GIB, option.size_bytes * 10)
        if free >= cfg.reserve_bytes + option.size_bytes + scratch_budget:
            return option, "candidate_single, matching unique Telegram metadata; headroom preflight passed"
    return None, (f"Insufficient free space for configured {cfg.reserve_bytes // GIB} GiB "
                  "reserve plus conservative pilot workspace")


def enqueue_next(db, cfg) -> tuple[Candidate | None, str]:
    """Atomically reserve quota + source; caller owns exclusive edzbot.lock."""
    candidate, reason = select_next(db, cfg)
    if candidate is None:
        return None, reason
    # Savepoint makes the message and its batch reservation indivisible,
    # including unexpected DB errors/crashes between the two INSERTs.
    db.execute('SAVEPOINT plan_batch_enqueue')
    try:
        gate = batch_gate(db, cfg)
        if gate:
            db.execute('RELEASE SAVEPOINT plan_batch_enqueue')
            return None, gate
        db.add_message(candidate.message_id, candidate.file_name,
                       'edz', candidate.size_bytes, None, None)
        if db.execute('SELECT changes()').fetchone()[0] != 1:
            db.execute('ROLLBACK TO SAVEPOINT plan_batch_enqueue')
            db.execute('RELEASE SAVEPOINT plan_batch_enqueue')
            return None, 'Source already indexed; no batch quota consumed'
        db.execute('INSERT INTO plan_batch_sources(batch_id,message_id,queued_at) VALUES(?,?,?)',
                   (cfg.plan_batch_id, candidate.message_id, time.time()))
        db.execute('RELEASE SAVEPOINT plan_batch_enqueue')
    except Exception:
        db.execute('ROLLBACK TO SAVEPOINT plan_batch_enqueue')
        db.execute('RELEASE SAVEPOINT plan_batch_enqueue')
        raise
    return candidate, reason
