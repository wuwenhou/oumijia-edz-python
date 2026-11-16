"""Conservative Telegram intake preflight. No file I/O and no Telegram calls."""
from __future__ import annotations


def capacity_reason(row, members, max_group_bytes: int) -> str | None:
    """Return reason to hold this input, or None if within the input-size cap.

    This does not prove a multipart archive is complete or has enough free
    space after extraction; those are separate safety checks.
    """
    if not members:
        return 'No metadata for source archive'
    if any(m['size_bytes'] <= 0 for m in members):
        return 'Missing/invalid Telegram document size'
    if row['kind'] == 'volume':
        indexes = [m['volume_index'] for m in members]
        if any(not isinstance(i, int) or i < 1 for i in indexes):
            return 'Invalid multi-volume index'
        if len(indexes) != len(set(indexes)):
            return 'Duplicate multi-volume index: manual review required'
    total_bytes = sum(m['size_bytes'] for m in members)
    if total_bytes > max_group_bytes:
        return (f'Source group {total_bytes / 1024**3:.2f} GiB '
                f'exceeds {max_group_bytes / 1024**3:.2f} GiB limit')
    return None
