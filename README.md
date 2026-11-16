# OUMIJIA EDZ Automation — Python v16.3.2

Python implementation of the OUMIJIA EDZ Telegram pipeline (Telethon + SQLite + 7-Zip + edzsplit).
This repository contains **source code only**. No `.env`, Telegram session, SQLite database, EDZ/7z
sample, log, backup or other production data is included.

## Components

- `edzbot/` — application package (CLI in `edzbot/__main__.py`).
  - `bot.py` — Telegram client, source discovery, manual inbox, control commands, v16.3.2 workers
    (Coordinator / Downloader / Processor / Publisher / housekeeping / reconciliation).
  - `pipeline.py`, `pipeline_parallel.py` — sequential and continuous processing modes.
  - `archive.py`, `metadata.py` — 7z/EDZ inspection and extraction.
  - `db.py` — SQLite state access; `config.py` — environment-based configuration.
  - `publish_guard.py`, `rate_schedule.py`, `manual_inbox.py`, `cleanup.py`, `watchdog.py` — safety features.
- `scripts/` — installation helpers; `systemd/edz-telegram.service` — sample unit.
- `tests/` — legacy unit tests from the v15 lineage (kept for reference; see STATUS.md).
- `docs/` — deployment, restore and status notes.

## Quick start (clean environment)

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
# fill API_ID / API_HASH / SOURCE_CHAT / DEST_CHAT / EDZ_INBOX_CHAT / EDZ_NOTIFY_BOT_TOKEN in .env
.venv/bin/python -m edzbot login          # interactive Telegram login (writes local session)
.venv/bin/python -m edzbot dialogs        # verify chat IDs
.venv/bin/python -m edzbot --backfill run # first history inventory
```

Keep `DRY_RUN=true` until the destination and permissions are verified. Never commit `.env`.

## Safety model

One process, one Publisher, one SQLite writer. Ambiguous Telegram sends are held for reconciliation,
never blindly retried; FloodWait is honoured; archive/volume size caps and disk reserve are enforced.
Generated split EDZs are held until explicitly approved after real EPLAN validation.

## Status

See `docs/STATUS.md`. This repository is a source restoration; it does not by itself switch
production from any other implementation.
