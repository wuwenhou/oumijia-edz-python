# Restoration status

- Restored from the Python source that was operating on host `oumijia` before the Rust migration.
- Version marker found in source: `CONTINUOUS v16.3.2` (`edzbot/__main__.py`) with v16.3.2
  housekeeping/reconciliation workers in `edzbot/bot.py`.
- `tests/` are legacy v15-era unit tests retained for reference. The v16.3.2 build report recorded
  140 offline tests in its build environment; those test files were not present in the restored
  tree and are not recreated here.
- No production data, credentials, session or database is part of this repository.
