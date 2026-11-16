# Deploy / restore notes

1. Install system tools: `7z` (p7zip-full), `python3-venv`, and `edzsplit` (see `scripts/install-edzsplit.sh`).
2. Create a fresh venv and install exactly `requirements.txt` (Telethon 1.x, python-dotenv, defusedxml).
3. Copy `.env.example` to `.env` and fill secrets locally. Keep the file mode 0600 and out of git.
4. Log in once with `python -m edzbot login` (produces a local Telegram session, never committed).
5. Verify chat IDs with `python -m edzbot dialogs`, keep `DRY_RUN=true` until confirmed.
6. Install the sample unit from `systemd/` after adjusting paths. It intentionally does **not** start
   the service automatically.
7. For database compatibility with a production SQLite previously used by another implementation,
   take a consistent snapshot first and test only on a copy. Do not downgrade or rewrite `posted`
   artifact rows. See the operator runbook for migration/compatibility checks.
