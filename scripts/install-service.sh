#!/usr/bin/env bash
set -euo pipefail
[[ $(id -u) -ne 0 ]] || { echo 'Run this script as the non-root operator user with sudo rights'; exit 1; }
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
U="$(id -un)"
[[ -f "$ROOT/.env" ]] || { echo 'Missing .env; copy .env.example'; exit 1; }
[[ -f "$ROOT/data/session/edzbot.session" ]] || { echo 'Missing session; run: .venv/bin/python -m edzbot login'; exit 1; }
UNIT="/etc/systemd/system/edz-telegram.service"
sudo tee "$UNIT" >/dev/null <<UNITFILE
[Unit]
Description=EDZ Telegram Automation (Telethon collector and publisher)
After=network-online.target
Wants=network-online.target
[Service]
Type=simple
User=$U
Group=$(id -gn)
WorkingDirectory=$ROOT
ExecStart=$ROOT/.venv/bin/python -m edzbot run
Restart=on-failure
RestartSec=15
TimeoutStopSec=90
UMask=0077
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=$ROOT/data
StandardOutput=journal
StandardError=journal
[Install]
WantedBy=multi-user.target
UNITFILE
sudo systemctl daemon-reload
sudo systemctl enable edz-telegram.service
echo 'Unit installed but NOT started. First verify .env settings and run: sudo systemctl start edz-telegram.service'
