#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="${EDZSPLIT_REPO:-https://github.com/wuwenhou/edzsplit.git}"
# Audit target from 2026-11-09; move only after test/review of new versions.
COMMIT="${EDZSPLIT_COMMIT:-5d102d9e8cb6ee707c7e0d56a2679c481548ce35}"
mkdir -p "$ROOT/vendor"
if [[ ! -d "$ROOT/vendor/edzsplit-src/.git" ]]; then
  git clone "$REPO" "$ROOT/vendor/edzsplit-src"
fi
git -C "$ROOT/vendor/edzsplit-src" fetch origin "$COMMIT"
git -C "$ROOT/vendor/edzsplit-src" checkout --detach "$COMMIT"
mkdir -p "$ROOT/vendor/edzsplit"
dotnet publish "$ROOT/vendor/edzsplit-src/src/edzsplit/edzsplit.csproj" \
    -c Release -r linux-x64 --self-contained false -o "$ROOT/vendor/edzsplit"
"$ROOT/vendor/edzsplit/edzsplit" selftest
