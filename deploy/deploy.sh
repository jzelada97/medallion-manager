#!/usr/bin/env bash
# Idempotent redeploy of the HR Insights ETL stack on the Oracle VM.
#
# Run by the CD workflow (.github/workflows/deploy.yml) over SSH after CI passes,
# or manually on the VM:  bash ~/hr-etl/deploy/deploy.sh
#
# What it does:
#   1. Pulls the latest `main` (fast-forward only, so a dirty tree fails loudly).
#   2. Rebuilds + restarts the query/ingest stack (app, api, frontend get rebuilt;
#      datastores/monitoring reuse their images).
#   3. Prunes dangling images so the boot volume doesn't fill up over time.
#
# Airflow is intentionally NOT restarted here: its DAGs are bind-mounted, so a plain
# `git pull` already refreshes them. Only re-run docker-compose.airflow.yml by hand if
# the Airflow compose file itself changed.
set -euo pipefail

APP_DIR="${APP_DIR:-$HOME/hr-etl}"
cd "$APP_DIR"

echo ">> [1/3] Updating code (git pull --ff-only origin main) ..."
git fetch origin main
git checkout main
git pull --ff-only origin main

echo ">> [2/3] Rebuilding + restarting the main stack ..."
# DOMAIN is required by the prod overlay (Caddy). Read it from .env if present.
if [ -f .env ] && grep -q '^DOMAIN=' .env; then
  export "$(grep '^DOMAIN=' .env | head -n1)"
fi
if [ -z "${DOMAIN:-}" ]; then
  echo "   WARNING: DOMAIN is not set (Caddy needs it). Set it in .env or the environment." >&2
fi

DOCKER="docker"
if ! docker ps >/dev/null 2>&1; then DOCKER="sudo docker"; fi

$DOCKER compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build

echo ">> [3/3] Pruning dangling images ..."
$DOCKER image prune -f >/dev/null 2>&1 || true

echo ">> Deploy done. Current services:"
$DOCKER compose -f docker-compose.yml -f docker-compose.prod.yml ps
