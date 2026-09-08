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
#   3. Refreshes the Gold layer (materialized gold_* tables the API/frontend read) so a
#      fresh schema or Gold-logic change shows up immediately, not 30 min later.
#   4. Prunes dangling images so the boot volume doesn't fill up over time.
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

echo ">> [3/4] Refreshing the Gold layer (gold_persons + gold_duplicate_groups + stats) ..."
# The API/frontend read only the materialized gold_* tables. Refresh them once on every
# deploy so a fresh schema (or a code change to the Gold/duplicate-group logic) is
# reflected immediately, instead of waiting up to 30 min for the maintenance DAG. The
# maintenance DAG still refreshes them periodically afterwards. Idempotent (DELETE+INSERT).
#
# Use a ONE-OFF `run --rm` container (not `exec` on the freshly-recreated `app`): right
# after `up -d --build`, `app` may still be initializing, so `exec` can hit it in a
# transient state and fail silently (which is exactly what left gold_duplicate_groups
# empty on the first deploy of this change). `run --rm` spins a dedicated container that
# waits for its depends_on (postgres healthy) and exits when done. A short readiness wait
# on Postgres is added as belt-and-braces. Never fails the deploy — the periodic
# maintenance DAG recovers on its next cycle if this step errors.
COMPOSE="$DOCKER compose -f docker-compose.yml -f docker-compose.prod.yml"
echo "   waiting for postgres to accept connections ..."
for i in $(seq 1 30); do
  if $COMPOSE exec -T postgres pg_isready -U "${POSTGRES_USER:-hr_user}" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
$COMPOSE run --rm --no-deps app python -m hr_etl.warehouse.gold_layer || \
  echo "   WARNING: gold refresh failed; the maintenance DAG will retry on its next run." >&2

echo ">> [4/4] Pruning dangling images ..."
$DOCKER image prune -f >/dev/null 2>&1 || true

echo ">> Deploy done. Current services:"
$DOCKER compose -f docker-compose.yml -f docker-compose.prod.yml ps
