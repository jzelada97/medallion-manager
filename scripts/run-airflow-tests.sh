#!/usr/bin/env bash
# Run the deferrable-sensor tests INSIDE the Airflow container.
#
# These tests (tests/test_persons_threshold.py) need the real apache-airflow package,
# which is intentionally NOT installed in the local dev venv (it's heavy and version
# fragile). So locally they self-skip via pytest.importorskip. This script runs them
# where Airflow actually lives — the running Airflow container — so the sensor/trigger
# logic is verified end to end.
#
# Prereq: the Airflow stack must be up:
#   docker compose -f docker-compose.airflow.yml up -d
#
# Usage:
#   ./scripts/run-airflow-tests.sh
set -euo pipefail

CONTAINER="proyecto1_modulo3_de2-airflow-scheduler-1"
TEST_FILE="tests/test_persons_threshold.py"

if [ -z "$(docker ps --filter "name=${CONTAINER}" --filter "status=running" --format '{{.Names}}')" ]; then
  echo "Container '${CONTAINER}' is not running. Start it with: docker compose -f docker-compose.airflow.yml up -d" >&2
  exit 1
fi

echo "Copying ${TEST_FILE} into ${CONTAINER} ..."
docker cp "${TEST_FILE}" "${CONTAINER}:/tmp/test_persons_threshold.py"

echo "Ensuring pytest is available in the container ..."
docker exec "${CONTAINER}" bash -lc "python -c 'import pytest' 2>/dev/null || pip install -q pytest"

echo "Running sensor tests inside the container ..."
set +e
docker exec "${CONTAINER}" bash -lc "cd /tmp && PYTHONPATH=/opt/airflow/src python -m pytest test_persons_threshold.py -q"
code=$?
set -e

docker exec "${CONTAINER}" bash -lc "rm -f /tmp/test_persons_threshold.py" >/dev/null 2>&1 || true

if [ "$code" -eq 0 ]; then
  echo -e "\nAirflow sensor tests PASSED inside the container."
else
  echo -e "\nAirflow sensor tests FAILED (exit $code)."
fi
exit $code
