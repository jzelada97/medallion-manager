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
#   .\scripts\run-airflow-tests.ps1

$ErrorActionPreference = "Stop"

$container = "proyecto1_modulo3_de2-airflow-scheduler-1"
$testFile = "tests/test_persons_threshold.py"

# 1) Is the container running?
$running = docker ps --filter "name=$container" --filter "status=running" --format "{{.Names}}"
if (-not $running) {
    Write-Error "Container '$container' is not running. Start it with: docker compose -f docker-compose.airflow.yml up -d"
    exit 1
}

Write-Host "Copying $testFile into $container ..."
docker cp $testFile "${container}:/tmp/test_persons_threshold.py"

Write-Host "Ensuring pytest is available in the container ..."
docker exec $container bash -lc "python -c 'import pytest' 2>/dev/null || pip install -q pytest"

Write-Host "Running sensor tests inside the container ..."
docker exec $container bash -lc "cd /tmp && PYTHONPATH=/opt/airflow/src python -m pytest test_persons_threshold.py -q"
$code = $LASTEXITCODE

# Cleanup the copied file (best-effort; ignore if perms don't allow it).
docker exec $container bash -lc "rm -f /tmp/test_persons_threshold.py 2>/dev/null || true" | Out-Null

if ($code -eq 0) {
    Write-Host "`nAirflow sensor tests PASSED inside the container." -ForegroundColor Green
} else {
    Write-Host "`nAirflow sensor tests FAILED (exit $code)." -ForegroundColor Red
}
exit $code
