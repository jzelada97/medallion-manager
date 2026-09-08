"""Unit tests for the reconcile/consolidation/Gold jobs — error/no-op/CLI branches.

These are infra-free (no Postgres): they exercise the fallback and orchestration paths
of the three batch jobs with mocks, so the modules keep meaningful coverage even when
the integration DB is unavailable. The happy-path SQL is covered by the integration
suites (test_reconcile.py, test_consolidate_merge.py, test_gold_layer.py).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from sqlalchemy.exc import SQLAlchemyError

from hr_etl.processing import consolidate_merge, reconcile
from hr_etl.warehouse import gold_layer


# ----------------------------------------------------------------------
# pg_trgm unavailable -> the jobs degrade to a safe no-op (return 0), never crash.
# ----------------------------------------------------------------------


def test_reconcile_noop_when_pg_trgm_unavailable():
    session = MagicMock()
    with patch.object(reconcile, "_ensure_pg_trgm", return_value=False):
        assert reconcile.run_reconciliation(session) == 0
    # pg_trgm unavailable => no-op: it returns BEFORE touching the table, so existing
    # groups are preserved (the delete now lives inside the rebuild transaction).
    session.execute.assert_not_called()


def test_consolidation_noop_when_pg_trgm_unavailable():
    session = MagicMock()
    with patch.object(consolidate_merge, "_ensure_pg_trgm", return_value=False):
        assert consolidate_merge.run_consolidation(session) == 0


def test_ensure_pg_trgm_returns_false_and_rolls_back_on_error():
    session = MagicMock()
    session.execute.side_effect = SQLAlchemyError("no privilege")
    assert reconcile._ensure_pg_trgm(session) is False
    session.rollback.assert_called_once()

    session2 = MagicMock()
    session2.execute.side_effect = SQLAlchemyError("no privilege")
    assert consolidate_merge._ensure_pg_trgm(session2) is False
    session2.rollback.assert_called_once()


def test_ensure_pg_trgm_returns_true_on_success():
    session = MagicMock()
    assert reconcile._ensure_pg_trgm(session) is True
    session.commit.assert_called_once()


# ----------------------------------------------------------------------
# SQLAlchemyError during detection -> rollback + return 0 (no partial write, DP-4).
# ----------------------------------------------------------------------


def test_reconcile_rolls_back_and_returns_zero_on_sql_error():
    session = MagicMock()
    # First execute (delete old groups) is fine; a later one raises.
    session.execute.side_effect = [MagicMock(), SQLAlchemyError("boom")]
    with patch.object(reconcile, "_ensure_pg_trgm", return_value=True):
        assert reconcile.run_reconciliation(session) == 0
    session.rollback.assert_called()


def test_consolidation_rolls_back_and_returns_zero_on_sql_error():
    session = MagicMock()
    session.execute.side_effect = SQLAlchemyError("boom")
    with patch.object(consolidate_merge, "_ensure_pg_trgm", return_value=True):
        assert consolidate_merge.run_consolidation(session) == 0
    session.rollback.assert_called()


# ----------------------------------------------------------------------
# CLI entrypoints (main) — wire settings -> engine -> job, then dispose.
# ----------------------------------------------------------------------


def _run_main(module, run_attr: str):
    """Helper: patch the DB wiring a job's main() imports lazily, then run it."""
    fake_settings = MagicMock(postgres_dsn="postgresql://x/y", log_level="INFO")
    fake_engine = MagicMock()
    fake_session = MagicMock()
    with (
        patch("hr_etl.config.get_settings", return_value=fake_settings),
        patch("hr_etl.logging_conf.configure_logging"),
        patch("hr_etl.warehouse.engine.create_db_engine", return_value=fake_engine),
        patch("hr_etl.warehouse.engine.init_schema"),
        patch("hr_etl.warehouse.engine.make_session_factory", return_value=lambda: fake_session),
        patch.object(module, run_attr, return_value=3) as run_fn,
    ):
        module.main()
    return run_fn, fake_engine


def test_reconcile_main_runs_and_disposes(capsys):
    run_fn, engine = _run_main(reconcile, "run_reconciliation")
    run_fn.assert_called_once()
    engine.dispose.assert_called_once()
    assert "Reconciliation done" in capsys.readouterr().out


def test_consolidate_main_runs_and_disposes(capsys):
    run_fn, engine = _run_main(consolidate_merge, "run_consolidation")
    run_fn.assert_called_once()
    engine.dispose.assert_called_once()
    assert "Consolidation done" in capsys.readouterr().out


def _run_gold_main(summary_row):
    fake_settings = MagicMock(postgres_dsn="postgresql://x/y", log_level="INFO")
    fake_engine = MagicMock()
    fake_conn = MagicMock()
    # `with engine.connect() as conn` -> conn.
    fake_engine.connect.return_value.__enter__.return_value = fake_conn
    fake_conn.execute.return_value.fetchone.return_value = summary_row
    with (
        patch("hr_etl.config.get_settings", return_value=fake_settings),
        patch("hr_etl.logging_conf.configure_logging"),
        patch("hr_etl.warehouse.engine.create_db_engine", return_value=fake_engine),
        patch("hr_etl.warehouse.engine.init_schema"),
        patch.object(gold_layer, "init_gold_schema"),
        patch.object(gold_layer, "refresh_gold", return_value=5) as refresh_fn,
    ):
        gold_layer.main()
    refresh_fn.assert_called_once()
    fake_engine.dispose.assert_called_once()


def test_gold_main_no_summary_row(capsys):
    """main() must not crash when gold_stats has no row yet."""
    _run_gold_main(summary_row=None)


def test_gold_main_prints_summary_when_row_exists(capsys):
    """main() prints the summary block when a gold_stats row is present."""
    row = MagicMock(total_persons=10, with_passport=8, cross_linked=5, avg_completeness=7.5)
    _run_gold_main(summary_row=row)
    out = capsys.readouterr().out
    assert "Gold layer refreshed" in out
    assert "Total persons: 10" in out
