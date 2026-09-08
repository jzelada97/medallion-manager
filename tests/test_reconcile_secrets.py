"""SEC-T7 — no hardcoded DSN/credentials in the reconcile/Gold subsystem source, and
`.env` is git-ignored (security-reconcile.md CS-1/CS-2).

This is a static, infra-free test (no Postgres needed): it greps the subsystem source
files for a hardcoded connection string / password and checks .gitignore. It guards the
"secrets by env var only" rule from creeping back into the batch jobs.
"""

from __future__ import annotations

import re
from pathlib import Path

# Repo root = two levels up from this file (tests/ -> repo root).
_ROOT = Path(__file__).resolve().parents[1]

# Subsystem source files in scope of this QA pass.
_SUBSYSTEM_SOURCES = [
    _ROOT / "src" / "hr_etl" / "processing" / "consolidate_merge.py",
    _ROOT / "src" / "hr_etl" / "processing" / "reconcile.py",
    _ROOT / "src" / "hr_etl" / "processing" / "sql_norm.py",
    _ROOT / "src" / "hr_etl" / "processing" / "normalizer.py",
    _ROOT / "src" / "hr_etl" / "warehouse" / "gold_layer.py",
    _ROOT / "src" / "hr_etl" / "warehouse" / "migrations" / "001_reconcile.sql",
]

# A postgres DSN with an inline password, e.g. postgresql://user:secret@host/db.
_DSN_WITH_PASSWORD = re.compile(r"postgres(?:ql)?(?:\+\w+)?://[^\s:]+:[^\s@]+@", re.IGNORECASE)


def test_sec_t7_no_hardcoded_dsn_in_subsystem_sources():
    offenders = []
    for path in _SUBSYSTEM_SOURCES:
        assert path.exists(), f"expected subsystem source missing: {path}"
        text = path.read_text(encoding="utf-8")
        if _DSN_WITH_PASSWORD.search(text):
            offenders.append(path.name)
    assert not offenders, f"hardcoded DSN/credentials found in: {offenders}"


def test_sec_t7_env_is_gitignored():
    gitignore = (_ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    entries = {line.strip() for line in gitignore}
    assert ".env" in entries, ".env must be git-ignored (secrets never committed)"
