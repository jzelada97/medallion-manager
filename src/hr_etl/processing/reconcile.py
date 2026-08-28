"""Batch reconciliation job: find probable duplicate persons in the warehouse.

This job scans persons consolidated under different match_keys and detects pairs
that are likely the same individual based on name prefix matching. Results are
stored in the `match_candidates` table for review — never auto-merged.

Run: python -m hr_etl.processing.reconcile
"""

from __future__ import annotations

from sqlalchemy import select, delete
from sqlalchemy.orm import Session

from hr_etl.logging_conf import get_logger
from hr_etl.models.db_models import MatchCandidate, PersonRow
from hr_etl.processing.normalizer import normalize_text, strip_titles

logger = get_logger(__name__)


def _normalize_name(raw: str | None) -> str:
    """Normalize a name for fuzzy comparison: lowercase, strip accents+titles."""
    if not raw:
        return ""
    return strip_titles(normalize_text(raw))


def find_candidates(session: Session, min_confidence: float = 0.5) -> list[MatchCandidate]:
    """Find probable duplicate pairs in the warehouse.

    Strategies:
    1. Passport-based records whose name is a prefix of a name-based record.
    2. Name-based records whose names are prefixes of each other.

    This covers cases where the same person has fragments under different keys
    due to name inconsistencies (extra surnames, titles that weren't stripped, etc.)
    """
    # Get all persons with a usable name, grouped by key type
    all_persons = (
        session.execute(select(PersonRow).where(PersonRow.full_name.isnot(None))).scalars().all()
    )

    passport_persons = [p for p in all_persons if p.match_key.startswith("passport:")]
    name_persons = [p for p in all_persons if p.match_key.startswith("name:")]

    # Build lookup: normalized name -> person (for passport records)
    passport_by_name: dict[str, PersonRow] = {}
    for pp in passport_persons:
        norm = _normalize_name(pp.full_name)
        if norm and len(norm) >= 3:
            passport_by_name[norm] = pp

    # Build lookup: normalized name -> person (for name records)
    name_by_name: dict[str, PersonRow] = {}
    for np in name_persons:
        norm = _normalize_name(np.full_name)
        if norm and len(norm) >= 3:
            name_by_name[norm] = np

    candidates: list[MatchCandidate] = []
    seen_pairs: set[tuple[int, int]] = set()

    def _add_candidate(a: PersonRow, b: PersonRow, confidence: float, reason: str) -> None:
        pair = (min(a.id, b.id), max(a.id, b.id))
        if pair not in seen_pairs:
            seen_pairs.add(pair)
            candidates.append(
                MatchCandidate(
                    person_id_a=pair[0],
                    person_id_b=pair[1],
                    confidence=confidence,
                    reason=reason,
                )
            )

    # Strategy 1: name-based record whose name starts with a passport record's name
    for np_name, np in name_by_name.items():
        words = np_name.split()
        for length in range(min(len(words), 3), 0, -1):
            prefix = " ".join(words[:length])
            pp = passport_by_name.get(prefix)
            if pp and prefix != np_name:
                confidence = round(len(prefix) / len(np_name), 3)
                if confidence >= min_confidence:
                    _add_candidate(
                        pp, np, confidence, f"passport_prefix: '{prefix}' -> '{np_name}'"
                    )
                break

    # Strategy 2: name-based records that are prefixes of each other
    sorted_names = sorted(name_by_name.keys())
    for i, name_a in enumerate(sorted_names):
        for j in range(i + 1, min(i + 20, len(sorted_names))):  # limit scan window
            name_b = sorted_names[j]
            if name_b.startswith(name_a) and name_a != name_b:
                confidence = round(len(name_a) / len(name_b), 3)
                if confidence >= min_confidence:
                    _add_candidate(
                        name_by_name[name_a],
                        name_by_name[name_b],
                        confidence,
                        f"name_prefix: '{name_a}' -> '{name_b}'",
                    )

    return candidates


def run_reconciliation(session: Session, min_confidence: float = 0.5) -> int:
    """Execute the full reconciliation: clear old candidates, find new ones, persist.

    Returns the number of candidates found.
    """
    # Clear previous candidates (full rebuild each run)
    session.execute(delete(MatchCandidate))
    session.commit()

    candidates = find_candidates(session, min_confidence=min_confidence)

    if candidates:
        session.add_all(candidates)
        session.commit()

    logger.info("reconciliation complete: %d candidates found", len(candidates))
    return len(candidates)


def main() -> None:
    """CLI entrypoint for batch reconciliation."""
    from hr_etl.config import get_settings
    from hr_etl.logging_conf import configure_logging
    from hr_etl.warehouse.engine import create_db_engine, init_schema, make_session_factory

    settings = get_settings()
    configure_logging(settings.log_level)

    engine = create_db_engine(settings.postgres_dsn)
    init_schema(engine)
    session_factory = make_session_factory(engine)
    session = session_factory()

    try:
        count = run_reconciliation(session, min_confidence=0.5)
        print(f"Reconciliation done: {count} candidate pairs found.")
    finally:
        session.close()
        engine.dispose()


if __name__ == "__main__":
    main()
