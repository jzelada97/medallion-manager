"""Batch reconciliation job: find probable duplicate persons in the warehouse.

This job scans persons consolidated under different match_keys and detects pairs
that are likely the same individual based on name prefix matching. Results are
stored in the `match_candidates` table for review — never auto-merged.

Run: python -m hr_etl.processing.reconcile
"""

from __future__ import annotations

from sqlalchemy import select, func, and_, delete
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
    """Find probable duplicate pairs between passport-based and name-based records.

    Strategy:
    - For each name-based record, check if any passport record has a full_name
      that is a prefix of it (after normalization).
    - Assign confidence based on how much of the name overlaps.
    """
    # Get all passport-based persons with a name
    passport_persons = (
        session.execute(
            select(PersonRow).where(
                and_(
                    PersonRow.match_key.like("passport:%"),
                    PersonRow.full_name.isnot(None),
                )
            )
        )
        .scalars()
        .all()
    )

    # Build lookup: normalized name -> passport person (for prefix search)
    passport_by_name: dict[str, PersonRow] = {}
    for pp in passport_persons:
        norm = _normalize_name(pp.full_name)
        if norm and len(norm) >= 3:
            passport_by_name[norm] = pp

    # Get all name-based persons
    name_persons = (
        session.execute(select(PersonRow).where(PersonRow.match_key.like("name:%")))
        .scalars()
        .all()
    )

    candidates: list[MatchCandidate] = []

    for np in name_persons:
        np_name = _normalize_name(np.full_name)
        if not np_name or len(np_name) < 3:
            continue

        # Check if any passport name is a prefix of this name-based record
        # Use first 2 words as lookup key for efficiency
        words = np_name.split()
        for length in range(min(len(words), 3), 0, -1):
            prefix = " ".join(words[:length])
            pp = passport_by_name.get(prefix)
            if pp and prefix != np_name:
                confidence = round(len(prefix) / len(np_name), 3)
                if confidence >= min_confidence:
                    candidates.append(
                        MatchCandidate(
                            person_id_a=pp.id,
                            person_id_b=np.id,
                            confidence=confidence,
                            reason=f"name_prefix: '{prefix}' -> '{np_name}'",
                        )
                    )
                break  # found best match, stop

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
