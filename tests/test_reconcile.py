"""Unit tests for the batch reconciliation job (processing/reconcile).

These run fully in-memory against SQLite (no Postgres/Mongo needed) using the
shared `sqlite_session_factory` fixture. The reconciliation logic operates on the
`persons` table via the SQLAlchemy ORM, which is portable to SQLite, so no
integration marker is required here.
"""

from __future__ import annotations

from hr_etl.models.db_models import MatchCandidate, PersonRow
from hr_etl.processing.reconcile import (
    _normalize_name,
    find_candidates,
    run_reconciliation,
)


def _add_person(session, match_key: str, full_name: str | None) -> PersonRow:
    row = PersonRow(match_key=match_key, full_name=full_name)
    session.add(row)
    session.flush()  # assign id without committing
    return row


# ----------------------------------------------------------------------
# _normalize_name helper
# ----------------------------------------------------------------------


def test_normalize_name_none_and_empty():
    assert _normalize_name(None) == ""
    assert _normalize_name("") == ""


def test_normalize_name_strips_accents_titles_and_case():
    # lowercase + accents removed + honorific title stripped
    assert _normalize_name("Dr. Álvaro Núñez") == "alvaro nunez"
    assert _normalize_name("  MRS.   Ana  Gil  ") == "ana gil"


# ----------------------------------------------------------------------
# Strategy 1: passport-record name is a prefix of a name-record name
# ----------------------------------------------------------------------


def test_find_candidates_passport_prefix_of_name(sqlite_session_factory):
    session = sqlite_session_factory()
    try:
        pp = _add_person(session, "passport:X1", "octavio ponce")
        np = _add_person(session, "name:octavio ponce gimenez", "octavio ponce gimenez")

        candidates = find_candidates(session, min_confidence=0.5)

        assert len(candidates) == 1
        cand = candidates[0]
        # pair stored sorted (min id, max id)
        assert cand.person_id_a == min(pp.id, np.id)
        assert cand.person_id_b == max(pp.id, np.id)
        assert cand.reason.startswith("passport_prefix:")
        # confidence = len('octavio ponce') / len('octavio ponce gimenez')
        expected = round(len("octavio ponce") / len("octavio ponce gimenez"), 3)
        assert cand.confidence == expected
    finally:
        session.close()


def test_find_candidates_min_confidence_filters_out_weak_pairs(sqlite_session_factory):
    session = sqlite_session_factory()
    try:
        # short passport name ("ana") vs a much longer name record -> low confidence
        _add_person(session, "passport:X1", "ana")
        _add_person(session, "name:ana beatriz carla dominguez", "ana beatriz carla dominguez")

        # default 0.5 should discard this weak match
        assert find_candidates(session, min_confidence=0.5) == []

        # a very permissive threshold should surface it
        loose = find_candidates(session, min_confidence=0.1)
        assert len(loose) == 1
        assert loose[0].confidence < 0.5
    finally:
        session.close()


# ----------------------------------------------------------------------
# Strategy 2: two name-records where one name is a prefix of the other
# ----------------------------------------------------------------------


def test_find_candidates_name_prefix_of_name(sqlite_session_factory):
    session = sqlite_session_factory()
    try:
        a = _add_person(session, "name:maria lopez", "maria lopez")
        b = _add_person(session, "name:maria lopez ruiz", "maria lopez ruiz")

        candidates = find_candidates(session, min_confidence=0.5)

        assert len(candidates) == 1
        cand = candidates[0]
        assert cand.person_id_a == min(a.id, b.id)
        assert cand.person_id_b == max(a.id, b.id)
        assert cand.reason.startswith("name_prefix:")
        expected = round(len("maria lopez") / len("maria lopez ruiz"), 3)
        assert cand.confidence == expected
    finally:
        session.close()


def test_find_candidates_ignores_short_and_null_names(sqlite_session_factory):
    session = sqlite_session_factory()
    try:
        # names shorter than 3 chars are ignored; NULL full_name filtered by query
        _add_person(session, "passport:X1", "ab")
        _add_person(session, "name:ab", "ab")
        _add_person(session, "passport:X2", None)

        assert find_candidates(session) == []
    finally:
        session.close()


def test_find_candidates_no_self_match_for_identical_names(sqlite_session_factory):
    session = sqlite_session_factory()
    try:
        # identical normalized names are not flagged (prefix != full requirement)
        _add_person(session, "passport:X1", "ana gil")
        _add_person(session, "name:ana gil", "ana gil")

        assert find_candidates(session) == []
    finally:
        session.close()


# ----------------------------------------------------------------------
# run_reconciliation: persistence + rebuild semantics
# ----------------------------------------------------------------------


def test_run_reconciliation_persists_candidates(sqlite_session_factory):
    session = sqlite_session_factory()
    try:
        _add_person(session, "passport:X1", "octavio ponce")
        _add_person(session, "name:octavio ponce gimenez", "octavio ponce gimenez")
        session.commit()

        count = run_reconciliation(session, min_confidence=0.5)

        assert count == 1
        stored = session.query(MatchCandidate).all()
        assert len(stored) == 1
        assert stored[0].reason.startswith("passport_prefix:")
    finally:
        session.close()


def test_run_reconciliation_rebuilds_and_clears_previous(sqlite_session_factory):
    session = sqlite_session_factory()
    try:
        # stale candidate left over from a previous run
        session.add(MatchCandidate(person_id_a=99, person_id_b=100, confidence=0.9, reason="stale"))
        session.commit()

        # no matching persons this run -> should end up with zero candidates
        count = run_reconciliation(session, min_confidence=0.5)

        assert count == 0
        assert session.query(MatchCandidate).count() == 0
    finally:
        session.close()


def test_run_reconciliation_empty_warehouse(sqlite_session_factory):
    session = sqlite_session_factory()
    try:
        assert run_reconciliation(session) == 0
    finally:
        session.close()
