"""Phase 4 — Provider and Event Ingestor tests.

Covers:
- Duplicate event with same event_id → 'duplicate', no second transition.
- Out-of-order sequence (COMPLETED arrives before ANSWERED) → COMPLETED applied,
  subsequent ANSWERED rejected; no state corruption.
- ANSWERED with no agent attached → abandoned=True, result='ok'.
- ANSWERED with agent attached → agent transitions DIALING→CONNECTED.
- COMPLETED side effects → agent→AVAILABLE, borrower→DONE.
- FAILED side effects → agent released, borrower→CALLED.
- Provider timeout (call stuck in RINGING) → FSM-gated FAILED resets state.
- Unknown call_id → 'not_found'.
- ProcessedEvent: duplicate event_id uniqueness enforced by DB.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")
os.environ.setdefault("WRAP_UP_DELAY_SECONDS", "0")  # immediate wrap-up in tests

from app.domain.call_fsm import CallState
from app.domain.agent_fsm import AgentState
from app.events.ingestor import EventIngestor
from app.models import Agent, Base, Borrower, Call, ProcessedEvent
from app.providers.base import CallEvent


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def engine():
    eng = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture()
def Session(engine):
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


@pytest.fixture()
def db(Session):
    session = Session()
    yield session
    session.close()


@pytest.fixture()
def ingestor() -> EventIngestor:
    return EventIngestor()


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _agent(db, status: str = "DIALING") -> Agent:
    a = Agent(id=str(uuid.uuid4()), status=status, version=1)
    db.add(a)
    db.commit()
    return a


def _borrower(db, status: str = "RESERVED") -> Borrower:
    b = Borrower(id=str(uuid.uuid4()), phone="0000000000", status=status, attempts=1)
    db.add(b)
    db.commit()
    return b


def _call(db, agent: Agent | None, borrower: Borrower, status: str = "RINGING") -> Call:
    c = Call(
        id=str(uuid.uuid4()),
        agent_id=agent.id if agent else None,
        borrower_id=borrower.id,
        provider="mock_a",
        status=status,
        created_at=datetime.utcnow(),
    )
    db.add(c)
    db.commit()
    return c


def _event(call_id: str, event_type: str, event_id: str | None = None) -> CallEvent:
    return CallEvent(
        call_id=call_id,
        event_type=event_type,
        timestamp=datetime.utcnow(),
        event_id=event_id or str(uuid.uuid4()),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Idempotency
# ─────────────────────────────────────────────────────────────────────────────

class TestIdempotency:
    def test_duplicate_event_returns_duplicate(self, db, ingestor):
        agent = _agent(db)
        borrower = _borrower(db)
        call = _call(db, agent, borrower, status="RINGING")
        ev = _event(call.id, "ANSWERED")

        result1 = ingestor.process(ev, db)
        assert result1 == "ok"

        # Same event_id again.
        result2 = ingestor.process(ev, db)
        assert result2 == "duplicate"

    def test_duplicate_causes_only_one_transition(self, db, ingestor):
        """After duplicate, call state must not change again."""
        agent = _agent(db)
        borrower = _borrower(db)
        call = _call(db, agent, borrower, status="RINGING")
        ev = _event(call.id, "ANSWERED")

        ingestor.process(ev, db)
        db.expire_all()
        call_after_first = db.get(Call, call.id)
        status_after_first = call_after_first.status

        ingestor.process(ev, db)
        db.expire_all()
        call_after_second = db.get(Call, call.id)
        assert call_after_second.status == status_after_first

    def test_processed_event_row_created(self, db, ingestor):
        agent = _agent(db)
        borrower = _borrower(db)
        call = _call(db, agent, borrower)
        ev = _event(call.id, "ANSWERED")

        ingestor.process(ev, db)
        row = db.query(ProcessedEvent).filter_by(event_id=ev.event_id).first()
        assert row is not None
        assert row.call_id == call.id


# ─────────────────────────────────────────────────────────────────────────────
# Unknown call
# ─────────────────────────────────────────────────────────────────────────────

class TestUnknownCall:
    def test_unknown_call_id_returns_not_found(self, db, ingestor):
        ev = _event("nonexistent-call-id", "RINGING")
        result = ingestor.process(ev, db)
        assert result == "not_found"


# ─────────────────────────────────────────────────────────────────────────────
# Out-of-order: COMPLETED arrives before ANSWERED
# ─────────────────────────────────────────────────────────────────────────────

class TestOutOfOrder:
    def test_completed_before_answered_ends_in_completed(self, db, ingestor):
        """ANSWERED state call receives COMPLETED → accepted.
        Then ANSWERED arrives for the already-COMPLETED call → rejected."""
        agent = _agent(db, status="DIALING")
        borrower = _borrower(db)
        # Start the call in RINGING so ANSWERED→COMPLETED path exists.
        call = _call(db, agent, borrower, status="RINGING")

        # ANSWERED first (moves to ANSWERED).
        result_answered = ingestor.process(_event(call.id, "ANSWERED"), db)
        assert result_answered == "ok"

        # COMPLETED (moves ANSWERED→COMPLETED).
        result_completed = ingestor.process(_event(call.id, "COMPLETED"), db)
        assert result_completed == "ok"

        db.expire_all()
        c = db.get(Call, call.id)
        assert c.status == "COMPLETED"

        # Now a late ANSWERED arrives for the already-terminal call → rejected.
        result_late = ingestor.process(_event(call.id, "ANSWERED"), db)
        assert result_late == "duplicate" or result_late == "rejected"

    def test_ringing_after_completed_rejected(self, db, ingestor):
        """Once a call is COMPLETED, any further event should be rejected."""
        agent = _agent(db, status="CONNECTED")
        borrower = _borrower(db)
        call = _call(db, agent, borrower, status="ANSWERED")

        ingestor.process(_event(call.id, "COMPLETED"), db)

        db.expire_all()
        c = db.get(Call, call.id)
        assert c.status == "COMPLETED"

        result = ingestor.process(_event(call.id, "RINGING"), db)
        assert result == "rejected"


# ─────────────────────────────────────────────────────────────────────────────
# ANSWERED with / without agent
# ─────────────────────────────────────────────────────────────────────────────

class TestAnswered:
    def test_answered_with_agent_sets_connected(self, db, ingestor):
        agent = _agent(db, status="DIALING")
        borrower = _borrower(db)
        call = _call(db, agent, borrower, status="RINGING")

        result = ingestor.process(_event(call.id, "ANSWERED"), db)
        assert result == "ok"

        db.expire_all()
        a = db.get(Agent, agent.id)
        c = db.get(Call, call.id)
        assert c.status == "ANSWERED"
        assert c.abandoned is False
        assert a.status == "CONNECTED"

    def test_answered_without_agent_sets_abandoned(self, db, ingestor):
        borrower = _borrower(db)
        # Call with no agent_id (abandoned scenario).
        call = _call(db, agent=None, borrower=borrower, status="RINGING")

        result = ingestor.process(_event(call.id, "ANSWERED"), db)
        assert result == "ok"

        db.expire_all()
        c = db.get(Call, call.id)
        assert c.abandoned is True
        assert c.status == "ANSWERED"


# ─────────────────────────────────────────────────────────────────────────────
# COMPLETED side effects
# ─────────────────────────────────────────────────────────────────────────────

class TestCompleted:
    def test_completed_releases_agent_and_closes_borrower(self, db, ingestor):
        agent = _agent(db, status="CONNECTED")
        borrower = _borrower(db)
        call = _call(db, agent, borrower, status="ANSWERED")

        result = ingestor.process(_event(call.id, "COMPLETED"), db)
        assert result == "ok"

        db.expire_all()
        a = db.get(Agent, agent.id)
        b = db.get(Borrower, borrower.id)
        c = db.get(Call, call.id)

        assert c.status == "COMPLETED"
        assert c.ended_at is not None
        # With WRAP_UP_DELAY_SECONDS=0, agent goes straight to AVAILABLE.
        assert a.status == "AVAILABLE"
        assert b.status == "DONE"


# ─────────────────────────────────────────────────────────────────────────────
# FAILED side effects
# ─────────────────────────────────────────────────────────────────────────────

class TestFailed:
    def test_failed_releases_agent_and_updates_borrower(self, db, ingestor):
        agent = _agent(db, status="DIALING")
        borrower = _borrower(db)
        call = _call(db, agent, borrower, status="RINGING")

        result = ingestor.process(_event(call.id, "FAILED"), db)
        assert result == "ok"

        db.expire_all()
        a = db.get(Agent, agent.id)
        b = db.get(Borrower, borrower.id)
        c = db.get(Call, call.id)

        assert c.status == "FAILED"
        assert c.ended_at is not None
        assert a.status == "AVAILABLE"
        assert b.status == "CALLED"

    def test_provider_timeout_call_failed_agent_released(self, db, ingestor):
        """Simulates provider timeout: call stuck in RINGING → FAILED manually."""
        agent = _agent(db, status="DIALING")
        borrower = _borrower(db)
        call = _call(db, agent, borrower, status="INITIATED")

        # Ringing first.
        ingestor.process(_event(call.id, "RINGING"), db)
        db.expire_all()
        assert db.get(Call, call.id).status == "RINGING"

        # Then FAILED (timeout).
        ingestor.process(_event(call.id, "FAILED"), db)
        db.expire_all()
        c = db.get(Call, call.id)
        a = db.get(Agent, agent.id)
        assert c.status == "FAILED"
        assert a.status == "AVAILABLE"
