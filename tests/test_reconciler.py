"""Tests for the crash-recovery reconciler — zombie call sweep.

Covers:
- test_zombie_call_sweep: CONNECTED call with expired created_at → terminal + agent AVAILABLE.
- test_zombie_ringing_call: RINGING call stuck past TTL → terminal + agent AVAILABLE.
- test_recent_call_not_swept: Call within TTL is NOT swept.
- test_zombie_no_agent: Zombie call with no agent attached → call terminated, borrower reset.
"""

from __future__ import annotations

import threading
from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.reconciler import ZOMBIE_TTL_SECONDS, sweep_zombie_calls
from app.models import Agent, Base, Borrower, Call


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def Session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def _make_agent(db, status: str = "CONNECTED") -> Agent:
    agent = Agent(status=status, version=0)
    db.add(agent)
    db.flush()
    return agent


def _make_borrower(db, status: str = "RESERVED") -> Borrower:
    borrower = Borrower(phone="+15550001234", status=status, attempts=0)
    db.add(borrower)
    db.flush()
    return borrower


def _make_call(db, agent: Agent | None, borrower: Borrower, status: str, age_seconds: int) -> Call:
    """Create a call with created_at set to `age_seconds` ago."""
    call = Call(
        agent_id=agent.id if agent else None,
        borrower_id=borrower.id,
        provider="mock_a",
        status=status,
        created_at=datetime.utcnow() - timedelta(seconds=age_seconds),
    )
    db.add(call)
    db.flush()
    return call


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestZombieCallSweep:

    def test_zombie_call_sweep_connected(self, Session):
        """A CONNECTED call older than ZOMBIE_TTL_SECONDS must be terminated
        and its agent returned to AVAILABLE."""
        db = Session()
        agent = _make_agent(db, status="CONNECTED")
        borrower = _make_borrower(db, status="RESERVED")
        call = _make_call(db, agent, borrower, status="CONNECTED", age_seconds=ZOMBIE_TTL_SECONDS + 60)
        db.commit()

        swept = sweep_zombie_calls(db)
        db.commit()
        db.expire_all()

        # Call must be in a terminal state.
        refreshed_call = db.get(Call, call.id)
        terminal_statuses = {"COMPLETED", "FAILED", "CANCELLED"}
        assert refreshed_call.status in terminal_statuses, (
            f"Expected terminal status, got '{refreshed_call.status}'"
        )
        assert refreshed_call.ended_at is not None

        # Agent must be AVAILABLE.
        refreshed_agent = db.get(Agent, agent.id)
        assert refreshed_agent.status == "AVAILABLE", (
            f"Expected AVAILABLE, got '{refreshed_agent.status}'"
        )
        assert refreshed_agent.lease_expires_at is None
        assert refreshed_agent.worker_id is None

        # Borrower must be re-queued.
        refreshed_borrower = db.get(Borrower, borrower.id)
        assert refreshed_borrower.status == "PENDING"
        assert refreshed_borrower.attempts == 1

        assert swept == 1
        db.close()

    def test_zombie_ringing_call(self, Session):
        """A RINGING call stuck past TTL must also be terminated."""
        db = Session()
        agent = _make_agent(db, status="DIALING")
        borrower = _make_borrower(db, status="RESERVED")
        call = _make_call(db, agent, borrower, status="RINGING", age_seconds=ZOMBIE_TTL_SECONDS + 30)
        db.commit()

        swept = sweep_zombie_calls(db)
        db.commit()
        db.expire_all()

        terminal_statuses = {"COMPLETED", "FAILED", "CANCELLED"}
        assert db.get(Call, call.id).status in terminal_statuses
        assert db.get(Agent, agent.id).status == "AVAILABLE"
        assert swept == 1
        db.close()

    def test_recent_call_not_swept(self, Session):
        """A call within the TTL window must NOT be swept."""
        db = Session()
        agent = _make_agent(db, status="CONNECTED")
        borrower = _make_borrower(db, status="RESERVED")
        # Call created 10 seconds ago — well within TTL.
        call = _make_call(db, agent, borrower, status="CONNECTED", age_seconds=10)
        db.commit()

        swept = sweep_zombie_calls(db)
        db.commit()
        db.expire_all()

        assert db.get(Call, call.id).status == "CONNECTED", "Recent call should not be swept"
        assert db.get(Agent, agent.id).status == "CONNECTED", "Agent should still be CONNECTED"
        assert swept == 0
        db.close()

    def test_zombie_no_agent(self, Session):
        """A zombie call with no attached agent is still terminated."""
        db = Session()
        borrower = _make_borrower(db, status="RESERVED")
        call = _make_call(db, None, borrower, status="RINGING", age_seconds=ZOMBIE_TTL_SECONDS + 60)
        db.commit()

        swept = sweep_zombie_calls(db)
        db.commit()
        db.expire_all()

        terminal_statuses = {"COMPLETED", "FAILED", "CANCELLED"}
        assert db.get(Call, call.id).status in terminal_statuses
        assert db.get(Borrower, borrower.id).status == "PENDING"
        assert swept == 1
        db.close()

    def test_campaign_scoped_sweep(self, Session):
        """sweep_zombie_calls with campaign_id only sweeps that campaign's calls."""
        db = Session()
        age = ZOMBIE_TTL_SECONDS + 60

        # Campaign A zombie.
        ag_a = _make_agent(db, status="CONNECTED")
        br_a = _make_borrower(db)
        call_a = _make_call(db, ag_a, br_a, status="CONNECTED", age_seconds=age)
        call_a.campaign_id = "camp-a"
        ag_a.campaign_id = "camp-a"

        # Campaign B zombie (should NOT be swept).
        ag_b = _make_agent(db, status="CONNECTED")
        br_b = _make_borrower(db)
        call_b = _make_call(db, ag_b, br_b, status="CONNECTED", age_seconds=age)
        call_b.campaign_id = "camp-b"
        ag_b.campaign_id = "camp-b"

        db.commit()

        # Only sweep campaign A.
        swept = sweep_zombie_calls(db, campaign_id="camp-a")
        db.commit()
        db.expire_all()

        terminal_statuses = {"COMPLETED", "FAILED", "CANCELLED"}
        assert db.get(Call, call_a.id).status in terminal_statuses
        assert db.get(Call, call_b.id).status == "CONNECTED", "Campaign B should not be swept"
        assert swept == 1
        db.close()
