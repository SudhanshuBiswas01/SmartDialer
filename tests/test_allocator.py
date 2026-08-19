"""Phase 2 — Allocator + Reconciler unit/integration tests.

Tests:
- test_race           : 20 threads fight over 1 agent + 10 borrowers.
                        Exactly 1 wins; agent version incremented exactly once.
- test_atomicity      : Fail the borrower update → full rollback (no orphaned reservation).
- test_lease_recovery : Manually expire a lease → reconciler restores consistent state.
- test_no_agent       : reserve_pair returns None when no AVAILABLE agents exist.
- test_no_borrower    : reserve_pair returns None (and undoes agent hold) when no PENDING borrowers.
"""

from __future__ import annotations

import os
import threading
import uuid
from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

# Force in-memory SQLite for all tests in this module.
os.environ["DATABASE_URL"] = "sqlite:///:memory:"

from app.core.allocator import LEASE_TTL_SECONDS, CallAllocator
from app.core.reconciler import release_expired_leases
from app.db import init_db
from app.models import Agent, Base, Borrower, Call


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def engine():
    """Fresh in-memory SQLite engine per test.

    StaticPool ensures ALL connections (including from spawned threads) share
    the same in-memory database, which is required for concurrency tests.
    check_same_thread=False is needed for the race test's worker threads.
    """
    eng = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    from sqlalchemy import event
    @event.listens_for(eng, "connect")
    def _pragma(dbapi_conn, _rec):
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()

    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture()
def Session(engine):
    """Session factory bound to the per-test engine."""
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


@pytest.fixture()
def db(Session):
    """A single session that is always rolled back after the test."""
    session = Session()
    yield session
    session.close()


def _make_agent(db, status: str = "AVAILABLE") -> Agent:
    a = Agent(id=str(uuid.uuid4()), status=status, version=0)
    db.add(a)
    db.commit()
    return a


def _make_borrower(db, status: str = "PENDING") -> Borrower:
    b = Borrower(id=str(uuid.uuid4()), phone="0000000000", status=status, attempts=0)
    db.add(b)
    db.commit()
    return b


# ─────────────────────────────────────────────────────────────────────────────
# Basic happy-path
# ─────────────────────────────────────────────────────────────────────────────

class TestAllocatorBasic:
    def test_reserve_pair_success(self, Session):
        db = Session()
        agent = _make_agent(db)
        borrower = _make_borrower(db)
        allocator = CallAllocator(provider="mock_a")

        result = allocator.reserve_pair(db, worker_id="w-1")

        assert result is not None
        assert result.agent_id == agent.id
        assert result.borrower_id == borrower.id
        assert result.call_id is not None

        db.expire_all()
        a = db.get(Agent, agent.id)
        b = db.get(Borrower, borrower.id)
        c = db.get(Call, result.call_id)

        assert a.status == "RESERVED"
        assert a.version == 1
        assert a.worker_id == "w-1"
        assert a.lease_expires_at is not None

        assert b.status == "RESERVED"

        assert c.status == "RESERVED"
        assert c.agent_id == agent.id
        assert c.borrower_id == borrower.id

        db.close()

    def test_no_agent_returns_none(self, Session):
        db = Session()
        _make_borrower(db)
        allocator = CallAllocator()
        result = allocator.reserve_pair(db, worker_id="w-1")
        assert result is None
        db.close()

    def test_no_borrower_returns_none_and_releases_agent(self, Session):
        db = Session()
        agent = _make_agent(db)
        allocator = CallAllocator()
        result = allocator.reserve_pair(db, worker_id="w-1")
        assert result is None

        db.expire_all()
        a = db.get(Agent, agent.id)
        assert a.status == "AVAILABLE", "Agent should be released when no borrower found"
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# Race test — 20 threads fight over 1 agent + 10 borrowers
# ─────────────────────────────────────────────────────────────────────────────

class TestRace:
    def test_race(self, Session):
        """Exactly one thread must win; agent version incremented exactly once."""
        db_setup = Session()
        agent = _make_agent(db_setup)
        for _ in range(10):
            _make_borrower(db_setup)
        db_setup.close()

        results: list[object] = []
        errors: list[Exception] = []
        lock = threading.Lock()

        def worker(worker_id: str) -> None:
            session = Session()
            try:
                allocator = CallAllocator(provider="mock_a")
                res = allocator.reserve_pair(session, worker_id=worker_id)
                with lock:
                    results.append(res)
            except Exception as exc:
                with lock:
                    errors.append(exc)
            finally:
                session.close()

        threads = [
            threading.Thread(target=worker, args=(f"w-{i}",)) for i in range(20)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Worker errors: {errors}"

        successes = [r for r in results if r is not None]
        assert len(successes) == 1, (
            f"Expected exactly 1 successful allocation, got {len(successes)}"
        )

        # Verify agent version incremented exactly once.
        verify_db = Session()
        a = verify_db.get(Agent, agent.id)
        assert a.version == 1, f"Expected version=1, got {a.version}"
        assert a.status == "RESERVED"
        verify_db.close()


# ─────────────────────────────────────────────────────────────────────────────
# Atomicity test
# ─────────────────────────────────────────────────────────────────────────────

class TestAtomicity:
    def test_atomicity_on_borrower_failure(self, Session, monkeypatch):
        """If borrower reservation fails, the agent reservation is rolled back."""
        db_setup = Session()
        agent = _make_agent(db_setup)
        borrower = _make_borrower(db_setup)
        db_setup.close()

        from sqlalchemy import update as sa_update
        original_execute = Session().__class__.execute

        call_count = {"n": 0}

        def patched_execute(self, stmt, *args, **kwargs):
            # Let the agent UPDATE through (call 1), then fail on the borrower UPDATE (call 2).
            if hasattr(stmt, "table") and getattr(stmt.table, "name", None) == "borrowers":
                call_count["n"] += 1
                if call_count["n"] == 1:
                    raise RuntimeError("Simulated DB failure on borrower update")
            return original_execute(self, stmt, *args, **kwargs)

        # Re-create the session to avoid interfering with setup.
        session = Session()
        monkeypatch.setattr(session.__class__, "execute", patched_execute)

        allocator = CallAllocator(provider="mock_a")
        with pytest.raises(RuntimeError, match="Simulated DB failure"):
            allocator.reserve_pair(session, worker_id="w-fail")

        # The session should have been rolled back by the allocator.
        session.close()

        verify = Session()
        a = verify.get(Agent, agent.id)
        b = verify.get(Borrower, borrower.id)
        calls = verify.query(Call).all()

        assert a.status == "AVAILABLE", "Agent must be rolled back to AVAILABLE"
        assert a.version == 0, "Version must not be incremented"
        assert b.status == "PENDING", "Borrower must not be modified"
        assert len(calls) == 0, "No call row should have been created"
        verify.close()


# ─────────────────────────────────────────────────────────────────────────────
# Lease recovery test
# ─────────────────────────────────────────────────────────────────────────────

class TestLeaseRecovery:
    def test_lease_recovery(self, Session):
        """Manually expire a lease; reconciler restores consistent state."""
        db = Session()
        # Create an agent that is already RESERVED with an expired lease.
        expired = datetime.utcnow() - timedelta(seconds=LEASE_TTL_SECONDS + 60)
        agent = Agent(
            id=str(uuid.uuid4()),
            status="RESERVED",
            version=1,
            worker_id="dead-worker",
            lease_expires_at=expired,
        )
        db.add(agent)

        borrower = Borrower(
            id=str(uuid.uuid4()),
            phone="9999999999",
            status="RESERVED",
            attempts=1,
        )
        db.add(borrower)

        call = Call(
            id=str(uuid.uuid4()),
            agent_id=agent.id,
            borrower_id=borrower.id,
            provider="mock_a",
            status="RESERVED",
            created_at=datetime.utcnow() - timedelta(minutes=5),
        )
        db.add(call)
        db.commit()

        # Run the reconciler.
        recovered = release_expired_leases(db)
        assert recovered == 1

        db.expire_all()
        a = db.get(Agent, agent.id)
        b = db.get(Borrower, borrower.id)
        c = db.get(Call, call.id)

        # Agent restored.
        assert a.status == "AVAILABLE", f"Expected AVAILABLE, got {a.status}"
        assert a.lease_expires_at is None
        assert a.worker_id is None

        # Call cancelled.
        assert c.status in ("CANCELLED", "FAILED"), (
            f"Expected terminal status, got {c.status}"
        )
        assert c.ended_at is not None

        # Borrower re-queued with incremented attempts.
        assert b.status == "PENDING", f"Expected PENDING, got {b.status}"
        assert b.attempts == 2, f"Expected attempts=2, got {b.attempts}"

        db.close()

    def test_no_expired_leases(self, Session):
        """No-op when no leases are expired."""
        db = Session()
        _make_agent(db)  # status=AVAILABLE, no lease
        recovered = release_expired_leases(db)
        assert recovered == 0
        db.close()

    def test_dialing_agent_recovered(self, Session):
        """A DIALING agent with expired lease is also recovered."""
        db = Session()
        expired = datetime.utcnow() - timedelta(seconds=120)
        agent = Agent(
            id=str(uuid.uuid4()),
            status="DIALING",
            version=2,
            worker_id="crashed",
            lease_expires_at=expired,
        )
        db.add(agent)

        borrower = Borrower(
            id=str(uuid.uuid4()), phone="1111111111", status="RESERVED", attempts=0
        )
        db.add(borrower)

        call = Call(
            id=str(uuid.uuid4()),
            agent_id=agent.id,
            borrower_id=borrower.id,
            provider="mock_a",
            status="INITIATED",
            created_at=datetime.utcnow() - timedelta(minutes=3),
        )
        db.add(call)
        db.commit()

        recovered = release_expired_leases(db)
        assert recovered == 1

        db.expire_all()
        a = db.get(Agent, agent.id)
        assert a.status == "AVAILABLE"
        db.close()
