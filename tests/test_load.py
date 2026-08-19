"""Phase 5 — Load test and safety invariant verification.

Load test (tests/test_load.py): 1000 agents, 10000 borrowers, assert no double-booked agent 
(no agent with 2 non-terminal calls), no lost borrowers, abandon rate within budget 
in progressive mode = 0.
"""

import threading
import uuid
import os
import pytest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")

from app.core.allocator import CallAllocator
from app.core.safety import SafetyController
from app.core.pacing.base import Snapshot
from app.models import Agent, Borrower, Call, Base


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


def test_high_concurrency_load(Session, db):
    """
    Test with 1000 agents and 10000 borrowers.
    Verify no double-booked agents (concurrent allocations don't stomp each other).
    """
    # Seed
    agents = [Agent(id=str(uuid.uuid4()), status="AVAILABLE", version=0) for _ in range(1000)]
    borrowers = [Borrower(id=str(uuid.uuid4()), phone=f"555{i:04d}", status="PENDING") for i in range(10000)]
    
    # Bulk insert for speed
    db.bulk_save_objects(agents)
    db.bulk_save_objects(borrowers)
    db.commit()

    allocator = CallAllocator(provider="mock_a")
    
    # Run 50 worker threads allocating concurrently
    results = []
    errors = []
    lock = threading.Lock()
    
    def worker(worker_id: str):
        # Each worker tries to allocate 50 times
        session = Session()
        try:
            for _ in range(50):
                res = allocator.reserve_pair(session, worker_id=worker_id)
                if res:
                    with lock:
                        results.append(res)
        except Exception as exc:
            with lock:
                errors.append(exc)
        finally:
            session.close()

    threads = [threading.Thread(target=worker, args=(f"w-{i}",)) for i in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Errors during load test: {errors}"
    
    # We started with 1000 agents, and ran 50 threads * 50 attempts = 2500 attempts.
    # We should have successfully allocated exactly 1000 pairs.
    assert len(results) == 1000, f"Expected 1000 allocations, got {len(results)}"
    
    # Check no double-booked agents
    calls = db.query(Call).all()
    assert len(calls) == 1000
    
    agent_counts = {}
    for c in calls:
        agent_counts[c.agent_id] = agent_counts.get(c.agent_id, 0) + 1
        
    for agent_id, count in agent_counts.items():
        assert count == 1, f"Agent {agent_id} was double-booked! Count: {count}"

    # Check borrowers are reserved
    reserved_borrowers = db.query(Borrower).filter(Borrower.status == "RESERVED").count()
    assert reserved_borrowers == 1000


def test_progressive_zero_abandon_budget(db):
    """
    Ensure in progressive mode, SafetyController never authorizes more than available agents.
    """
    allocator = CallAllocator(provider="mock_a")
    sc = SafetyController(allocator=allocator)
    
    # Progressive mode snapshot
    snap = Snapshot(
        available_agents=10,
        ringing_calls=0,
        connected_calls=0,
        mode="progressive"
    )
    
    # Even if proposed is 100 (which shouldn't happen, but just testing safety gate)
    decision = sc.authorize(proposal=100, snapshot=snap, db=db)
    
    assert decision.authorized == 10, "Must strictly cap to available agents in progressive mode"
