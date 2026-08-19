"""Phase 3 — Safety Controller + Pacing Engine tests.

Covers:
- ProgressiveStrategy: always proposes available_agents.
- PredictiveStrategy:  formula, k bounds, AIMD on_abandon / on_clean_tick.
- SafetyController.authorize():
    * proposal exceeding capacity is clamped.
    * abandon-rate breach forces 0 + force_progressive=True.
    * provider circuit open forces 0.
    * progressive mode enforces strict 1:1.
    * every decision writes an audit row to pacing_decisions.
- PacingStrategy Protocol structural subtyping.
"""

from __future__ import annotations

import json
import os
import math

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")

from app.core.pacing.base import PacingStrategy, Snapshot
from app.core.pacing.predictive import PredictiveStrategy, K_MIN, K_MAX
from app.core.pacing.progressive import ProgressiveStrategy
from app.core.safety import ABANDON_RATE_THRESHOLD, Decision, SafetyController
from app.models import Base, PacingDecision


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
def db(engine):
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    session = Session()
    yield session
    session.close()


class _FakeAllocator:
    """Minimal allocator stub for SafetyController tests."""
    def reserve_pair(self, db, worker_id: str):
        return None  # always exhausted


def _safety(db) -> SafetyController:
    return SafetyController(allocator=_FakeAllocator())


def _snap(**kwargs) -> Snapshot:
    """Build a Snapshot with sensible defaults, override with kwargs."""
    defaults = dict(
        available_agents=5,
        ringing_calls=2,
        connected_calls=3,
        frees_soon=1,
        ewma_answer_rate=0.5,
        ewma_talk_time=60.0,
        ewma_setup_time=5.0,
        abandon_rate_window=0.0,
        provider_healthy=True,
        k=1.0,
        tick=1,
        mode="predictive",
    )
    defaults.update(kwargs)
    return Snapshot(**defaults)


# ─────────────────────────────────────────────────────────────────────────────
# Progressive Strategy
# ─────────────────────────────────────────────────────────────────────────────

class TestProgressiveStrategy:
    def test_proposes_available_agents(self):
        ps = ProgressiveStrategy()
        assert ps.propose(_snap(available_agents=7)) == 7

    def test_zero_when_no_agents(self):
        ps = ProgressiveStrategy()
        assert ps.propose(_snap(available_agents=0)) == 0

    def test_non_negative(self):
        ps = ProgressiveStrategy()
        # available_agents should never be negative but guard it anyway.
        s = _snap(available_agents=0)
        assert ps.propose(s) >= 0

    def test_protocol_compliance(self):
        assert isinstance(ProgressiveStrategy(), PacingStrategy)


# ─────────────────────────────────────────────────────────────────────────────
# Predictive Strategy
# ─────────────────────────────────────────────────────────────────────────────

class TestPredictiveStrategy:
    def test_basic_formula(self):
        """dials = floor((A + F) * k / p_hat) - R."""
        ps = PredictiveStrategy(initial_k=1.0)
        snap = _snap(
            available_agents=5, frees_soon=1, ewma_answer_rate=0.5,
            ringing_calls=2, k=1.0
        )
        expected = max(0, math.floor((5 + 1) * 1.0 / 0.5) - 2)  # 12 - 2 = 10
        assert ps.propose(snap) == expected

    def test_never_negative(self):
        ps = PredictiveStrategy(initial_k=0.1)
        snap = _snap(available_agents=0, frees_soon=0, ringing_calls=100, k=0.1)
        assert ps.propose(snap) == 0

    def test_p_hat_floor(self):
        """answer_rate=0 should not cause division by zero; floor=0.05."""
        ps = PredictiveStrategy(initial_k=1.0)
        snap = _snap(available_agents=5, frees_soon=0, ewma_answer_rate=0.0,
                     ringing_calls=0, k=1.0)
        # Should use 0.05 floor: floor(5 * 1.0 / 0.05) = 100
        assert ps.propose(snap) == 100

    def test_aimd_on_abandon_halves_k(self):
        ps = PredictiveStrategy(initial_k=1.0)
        new_k = ps.on_abandon()
        assert new_k == 0.5

    def test_aimd_on_abandon_floor_at_k_min(self):
        ps = PredictiveStrategy(initial_k=K_MIN)
        new_k = ps.on_abandon()
        assert new_k == K_MIN

    def test_aimd_on_clean_tick_increments_k(self):
        ps = PredictiveStrategy(initial_k=0.5)
        new_k = ps.on_clean_tick()
        assert abs(new_k - 0.55) < 1e-9

    def test_aimd_on_clean_tick_ceiling_at_k_max(self):
        ps = PredictiveStrategy(initial_k=K_MAX)
        new_k = ps.on_clean_tick()
        assert new_k == K_MAX

    def test_protocol_compliance(self):
        assert isinstance(PredictiveStrategy(), PacingStrategy)


# ─────────────────────────────────────────────────────────────────────────────
# SafetyController
# ─────────────────────────────────────────────────────────────────────────────

class TestSafetyControllerProviderCircuitBreaker:
    def test_provider_down_authorizes_zero(self, db):
        sc = _safety(db)
        snap = _snap(provider_healthy=False)
        dec = sc.authorize(proposal=10, snapshot=snap, db=db)
        assert dec.authorized == 0
        assert dec.reason == "provider_circuit_open"
        assert not dec.force_progressive

    def test_provider_down_writes_audit_row(self, db):
        sc = _safety(db)
        snap = _snap(provider_healthy=False)
        sc.authorize(proposal=5, snapshot=snap, db=db)
        rows = db.query(PacingDecision).all()
        assert len(rows) == 1
        assert rows[0].authorized == 0
        assert rows[0].reason == "provider_circuit_open"


class TestSafetyControllerAbandonBudget:
    def test_abandon_breach_authorizes_zero(self, db):
        sc = _safety(db)
        snap = _snap(abandon_rate_window=ABANDON_RATE_THRESHOLD + 0.01)
        dec = sc.authorize(proposal=10, snapshot=snap, db=db)
        assert dec.authorized == 0
        assert dec.reason == "abandon_budget_exceeded"
        assert dec.force_progressive is True

    def test_abandon_exactly_at_threshold_passes(self, db):
        sc = _safety(db)
        # Exactly at 3% should NOT trigger the breach (only > 3%)
        snap = _snap(abandon_rate_window=ABANDON_RATE_THRESHOLD)
        dec = sc.authorize(proposal=3, snapshot=snap, db=db)
        # Should not be blocked by abandon budget (may still be capped by capacity)
        assert dec.reason != "abandon_budget_exceeded"

    def test_abandon_breach_audit_row(self, db):
        sc = _safety(db)
        snap = _snap(abandon_rate_window=0.05)
        sc.authorize(proposal=8, snapshot=snap, db=db)
        row = db.query(PacingDecision).first()
        assert row.reason == "abandon_budget_exceeded"
        assert row.authorized == 0
        assert row.proposed == 8


class TestSafetyControllerCapacityCap:
    def test_proposal_clamped_to_capacity(self, db):
        sc = _safety(db)
        # available=5, frees_soon=1, ringing=10, ewma_answer=0.5
        # expected_answers = 10 * 0.5 = 5
        # max_safe = max(0, 5 + 1 - 5) = 1
        snap = _snap(
            available_agents=5, frees_soon=1,
            ringing_calls=10, ewma_answer_rate=0.5,
            mode="predictive",
        )
        dec = sc.authorize(proposal=100, snapshot=snap, db=db)
        assert dec.authorized == 1
        assert dec.reason == "capacity_capped"

    def test_capacity_zero_when_fully_loaded(self, db):
        sc = _safety(db)
        # expected_answers = 20 * 0.5 = 10 > available(5) + frees_soon(0) = 5
        snap = _snap(
            available_agents=5, frees_soon=0,
            ringing_calls=20, ewma_answer_rate=0.5,
            mode="predictive",
        )
        dec = sc.authorize(proposal=10, snapshot=snap, db=db)
        assert dec.authorized == 0

    def test_proposal_passes_through_when_under_cap(self, db):
        sc = _safety(db)
        snap = _snap(
            available_agents=10, frees_soon=5,
            ringing_calls=2, ewma_answer_rate=0.5,
            mode="predictive",
        )
        # max_safe = max(0, 10 + 5 - int(2*0.5)) = 14
        dec = sc.authorize(proposal=5, snapshot=snap, db=db)
        assert dec.authorized == 5
        assert dec.reason == "ok"


class TestSafetyControllerProgressiveMode:
    def test_progressive_caps_to_available_agents(self, db):
        sc = _safety(db)
        snap = _snap(
            available_agents=3, frees_soon=10,
            ringing_calls=0, ewma_answer_rate=0.5,
            mode="progressive",
        )
        dec = sc.authorize(proposal=20, snapshot=snap, db=db)
        # progressive 1:1 caps to available_agents=3
        assert dec.authorized == 3
        assert dec.reason == "progressive_1to1"

    def test_progressive_zero_when_no_agents(self, db):
        sc = _safety(db)
        snap = _snap(available_agents=0, mode="progressive")
        dec = sc.authorize(proposal=5, snapshot=snap, db=db)
        assert dec.authorized == 0

    def test_progressive_abandon_rate_still_blocks(self, db):
        """Abandon budget breach overrides progressive mode too."""
        sc = _safety(db)
        snap = _snap(
            abandon_rate_window=0.05,
            available_agents=5,
            mode="progressive",
        )
        dec = sc.authorize(proposal=3, snapshot=snap, db=db)
        assert dec.authorized == 0
        assert dec.force_progressive is True


class TestSafetyControllerAuditLog:
    def test_every_authorize_writes_one_row(self, db):
        sc = _safety(db)
        for i in range(5):
            sc.authorize(proposal=i, snapshot=_snap(tick=i), db=db)
        rows = db.query(PacingDecision).all()
        assert len(rows) == 5

    def test_audit_row_contains_snapshot_json(self, db):
        sc = _safety(db)
        snap = _snap(tick=42, mode="predictive", available_agents=7)
        sc.authorize(proposal=3, snapshot=snap, db=db)
        row = db.query(PacingDecision).first()
        data = json.loads(row.inputs_json)
        assert data["available_agents"] == 7
        assert data["tick"] == 42
        assert data["mode"] == "predictive"

    def test_audit_row_fields(self, db):
        sc = _safety(db)
        snap = _snap(tick=1, mode="progressive", available_agents=4)
        dec = sc.authorize(proposal=10, snapshot=snap, db=db)
        row = db.query(PacingDecision).first()
        assert row.tick == 1
        assert row.mode == "progressive"
        assert row.proposed == 10
        assert row.authorized == dec.authorized
        assert row.reason == dec.reason
