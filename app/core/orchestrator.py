"""Orchestrator — tick loop that drives the pacing/safety/allocation cycle.

Every tick (default 1 s):
1. Reconciler sweep — recover expired leases.
2. Build Snapshot from DB counts and running EWMAs.
3. Pacing strategy proposes a dial count.
4. Safety Controller authorizes (and caps) the proposal.
5. Allocator dials the authorized count.
6. EWMA metrics are updated from DB events since last tick.

N worker threads may run concurrently — correctness comes from the DB, not
from thread-level coordination.  All shared state (EWMAs, k, tick counter) is
protected by a single threading.Lock; the lock is held only for short reads
and writes, never across DB calls.
"""

from __future__ import annotations

import logging
import os
import threading
import time
import uuid
from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from app.core.allocator import CallAllocator
from app.core.pacing.base import Snapshot
from app.core.pacing.predictive import PredictiveStrategy
from app.core.pacing.progressive import ProgressiveStrategy
from app.core.reconciler import release_expired_leases
from app.core.safety import SafetyController
from app.db import SessionLocal
from app.models import Agent, Borrower, Call, PacingDecision

logger = logging.getLogger(__name__)

TICK_INTERVAL: float = float(os.environ.get("TICK_INTERVAL_SECONDS", "1.0"))
EWMA_ALPHA: float = 0.3
ABANDON_WINDOW_SECONDS: int = 60  # rolling window for abandon-rate


class Orchestrator:
    """Drives the SmartDialer tick loop.

    Args:
        mode:          'progressive' or 'predictive'.
        provider_name: Name of the active telecom provider.
        tick_interval: Seconds between ticks (default 1.0).
    """

    def __init__(
        self,
        mode: str = "progressive",
        provider_name: str = "mock_a",
        tick_interval: float = TICK_INTERVAL,
    ) -> None:
        self.mode = mode
        self.provider_name = provider_name
        self.tick_interval = tick_interval

        self._lock = threading.Lock()
        self._running = False
        self._tick = 0
        self._worker_id = f"orchestrator-{uuid.uuid4().hex[:8]}"

        # EWMA state (updated under _lock)
        self._ewma_answer_rate: float = 0.5
        self._ewma_talk_time: float = 60.0
        self._ewma_setup_time: float = 5.0

        # Pacing engines
        self._progressive = ProgressiveStrategy()
        self._predictive = PredictiveStrategy(initial_k=1.0)

        # Allocator + Safety
        self._allocator = CallAllocator(provider=provider_name)
        self._safety = SafetyController(allocator=self._allocator)

        self._thread: threading.Thread | None = None

    # ─────────────────────────────────────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the background tick loop."""
        self._running = True
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="orchestrator-tick"
        )
        self._thread.start()
        logger.info("Orchestrator started (mode=%s, interval=%.1fs)", self.mode, self.tick_interval)

    def stop(self) -> None:
        """Signal the tick loop to stop and wait for it to finish."""
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=self.tick_interval * 3)
        logger.info("Orchestrator stopped at tick %d.", self._tick)

    # ─────────────────────────────────────────────────────────────────────────
    # Main loop
    # ─────────────────────────────────────────────────────────────────────────

    def _loop(self) -> None:
        while self._running:
            start = time.monotonic()
            try:
                self._tick_once()
            except Exception as exc:
                logger.exception("Orchestrator tick error: %s", exc)
            elapsed = time.monotonic() - start
            sleep_time = max(0.0, self.tick_interval - elapsed)
            time.sleep(sleep_time)

    def _tick_once(self) -> None:
        """Execute one full tick."""
        db: Session = SessionLocal()
        try:
            with self._lock:
                self._tick += 1
                tick = self._tick

            # 1. Reconciler sweep.
            release_expired_leases(db)

            # 2. Build Snapshot.
            snapshot = self._build_snapshot(db, tick)

            # 3. Pacing proposal.
            if snapshot.mode == "progressive":
                proposed = self._progressive.propose(snapshot)
            else:
                snapshot.k = self._predictive.k  # type: ignore[misc]
                proposed = self._predictive.propose(snapshot)

            # 4. Safety authorization.
            decision = self._safety.authorize(proposed, snapshot, db)
            db.commit()

            # 5. AIMD adjustment.
            if decision.force_progressive:
                with self._lock:
                    self.mode = "progressive"
            elif snapshot.mode == "predictive":
                if snapshot.abandon_rate_window > 0:
                    self._predictive.on_abandon()
                else:
                    self._predictive.on_clean_tick()

            # 6. Dial authorized count.
            if decision.authorized > 0:
                self._safety.execute_dials(decision.authorized, db, self._worker_id)

            # 7. Update EWMAs from recent DB events.
            self._update_ewmas(db)

            logger.debug(
                "Tick %d | mode=%s | proposed=%d | authorized=%d | reason=%s",
                tick, snapshot.mode, proposed, decision.authorized, decision.reason,
            )
        finally:
            db.close()

    # ─────────────────────────────────────────────────────────────────────────
    # Snapshot construction
    # ─────────────────────────────────────────────────────────────────────────

    def _build_snapshot(self, db: Session, tick: int) -> Snapshot:
        """Query DB counts and assemble a Snapshot for this tick."""
        available = db.query(Agent).filter(Agent.status == "AVAILABLE").count()
        ringing = db.query(Call).filter(Call.status == "RINGING").count()
        connected = db.query(Call).filter(Call.status == "CONNECTED").count()
        answered = db.query(Call).filter(Call.status == "ANSWERED").count()

        # Agents finishing soon (connected and past expected talk time).
        with self._lock:
            ewma_answer_rate = self._ewma_answer_rate
            ewma_talk_time = self._ewma_talk_time
            ewma_setup_time = self._ewma_setup_time
            mode = self.mode
            k = self._predictive.k

        threshold_secs = ewma_talk_time - ewma_setup_time
        threshold_dt = datetime.utcnow() - timedelta(seconds=threshold_secs)
        frees_soon_query = (
            db.query(Call)
            .filter(Call.status == "CONNECTED", Call.answered_at < threshold_dt)
            .count()
        )

        # Abandon rate in rolling window.
        window_start = datetime.utcnow() - timedelta(seconds=ABANDON_WINDOW_SECONDS)
        recent_answered = db.query(Call).filter(
            Call.answered_at > window_start
        ).count()
        recent_abandoned = db.query(Call).filter(
            Call.answered_at > window_start, Call.abandoned == True  # noqa: E712
        ).count()
        abandon_rate = (
            recent_abandoned / recent_answered if recent_answered > 0 else 0.0
        )

        return Snapshot(
            available_agents=available,
            ringing_calls=ringing,
            connected_calls=connected,
            answered_calls=answered,
            ewma_answer_rate=ewma_answer_rate,
            ewma_talk_time=ewma_talk_time,
            ewma_setup_time=ewma_setup_time,
            frees_soon=frees_soon_query,
            abandon_rate_window=abandon_rate,
            provider_healthy=True,  # Circuit breaker integration in Phase 5+
            k=k,
            tick=tick,
            mode=mode,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # EWMA updates
    # ─────────────────────────────────────────────────────────────────────────

    def _update_ewmas(self, db: Session) -> None:
        """Update EWMA metrics from recently completed calls."""
        window_start = datetime.utcnow() - timedelta(seconds=ABANDON_WINDOW_SECONDS)

        completed_calls = (
            db.query(Call)
            .filter(
                Call.status.in_(["COMPLETED", "FAILED"]),
                Call.ended_at > window_start,
                Call.answered_at.is_not(None),
            )
            .all()
        )

        if not completed_calls:
            return

        alpha = EWMA_ALPHA
        with self._lock:
            for call in completed_calls:
                if call.answered_at and call.ended_at:
                    talk = (call.ended_at - call.answered_at).total_seconds()
                    self._ewma_talk_time = (
                        alpha * talk + (1 - alpha) * self._ewma_talk_time
                    )
                if call.created_at and call.answered_at:
                    setup = (call.answered_at - call.created_at).total_seconds()
                    self._ewma_setup_time = (
                        alpha * setup + (1 - alpha) * self._ewma_setup_time
                    )

        # Answer rate: answered / (answered + not_answered) in window.
        total_initiated = db.query(Call).filter(
            Call.created_at > window_start
        ).count()
        total_answered = db.query(Call).filter(
            Call.created_at > window_start,
            Call.answered_at.is_not(None),
        ).count()

        if total_initiated > 0:
            observed_rate = total_answered / total_initiated
            with self._lock:
                self._ewma_answer_rate = (
                    alpha * observed_rate + (1 - alpha) * self._ewma_answer_rate
                )

    # ─────────────────────────────────────────────────────────────────────────
    # Metrics (for API)
    # ─────────────────────────────────────────────────────────────────────────

    def get_metrics(self, db: Session) -> dict:
        """Return current system metrics for the API /metrics endpoint."""
        total = db.query(Call).count()
        connected = db.query(Call).filter(Call.status == "CONNECTED").count()
        completed = db.query(Call).filter(Call.status == "COMPLETED").count()
        failed = db.query(Call).filter(Call.status == "FAILED").count()
        abandoned_count = db.query(Call).filter(Call.abandoned == True).count()  # noqa: E712
        available = db.query(Agent).filter(Agent.status == "AVAILABLE").count()
        busy = db.query(Agent).filter(
            Agent.status.in_(["RESERVED", "DIALING", "CONNECTED", "WRAP_UP"])
        ).count()

        with self._lock:
            ewma_ar = self._ewma_answer_rate
            ewma_tt = self._ewma_talk_time
            k = self._predictive.k
            tick = self._tick
            mode = self.mode

        return {
            "tick": tick,
            "mode": mode,
            "agents": {"available": available, "busy": busy},
            "calls": {
                "total": total,
                "connected": connected,
                "completed": completed,
                "failed": failed,
                "abandoned": abandoned_count,
                "abandon_rate": abandoned_count / max(1, total),
            },
            "ewma": {
                "answer_rate": round(ewma_ar, 4),
                "talk_time_s": round(ewma_tt, 1),
                "k": round(k, 4),
            },
        }
