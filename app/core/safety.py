"""Safety Controller — Phase 3.

The SafetyController is the single authoritative gate between a pacing
strategy's proposal and actual dialling.  It is the **only** component
allowed to invoke the CallAllocator.

Invariants enforced (in order):
1. Provider circuit breaker: if the provider health is broken →  authorize 0.
2. Abandon-rate budget:      if abandon_rate > 3% →  authorize 0 + force
                             progressive fallback mode.
3. Capacity cap:             authorized = min(proposal,
                                              max(0, available + frees_soon
                                                     - expected_answers_ringing))
   where expected_answers_ringing = ringing * ewma_answer_rate.
4. Progressive-mode override: in progressive mode, further cap to available_agents
   (strict 1:1 guarantee).

Every decision writes a ``pacing_decisions`` row so the audit trail is complete.

Architecture constraint (spec §3):
- Allocator is constructor-injected and kept **private**.
- Pacing engines MUST NOT hold a reference to the Allocator.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

from sqlalchemy.orm import Session

from app.core.pacing.base import Snapshot
from app.models import PacingDecision

logger = logging.getLogger(__name__)

ABANDON_RATE_THRESHOLD: float = 0.03


@dataclass
class Decision:
    """Result of a SafetyController authorization check.

    Attributes:
        authorized:      Number of dials approved (0 ≤ authorized ≤ proposal).
        reason:          Short machine-readable reason string for the audit log.
        force_progressive: True when the SafetyController overrides pacing mode
                          to progressive due to an abandon-rate breach.
    """

    authorized: int
    reason: str
    force_progressive: bool = False


class SafetyController:
    """Gates every dial proposal through hard safety invariants.

    The Allocator is injected at construction and kept private — no external
    code should call ``allocator.reserve_pair`` directly.

    Args:
        allocator: The :class:`~app.core.allocator.CallAllocator` instance.
                   Kept private; only SafetyController calls it.
    """

    def __init__(self, allocator, campaign_id: str | None = None) -> None:  # type: ignore[type-arg]
        self._allocator = allocator
        self.campaign_id = campaign_id

    # ─────────────────────────────────────────────────────────────────────────
    # Primary gate
    # ─────────────────────────────────────────────────────────────────────────

    def authorize(self, proposal: int, snapshot: Snapshot, db: Session) -> Decision:
        """Apply all safety invariants and return the approved dial count.

        Side-effects:
            - Writes a ``pacing_decisions`` row to ``db`` (flushed, not committed).

        Args:
            proposal: Raw proposal from a PacingStrategy.
            snapshot: Snapshot used to compute the proposal (same tick).
            db:       Active SQLAlchemy session for audit logging.

        Returns:
            :class:`Decision` with the authorized count and reason.
        """
        force_progressive = False

        # ── Rule 1: Provider circuit breaker ─────────────────────────────────
        if not snapshot.provider_healthy:
            decision = Decision(
                authorized=0,
                reason="provider_circuit_open",
                force_progressive=False,
            )
            self._audit(db, proposal, decision, snapshot)
            return decision

        # ── Rule 2: Abandon-rate budget ───────────────────────────────────────
        if snapshot.abandon_rate_window > ABANDON_RATE_THRESHOLD:
            decision = Decision(
                authorized=0,
                reason="abandon_budget_exceeded",
                force_progressive=True,
            )
            self._audit(db, proposal, decision, snapshot)
            return decision

        # ── Rule 3: Capacity cap ──────────────────────────────────────────────
        expected_answers = snapshot.ringing_calls * snapshot.ewma_answer_rate
        max_safe = max(
            0,
            snapshot.available_agents
            + snapshot.frees_soon
            - int(expected_answers),
        )
        authorized = min(proposal, max_safe)

        # ── Rule 4: Progressive-mode strict 1:1 cap ───────────────────────────
        if snapshot.mode == "progressive":
            authorized = min(authorized, snapshot.available_agents)
            reason = "progressive_1to1"
        else:
            reason = "ok" if authorized == proposal else "capacity_capped"

        decision = Decision(
            authorized=max(0, authorized),
            reason=reason,
            force_progressive=force_progressive,
        )
        self._audit(db, proposal, decision, snapshot)
        return decision

    # ─────────────────────────────────────────────────────────────────────────
    # Allocator delegation (only entry-point to CallAllocator)
    # ─────────────────────────────────────────────────────────────────────────

    def execute_dials(
        self, authorized: int, db: Session, worker_id: str
    ) -> int:
        """Call the allocator up to *authorized* times.

        Args:
            authorized: Number of dials to attempt.
            db:         Active session passed to each reserve_pair call.
            worker_id:  Worker ID for lease attribution.

        Returns:
            Number of successful allocations.
        """
        dialled = 0
        for _ in range(authorized):
            result = self._allocator.reserve_pair(db, worker_id=worker_id)
            if result is None:
                break  # Pool exhausted
            dialled += 1
        return dialled

    # ─────────────────────────────────────────────────────────────────────────
    # Internal audit logging
    # ─────────────────────────────────────────────────────────────────────────

    def _audit(
        self,
        db: Session,
        proposal: int,
        decision: Decision,
        snapshot: Snapshot,
    ) -> None:
        """Write a pacing_decisions row for this tick.

        Flushed (not committed) — the caller controls the transaction boundary.
        """
        row = PacingDecision(
            campaign_id=self.campaign_id,
            tick=snapshot.tick,
            mode=snapshot.mode,
            proposed=proposal,
            authorized=decision.authorized,
            reason=decision.reason,
            inputs_json=json.dumps(snapshot.as_dict()),
        )
        db.add(row)
        try:
            db.flush()
        except Exception as exc:
            logger.warning("Failed to flush pacing_decision audit row: %s", exc)
