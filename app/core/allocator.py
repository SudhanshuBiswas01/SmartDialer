"""Atomic (agent, borrower) reservation and call creation — Phase 2.

The core concurrency primitive used throughout SmartDialer.  A single call to
:meth:`CallAllocator.reserve_pair` performs an indivisible database transaction:

1. Find the longest-waiting AVAILABLE agent.
2. Lock it via an optimistic-locking conditional UPDATE (version bump + lease).
3. Find the oldest PENDING borrower.
4. Lock it the same way.
5. Insert a ``calls`` row (QUEUED → RESERVED via the FSM).

If any step races or fails the transaction is rolled back completely — there
are never half-reserved pairs.

Design constraints (spec §3)
------------------------------
- The Pacing Engine **must never** hold a reference to the Allocator.
- Only the SafetyController is permitted to call ``reserve_pair`` — enforced
  by construction (Phase 3 wires this up).
- No Redis, Celery, or external queues.  The DB is the only coordination point.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta

from sqlalchemy import text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.domain import agent_fsm, call_fsm
from app.models import Agent, Borrower, Call

logger = logging.getLogger(__name__)

# How long a reservation lease is valid before the reconciler can steal it.
LEASE_TTL_SECONDS: int = 30

# Maximum retries on an optimistic-lock race before giving up.
MAX_RETRIES: int = 3


class AllocationResult:
    """Returned by :meth:`CallAllocator.reserve_pair` on success."""

    __slots__ = ("agent_id", "borrower_id", "call_id")

    def __init__(self, agent_id: str, borrower_id: str, call_id: str) -> None:
        self.agent_id = agent_id
        self.borrower_id = borrower_id
        self.call_id = call_id

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"AllocationResult(agent={self.agent_id!r}, "
            f"borrower={self.borrower_id!r}, call={self.call_id!r})"
        )


class CallAllocator:
    """Reserves an (agent, borrower) pair and creates a call record atomically.

    Usage::

        allocator = CallAllocator(provider="mock_a")
        result = allocator.reserve_pair(session, worker_id="worker-1")
        if result:
            # proceed to dial
            ...

    All DB interactions happen through the ``Session`` passed in so that the
    caller controls transaction boundaries.
    """

    def __init__(self, provider: str = "mock_a") -> None:
        """Initialise the allocator.

        Args:
            provider: Name of the telecom provider to record on new call rows.
        """
        self.provider = provider

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    def reserve_pair(
        self, db: Session, worker_id: str
    ) -> AllocationResult | None:
        """Atomically reserve one (agent, borrower) pair and create a call row.

        Retries up to :data:`MAX_RETRIES` times on optimistic-lock races before
        giving up and returning ``None``.

        Args:
            db:        An active SQLAlchemy Session (caller manages commit/rollback).
            worker_id: Identifier of the calling worker thread for lease tracking.

        Returns:
            :class:`AllocationResult` on success, ``None`` if no pair is
            available or all retry attempts failed.
        """
        for attempt in range(MAX_RETRIES):
            try:
                result = self._try_reserve(db, worker_id)
                if result is not None:
                    db.commit()
                    logger.debug(
                        "Reserved pair: agent=%s borrower=%s call=%s (attempt=%d)",
                        result.agent_id, result.borrower_id, result.call_id, attempt + 1,
                    )
                    return result
                # No pair available (nothing left to pick).
                db.rollback()
                return None
            except _RaceLost:
                db.rollback()
                logger.debug("Lost optimistic-lock race (attempt %d/%d)", attempt + 1, MAX_RETRIES)
                continue
            except Exception:
                db.rollback()
                raise

        logger.debug("All %d reserve_pair attempts failed; giving up.", MAX_RETRIES)
        return None

    def release_agent(self, db: Session, agent_id: str) -> None:
        """Release a RESERVED or DIALING agent back to AVAILABLE.

        Used by the reconciler and by callers that need to undo a reservation
        without going through the full FSM lifecycle.

        Args:
            db:       Active SQLAlchemy Session.
            agent_id: The agent to release.
        """
        agent = db.get(Agent, agent_id)
        if agent is None:
            logger.warning("release_agent: agent %s not found", agent_id)
            return
        try:
            new_status = agent_fsm.apply(
                agent_fsm.AgentState(agent.status), agent_fsm.AgentState.AVAILABLE
            )
            agent.status = new_status.value
            agent.lease_expires_at = None
            agent.worker_id = None
            db.flush()
        except agent_fsm.IllegalTransition as exc:
            logger.error("release_agent FSM error: %s", exc)

    # ─────────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _try_reserve(
        self, db: Session, worker_id: str
    ) -> AllocationResult | None:
        """One reservation attempt — raises _RaceLost on optimistic-lock failure."""

        now = datetime.utcnow()
        lease_until = now + timedelta(seconds=LEASE_TTL_SECONDS)

        # ── Step 1: pick the longest-waiting AVAILABLE agent ─────────────────
        agent = (
            db.query(Agent)
            .filter(Agent.status == "AVAILABLE")
            .order_by(Agent.updated_at.asc())
            .with_for_update(skip_locked=True)
            .first()
        )
        if agent is None:
            return None  # no agents available

        seen_version = agent.version

        # ── Step 2: optimistic-lock UPDATE on the agent ───────────────────────
        rows = db.execute(
            update(Agent)
            .where(
                Agent.id == agent.id,
                Agent.status == "AVAILABLE",
                Agent.version == seen_version,
            )
            .values(
                status="RESERVED",
                version=Agent.version + 1,
                worker_id=worker_id,
                lease_expires_at=lease_until,
                updated_at=now,
            )
        ).rowcount

        if rows == 0:
            raise _RaceLost("Agent lock lost")

        # Sync the in-memory object so subsequent reads are consistent.
        agent.status = "RESERVED"
        agent.version = seen_version + 1
        agent.worker_id = worker_id
        agent.lease_expires_at = lease_until

        # ── Step 3: pick the oldest PENDING borrower ──────────────────────────
        borrower = (
            db.query(Borrower)
            .filter(Borrower.status == "PENDING")
            .order_by(Borrower.last_attempt_at.asc().nullsfirst(), Borrower.id.asc())
            .with_for_update(skip_locked=True)
            .first()
        )
        if borrower is None:
            # No borrowers — undo the agent reservation.
            db.execute(
                update(Agent)
                .where(Agent.id == agent.id)
                .values(status="AVAILABLE", lease_expires_at=None, worker_id=None)
            )
            return None

        seen_borrower_version = borrower.attempts  # use attempts as a lightweight CAS

        # ── Step 4: reserve the borrower ─────────────────────────────────────
        b_rows = db.execute(
            update(Borrower)
            .where(
                Borrower.id == borrower.id,
                Borrower.status == "PENDING",
                Borrower.attempts == seen_borrower_version,
            )
            .values(status="RESERVED", last_attempt_at=now)
        ).rowcount

        if b_rows == 0:
            raise _RaceLost("Borrower lock lost")

        borrower.status = "RESERVED"

        # ── Step 5: create a QUEUED call row, then FSM it to RESERVED ─────────
        call_id = str(uuid.uuid4())
        call = Call(
            id=call_id,
            agent_id=agent.id,
            borrower_id=borrower.id,
            provider=self.provider,
            status=call_fsm.CallState.QUEUED.value,
            created_at=now,
        )
        db.add(call)
        db.flush()  # assign the row so we can update it

        # FSM: QUEUED → RESERVED
        new_call_status = call_fsm.apply(
            call_fsm.CallState.QUEUED, call_fsm.CallState.RESERVED
        )
        call.status = new_call_status.value

        return AllocationResult(
            agent_id=agent.id,
            borrower_id=borrower.id,
            call_id=call_id,
        )


class _RaceLost(Exception):
    """Internal sentinel raised when an optimistic-lock CAS fails."""
