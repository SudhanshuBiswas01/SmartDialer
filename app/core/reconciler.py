"""Lease sweeper / crash-recovery reconciler — Phase 2.

``release_expired_leases`` is designed to be run periodically (e.g. once per
orchestrator tick) by any worker thread.  It is idempotent and safe for
concurrent execution because the underlying UPDATE uses the same optimistic-lock
pattern as the allocator.

Recovery logic
--------------
For each agent whose ``lease_expires_at < now`` and whose status is in
``{RESERVED, DIALING}``:

1. Transition the agent → AVAILABLE (via FSM).
2. Find the agent's non-terminal call → CANCELLED (via FSM).
3. Increment the borrower's ``attempts`` counter and reset status → PENDING
   so the borrower is re-queued for the next dial attempt.

All three updates happen inside a single transaction per expired agent so that
the system is always in a consistent state even if the process crashes
mid-sweep.
"""

from __future__ import annotations

import logging
from datetime import datetime

from sqlalchemy.orm import Session

from app.domain import agent_fsm, call_fsm
from app.models import Agent, Borrower, Call

logger = logging.getLogger(__name__)

# Agent statuses that can have active leases.
_LEASED_STATUSES: frozenset[str] = frozenset({"RESERVED", "DIALING"})

# Call statuses that are NOT terminal (i.e. can still be cancelled).
_NON_TERMINAL_CALL_STATUSES: frozenset[str] = frozenset(
    {
        call_fsm.CallState.QUEUED.value,
        call_fsm.CallState.RESERVED.value,
        call_fsm.CallState.INITIATED.value,
        call_fsm.CallState.RINGING.value,
        call_fsm.CallState.ANSWERED.value,
        call_fsm.CallState.CONNECTED.value,
    }
)


def release_expired_leases(db: Session) -> int:
    """Sweep for expired agent leases and restore consistent state.

    For each expired lease:
    - Agent → AVAILABLE
    - Active call → CANCELLED
    - Borrower → PENDING (attempts incremented)

    Each recovery is committed individually so that a crash mid-sweep does not
    roll back already-fixed agents.

    Args:
        db: An active SQLAlchemy Session.

    Returns:
        The number of leases recovered.
    """
    now = datetime.utcnow()

    expired_agents = (
        db.query(Agent)
        .filter(
            Agent.status.in_(_LEASED_STATUSES),
            Agent.lease_expires_at < now,
        )
        .all()
    )

    recovered = 0
    for agent in expired_agents:
        try:
            _recover_agent(db, agent, now)
            db.commit()
            recovered += 1
            logger.info(
                "Recovered expired lease for agent=%s (was %s, worker=%s)",
                agent.id, agent.status, agent.worker_id,
            )
        except Exception as exc:
            db.rollback()
            logger.error(
                "Failed to recover agent=%s: %s", agent.id, exc, exc_info=True
            )

    return recovered


def _recover_agent(db: Session, agent: Agent, now: datetime) -> None:
    """Restore one expired-lease agent and its associated call/borrower.

    All writes are flushed (not committed) so the caller can commit atomically.
    """
    old_status = agent_fsm.AgentState(agent.status)

    # ── 1. Agent → AVAILABLE ─────────────────────────────────────────────────
    # RESERVED → AVAILABLE  or  DIALING → AVAILABLE (both are legal transitions)
    new_agent_status = agent_fsm.apply(old_status, agent_fsm.AgentState.AVAILABLE)
    agent.status = new_agent_status.value
    agent.lease_expires_at = None
    agent.worker_id = None
    agent.updated_at = now
    db.flush()

    # ── 2. Find the agent's non-terminal call ─────────────────────────────────
    active_call: Call | None = (
        db.query(Call)
        .filter(
            Call.agent_id == agent.id,
            Call.status.in_(_NON_TERMINAL_CALL_STATUSES),
        )
        .order_by(Call.created_at.desc())
        .first()
    )

    if active_call is not None:
        # Transition the call to CANCELLED through every necessary intermediate
        # state — we may need to step through the FSM if we can't jump directly.
        _cancel_call(db, active_call, now)

        # ── 3. Reset the borrower ─────────────────────────────────────────────
        borrower = db.get(Borrower, active_call.borrower_id)
        if borrower is not None:
            borrower.status = "PENDING"
            borrower.attempts = (borrower.attempts or 0) + 1
            db.flush()
    else:
        logger.debug("No active call found for expired-lease agent=%s", agent.id)


def _cancel_call(db: Session, call: Call, now: datetime) -> None:
    """Transition a call to CANCELLED, stepping through intermediate states as needed."""
    current = call_fsm.CallState(call.status)

    # States from which we can cancel directly.
    _cancellable = {
        call_fsm.CallState.QUEUED,
        call_fsm.CallState.RESERVED,
        call_fsm.CallState.INITIATED,
        call_fsm.CallState.RINGING,
    }

    if current in _cancellable:
        call.status = call_fsm.apply(current, call_fsm.CallState.CANCELLED).value
    elif current == call_fsm.CallState.ANSWERED:
        # ANSWERED → FAILED (CANCELLED not a valid target from ANSWERED per spec,
        # but FAILED is, and it's terminal — use FAILED for crash recovery)
        call.status = call_fsm.apply(current, call_fsm.CallState.FAILED).value
    elif current == call_fsm.CallState.CONNECTED:
        # CONNECTED → COMPLETED is the only legal transition
        call.status = call_fsm.apply(current, call_fsm.CallState.COMPLETED).value
    else:
        # Already terminal — nothing to do.
        pass

    call.ended_at = now
    db.flush()
