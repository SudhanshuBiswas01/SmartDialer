"""SQLAlchemy ORM models for SmartDialer.

All tables represent the full system state.  The database is the single
source of truth and the only coordination mechanism between worker threads.

Key design decisions:
- ``agents.version`` enables optimistic-lock concurrency (CAS-style UPDATE).
- ``calls.abandoned`` is a boolean flag set when ANSWERED fires but no agent
  can be attached — feeds the SafetyController abandon-rate calculation.
- ``processed_events.event_id`` has a UNIQUE constraint to provide idempotency
  for duplicate events from unreliable telecom providers.
- ``pacing_decisions`` provides a full audit trail so every dial decision can
  be reconstructed and explained after the fact.
- ``campaigns`` tracks campaign lifecycle (RUNNING/STOPPED/COMPLETED) and ties
  all agents, borrowers, calls, and pacing decisions to a single campaign_id.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def _utcnow() -> datetime:
    """Return the current UTC time (timezone-naive for SQLite compatibility)."""
    return datetime.utcnow()


def _new_uuid() -> str:
    """Generate a new UUID4 string."""
    return str(uuid.uuid4())


class Base(DeclarativeBase):
    """Shared declarative base for all ORM models."""


# ─────────────────────────────────────────────────────────────────────────────
# Campaigns
# ─────────────────────────────────────────────────────────────────────────────

class Campaign(Base):
    """Tracks a single dialling campaign lifecycle.

    Status lifecycle: RUNNING → STOPPED | COMPLETED.
    """

    __tablename__ = "campaigns"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_new_uuid)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="RUNNING", index=True
    )
    """RUNNING | STOPPED | COMPLETED."""
    mode: Mapped[str] = mapped_column(String(20), nullable=False)
    """'progressive' or 'predictive'."""
    provider: Mapped[str] = mapped_column(String(50), nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=_utcnow
    )
    stopped_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    agents: Mapped[list["Agent"]] = relationship("Agent", back_populates="campaign")
    borrowers: Mapped[list["Borrower"]] = relationship("Borrower", back_populates="campaign")


# ─────────────────────────────────────────────────────────────────────────────
# Agents
# ─────────────────────────────────────────────────────────────────────────────

class Agent(Base):
    """Represents a call-center agent and tracks their FSM state + lease."""

    __tablename__ = "agents"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_new_uuid)
    campaign_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("campaigns.id"), nullable=True, index=True
    )
    """FK to the campaign this agent belongs to. Nullable for test compatibility."""
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="OFFLINE", index=True
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    """Monotonically increasing version number used for optimistic locking.

    The concurrency primitive:
        UPDATE agents
        SET status='RESERVED', version=version+1,
            worker_id=:worker, lease_expires_at=:now+:ttl
        WHERE id=:id AND status='AVAILABLE' AND version=:seen_version;
    rowcount == 0  →  you lost the race; walk away.
    """
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    """Wall-clock time after which the reconciler may steal this reservation."""
    worker_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    """Identifier of the worker thread that holds the current lease."""
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=_utcnow, onupdate=_utcnow
    )

    campaign: Mapped["Campaign | None"] = relationship("Campaign", back_populates="agents")
    calls: Mapped[list["Call"]] = relationship("Call", back_populates="agent")


# ─────────────────────────────────────────────────────────────────────────────
# Borrowers (dial queue)
# ─────────────────────────────────────────────────────────────────────────────

class Borrower(Base):
    """A borrower to be dialled — one entry per unique contact attempt target.

    Status lifecycle: PENDING → RESERVED → CALLED → DONE.
    """

    __tablename__ = "borrowers"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_new_uuid)
    campaign_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("campaigns.id"), nullable=True, index=True
    )
    """FK to the campaign this borrower belongs to. Nullable for test compatibility."""
    phone: Mapped[str] = mapped_column(String(30), nullable=False)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="PENDING", index=True
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    """Number of dial attempts so far (incremented by the reconciler on retry)."""
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    campaign: Mapped["Campaign | None"] = relationship("Campaign", back_populates="borrowers")
    calls: Mapped[list["Call"]] = relationship("Call", back_populates="borrower")


# ─────────────────────────────────────────────────────────────────────────────
# Calls
# ─────────────────────────────────────────────────────────────────────────────

class Call(Base):
    """A single dial attempt — tracks FSM state and timing for metrics/audit."""

    __tablename__ = "calls"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_new_uuid)
    campaign_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("campaigns.id"), nullable=True, index=True
    )
    """FK to the campaign this call belongs to. Nullable for test compatibility."""
    agent_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("agents.id"), nullable=True, index=True
    )
    borrower_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("borrowers.id"), nullable=False, index=True
    )
    provider: Mapped[str] = mapped_column(String(50), nullable=False)
    """Name of the telecom provider that initiated the dial."""
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="QUEUED", index=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=_utcnow
    )
    answered_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    abandoned: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    """True when ANSWERED fired but no agent could be attached.
    Feeds the SafetyController abandon-rate window calculation."""

    agent: Mapped["Agent | None"] = relationship("Agent", back_populates="calls")
    borrower: Mapped["Borrower"] = relationship("Borrower", back_populates="calls")


# ─────────────────────────────────────────────────────────────────────────────
# Idempotency ledger
# ─────────────────────────────────────────────────────────────────────────────

class ProcessedEvent(Base):
    """Idempotency ledger — prevents duplicate events from being applied twice.

    The ingestor attempts an INSERT; if the UNIQUE constraint on *event_id*
    fires, the event has already been processed and is silently ignored.
    """

    __tablename__ = "processed_events"
    __table_args__ = (UniqueConstraint("event_id", name="uq_processed_events_event_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(36), nullable=False, unique=True)
    call_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(30), nullable=False)
    received_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=_utcnow
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pacing audit trail
# ─────────────────────────────────────────────────────────────────────────────

class PacingDecision(Base):
    """Audit record written by SafetyController for every tick decision.

    Answers the question: "Why did the system dial N calls at tick T?"
    ``inputs_json`` stores the full :class:`~app.core.pacing.base.Snapshot`
    so the decision can be fully replayed offline.
    """

    __tablename__ = "pacing_decisions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    campaign_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("campaigns.id"), nullable=True, index=True
    )
    """FK to the campaign this decision belongs to. Nullable for test compatibility."""
    tick: Mapped[int] = mapped_column(Integer, nullable=False)
    mode: Mapped[str] = mapped_column(String(20), nullable=False)
    """'progressive' or 'predictive'."""
    proposed: Mapped[int] = mapped_column(Integer, nullable=False)
    authorized: Mapped[int] = mapped_column(Integer, nullable=False)
    reason: Mapped[str] = mapped_column(String(100), nullable=False)
    inputs_json: Mapped[str] = mapped_column(Text, nullable=False)
    """JSON-encoded Snapshot used as input — full audit trail."""
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=_utcnow
    )
