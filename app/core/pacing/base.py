"""Pacing strategy protocol and shared Snapshot dataclass — Phase 3.

Design constraints (spec §3):
- Pacing classes are **PURE** — no DB writes, no dialing, no allocator import.
- ``propose(snapshot) -> int`` returns the desired number of dials this tick.
- The SafetyController then gates the proposal against hard safety invariants.

Snapshot captures everything a pacing engine needs to make a proposal from
a DB query at the start of each orchestrator tick.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable


@dataclass
class Snapshot:
    """Immutable point-in-time view of the system state for one pacing tick.

    All values are computed from the database immediately before the pacing
    calculation; pacing strategies must not query the DB themselves.

    Attributes:
        available_agents:    Agents in AVAILABLE state.
        ringing_calls:       Calls in RINGING state (dialled, awaiting answer).
        connected_calls:     Calls in CONNECTED state (agent on an active call).
        answered_calls:      Calls in ANSWERED state (borrower answered, agent attaching).
        ewma_answer_rate:    Exponentially-weighted moving average of answer rate [0, 1].
        ewma_talk_time:      EWMA of call talk duration in seconds.
        ewma_setup_time:     EWMA of call setup (initiation-to-ringing) time in seconds.
        frees_soon:          Agents in CONNECTED whose elapsed talk ≥ (ewma_talk_time − ewma_setup_time).
                             These are expected to finish their call before the next answer.
        abandon_rate_window: Fraction of recently-connected calls that were abandoned
                             (ANSWERED with no agent attached) over the rolling window.
        provider_healthy:    Whether the active telecom provider is considered healthy
                             (circuit breaker open = False).
        k:                   Aggressiveness factor in [0.1, 1.0] managed by AIMD in SafetyController.
        tick:                Current orchestrator tick counter (for audit logging).
        mode:                Current pacing mode ('progressive' or 'predictive').
    """

    available_agents: int = 0
    ringing_calls: int = 0
    connected_calls: int = 0
    answered_calls: int = 0
    ewma_answer_rate: float = 0.5
    ewma_talk_time: float = 60.0
    ewma_setup_time: float = 5.0
    frees_soon: int = 0
    abandon_rate_window: float = 0.0
    provider_healthy: bool = True
    k: float = 1.0
    tick: int = 0
    mode: str = "progressive"

    def as_dict(self) -> dict:
        """Return a JSON-serialisable dict for audit logging."""
        return {
            "available_agents": self.available_agents,
            "ringing_calls": self.ringing_calls,
            "connected_calls": self.connected_calls,
            "answered_calls": self.answered_calls,
            "ewma_answer_rate": round(self.ewma_answer_rate, 4),
            "ewma_talk_time": round(self.ewma_talk_time, 2),
            "ewma_setup_time": round(self.ewma_setup_time, 2),
            "frees_soon": self.frees_soon,
            "abandon_rate_window": round(self.abandon_rate_window, 4),
            "provider_healthy": self.provider_healthy,
            "k": round(self.k, 4),
            "tick": self.tick,
            "mode": self.mode,
        }


@runtime_checkable
class PacingStrategy(Protocol):
    """Protocol that all pacing engines must implement.

    Implementations must be **pure** — they may not write to the database,
    make network calls, or import :mod:`app.core.allocator`.
    """

    def propose(self, snapshot: Snapshot) -> int:
        """Propose the number of new dials for this tick.

        Args:
            snapshot: Current system state captured from the database.

        Returns:
            Non-negative integer: the desired number of calls to initiate.
            The SafetyController will then cap this to hard safety limits.
        """
        ...
