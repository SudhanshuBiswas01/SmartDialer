"""Phase 1 — FSM unit tests.

Covers:
- Every legal Agent transition passes (returns the new state).
- Every legal Call transition passes (returns the new state).
- Representative illegal transitions raise IllegalTransition.
- Terminal Call states are absorbing: any target raises IllegalTransition.
- DB init: tables are created, WAL mode is active.
"""

from __future__ import annotations

import os
import pytest

# ─────────────────────────────────────────────────────────────────────────────
# Use an in-memory SQLite DB for all DB-level tests (never touches disk)
# ─────────────────────────────────────────────────────────────────────────────
os.environ.setdefault("DATABASE_URL", "sqlite:///:memory:")


from app.domain.agent_fsm import (
    AgentState,
    IllegalTransition as AgentIllegalTransition,
    LEGAL_TRANSITIONS as AGENT_TRANSITIONS,
    apply as agent_apply,
)
from app.domain.call_fsm import (
    CallState,
    IllegalTransition as CallIllegalTransition,
    LEGAL_TRANSITIONS as CALL_TRANSITIONS,
    TERMINAL_STATES,
    apply as call_apply,
)


# ─────────────────────────────────────────────────────────────────────────────
# Agent FSM
# ─────────────────────────────────────────────────────────────────────────────

class TestAgentFSMLegalTransitions:
    """Every entry in LEGAL_TRANSITIONS should succeed."""

    @pytest.mark.parametrize("from_state,to_state", sorted(AGENT_TRANSITIONS))
    def test_legal_agent_transitions(
        self, from_state: AgentState, to_state: AgentState
    ) -> None:
        result = agent_apply(from_state, to_state)
        assert result is to_state

    def test_apply_returns_target_state(self) -> None:
        result = agent_apply(AgentState.OFFLINE, AgentState.AVAILABLE)
        assert result == AgentState.AVAILABLE

    def test_login_flow(self) -> None:
        """Full happy-path: login → reserve → dial → connected → wrap-up → available."""
        state = AgentState.OFFLINE
        state = agent_apply(state, AgentState.AVAILABLE)
        state = agent_apply(state, AgentState.RESERVED)
        state = agent_apply(state, AgentState.DIALING)
        state = agent_apply(state, AgentState.CONNECTED)
        state = agent_apply(state, AgentState.WRAP_UP)
        state = agent_apply(state, AgentState.AVAILABLE)
        assert state == AgentState.AVAILABLE

    def test_pause_and_resume(self) -> None:
        state = agent_apply(AgentState.OFFLINE, AgentState.AVAILABLE)
        state = agent_apply(state, AgentState.PAUSED)
        state = agent_apply(state, AgentState.AVAILABLE)
        assert state == AgentState.AVAILABLE

    def test_logout_from_wrap_up(self) -> None:
        result = agent_apply(AgentState.WRAP_UP, AgentState.OFFLINE)
        assert result == AgentState.OFFLINE


class TestAgentFSMIllegalTransitions:
    """Illegal pairs must raise AgentIllegalTransition."""

    @pytest.mark.parametrize(
        "from_state,to_state",
        [
            (AgentState.OFFLINE, AgentState.RESERVED),       # must go via AVAILABLE
            (AgentState.OFFLINE, AgentState.DIALING),
            (AgentState.AVAILABLE, AgentState.CONNECTED),    # must go via RESERVED/DIALING
            (AgentState.AVAILABLE, AgentState.WRAP_UP),
            (AgentState.RESERVED, AgentState.CONNECTED),     # must go via DIALING
            (AgentState.RESERVED, AgentState.OFFLINE),
            (AgentState.DIALING, AgentState.WRAP_UP),        # must go via CONNECTED
            (AgentState.DIALING, AgentState.PAUSED),
            (AgentState.CONNECTED, AgentState.AVAILABLE),    # must wrap up first
            (AgentState.CONNECTED, AgentState.OFFLINE),
            (AgentState.PAUSED, AgentState.RESERVED),
            (AgentState.WRAP_UP, AgentState.DIALING),
            # Self-transitions are illegal everywhere
            (AgentState.AVAILABLE, AgentState.AVAILABLE),
            (AgentState.RESERVED, AgentState.RESERVED),
        ],
    )
    def test_illegal_agent_transitions(
        self, from_state: AgentState, to_state: AgentState
    ) -> None:
        with pytest.raises(AgentIllegalTransition):
            agent_apply(from_state, to_state)

    def test_exception_carries_states(self) -> None:
        with pytest.raises(AgentIllegalTransition) as exc_info:
            agent_apply(AgentState.OFFLINE, AgentState.DIALING)
        err = exc_info.value
        assert err.current == AgentState.OFFLINE
        assert err.target == AgentState.DIALING
        assert "OFFLINE" in str(err)
        assert "DIALING" in str(err)


# ─────────────────────────────────────────────────────────────────────────────
# Call FSM
# ─────────────────────────────────────────────────────────────────────────────

class TestCallFSMLegalTransitions:
    """Every entry in LEGAL_TRANSITIONS should succeed."""

    @pytest.mark.parametrize("from_state,to_state", sorted(CALL_TRANSITIONS))
    def test_legal_call_transitions(
        self, from_state: CallState, to_state: CallState
    ) -> None:
        result = call_apply(from_state, to_state)
        assert result is to_state

    def test_happy_path(self) -> None:
        """QUEUED → RESERVED → INITIATED → RINGING → ANSWERED → CONNECTED → COMPLETED."""
        state = CallState.QUEUED
        state = call_apply(state, CallState.RESERVED)
        state = call_apply(state, CallState.INITIATED)
        state = call_apply(state, CallState.RINGING)
        state = call_apply(state, CallState.ANSWERED)
        state = call_apply(state, CallState.CONNECTED)
        state = call_apply(state, CallState.COMPLETED)
        assert state == CallState.COMPLETED

    def test_fail_during_ringing(self) -> None:
        state = call_apply(CallState.RINGING, CallState.FAILED)
        assert state == CallState.FAILED

    def test_cancel_at_any_pre_terminal_stage(self) -> None:
        for from_state in (
            CallState.QUEUED,
            CallState.RESERVED,
            CallState.INITIATED,
            CallState.RINGING,
        ):
            assert call_apply(from_state, CallState.CANCELLED) == CallState.CANCELLED


class TestCallFSMIllegalTransitions:
    """Illegal pairs must raise CallIllegalTransition."""

    @pytest.mark.parametrize(
        "from_state,to_state",
        [
            (CallState.QUEUED, CallState.INITIATED),    # must be RESERVED first
            (CallState.QUEUED, CallState.RINGING),
            (CallState.QUEUED, CallState.ANSWERED),
            (CallState.RESERVED, CallState.RINGING),   # must be INITIATED first
            (CallState.RESERVED, CallState.ANSWERED),
            (CallState.INITIATED, CallState.ANSWERED), # must RING first
            (CallState.INITIATED, CallState.CONNECTED),
            (CallState.RINGING, CallState.CONNECTED),  # must be ANSWERED first
            (CallState.ANSWERED, CallState.QUEUED),
            (CallState.ANSWERED, CallState.RESERVED),
            (CallState.CONNECTED, CallState.FAILED),   # once connected, only COMPLETED
            (CallState.CONNECTED, CallState.CANCELLED),
            # Self-transitions
            (CallState.QUEUED, CallState.QUEUED),
            (CallState.RINGING, CallState.RINGING),
        ],
    )
    def test_illegal_call_transitions(
        self, from_state: CallState, to_state: CallState
    ) -> None:
        with pytest.raises(CallIllegalTransition):
            call_apply(from_state, to_state)


class TestTerminalStatesAbsorbing:
    """Terminal states (COMPLETED, FAILED, CANCELLED) must reject ALL transitions."""

    @pytest.mark.parametrize("terminal", sorted(TERMINAL_STATES))
    @pytest.mark.parametrize("target", list(CallState))
    def test_terminal_absorbs_everything(
        self, terminal: CallState, target: CallState
    ) -> None:
        """Any target from a terminal state must raise IllegalTransition."""
        with pytest.raises(CallIllegalTransition):
            call_apply(terminal, target)

    def test_terminal_states_set(self) -> None:
        assert CallState.COMPLETED in TERMINAL_STATES
        assert CallState.FAILED in TERMINAL_STATES
        assert CallState.CANCELLED in TERMINAL_STATES
        assert CallState.RINGING not in TERMINAL_STATES
        assert CallState.ANSWERED not in TERMINAL_STATES


# ─────────────────────────────────────────────────────────────────────────────
# DB initialisation
# ─────────────────────────────────────────────────────────────────────────────

class TestDBInit:
    """Verify that init_db() creates all tables and WAL mode is active."""

    def test_init_db_creates_tables(self) -> None:
        from app.db import engine, init_db
        from sqlalchemy import inspect

        init_db()
        insp = inspect(engine)
        tables = insp.get_table_names()

        expected = {
            "agents",
            "borrowers",
            "calls",
            "processed_events",
            "pacing_decisions",
        }
        assert expected.issubset(set(tables)), (
            f"Missing tables: {expected - set(tables)}"
        )

    def test_wal_mode_active(self) -> None:
        """For SQLite, journal mode should be 'wal' after init."""
        from app.db import DATABASE_URL, verify_wal_mode

        if not DATABASE_URL.startswith("sqlite"):
            pytest.skip("WAL mode check only applies to SQLite")

        # In-memory SQLite may return 'memory' instead of 'wal'.
        # Skip for :memory: since WAL doesn't apply there.
        if ":memory:" in DATABASE_URL:
            pytest.skip("WAL pragma not meaningful on :memory: SQLite")

        mode = verify_wal_mode()
        assert mode == "wal", f"Expected WAL mode, got {mode!r}"

    def test_agents_table_columns(self) -> None:
        from app.db import engine, init_db
        from sqlalchemy import inspect

        init_db()
        insp = inspect(engine)
        cols = {c["name"] for c in insp.get_columns("agents")}
        assert {"id", "status", "version", "lease_expires_at", "worker_id", "updated_at"}.issubset(cols)

    def test_calls_table_columns(self) -> None:
        from app.db import engine, init_db
        from sqlalchemy import inspect

        init_db()
        insp = inspect(engine)
        cols = {c["name"] for c in insp.get_columns("calls")}
        assert {
            "id", "agent_id", "borrower_id", "provider", "status",
            "created_at", "answered_at", "ended_at", "abandoned"
        }.issubset(cols)

    def test_processed_events_unique_constraint(self) -> None:
        """The idempotency ledger must have a UNIQUE constraint on event_id."""
        from app.db import engine, init_db
        from sqlalchemy import inspect

        init_db()
        insp = inspect(engine)
        unique_constraints = insp.get_unique_constraints("processed_events")
        constrained_cols = [
            col
            for uc in unique_constraints
            for col in uc["column_names"]
        ]
        assert "event_id" in constrained_cols, (
            "processed_events must have UNIQUE constraint on event_id"
        )
