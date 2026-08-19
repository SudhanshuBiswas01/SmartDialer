"""Mock provider B — slow, chaotic telecom stub — Phase 4.

Characteristics (all configurable, with spec-specified defaults):
- Setup delay:    0.5–2.0 s uniform random.
- Timeout rate:   10 % of calls silently never complete.
- Duplicate rate: 15 % chance each event is emitted twice (back-to-back).
- Out-of-order:   20 % chance ANSWERED and RINGING are swapped.
- Pre-completion: occasional COMPLETED before ANSWERED (extremely chaotic).
- Failure rate:   10 % (call goes to FAILED before ring).

These chaos features stress-test the ingestor's idempotency and FSM-gate.
The event_queue is the same shared queue used by Provider A, so the ingestor
does not know which provider emitted a given event.
"""

from __future__ import annotations

import logging
import queue
import random
import threading
import time
from datetime import datetime

from app.providers.base import CallEvent

logger = logging.getLogger(__name__)

_FAILURE_RATE = 0.10
_TIMEOUT_RATE = 0.10
_DUPLICATE_RATE = 0.15
_OUT_OF_ORDER_RATE = 0.20
_PREMATURE_COMPLETE_RATE = 0.05


class MockProviderB:
    """Slow, unreliable chaos mock telecom provider.

    Satisfies the :class:`~app.providers.base.TelecomProvider` protocol.
    Designed to expose bugs in idempotency handling and FSM-gate validation.
    """

    def __init__(
        self,
        answer_rate: float = 0.5,
        talk_time_mean: float = 90.0,
        event_queue: queue.Queue | None = None,  # type: ignore[type-arg]
    ) -> None:
        self.answer_rate = answer_rate
        self.talk_time_mean = talk_time_mean
        self.event_queue: queue.Queue = event_queue or queue.Queue()  # type: ignore[type-arg]

        self._lock = threading.Lock()
        self._total: int = 0
        self._timeouts: int = 0
        self._errors: int = 0

    @property
    def name(self) -> str:
        return "mock_b"

    def initiate_call(self, call_id: str, phone: str) -> None:
        """Begin dialling in a background daemon thread."""
        with self._lock:
            self._total += 1
        t = threading.Thread(
            target=self._run_call, args=(call_id, phone), daemon=True
        )
        t.start()

    def get_health(self) -> dict:
        with self._lock:
            total = self._total
            timeouts = self._timeouts
            errors = self._errors
        timeout_rate = timeouts / total if total > 0 else 0.0
        error_rate = errors / total if total > 0 else 0.0
        return {
            "is_healthy": (timeout_rate + error_rate) < 0.5,
            "error_rate": error_rate,
            "timeout_rate": timeout_rate,
            "total_calls": total,
        }

    def _emit(self, event_type: str, call_id: str) -> None:
        """Emit an event, potentially duplicated."""
        event = CallEvent(
            call_id=call_id,
            event_type=event_type,
            timestamp=datetime.utcnow(),
        )
        self.event_queue.put(event)

        # 15% chance of duplicate (same type, new event_id → dedup catches it
        # only if ingestor checks processed_events; same event_id = idempotency).
        # We emit a NEW event_id so the ingestor must be FSM-gated, not just
        # checking event_id (except if we deliberately reuse event_id below).
        if random.random() < _DUPLICATE_RATE:
            # Reuse the same event_id to test idempotency ledger.
            dup = CallEvent(
                call_id=event.call_id,
                event_type=event.event_type,
                timestamp=datetime.utcnow(),
                event_id=event.event_id,  # same UUID → triggers unique constraint
            )
            self.event_queue.put(dup)

    def _run_call(self, call_id: str, phone: str) -> None:
        """Simulate a chaotic call lifecycle."""
        try:
            time.sleep(random.uniform(0.5, 2.0))

            if random.random() < _FAILURE_RATE:
                self._emit("FAILED", call_id)
                with self._lock:
                    self._errors += 1
                return

            # 10% silently times out — never emits anything more.
            if random.random() < _TIMEOUT_RATE:
                with self._lock:
                    self._timeouts += 1
                return

            # Premature COMPLETED before ring (extremely chaotic).
            if random.random() < _PREMATURE_COMPLETE_RATE:
                self._emit("COMPLETED", call_id)
                # Also emit the normal sequence so the call actually closes.
                self._emit("RINGING", call_id)
                self._emit("ANSWERED", call_id)
                return

            ringing_event = "RINGING"
            answered_event = "ANSWERED"

            # 20% chance: swap RINGING and ANSWERED order.
            if random.random() < _OUT_OF_ORDER_RATE:
                ringing_event, answered_event = answered_event, ringing_event

            self._emit(ringing_event, call_id)
            time.sleep(random.uniform(0.1, 0.5))
            self._emit(answered_event, call_id)
            time.sleep(random.uniform(0.1, 0.3))

            if random.random() > self.answer_rate:
                self._emit("FAILED", call_id)
                return

            self._emit("CONNECTED", call_id)
            talk = random.uniform(
                self.talk_time_mean * 0.5, self.talk_time_mean * 1.5
            )
            time.sleep(min(talk, 2.0))
            self._emit("COMPLETED", call_id)

        except Exception as exc:
            logger.exception("MockProviderB._run_call error for %s: %s", call_id, exc)
            with self._lock:
                self._errors += 1
