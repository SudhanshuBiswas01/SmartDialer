"""Mock provider A — fast, reliable telecom stub — Phase 4.

Characteristics:
- Setup delay: 0.1–0.3 s uniform random.
- Failure rate: 2% (call goes straight to FAILED).
- Events always arrive in the correct order, never duplicated.
- Realistic event sequence: RINGING → ANSWERED → CONNECTED → COMPLETED.

All dial sequences run in daemon threads so the main thread (orchestrator) is
never blocked.  Events are pushed into ``self.event_queue`` for the ingestor.
"""

from __future__ import annotations

import logging
import queue
import random
import threading
import time
import uuid
from datetime import datetime

from app.providers.base import CallEvent

logger = logging.getLogger(__name__)

_FAILURE_RATE = 0.02


class MockProviderA:
    """Fast, reliable mock telecom provider.

    Satisfies the :class:`~app.providers.base.TelecomProvider` protocol.
    """

    def __init__(
        self,
        answer_rate: float = 0.7,
        talk_time_mean: float = 60.0,
        event_queue: queue.Queue | None = None,  # type: ignore[type-arg]
    ) -> None:
        """Initialise provider.

        Args:
            answer_rate:    Fraction of initiated calls that get answered [0, 1].
            talk_time_mean: Mean talk duration in seconds.
            event_queue:    Shared queue for events; created if not provided.
        """
        self.answer_rate = answer_rate
        self.talk_time_mean = talk_time_mean
        self.event_queue: queue.Queue = event_queue or queue.Queue()  # type: ignore[type-arg]

        # Health tracking
        self._lock = threading.Lock()
        self._total: int = 0
        self._errors: int = 0

    @property
    def name(self) -> str:
        """Provider identifier."""
        return "mock_a"

    def initiate_call(self, call_id: str, phone: str) -> None:
        """Begin dialling ``phone`` in a background daemon thread.

        Args:
            call_id: DB call ID — embedded in all emitted events.
            phone:   Phone number to dial (not used in the mock).
        """
        with self._lock:
            self._total += 1
        t = threading.Thread(
            target=self._run_call, args=(call_id, phone), daemon=True
        )
        t.start()

    def get_health(self) -> dict:
        """Return provider health metrics."""
        with self._lock:
            total = self._total
            errors = self._errors
        error_rate = errors / total if total > 0 else 0.0
        return {
            "is_healthy": error_rate < 0.5,
            "error_rate": error_rate,
            "timeout_rate": 0.0,
            "total_calls": total,
        }

    def _emit(self, event_type: str, call_id: str) -> None:
        self.event_queue.put(
            CallEvent(
                call_id=call_id,
                event_type=event_type,
                timestamp=datetime.utcnow(),
            )
        )

    def _run_call(self, call_id: str, phone: str) -> None:
        """Simulate a single call lifecycle in a background thread."""
        try:
            # Setup delay.
            time.sleep(random.uniform(0.1, 0.3))

            # 2% hard failure before ring.
            if random.random() < _FAILURE_RATE:
                self._emit("FAILED", call_id)
                with self._lock:
                    self._errors += 1
                return

            self._emit("RINGING", call_id)
            time.sleep(random.uniform(0.05, 0.2))

            # Answer decision.
            if random.random() > self.answer_rate:
                # No answer — treat as FAILED after timeout.
                self._emit("FAILED", call_id)
                return

            self._emit("ANSWERED", call_id)
            time.sleep(0.05)
            self._emit("CONNECTED", call_id)

            # Talk time (exponential-ish: uniform ± 20%).
            talk = random.uniform(
                self.talk_time_mean * 0.8, self.talk_time_mean * 1.2
            )
            time.sleep(min(talk, 2.0))  # cap at 2 s in tests/simulator

            self._emit("COMPLETED", call_id)
        except Exception as exc:
            logger.exception("MockProviderA._run_call error for %s: %s", call_id, exc)
            with self._lock:
                self._errors += 1
