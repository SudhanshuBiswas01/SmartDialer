"""Plivo Telecom Provider — Phase 6 Stretch Goal.

Implements the TelecomProvider protocol using Plivo's real API.
Configured via PLIVO_AUTH_ID, PLIVO_AUTH_TOKEN, and PLIVO_SOURCE_NUMBER.
Events arrive asynchronously via a webhook endpoint in app/api.py.
"""

from __future__ import annotations

import logging
import os
import queue
from typing import Any

import httpx

from app.providers.base import CallEvent

logger = logging.getLogger(__name__)


class PlivoProvider:
    """Real telecom provider using Plivo.

    Satisfies the :class:`~app.providers.base.TelecomProvider` protocol.
    """

    def __init__(self, event_queue: queue.Queue | None = None) -> None:  # type: ignore[type-arg]
        self.auth_id = os.environ.get("PLIVO_AUTH_ID")
        self.auth_token = os.environ.get("PLIVO_AUTH_TOKEN")
        self.source_number = os.environ.get("PLIVO_SOURCE_NUMBER")
        self.webhook_url = os.environ.get("PLIVO_WEBHOOK_URL")

        self.event_queue: queue.Queue = event_queue or queue.Queue()  # type: ignore[type-arg]
        
        self._total = 0
        self._errors = 0

        if not all([self.auth_id, self.auth_token, self.source_number]):
            logger.warning("Plivo credentials not fully configured in env vars.")

    @property
    def name(self) -> str:
        return "plivo"

    def initiate_call(self, call_id: str, phone: str) -> None:
        """Call Plivo to initiate the dial.

        The `call_id` is passed as a custom header or query param in the webhook URL
        so we can trace it back to our DB when the webhook fires.
        """
        if not self.auth_id or not self.auth_token:
            logger.error("Cannot initiate Plivo call: missing credentials")
            self._errors += 1
            return

        self._total += 1
        
        # We append call_id to the answer URL so Plivo sends it back in webhooks
        answer_url = f"{self.webhook_url}?call_id={call_id}" if self.webhook_url else ""

        payload = {
            "from": self.source_number,
            "to": phone,
            "answer_url": answer_url,
            "answer_method": "POST",
            "fallback_url": answer_url,
            "fallback_method": "POST",
            "machine_detection": "true",
            "machine_detection_url": answer_url,
            "machine_detection_method": "POST",
        }

        # Fire and forget. In a real system, you'd handle HTTP errors and retries.
        # For this prototype, we'll use httpx synchronously or run in a thread.
        try:
            resp = httpx.post(
                f"https://api.plivo.com/v1/Account/{self.auth_id}/Call/",
                json=payload,
                auth=(self.auth_id, self.auth_token),
                timeout=5.0
            )
            resp.raise_for_status()
            logger.info("Initiated Plivo call %s to %s", call_id, phone)
        except Exception as exc:
            logger.error("Failed to initiate Plivo call %s: %s", call_id, exc)
            self._errors += 1

    def get_health(self) -> dict[str, Any]:
        """Return provider health metrics."""
        error_rate = self._errors / self._total if self._total > 0 else 0.0
        return {
            "is_healthy": error_rate < 0.5,
            "error_rate": error_rate,
            "timeout_rate": 0.0,
            "total_calls": self._total,
        }
