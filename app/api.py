"""FastAPI application — campaign control, metrics, and audit endpoints.

Endpoints:
    POST /campaign/start     Start the orchestrator with the given configuration.
    POST /campaign/stop      Stop the orchestrator.
    GET  /metrics            Current system metrics (utilization, calls, EWMAs).
    GET  /decisions          Last N pacing_decisions audit rows.
    GET  /health             Simple health-check.
    WS   /ws/metrics         WebSocket pushing tick metrics every second.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.core.orchestrator import Orchestrator
from app.db import get_db, init_db
from app.events.ingestor import EventIngestor
from app.models import Agent, Borrower, PacingDecision
from app.providers.base import CallEvent

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Application factory
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="SmartDialer API",
    description=(
        "Predictive call-center dialer — single-process, DB-coordinated, "
        "FSM-gated state machine with progressive and predictive pacing modes."
    ),
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount dashboard directory
dashboard_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dashboard")
app.mount("/dashboard", StaticFiles(directory=dashboard_dir, html=True), name="dashboard")

# Module-level orchestrator instance (one per process).
_orchestrator: Optional[Orchestrator] = None


@app.on_event("startup")
def _startup() -> None:
    init_db()
    logger.info("SmartDialer API started — DB initialised.")


# ─────────────────────────────────────────────────────────────────────────────
# Request/Response models
# ─────────────────────────────────────────────────────────────────────────────

class CampaignStartRequest(BaseModel):
    mode: str = Field("progressive", pattern="^(progressive|predictive)$")
    provider: str = Field("mock_a", pattern="^(mock_a|mock_b|plivo)$")
    num_agents: int = Field(5, ge=1, le=10_000)
    num_borrowers: int = Field(20, ge=1, le=100_000)
    answer_rate: float = Field(0.5, ge=0.0, le=1.0)
    talk_time_mean: float = Field(60.0, ge=1.0, le=3600.0)
    tick_interval: float = Field(1.0, ge=0.05, le=10.0)


class CampaignStartResponse(BaseModel):
    status: str
    agents_created: int
    borrowers_created: int
    mode: str


class CampaignStopResponse(BaseModel):
    status: str
    tick: int


# ─────────────────────────────────────────────────────────────────────────────
# Campaign control
# ─────────────────────────────────────────────────────────────────────────────

@app.post(
    "/campaign/start",
    response_model=CampaignStartResponse,
    summary="Start a dialling campaign",
    tags=["Campaign"],
)
def campaign_start(
    req: CampaignStartRequest, db: Session = Depends(get_db)
) -> CampaignStartResponse:
    """Seed agents and borrowers, then start the orchestrator tick loop."""
    global _orchestrator

    if _orchestrator is not None and _orchestrator._running:
        raise HTTPException(status_code=409, detail="Campaign already running")

    # Seed agents.
    agents = [
        Agent(status="AVAILABLE", version=0) for _ in range(req.num_agents)
    ]
    db.add_all(agents)

    # Seed borrowers.
    borrowers = [
        Borrower(phone=f"+1000{i:06d}", status="PENDING", attempts=0)
        for i in range(req.num_borrowers)
    ]
    db.add_all(borrowers)
    db.commit()

    _orchestrator = Orchestrator(
        mode=req.mode,
        provider_name=req.provider,
        tick_interval=req.tick_interval,
    )
    _orchestrator.start()

    logger.info(
        "Campaign started: mode=%s provider=%s agents=%d borrowers=%d",
        req.mode, req.provider, req.num_agents, req.num_borrowers,
    )
    return CampaignStartResponse(
        status="started",
        agents_created=req.num_agents,
        borrowers_created=req.num_borrowers,
        mode=req.mode,
    )


@app.post(
    "/campaign/stop",
    response_model=CampaignStopResponse,
    summary="Stop the running campaign",
    tags=["Campaign"],
)
def campaign_stop() -> CampaignStopResponse:
    """Stop the orchestrator tick loop."""
    global _orchestrator
    if _orchestrator is None or not _orchestrator._running:
        raise HTTPException(status_code=404, detail="No campaign is running")
    tick = _orchestrator._tick
    _orchestrator.stop()
    _orchestrator = None
    return CampaignStopResponse(status="stopped", tick=tick)


# ─────────────────────────────────────────────────────────────────────────────
# Metrics
# ─────────────────────────────────────────────────────────────────────────────

@app.get(
    "/metrics",
    summary="Current system metrics",
    tags=["Observability"],
)
def get_metrics(db: Session = Depends(get_db)) -> dict:
    """Return utilization, call counts, abandon rate, and EWMA values."""
    if _orchestrator is None:
        raise HTTPException(status_code=404, detail="No campaign running")
    return _orchestrator.get_metrics(db)


@app.get(
    "/decisions",
    summary="Pacing decision audit log",
    tags=["Observability"],
)
def get_decisions(
    limit: int = 50, db: Session = Depends(get_db)
) -> list[dict]:
    """Return the last *limit* pacing_decisions rows (most recent first)."""
    rows = (
        db.query(PacingDecision)
        .order_by(PacingDecision.id.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "id": r.id,
            "tick": r.tick,
            "mode": r.mode,
            "proposed": r.proposed,
            "authorized": r.authorized,
            "reason": r.reason,
            "created_at": r.created_at.isoformat() if r.created_at else None,
        }
        for r in rows
    ]


@app.get("/health", summary="Health check", tags=["Observability"])
def health() -> dict:
    """Return service health status."""
    return {"status": "ok", "running": _orchestrator is not None and _orchestrator._running}


# ─────────────────────────────────────────────────────────────────────────────
# WebSocket — live metrics feed
# ─────────────────────────────────────────────────────────────────────────────

@app.websocket("/ws/metrics")
async def ws_metrics(websocket: WebSocket) -> None:
    """Push tick metrics over WebSocket once per second."""
    await websocket.accept()
    try:
        while True:
            await asyncio.sleep(1.0)
            if _orchestrator is None:
                await websocket.send_json({"error": "no campaign running"})
                continue
            db = next(get_db())
            try:
                metrics = _orchestrator.get_metrics(db)
            finally:
                db.close()
            await websocket.send_json(metrics)
    except WebSocketDisconnect:
        logger.debug("WebSocket client disconnected from /ws/metrics")


# ─────────────────────────────────────────────────────────────────────────────
# Webhooks
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/webhooks/plivo", summary="Plivo Callbacks", tags=["Webhooks"])
async def plivo_webhook(request: Request, db: Session = Depends(get_db)):
    """Translate Plivo webhook events into CallEvent and run through the Ingestor."""
    # Note: Depending on method configured in Plivo, could be form data or JSON.
    content_type = request.headers.get("content-type", "")
    
    if "application/json" in content_type:
        payload = await request.json()
    else:
        form = await request.form()
        payload = dict(form)
        
    # We passed call_id in the query string of the answer/fallback URL
    call_id = request.query_params.get("call_id")
    if not call_id:
        # Check payload
        call_id = payload.get("call_id")

    if not call_id:
        logger.warning("Plivo webhook missing call_id: %s", payload)
        return {"status": "ignored"}

    status = payload.get("CallStatus", payload.get("Event", "")).lower()
    event_type = None

    if status in ("ringing", "in-progress"):
        event_type = "RINGING"
    elif status == "answered" or payload.get("MachineDetection") == "human":
        event_type = "ANSWERED"
    elif status == "completed":
        event_type = "COMPLETED"
    elif status in ("failed", "busy", "no-answer", "canceled", "rejected"):
        event_type = "FAILED"

    if event_type:
        ingestor = EventIngestor()
        # Create a CallEvent
        # In a real production system we'd use a unique ID from Plivo for idempotency,
        # such as CallUUID + status
        event_id = f"{payload.get('CallUUID', 'unknown')}-{event_type}"
        
        event = CallEvent(
            call_id=call_id,
            event_type=event_type,
            timestamp=datetime.utcnow(),
            event_id=event_id,
        )
        
        # Process the event synchronously since the ingestor uses SQLAlchemy sync engine
        result = ingestor.process(event, db)
        logger.info("Processed Plivo event %s for call %s: %s", event_type, call_id, result)
        
    return {"status": "ok"}

