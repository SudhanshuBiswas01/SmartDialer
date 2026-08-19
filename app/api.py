"""FastAPI application — campaign control, metrics, and audit endpoints.

Endpoints:
    POST /campaign/start     Start the orchestrator with the given configuration.
                             Stops and cleans up any running campaign first.
    POST /campaign/stop      Stop the orchestrator and mark campaign STOPPED.
    GET  /campaign/status    Get the current campaign status.
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
from app.models import Agent, Borrower, Call, Campaign, PacingDecision
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
    campaign_id: str
    agents_created: int
    borrowers_created: int
    mode: str


class CampaignStopResponse(BaseModel):
    status: str
    campaign_id: Optional[str]
    tick: int


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _stop_current_campaign(db: Session) -> None:
    """Stop the running orchestrator and mark the old campaign STOPPED in DB.

    Also cancels in-flight calls and releases agents so the DB is clean for
    the next campaign.
    """
    global _orchestrator
    if _orchestrator is None:
        return

    old_campaign_id = _orchestrator.campaign_id

    # Stop orchestrator loop.
    _orchestrator.stop()
    _orchestrator = None

    if old_campaign_id is None:
        return

    now = datetime.utcnow()

    # Mark campaign STOPPED.
    campaign = db.get(Campaign, old_campaign_id)
    if campaign and campaign.status == "RUNNING":
        campaign.status = "STOPPED"
        campaign.stopped_at = now

    # Cancel all in-flight calls.
    in_flight_statuses = {"RESERVED", "INITIATED", "RINGING", "ANSWERED", "CONNECTED", "QUEUED"}
    in_flight_calls = (
        db.query(Call)
        .filter(
            Call.campaign_id == old_campaign_id,
            Call.status.in_(in_flight_statuses),
        )
        .all()
    )
    for call in in_flight_calls:
        call.status = "CANCELLED"
        call.ended_at = now

    # Release all non-OFFLINE agents.
    active_agent_statuses = {"AVAILABLE", "RESERVED", "DIALING", "CONNECTED", "WRAP_UP"}
    active_agents = (
        db.query(Agent)
        .filter(
            Agent.campaign_id == old_campaign_id,
            Agent.status.in_(active_agent_statuses),
        )
        .all()
    )
    for agent in active_agents:
        agent.status = "OFFLINE"
        agent.lease_expires_at = None
        agent.worker_id = None

    db.commit()
    logger.info("Stopped campaign %s and released %d agents, cancelled %d calls.",
                old_campaign_id, len(active_agents), len(in_flight_calls))


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
    """Seed agents and borrowers, then start the orchestrator tick loop.

    If a campaign is already running, it is stopped and cleaned up first.
    The new campaign gets a fresh campaign_id so all metrics are scoped
    exclusively to this run.
    """
    global _orchestrator

    # Stop any previously running campaign cleanly.
    if _orchestrator is not None:
        _stop_current_campaign(db)

    # Create the Campaign row.
    campaign = Campaign(
        mode=req.mode,
        provider=req.provider,
        status="RUNNING",
        started_at=datetime.utcnow(),
    )
    db.add(campaign)
    db.flush()  # get campaign.id before seeding

    campaign_id = campaign.id

    # Seed agents scoped to this campaign.
    agents = [
        Agent(campaign_id=campaign_id, status="AVAILABLE", version=0)
        for _ in range(req.num_agents)
    ]
    db.add_all(agents)

    # Seed borrowers scoped to this campaign.
    borrowers = [
        Borrower(campaign_id=campaign_id, phone=f"+1000{i:06d}", status="PENDING", attempts=0)
        for i in range(req.num_borrowers)
    ]
    db.add_all(borrowers)
    db.commit()

    _orchestrator = Orchestrator(
        mode=req.mode,
        provider_name=req.provider,
        tick_interval=req.tick_interval,
        campaign_id=campaign_id,
    )
    _orchestrator.start()

    logger.info(
        "Campaign %s started: mode=%s provider=%s agents=%d borrowers=%d",
        campaign_id, req.mode, req.provider, req.num_agents, req.num_borrowers,
    )
    return CampaignStartResponse(
        status="started",
        campaign_id=campaign_id,
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
def campaign_stop(db: Session = Depends(get_db)) -> CampaignStopResponse:
    """Stop the orchestrator tick loop and mark the campaign STOPPED."""
    global _orchestrator
    if _orchestrator is None or not _orchestrator._running:
        raise HTTPException(status_code=404, detail="No campaign is running")
    tick = _orchestrator._tick
    campaign_id = _orchestrator.campaign_id
    _stop_current_campaign(db)
    return CampaignStopResponse(status="stopped", campaign_id=campaign_id, tick=tick)


@app.get(
    "/campaign/status",
    summary="Get current campaign status",
    tags=["Campaign"],
)
def campaign_status(db: Session = Depends(get_db)) -> dict:
    """Return the status of the current (or most recent) campaign."""
    if _orchestrator is None:
        # Look for last campaign in DB.
        last = (
            db.query(Campaign)
            .order_by(Campaign.started_at.desc())
            .first()
        )
        if last is None:
            return {"status": "no_campaign"}
        return {
            "campaign_id": last.id,
            "status": last.status,
            "mode": last.mode,
            "provider": last.provider,
            "started_at": last.started_at.isoformat() if last.started_at else None,
            "stopped_at": last.stopped_at.isoformat() if last.stopped_at else None,
        }

    return {
        "campaign_id": _orchestrator.campaign_id,
        "status": _orchestrator._campaign_status,
        "mode": _orchestrator.mode,
        "provider": _orchestrator.provider_name,
        "tick": _orchestrator._tick,
    }


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
    q = db.query(PacingDecision)
    # Scope to current campaign if one is running.
    if _orchestrator is not None and _orchestrator.campaign_id:
        q = q.filter(PacingDecision.campaign_id == _orchestrator.campaign_id)
    rows = q.order_by(PacingDecision.id.desc()).limit(limit).all()
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
    return {
        "status": "ok",
        "running": _orchestrator is not None and _orchestrator._running,
        "campaign_id": _orchestrator.campaign_id if _orchestrator else None,
    }


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
    content_type = request.headers.get("content-type", "")

    if "application/json" in content_type:
        payload = await request.json()
    else:
        form = await request.form()
        payload = dict(form)

    # We passed call_id in the query string of the answer/fallback URL
    call_id = request.query_params.get("call_id")
    if not call_id:
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
        event_id = f"{payload.get('CallUUID', 'unknown')}-{event_type}"
        event = CallEvent(
            call_id=call_id,
            event_type=event_type,
            timestamp=datetime.utcnow(),
            event_id=event_id,
        )
        result = ingestor.process(event, db)
        logger.info("Processed Plivo event %s for call %s: %s", event_type, call_id, result)

    return {"status": "ok"}
