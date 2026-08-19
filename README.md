# 📞 SmartDialer

![Python 3.11+](https://img.shields.io/badge/Python-3.11%2B-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100%2B-00a393.svg)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0%2B-red.svg)
![SQLite](https://img.shields.io/badge/SQLite-WAL_Mode-003b57.svg)

A robust, high-throughput, predictive call-center dialer built purely with **Python, FastAPI, SQLAlchemy, and SQLite (WAL mode)**.

SmartDialer eliminates the need for message brokers (like Redis, RabbitMQ, or Celery) by relying on the database as the *sole source of truth and coordination*. It features an unbypassable Safety Controller and strict Finite State Machines (FSMs) for bulletproof reliability and concurrency.

---

## 🚀 Quick Start (in < 5 mins)

Get the dialer running, tested, and visualized in under 5 minutes.

### 1. Install Dependencies
Make sure you have Python 3.11+ installed.
```bash
pip install fastapi "uvicorn[standard]" sqlalchemy pydantic websockets python-dotenv pytest pytest-timeout httpx
```

### 2. Run the Tests
Verify the atomic allocator, FSMs, and safety invariants.
```bash
pytest -v
```

### 3. Launch Mission Control (API + Dashboard)
Run the FastAPI server which also serves the live monitoring dashboard.
```bash
uvicorn app.api:app --reload
```
- **Dashboard:** Open [http://127.0.0.1:8000/dashboard/index.html](http://127.0.0.1:8000/dashboard/index.html) in your browser.
- **Swagger Docs:** [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

*(From the dashboard, you can click "Start Campaign" to watch the dialer run in real-time.)*

---

## 🏗️ Architecture Highlights

- **No External Message Queues:** No Redis, Kafka, or Celery. Single process, multiple worker threads. The DB is the only coordination mechanism.
- **Optimistic Concurrency Control:** Workers lock agents and borrowers atomically using `UPDATE ... WHERE version=X` to prevent double-booking.
- **Strict FSM Invariants:** All state transitions go through explicitly defined legal paths in `agent_fsm` and `call_fsm`.
- **Safety First:** The `SafetyController` is the only component allowed to authorize calls, enforcing a strict `<3% abandon rate limit` and hardware capacity checks.

> 📚 **Deep Dive:** Check out the full [Architecture & FSM Diagrams](docs/architecture.md) and our [Architecture Decision Records (ADR)](docs/adr.md) for details on why we chose CP-over-AP and how we scale to 10k agents.

---

## 🕹️ Running the CLI Simulator

The simulator stress-tests the pacing algorithm, event ingestor, and safety limits under various telecom conditions without requiring a real telecom provider.

```bash
# Run a standard predictive scenario
python -m simulator.run --scenario B --mode predictive
```

### 🌪️ Chaos Engineering Options
You can stress-test the system's idempotency and crash recovery by injecting chaos:
```bash
python -m simulator.run --scenario C --provider b --agent-drop 5
```
- `--provider b`: Uses the chaotic telecom mock (delays, duplicate events, out-of-order events, premature completions).
- `--agent-drop <N>`: Simulates agents disconnecting midway through the run.
- `--scenario D`: Starts with a high answer rate and rapidly degrades it mid-run.

---

## 📡 API Endpoints

When running `uvicorn app.api:app`, you can interact directly via HTTP:
- **Start Campaign:** `POST /campaign/start` (Configure mode, provider, agents, etc.)
- **Stop Campaign:** `POST /campaign/stop`
- **System Metrics:** `GET /metrics`
- **Audit Log:** `GET /decisions` (Answers "why did the system dial X calls?")
- **Live Feed:** `WS /ws/metrics` (Used by the dashboard)
