# SmartDialer

A robust, single-process, predictive call-center dialer built with Python 3.11+, FastAPI, SQLAlchemy, and SQLite (WAL mode).

SmartDialer relies on the database as the sole source of truth and coordination. It implements strict Finite State Machines (FSMs) for call and agent lifecycles, and decouples pure pacing logic from safety enforcement.

## Architecture Highlights
- **No external message queues (Redis/Kafka/RabbitMQ).**
- **Optimistic Concurrency Control:** Workers lock agents and borrowers using `UPDATE ... WHERE version=X` to prevent double-booking.
- **Strict FSM:** All state transitions run through explicitly defined legal paths. Terminal states are absorbing.
- **Safety First:** The `SafetyController` is the only component allowed to authorize calls, enforcing a strict <3% abandon rate limit and hardware capacity checks.

## Setup

1. **Install dependencies:**
   ```bash
   pip install fastapi "uvicorn[standard]" sqlalchemy pydantic websockets python-dotenv pytest pytest-timeout httpx
   ```

2. **Run the tests:**
   ```bash
   pytest -v
   ```

## Running the Simulator

The simulator tests the pacing algorithm, event ingestor, and safety limits under various telecom conditions without requiring a real provider.

```bash
python -m simulator.run --scenario B --mode predictive
```

### Chaos Options
You can stress-test the system's idempotency and crash recovery by injecting chaos:
```bash
python -m simulator.run --scenario C --provider b --agent-drop 5
```
- `--provider b`: Uses the chaotic telecom mock (delays, duplicate events, out-of-order events, premature completions).
- `--agent-drop <N>`: Simulates agents disconnecting midway through the run.
- `--scenario D`: Starts with a high answer rate and rapidly degrades it mid-run.

## Running the API

You can run the FastAPI server to interact with the system via HTTP:
```bash
uvicorn app.api:app --reload
```
- **Swagger Docs:** http://localhost:8000/docs
- **Start Campaign:** `POST /campaign/start`
- **Metrics:** `GET /metrics`
- **Audit Log:** `GET /decisions`
