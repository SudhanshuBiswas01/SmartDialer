# SmartDialer Architecture

SmartDialer is a single-process, high-concurrency predictive dialer for call centers. It avoids complex microservice coordination, queues, or distributed state mechanisms (like Redis, Kafka, or Celery). Instead, it relies on a single relational database (SQLite in WAL mode by default, or Postgres) as its sole source of truth and coordination.

## Core Principles
1. **Single Source of Truth (Database):** All state, concurrency control, and audit trailing exist in the database.
2. **Stateless Components:** The pacing engine, safety controller, and event ingestor hold no persistent internal state across ticks (aside from tracking EWMA, which is synchronized).
3. **Optimistic Concurrency Control:** Worker threads compete to reserve agents and borrowers using database-level `UPDATE ... WHERE version = X` locks.
4. **FSM Enforcement:** Agents and calls follow strict Finite State Machines (FSMs). State changes outside these paths are rejected.

## Component Diagram

```mermaid
graph TD
    API[FastAPI Endpoints] --> |Starts/Stops| Orch[Orchestrator Tick Loop]
    API --> |Reads| DB[(Database / SQLite WAL)]
    
    Orch --> Recon[Reconciler]
    Orch --> Pacing[Pacing Strategy]
    Orch --> Safety[Safety Controller]
    
    Recon --> |Cleans expired leases| DB
    Pacing --> |Reads Snapshot| DB
    Safety --> |Authorizes & Audits| DB
    Safety --> |Triggers| Alloc[Call Allocator]
    
    Alloc --> |Optimistic Locks| DB
    Alloc --> |Initiates| Prov[Telecom Provider]
    
    Prov --> |Emits Events| EQ[Event Queue]
    EQ --> Ingest[Event Ingestor]
    
    Ingest --> |Checks idempotency| DB
    Ingest --> |Validates transition| CallFSM[Call FSM]
    Ingest --> |Validates transition| AgentFSM[Agent FSM]
    Ingest --> |Updates State| DB
```

## Finite State Machines

### Agent FSM

```mermaid
stateDiagram-v2
    [*] --> OFFLINE
    OFFLINE --> AVAILABLE: Login
    AVAILABLE --> RESERVED: Allocator Reserve
    AVAILABLE --> PAUSED: Supervisor Pause
    AVAILABLE --> OFFLINE: Logout
    
    RESERVED --> DIALING: Call Initiated
    RESERVED --> AVAILABLE: Lease Expired / Failed
    
    DIALING --> CONNECTED: Answered
    DIALING --> AVAILABLE: Failed/Cancelled
    
    CONNECTED --> WRAP_UP: Call Completed
    
    WRAP_UP --> AVAILABLE: Ready
    WRAP_UP --> PAUSED: Break
    WRAP_UP --> OFFLINE: Logout
    
    PAUSED --> AVAILABLE: Resume
    PAUSED --> OFFLINE: Logout
```

### Call FSM

```mermaid
stateDiagram-v2
    [*] --> QUEUED
    QUEUED --> RESERVED: Allocated
    QUEUED --> CANCELLED: Cancel
    
    RESERVED --> INITIATED: Dialing Started
    RESERVED --> CANCELLED: Cancel
    
    INITIATED --> RINGING: Handshake OK
    INITIATED --> FAILED: Provider Error
    INITIATED --> CANCELLED: Cancel
    
    RINGING --> ANSWERED: Picked Up
    RINGING --> FAILED: Timeout/Drop
    RINGING --> CANCELLED: Cancel
    
    ANSWERED --> CONNECTED: Agent Attached
    ANSWERED --> COMPLETED: Dropped immediately
    ANSWERED --> FAILED: Error
    
    CONNECTED --> COMPLETED: Hang up
```
