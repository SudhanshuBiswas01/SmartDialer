# Architecture Decision Records (ADRs)

## 1. Single Process, Database-Only State (No Queues)
**What:** The system avoids Redis, RabbitMQ, Kafka, or Celery. All state, events, and concurrency locks are managed through the relational database (SQLite/PostgreSQL).
**Why:** Simplicity and robustness. Distributed state introduces network partitions, split-brain scenarios, and message acknowledgement races. By pushing all state into the DB, the system is fundamentally crash-recoverable.
**What it makes harder:** Scaling beyond a single massive database instance. At 10,000+ agents, lock contention on optimistic updates might throttle throughput.
**Fix for 10k agents:** If scaling to massive levels, we would need to shard the `agents` and `borrowers` tables, or switch to a high-throughput partitioned database mechanism, potentially relaxing strict serializability for eventual consistency on pacing checks.

## 2. CP over AP (Consistency/Partition Tolerance over Availability)
**What:** We choose Consistency over Availability. If the database goes down or a transaction fails, the dialer stops dialing rather than guessing.
**Why:** Calling borrowers when no agents are actually available violates FCC guidelines (abandon rate > 3%). It is better to stop calling than to risk compliance violations.

## 3. Optimistic Locking with Leases
**What:** Allocations increment a `version` column and set a `lease_expires_at` timestamp.
**Why:** Prevents two workers from claiming the same agent (race condition). The lease ensures that if a worker crashes midway through an allocation or dialing process, the `reconciler` can sweep expired leases and return the agent to the `AVAILABLE` pool without permanent deadlocks.

## 4. EWMA and AIMD for Predictive Pacing
**What:** Predictive pacing uses an Exponentially Weighted Moving Average (EWMA) for answer rates and talk times, paired with an Additive Increase / Multiplicative Decrease (AIMD) aggressiveness factor (`k`).
**Why:** 
- **EWMA:** Smooths out bursty telecom data but reacts quickly to real changes.
- **AIMD:** Safely explores maximum capacity. If the abandon rate spikes, `k` is immediately halved (multiplicative decrease). When safe, `k` slowly climbs back up (additive increase).
**What it makes harder:** Requires careful tuning of `alpha` and `k` step sizes to avoid wild oscillations in dial rates.

## 5. Strict Separation of Pacing and Safety
**What:** Pacing strategies are pure mathematical functions that output a proposed dial count. The `SafetyController` is the only entity allowed to actually command the allocator.
**Why:** Prevents "smart" predictive algorithms from accidentally violating compliance. The SafetyController enforces the 3% abandon rate cap and provider circuit breakers as hard, un-bypassable rules.

## 6. Workers = Processes
**What:** Worker threads in this architecture stand in for separate worker processes. Correctness relies entirely on atomic conditional database updates (optimistic concurrency control), rather than in-memory locks or thread-level coordination.
**Why:** This design guarantees that the exact same code runs correctly and safely as `N` completely independent, horizontally scaled processes (e.g. running across multiple containers against a central PostgreSQL database).
**Proof:** We cite the `test_multi_instance` integration test, which spins up concurrent, isolated database connections to mimic completely independent worker processes racing for allocations without causing double bookings.
