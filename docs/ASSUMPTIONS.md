# Pulse: Architectural Assumptions & Design Decisions

This document records the architectural assumptions, trade-offs, and operational decisions made during the design and implementation of Pulse.

---

## 1. Delivery & Execution Semantics: At-Least-Once Delivery
- **Distributed Invariant:** In a distributed system with potential worker crashes, network partitions, and host reboots, achieving true exactly-once execution across arbitrary task side-effects without distributed transactions (e.g., 2PC) is impossible.
- **Pulse Design:** Pulse guarantees **at-least-once delivery**. Every submitted job will be executed at least once and reach a terminal state (`SUCCEEDED` or `FAILED`).
- **Idempotency:** State transitions in PostgreSQL use optimistic conditional updates:
  ```sql
  UPDATE jobs 
  SET status = 'RUNNING', attempts = attempts + 1, worker_id = :worker_id, locked_at = :now
  WHERE id = :id AND status IN ('PENDING', 'RETRYING');
  ```
  If a worker recovers a job that was already succeeded or failed by another thread/process, the update affects 0 rows and the duplicate execution is safely skipped.

---

## 2. Storage Roles: PostgreSQL as Single Source of Truth, Redis as Work Queue
- **PostgreSQL:** Serves as the durable system of record. Every job is committed to PostgreSQL before returning `202 Accepted` to the client. Terminal results, error traces, attempt counters, and timestamps are durably recorded in PostgreSQL.
- **Redis:** Serves as the volatile, low-latency queuing and scheduling buffer. It enables sub-15ms enqueue latencies and non-polling atomic reliable dequeue (`BLMOVE`).
- **Resilience to Redis Failure:** If Redis is restarted, cleared, or flushed (`FLUSHALL`), no jobs are lost. The startup reconciliation worker queries PostgreSQL for all non-terminal jobs (`status IN ('PENDING', 'RUNNING', 'RETRYING')`) and re-enqueues any missing jobs into Redis.

---

## 3. Reliable Dequeue & Worker Crash Protection
- **Reliable Queue Pattern:** Workers dequeue jobs using Redis `BLMOVE` (from the shared `pulse:queue:jobs` into a dedicated per-worker list `pulse:processing:{worker_id}`).
- **Crash Safety:** If a worker process is killed (`SIGKILL` or hardware crash) while executing a job, the job ID remains in `pulse:processing:{worker_id}`.
- **Heartbeats & Reaper:** Every worker refreshes an ephemeral heartbeat key `pulse:heartbeat:{worker_id}` in Redis every 3 seconds (TTL: 10 seconds). A reaper process scans for expired worker heartbeats and atomically moves orphaned jobs back into `pulse:queue:jobs`.

---

## 4. Retries & Exponential Backoff
- **Backoff Formula:**
  $$\text{delay} = \text{base} \times 2^{\text{attempt}} + \text{jitter}$$
  where $\text{base} = 1.0\text{s}$ (configurable) and $\text{jitter} \sim \text{Uniform}(0, 0.5 \times \text{delay})$.
- **Delayed Queue:** Scheduled retries are added to a Redis sorted set `pulse:queue:delayed` with the target execution timestamp as score. An atomic Lua script promotes ready tasks to the active list without duplicate promotion.
- **Max Attempts:** Configurable (default: 3). When attempts exceed `max_attempts`, the job transitions to `FAILED` and preserves the last exception traceback.

---

## 5. Worker Concurrency & Multi-Process Execution
- Configured via `WORKER_CONCURRENCY=8` (default: 8 worker processes).
- Multi-processing ensures that CPU-intensive or long-running tasks in Python do not block the event loop or hit Global Interpreter Lock (GIL) bottlenecks across worker cores.

---

## 6. Portability & Stack Orchestration
- The project is fully configured for containerized execution via `docker compose up --build` with exactly 4 services (`postgres`, `redis`, `api`, `worker`) and healthchecks on all services.
- For host environments lacking setuid unprivileged user namespaces or Docker daemons, an equivalent native process orchestrator (`scripts/dev_stack.sh`) is provided so that the entire 4-service stack, migrations, tests, load benchmarks, and chaos suites execute cleanly with zero manual intervention.
