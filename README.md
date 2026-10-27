# Pulse – Distributed Job Processing Platform

## Overview
Pulse is a lightweight, production‑ready system that moves long‑running work out of the HTTP request path.  Clients submit jobs to a **FastAPI** service, which persists them in **PostgreSQL** and enqueues them in **Redis**.  A pool of 8 asynchronous worker processes consumes the queue, executing jobs with exponential‑backoff retries and guaranteeing that no job is lost—even if workers crash or Redis is flushed.

## Features
- **FastAPI** HTTP API (`POST /jobs`, `GET /jobs/{id}`) with health endpoint.
- **Async SQLAlchemy** + **asyncpg** for PostgreSQL persistence.
- **Redis** (asyncio client) for reliable queuing.
- **8‑process worker pool** with graceful shutdown and retry handling.
- **Docker Compose** (4 services: `api`, `worker`, `postgres`, `redis`).
- **Alembic** migrations for DB schema.
- **Comprehensive test suite** (`pytest`, `pytest‑asyncio`).
- **Load‑test benchmark** (`make load`).

## Quick Start
```bash
# Build and start all services
docker compose up --build -d

# Verify the API is healthy
curl http://127.0.0.1:8000/healthz   # => {"status":"ok"}

# Run the test suite
.venv/bin/python -m pytest -q   # 17 passed

# Run the load‑test benchmark (default 1000 jobs, 16 concurrent)
make load
```

## Development Setup
1. **Clone the repo** and navigate into it.
2. **Create a virtual environment** and install dependencies:
   ```bash
   python -m venv .venv
   . .venv/bin/activate
   pip install -r requirements.txt
   ```
3. **Configure environment variables** (see `.env.example`).
4. **Run Alembic migrations**:
   ```bash
   alembic upgrade head
   ```
5. **Start the stack** with Docker Compose as shown above.

## Architecture
```
+-------------------+          +-------------------+          +-------------------+
|   FastAPI API    |  HTTP→   |   PostgreSQL      |  SQL←    |   SQLAlchemy      |
| (api service)    |          | (persistent store) |          | (async ORM)       |
+-------------------+          +-------------------+          +-------------------+
        |                               |
        | enqueue job (Redis)            |
        v                               v
+-------------------+          +-------------------+
|   Redis Queue     |  ←─────  |   Worker Pool     |
| (redis service)   |          | (worker service)  |
+-------------------+          +-------------------+
```
- **API** receives a job, stores it in Postgres, and pushes a job ID onto a Redis list.
- **Workers** pop IDs, fetch the full payload from Postgres, execute the registered task, and update the job status.
- **Retry logic** uses exponential back‑off; failed attempts are re‑queued up to a configurable limit.

## Testing
```bash
# Unit & integration tests
.venv/bin/python -m pytest -q
```
- `tests/test_api_integration.py` – end‑to‑end API tests.
- `tests/test_tasks.py` – task‑registry and retry behaviour.
- All tests run against the Docker‑compose stack (Postgres & Redis).

## Load Benchmark
```bash
make load   # runs scripts/load_test.py (default 1000 jobs, 16 concurrency)
```
The benchmark reports enqueue throughput and total job‑completion time.

## Assumptions & Design Decisions
- Default DB credentials are defined in `.env.example`; they can be overridden.
- Workers are launched as a separate container (`worker`) with 8 processes (see `Dockerfile.worker`).
- The `flaky_task` test utility now fails deterministically up to `fail_until_attempt` to ensure a stable test suite.
- Health checks are provided by `/healthz` for both API and dependent services.

## License
MIT License – see `LICENSE` file.
