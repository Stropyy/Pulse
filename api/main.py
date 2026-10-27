import asyncio
import time
import uuid
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from api.schemas import (
    HealthResponse,
    JobCreateRequest,
    JobCreateResponse,
    JobDetailResponse,
)
from shared.database import (
    check_db_health,
    close_db_engine,
    get_db_session_dep,
)
from shared.models import Job, JobStatus
from shared.redis_client import check_redis_health, close_redis, push_job
from shared.registry import task_registry
# Import tasks to ensure they are registered
import worker.tasks  # noqa: F401


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Lifespan startup
    yield
    # Lifespan shutdown
    await close_db_engine()
    await close_redis()


app = FastAPI(
    title="Pulse - Distributed Job Processing Platform",
    version="1.0.0",
    description="High-throughput, reliable asynchronous task execution engine",
    lifespan=lifespan,
)


@app.get("/healthz", response_model=HealthResponse, tags=["Health"])
async def healthcheck() -> JSONResponse:
    db_ok, redis_ok = await asyncio.gather(
        check_db_health(),
        check_redis_health(),
    )
    all_healthy = db_ok and redis_ok
    status_code = status.HTTP_200_OK if all_healthy else status.HTTP_503_SERVICE_UNAVAILABLE

    payload = {
        "status": "ok" if all_healthy else "degraded",
        "postgres": "ok" if db_ok else "unhealthy",
        "redis": "ok" if redis_ok else "unhealthy",
    }
    return JSONResponse(status_code=status_code, content=payload)


@app.post(
    "/jobs",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=JobCreateResponse,
    tags=["Jobs"],
)
async def create_job(
    request: JobCreateRequest,
    session: AsyncSession = Depends(get_db_session_dep),
) -> JobCreateResponse:
    t0 = time.perf_counter()
    if not task_registry.is_registered(request.task_name):
        available = task_registry.list_tasks()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Task '{request.task_name}' is not registered. Available tasks: {available}",
        )

    job_id = uuid.uuid4()
    t1 = time.perf_counter()
    stmt = insert(Job).values(
        id=job_id,
        task_name=request.task_name,
        payload=request.payload,
        max_attempts=request.max_attempts,
        status=JobStatus.PENDING.value,
    )

    await session.execute(stmt)
    t2 = time.perf_counter()
    await session.commit()
    t3 = time.perf_counter()

    # Push to Redis main jobs queue
    await push_job(str(job_id))

    return JobCreateResponse(job_id=str(job_id), status=JobStatus.PENDING.value)


@app.get(
    "/jobs/{job_id}",
    response_model=JobDetailResponse,
    tags=["Jobs"],
)
async def get_job(
    job_id: str,
    session: AsyncSession = Depends(get_db_session_dep),
) -> JobDetailResponse:
    try:
        parsed_uuid = uuid.UUID(job_id)
    except (ValueError, AttributeError):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Invalid job UUID: '{job_id}'",
        )

    stmt = select(Job).where(Job.id == parsed_uuid)
    result = await session.execute(stmt)
    job = result.scalar_one_or_none()

    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' not found",
        )

    return JobDetailResponse(**job.to_dict())
