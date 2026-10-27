import asyncio
import datetime
import logging
import time
from typing import Set

from sqlalchemy import select, update

from shared.config import get_settings
from shared.database import get_db_session
from shared.models import Job, JobStatus, utc_now
from shared.redis_client import (
    ACTIVE_WORKERS,
    PROCESSING_PREFIX,
    QUEUE_DELAYED,
    QUEUE_JOBS,
    get_redis_client,
    is_worker_alive,
    promote_delayed_jobs,
    push_job,
)

logger = logging.getLogger(__name__)


async def startup_reconciliation() -> int:
    """Reconciles PostgreSQL against Redis on startup.

    Finds any PENDING, RUNNING, or RETRYING jobs in PostgreSQL that are missing
    from Redis (active, delayed, or processing queues) and re-enqueues them.
    Guarantees no submitted job is lost even after Redis FLUSHALL or reboot.
    """
    settings = get_settings()
    logger.info("[Reconciliation] Starting database-to-Redis reconciliation...")

    # 1. Fetch all non-terminal jobs from Postgres
    async with get_db_session() as session:
        stmt = select(Job.id, Job.status).where(
            Job.status.in_([
                JobStatus.PENDING.value,
                JobStatus.RUNNING.value,
                JobStatus.RETRYING.value,
            ])
        )
        result = await session.execute(stmt)
        non_terminal_jobs = result.all()

    if not non_terminal_jobs:
        logger.info("[Reconciliation] No non-terminal jobs found in PostgreSQL.")
        return 0

    logger.info(f"[Reconciliation] Found {len(non_terminal_jobs)} non-terminal jobs in PostgreSQL. Inspecting Redis...")

    # 2. Gather all job IDs currently known to Redis
    redis = get_redis_client()
    redis_job_ids: Set[str] = set()

    # Active queue
    active_ids = await redis.lrange(QUEUE_JOBS, 0, -1)
    redis_job_ids.update(active_ids)

    # Delayed queue
    delayed_ids = await redis.zrange(QUEUE_DELAYED, 0, -1)
    redis_job_ids.update(delayed_ids)

    # Processing queues
    cursor = 0
    while True:
        cursor, keys = await redis.scan(cursor=cursor, match=f"{PROCESSING_PREFIX}*", count=100)
        for key in keys:
            proc_ids = await redis.lrange(key, 0, -1)
            redis_job_ids.update(proc_ids)
        if cursor == 0:
            break

    # 3. Identify and re-enqueue missing jobs
    re_enqueued_count = 0
    missing_ids = []
    for job_id, current_status in non_terminal_jobs:
        job_id_str = str(job_id)
        if job_id_str not in redis_job_ids:
            missing_ids.append((job_id, current_status))

    if missing_ids:
        logger.warning(
            f"[Reconciliation] Discovered {len(missing_ids)} non-terminal jobs missing from Redis! "
            f"Restoring to queue..."
        )
        async with get_db_session() as session:
            for job_id, current_status in missing_ids:
                job_id_str = str(job_id)
                # Reset RUNNING state back to PENDING so workers can claim fresh
                if current_status == JobStatus.RUNNING.value:
                    await session.execute(
                        update(Job)
                        .where(Job.id == job_id, Job.status == JobStatus.RUNNING.value)
                        .values(status=JobStatus.PENDING.value, locked_at=None, worker_id=None)
                    )
                await push_job(job_id_str)
                re_enqueued_count += 1
            await session.commit()

        logger.info(f"[Reconciliation] Successfully re-enqueued {re_enqueued_count} jobs.")
    else:
        logger.info("[Reconciliation] All PostgreSQL non-terminal jobs are present in Redis.")

    return re_enqueued_count


class Reaper:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.running = False
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        self.running = True
        logger.info("[Reaper] Reaper background engine started.")
        reaper_task = asyncio.create_task(self._reaper_loop())
        promoter_task = asyncio.create_task(self._delayed_promoter_loop())

        await self._stop_event.wait()
        reaper_task.cancel()
        promoter_task.cancel()
        await asyncio.gather(reaper_task, promoter_task, return_exceptions=True)
        logger.info("[Reaper] Reaper background engine stopped.")

    def stop(self) -> None:
        self.running = False
        self._stop_event.set()

    async def _delayed_promoter_loop(self) -> None:
        """Continuously checks the delayed sorted set and promotes ready jobs."""
        while self.running:
            try:
                promoted = await promote_delayed_jobs(batch_size=100)
                if promoted > 0:
                    logger.info(f"[Reaper] Promoted {promoted} delayed jobs to main queue.")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[Reaper] Error promoting delayed jobs: {e}")
            await asyncio.sleep(0.5)

    async def _reaper_loop(self) -> None:
        """Periodically scans for dead workers and stale running tasks."""
        while self.running:
            try:
                await self.reap_stale_workers()
                await self.reap_stale_database_jobs()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[Reaper] Error during reaping cycle: {e}")
            await asyncio.sleep(self.settings.REAPER_INTERVAL_SECONDS)

    async def reap_stale_workers(self) -> int:
        """Inspects all per-worker processing queues.

        If a worker has no active heartbeat, its processing queue is emptied
        back into pulse:queue:jobs.
        """
        redis = get_redis_client()
        reaped_jobs = 0

        cursor = 0
        while True:
            cursor, keys = await redis.scan(cursor=cursor, match=f"{PROCESSING_PREFIX}*", count=100)
            for key in keys:
                worker_id = key.removeprefix(PROCESSING_PREFIX)
                alive = await is_worker_alive(worker_id)
                if not alive:
                    # Worker is dead; recover all in-flight jobs
                    stale_jobs = await redis.lrange(key, 0, -1)
                    if stale_jobs:
                        logger.warning(
                            f"[Reaper] Worker '{worker_id}' is DEAD. "
                            f"Recovering {len(stale_jobs)} jobs from {key}..."
                        )
                        pipe = redis.pipeline(transaction=True)
                        for jid in stale_jobs:
                            pipe.lpush(QUEUE_JOBS, jid)
                        pipe.delete(key)
                        pipe.srem(ACTIVE_WORKERS, worker_id)
                        await pipe.execute()
                        reaped_jobs += len(stale_jobs)
                    else:
                        # Queue is empty, delete key
                        await redis.delete(key)
                        await redis.srem(ACTIVE_WORKERS, worker_id)
            if cursor == 0:
                break

        return reaped_jobs

    async def reap_stale_database_jobs(self) -> int:
        """Finds jobs stuck in RUNNING in Postgres whose lock has expired or whose worker is dead."""
        threshold = utc_now() - datetime.timedelta(seconds=self.settings.STALE_JOB_TIMEOUT_SECONDS)
        recovered_count = 0

        async with get_db_session() as session:
            # Query RUNNING jobs locked before the threshold
            stmt = select(Job).where(
                Job.status == JobStatus.RUNNING.value,
                Job.locked_at < threshold,
            )
            res = await session.execute(stmt)
            stale_jobs = res.scalars().all()

            for job in stale_jobs:
                # Check if worker is dead or missing
                is_alive = False
                if job.worker_id:
                    is_alive = await is_worker_alive(job.worker_id)

                if not is_alive:
                    logger.warning(
                        f"[Reaper] Job {job.id} stuck in RUNNING under dead worker '{job.worker_id}'. "
                        f"Resetting to PENDING..."
                    )
                    job.status = JobStatus.PENDING.value
                    job.worker_id = None
                    job.locked_at = None
                    # Ensure it is enqueued in Redis
                    await push_job(str(job.id))
                    recovered_count += 1

            if recovered_count > 0:
                await session.commit()
                logger.info(f"[Reaper] Recovered {recovered_count} stale database jobs.")

        return recovered_count
