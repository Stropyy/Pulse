import asyncio
import logging
import os
import signal
import time
import traceback
import uuid
from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from shared.config import get_settings
from shared.database import close_db_engine, get_db_session
from shared.models import Job, JobStatus, utc_now
from shared.redis_client import (
    ack_job,
    close_redis,
    get_redis_client,
    pop_job_reliable,
    schedule_delayed_job,
    set_worker_heartbeat,
)
from shared.registry import task_registry
from shared.retry import calculate_backoff_delay, should_retry
# Import tasks to ensure registration
import worker.tasks  # noqa: F401

logger = logging.getLogger(__name__)


class WorkerProcess:
    def __init__(self, worker_index: int, worker_id: Optional[str] = None) -> None:
        self.worker_index = worker_index
        self.worker_id = worker_id or f"worker-{os.getpid()}-{worker_index}"
        self.settings = get_settings()
        self.running = False
        self.current_job_id: Optional[str] = None
        self._shutdown_event = asyncio.Event()

    async def start(self) -> None:
        self.running = True
        logger.info(f"[{self.worker_id}] Starting worker process (Index: {self.worker_index})...")

        # Setup signal handlers within async loop
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._handle_signal)
            except NotImplementedError:
                # Windows or unsupported environment
                pass

        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        worker_task = asyncio.create_task(self._work_loop())

        await self._shutdown_event.wait()
        logger.info(f"[{self.worker_id}] Shutdown event received. Cleaning up...")

        worker_task.cancel()
        heartbeat_task.cancel()
        await asyncio.gather(worker_task, heartbeat_task, return_exceptions=True)

        # Cleanup Redis heartbeat
        try:
            r = get_redis_client()
            await r.delete(f"pulse:heartbeat:{self.worker_id}")
            await r.srem("pulse:active_workers", self.worker_id)
        except Exception:
            pass

        await close_db_engine()
        await close_redis()
        logger.info(f"[{self.worker_id}] Worker stopped cleanly.")

    def _handle_signal(self) -> None:
        logger.info(f"[{self.worker_id}] Received termination signal.")
        self.running = False
        self._shutdown_event.set()

    async def _heartbeat_loop(self) -> None:
        while self.running:
            try:
                await set_worker_heartbeat(self.worker_id, self.settings.HEARTBEAT_TTL_SECONDS)
            except Exception as e:
                logger.warning(f"[{self.worker_id}] Heartbeat update error: {e}")
            await asyncio.sleep(self.settings.HEARTBEAT_INTERVAL_SECONDS)

    async def _work_loop(self) -> None:
        while self.running:
            try:
                # Reliable dequeue: pops from pulse:queue:jobs into pulse:processing:{worker_id}
                job_id = await pop_job_reliable(self.worker_id, timeout=2)
                if not job_id:
                    continue

                self.current_job_id = job_id
                await self._process_job(job_id)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.worker_id}] Unexpected error in work loop: {e}\n{traceback.format_exc()}")
                await asyncio.sleep(0.5)
            finally:
                self.current_job_id = None

    async def _process_job(self, job_id_str: str) -> None:
        try:
            job_uuid = uuid.UUID(job_id_str)
        except (ValueError, AttributeError):
            logger.error(f"[{self.worker_id}] Malformed job UUID: {job_id_str}. Dropping.")
            await ack_job(self.worker_id, job_id_str)
            return

        async with get_db_session() as session:
            # 1. Fetch current job state
            stmt = select(Job).where(Job.id == job_uuid)
            result = await session.execute(stmt)
            job = result.scalar_one_or_none()

            if job is None:
                logger.warning(f"[{self.worker_id}] Job {job_id_str} not found in DB. Acknowledging.")
                await ack_job(self.worker_id, job_id_str)
                return

            if job.status in JobStatus.terminal_statuses():
                logger.info(f"[{self.worker_id}] Job {job_id_str} already in terminal state {job.status}. Skipping.")
                await ack_job(self.worker_id, job_id_str)
                return

            # 2. Idempotent state transition to RUNNING
            # Only transitions from PENDING or RETRYING
            now = utc_now()
            claim_stmt = (
                update(Job)
                .where(
                    Job.id == job_uuid,
                    Job.status.in_([JobStatus.PENDING.value, JobStatus.RETRYING.value, JobStatus.RUNNING.value]),
                )
                .values(
                    status=JobStatus.RUNNING.value,
                    attempts=Job.attempts + 1,
                    worker_id=self.worker_id,
                    started_at=now if job.started_at is None else job.started_at,
                    locked_at=now,
                )
                .returning(Job.attempts, Job.max_attempts, Job.task_name, Job.payload)
            )
            claim_res = await session.execute(claim_stmt)
            claimed_row = claim_res.fetchone()
            await session.commit()

            if not claimed_row:
                logger.warning(f"[{self.worker_id}] Failed to claim job {job_id_str} (concurrency check). Skipping.")
                await ack_job(self.worker_id, job_id_str)
                return

            current_attempt, max_attempts, task_name, payload = claimed_row

        # 3. Execute the task
        logger.info(
            f"[{self.worker_id}] Executing task '{task_name}' for job {job_id_str} "
            f"(attempt {current_attempt}/{max_attempts})..."
        )

        task_success = False
        task_result = None
        error_msg = None

        try:
            task_result = await task_registry.execute(
                task_name,
                payload,
                attempt=current_attempt,
            )
            task_success = True
        except Exception as exc:
            task_success = False
            error_msg = f"{type(exc).__name__}: {str(exc)}\n{traceback.format_exc()}"
            logger.warning(
                f"[{self.worker_id}] Task execution failed for job {job_id_str}: {exc}"
            )

        # 4. Handle outcome: SUCCEEDED, RETRYING, or FAILED
        async with get_db_session() as session:
            completed_time = utc_now()
            if task_success:
                success_stmt = (
                    update(Job)
                    .where(Job.id == job_uuid, Job.status == JobStatus.RUNNING.value)
                    .values(
                        status=JobStatus.SUCCEEDED.value,
                        result=task_result,
                        error=None,
                        completed_at=completed_time,
                        locked_at=None,
                    )
                )
                await session.execute(success_stmt)
                await session.commit()
                # Remove from reliable processing queue
                await ack_job(self.worker_id, job_id_str)
                logger.info(f"[{self.worker_id}] Job {job_id_str} reached SUCCEEDED.")
            else:
                # Check retry eligibility
                if should_retry(current_attempt, max_attempts):
                    delay = calculate_backoff_delay(
                        current_attempt,
                        base=self.settings.RETRY_BASE_DELAY,
                        jitter_max=self.settings.RETRY_JITTER_MAX,
                    )
                    next_run_timestamp = time.time() + delay
                    retry_stmt = (
                        update(Job)
                        .where(Job.id == job_uuid, Job.status == JobStatus.RUNNING.value)
                        .values(
                            status=JobStatus.RETRYING.value,
                            error=error_msg,
                            locked_at=None,
                        )
                    )
                    await session.execute(retry_stmt)
                    await session.commit()

                    # Add to delayed sorted set and remove from worker processing queue
                    await schedule_delayed_job(job_id_str, next_run_timestamp)
                    await ack_job(self.worker_id, job_id_str)
                    logger.info(
                        f"[{self.worker_id}] Job {job_id_str} scheduled for retry in {delay:.2f}s "
                        f"(attempt {current_attempt}/{max_attempts})."
                    )
                else:
                    # Max attempts exceeded -> mark FAILED
                    fail_stmt = (
                        update(Job)
                        .where(Job.id == job_uuid, Job.status == JobStatus.RUNNING.value)
                        .values(
                            status=JobStatus.FAILED.value,
                            error=error_msg,
                            completed_at=completed_time,
                            locked_at=None,
                        )
                    )
                    await session.execute(fail_stmt)
                    await session.commit()
                    await ack_job(self.worker_id, job_id_str)
                    logger.info(f"[{self.worker_id}] Job {job_id_str} reached terminal state FAILED.")


def run_worker_process(worker_index: int, worker_id: Optional[str] = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    )
    worker = WorkerProcess(worker_index=worker_index, worker_id=worker_id)
    asyncio.run(worker.start())


if __name__ == "__main__":
    import sys
    idx = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    run_worker_process(idx)
