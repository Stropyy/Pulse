import uuid
import pytest
from sqlalchemy import select, update
from shared.database import get_db_session
from shared.models import Job, JobStatus, utc_now


@pytest.mark.asyncio
async def test_job_state_transitions_and_idempotency():
    async with get_db_session() as session:
        job_id = uuid.uuid4()
        job = Job(
            id=job_id,
            task_name="data_transform",
            payload={"records": []},
            status=JobStatus.PENDING.value,
            max_attempts=3,
        )
        session.add(job)
        await session.commit()

        # Transition PENDING -> RUNNING
        claim_stmt = (
            update(Job)
            .where(Job.id == job_id, Job.status.in_([JobStatus.PENDING.value, JobStatus.RETRYING.value]))
            .values(status=JobStatus.RUNNING.value, attempts=Job.attempts + 1, locked_at=utc_now())
        )
        res = await session.execute(claim_stmt)
        assert res.rowcount == 1
        await session.commit()

        # Verify state is RUNNING and attempts == 1
        fetched = (await session.execute(select(Job).where(Job.id == job_id))).scalar_one()
        assert fetched.status == JobStatus.RUNNING.value
        assert fetched.attempts == 1

        # Transition RUNNING -> RETRYING
        retry_stmt = (
            update(Job)
            .where(Job.id == job_id, Job.status == JobStatus.RUNNING.value)
            .values(status=JobStatus.RETRYING.value, error="Temporary failure", locked_at=None)
        )
        res = await session.execute(retry_stmt)
        assert res.rowcount == 1
        await session.commit()

        # Re-claim RETRYING -> RUNNING
        claim_stmt_2 = (
            update(Job)
            .where(Job.id == job_id, Job.status.in_([JobStatus.PENDING.value, JobStatus.RETRYING.value]))
            .values(status=JobStatus.RUNNING.value, attempts=Job.attempts + 1, locked_at=utc_now())
        )
        res = await session.execute(claim_stmt_2)
        assert res.rowcount == 1
        await session.commit()

        fetched = (await session.execute(select(Job).where(Job.id == job_id))).scalar_one()
        assert fetched.status == JobStatus.RUNNING.value
        assert fetched.attempts == 2

        # Transition RUNNING -> SUCCEEDED (Terminal)
        success_stmt = (
            update(Job)
            .where(Job.id == job_id, Job.status == JobStatus.RUNNING.value)
            .values(status=JobStatus.SUCCEEDED.value, result={"status": "ok"}, locked_at=None)
        )
        res = await session.execute(success_stmt)
        assert res.rowcount == 1
        await session.commit()

        fetched = (await session.execute(select(Job).where(Job.id == job_id))).scalar_one()
        assert fetched.status == JobStatus.SUCCEEDED.value

        # Idempotency check: Cannot transition back to RUNNING once SUCCEEDED
        invalid_claim = (
            update(Job)
            .where(Job.id == job_id, Job.status.in_([JobStatus.PENDING.value, JobStatus.RETRYING.value]))
            .values(status=JobStatus.RUNNING.value)
        )
        res = await session.execute(invalid_claim)
        assert res.rowcount == 0  # 0 rows affected! Safe from race conditions
        await session.commit()
