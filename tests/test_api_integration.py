import asyncio
import time
import httpx
import pytest

BASE_URL = "http://127.0.0.1:8000"


@pytest.mark.asyncio
async def test_healthz_endpoint():
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=5.0) as client:
        resp = await client.get("/healthz")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["postgres"] == "ok"
        assert data["redis"] == "ok"


@pytest.mark.asyncio
async def test_create_job_invalid_task():
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=5.0) as client:
        resp = await client.post("/jobs", json={"task_name": "unknown_random_task", "payload": {}})
        assert resp.status_code == 400
        assert "not registered" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_submit_job_reaches_succeeded():
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        # Submit job
        create_resp = await client.post(
            "/jobs",
            json={
                "task_name": "data_transform",
                "payload": {
                    "records": [{"id": 1, "name": "pulse", "val": 100}],
                    "operations": ["uppercase", "scale"],
                    "scale_factor": 1.1,
                },
            },
        )
        assert create_resp.status_code == 202
        job_id = create_resp.json()["job_id"]
        assert job_id is not None

        # Poll status until SUCCEEDED
        start_time = time.time()
        final_job = None
        while time.time() - start_time < 10.0:
            status_resp = await client.get(f"/jobs/{job_id}")
            assert status_resp.status_code == 200
            data = status_resp.json()
            if data["status"] == "SUCCEEDED":
                final_job = data
                break
            await asyncio.sleep(0.2)

        assert final_job is not None, f"Job {job_id} did not reach SUCCEEDED within timeout"
        assert final_job["status"] == "SUCCEEDED"
        assert final_job["attempts"] >= 1
        assert final_job["result"] is not None
        assert final_job["result"]["transformed_records"][0]["name"] == "PULSE"
        assert final_job["completed_at"] is not None


@pytest.mark.asyncio
async def test_flaky_task_retries_and_eventually_succeeds():
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=15.0) as client:
        # Submit flaky job configured to fail on attempt 1, succeed on attempt 2
        create_resp = await client.post(
            "/jobs",
            json={
                "task_name": "flaky_task",
                "payload": {"fail_until_attempt": 1, "fail_rate": 0.0},
                "max_attempts": 3,
            },
        )
        assert create_resp.status_code == 202
        job_id = create_resp.json()["job_id"]

        start_time = time.time()
        final_job = None
        seen_statuses = set()

        while time.time() - start_time < 15.0:
            status_resp = await client.get(f"/jobs/{job_id}")
            data = status_resp.json()
            seen_statuses.add(data["status"])
            if data["status"] in ("SUCCEEDED", "FAILED"):
                final_job = data
                break
            await asyncio.sleep(0.3)

        assert final_job is not None, f"Job {job_id} did not complete within timeout"
        assert final_job["status"] == "SUCCEEDED"
        assert final_job["attempts"] >= 2, f"Expected at least 2 attempts, got {final_job['attempts']}"
        assert final_job["result"]["status"] == "recovered"


@pytest.mark.asyncio
async def test_flaky_task_exceeding_max_attempts_fails():
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=15.0) as client:
        # Submit job configured to fail until attempt 5, but max_attempts is only 2
        create_resp = await client.post(
            "/jobs",
            json={
                "task_name": "flaky_task",
                "payload": {"fail_until_attempt": 5, "fail_rate": 1.0},
                "max_attempts": 2,
            },
        )
        assert create_resp.status_code == 202
        job_id = create_resp.json()["job_id"]

        start_time = time.time()
        final_job = None
        while time.time() - start_time < 15.0:
            status_resp = await client.get(f"/jobs/{job_id}")
            data = status_resp.json()
            if data["status"] == "FAILED":
                final_job = data
                break
            await asyncio.sleep(0.3)

        assert final_job is not None, f"Job {job_id} did not reach FAILED"
        assert final_job["status"] == "FAILED"
        assert final_job["attempts"] == 2
        assert final_job["error"] is not None
        assert "Transient upstream failure" in final_job["error"]
