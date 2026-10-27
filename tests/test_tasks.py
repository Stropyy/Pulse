import pytest
from worker.tasks import data_transform, flaky_task, slow_report


@pytest.mark.asyncio
async def test_slow_report_execution():
    payload = {"report_type": "quarterly_audit", "duration": 0.01, "rows": 100}
    result = await slow_report(payload)
    assert result["report_type"] == "quarterly_audit"
    assert result["rows_processed"] == 100
    assert result["total_revenue"] == 100 * 42.50
    assert result["status"] == "completed"


@pytest.mark.asyncio
async def test_data_transform_execution():
    payload = {
        "records": [
            {"id": 1, "name": "alice", "val": 10},
            {"id": 2, "name": "bob", "val": 20},
        ],
        "operations": ["uppercase", "scale"],
        "scale_factor": 2.0,
    }
    result = await data_transform(payload)
    assert result["record_count"] == 2
    assert result["total_value"] == 60.0  # 20.0 + 40.0
    assert result["transformed_records"][0]["name"] == "ALICE"
    assert result["transformed_records"][1]["name"] == "BOB"


@pytest.mark.asyncio
async def test_flaky_task_failure_and_recovery():
    payload = {"fail_until_attempt": 2}

    # Attempt 1 must fail
    with pytest.raises(RuntimeError, match="Transient upstream failure"):
        await flaky_task(payload, attempt=1)

    # Attempt 2 must fail
    with pytest.raises(RuntimeError, match="Transient upstream failure"):
        await flaky_task(payload, attempt=2)

    # Attempt 3 must succeed
    result = await flaky_task(payload, attempt=3)
    assert result["status"] == "recovered"
    assert result["attempt_succeeded"] == 3
