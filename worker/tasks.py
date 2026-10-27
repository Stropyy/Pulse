import asyncio
import random
import time
from typing import Any

from shared.registry import task_registry


@task_registry.register("slow_report")
async def slow_report(payload: dict[str, Any]) -> dict[str, Any]:
    """Generates an aggregated report after a simulated slow calculation."""
    duration = float(payload.get("duration", 0.5))
    rows = int(payload.get("rows", 500))
    report_type = payload.get("report_type", "financial_summary")

    # Simulate I/O or compute-heavy aggregation
    await asyncio.sleep(duration)

    summary = {
        "report_type": report_type,
        "rows_processed": rows,
        "total_revenue": rows * 42.50,
        "average_ticket": 42.50,
        "generated_at": time.time(),
        "status": "completed",
    }
    return summary


@task_registry.register("flaky_task")
async def flaky_task(payload: dict[str, Any], attempt: int = 1) -> dict[str, Any]:
    """Simulates a task that fails transiently to exercise retries & exponential backoff.

    Configurable via payload:
      - fail_until_attempt: always fail if attempt <= fail_until_attempt (default 2)
      - fail_rate: random probability of failure (default 0.7 if fail_until_attempt not reached)
    """
    fail_until_attempt = int(payload.get("fail_until_attempt", 2))
    fail_rate = float(payload.get("fail_rate", 0.7))

    if attempt <= fail_until_attempt:
        raise RuntimeError(
            f"Transient upstream failure during attempt {attempt}: connection reset by peer"
        )

    return {
        "status": "recovered",
        "attempt_succeeded": attempt,
        "message": f"Flaky task successfully recovered on attempt {attempt}",
        "timestamp": time.time(),
    }


@task_registry.register("data_transform")
async def data_transform(payload: dict[str, Any]) -> dict[str, Any]:
    """Transforms and normalizes a dataset of records."""
    records = payload.get("records", [
        {"id": 1, "name": "alpha", "val": 10},
        {"id": 2, "name": "beta", "val": 20},
        {"id": 3, "name": "gamma", "val": 30},
    ])
    operations = payload.get("operations", ["uppercase", "scale"])
    scale_factor = float(payload.get("scale_factor", 1.5))

    transformed = []
    total_val = 0.0
    for r in records:
        item = dict(r)
        if "uppercase" in operations and "name" in item:
            item["name"] = str(item["name"]).upper()
        if "scale" in operations and "val" in item:
            item["val"] = float(item["val"]) * scale_factor
        total_val += float(item.get("val", 0))
        transformed.append(item)

    return {
        "transformed_records": transformed,
        "record_count": len(transformed),
        "total_value": total_val,
        "processed_at": time.time(),
    }
