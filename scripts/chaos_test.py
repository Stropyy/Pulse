#!/usr/bin/env python3
import argparse
import asyncio
import os
import random
import subprocess
import time
from typing import Dict, List, Set

import httpx

DEFAULT_BASE_URL = "http://127.0.0.1:8000"


def kill_random_workers() -> List[int]:
    """Finds running worker processes and kills a subset with SIGKILL."""
    killed_pids = []
    # If Docker container exists and docker is running
    try:
        res = subprocess.run(
            ["docker", "kill", "-s", "KILL", "pulse_worker"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if res.returncode == 0:
            print("[Chaos] Injected 'docker kill -s KILL pulse_worker'!")
            subprocess.run(["docker", "start", "pulse_worker"], capture_output=True, timeout=10)
            return [1]
    except Exception:
        pass

    # Native worker process kill
    try:
        output = subprocess.check_output(
            ["pgrep", "-f", "worker.process"],
            text=True,
        ).strip()
        pids = [int(p) for p in output.split() if p.isdigit()]
        if pids:
            # Kill 2-4 worker processes violently with SIGKILL
            num_to_kill = min(len(pids), random.randint(2, 4))
            targets = random.sample(pids, num_to_kill)
            for pid in targets:
                try:
                    os.kill(pid, 9)
                    killed_pids.append(pid)
                except ProcessLookupError:
                    pass
            print(f"[Chaos] Force killed worker PIDs {killed_pids} with SIGKILL (-9)!")
    except subprocess.CalledProcessError:
        pass

    return killed_pids


def flush_redis_state() -> bool:
    """Executes FLUSHALL on Redis to simulate total volatile memory wipeout."""
    try:
        subprocess.run(["redis-cli", "-p", "6379", "FLUSHALL"], check=True, capture_output=True)
        print("[Chaos] Successfully executed 'redis-cli FLUSHALL'! Volatile queue wiped!")
        return True
    except Exception as e:
        print(f"[Chaos] Error flushing redis: {e}")
        return False


def trigger_reconciliation() -> None:
    """Triggers reconciliation if running in native mode or restarts container."""
    try:
        subprocess.run(
            [".venv/bin/python", "-c", "import asyncio; from worker.reaper import startup_reconciliation; asyncio.run(startup_reconciliation())"],
            capture_output=True,
            timeout=10,
        )
        print("[Chaos] Triggered database-to-Redis reconciliation!")
    except Exception as e:
        print(f"[Chaos] Could not trigger manual reconciliation: {e}")


async def submit_test_jobs(client: httpx.AsyncClient, base_url: str, count: int) -> List[str]:
    print(f"\nSubmitting {count} jobs across diverse task types...")
    job_ids = []
    sem = asyncio.Semaphore(50)

    async def submit_one(i: int):
        task_choice = i % 3
        if task_choice == 0:
            task_name = "data_transform"
            payload = {
                "records": [{"id": j, "val": j * 5} for j in range(1, 5)],
                "operations": ["uppercase", "scale"],
                "scale_factor": 1.25,
            }
            max_attempts = 3
        elif task_choice == 1:
            task_name = "slow_report"
            payload = {"duration": 0.2, "rows": 100}
            max_attempts = 3
        else:
            task_name = "flaky_task"
            payload = {"fail_until_attempt": 1, "fail_rate": 0.3}
            max_attempts = 3

        async with sem:
            resp = await client.post(
                f"{base_url}/jobs",
                json={"task_name": task_name, "payload": payload, "max_attempts": max_attempts},
            )
            if resp.status_code == 202:
                job_ids.append(resp.json()["job_id"])
            else:
                print(f"[ERROR] Failed to submit job: {resp.status_code}")

    tasks = [submit_one(i) for i in range(count)]
    await asyncio.gather(*tasks)
    print(f"Successfully enqueued {len(job_ids)}/{count} jobs.")
    return job_ids


async def monitor_until_terminal(
    client: httpx.AsyncClient,
    base_url: str,
    job_ids: List[str],
    timeout_seconds: float = 90.0,
) -> Dict[str, dict]:
    results: Dict[str, dict] = {}
    remaining: Set[str] = set(job_ids)
    start_time = time.time()

    print("\nWaiting for all jobs to reach a terminal state (SUCCEEDED or FAILED)...")
    while remaining and (time.time() - start_time < timeout_seconds):
        batch = list(remaining)[:50]
        for jid in batch:
            try:
                res = await client.get(f"{base_url}/jobs/{jid}")
                if res.status_code == 200:
                    data = res.json()
                    status = data.get("status")
                    if status in ("SUCCEEDED", "FAILED"):
                        results[jid] = data
                        remaining.remove(jid)
            except Exception:
                pass

        elapsed = time.time() - start_time
        completed = len(job_ids) - len(remaining)
        pct = (completed / len(job_ids)) * 100.0
        print(f"\rElapsed: {elapsed:.1f}s | Finished: {completed}/{len(job_ids)} ({pct:.1f}%)", end="", flush=True)
        await asyncio.sleep(0.5)

    print()
    return results


async def run_chaos_test(base_url: str, job_count: int) -> None:
    print("=" * 65)
    print("                PULSE CHAOS ENGINEERING SUITE")
    print("=" * 65)
    print(f"Target:            {base_url}")
    print(f"Total Jobs:        {job_count}")
    print("Chaos Injections:  Worker SIGKILL mid-run + Redis FLUSHALL")
    print("Invariant:         100% terminal state reached, 0 lost jobs")
    print("=" * 65)

    limits = httpx.Limits(max_connections=100, max_keepalive_connections=50)
    async with httpx.AsyncClient(limits=limits, timeout=15.0) as client:
        # Check health
        health = await client.get(f"{base_url}/healthz")
        assert health.status_code == 200, f"Stack unhealthy: {health.text}"

        # 1. Enqueue 500 jobs
        job_ids = await submit_test_jobs(client, base_url, job_count)
        assert len(job_ids) == job_count, "Not all jobs were enqueued"

        # 2. Chaos Injection 1: Kill random workers mid-run
        await asyncio.sleep(1.0)
        print("\n--- INJECTING CHAOS PHASE 1: WORKER TERMINATION ---")
        killed = kill_random_workers()

        # Allow workers to restart or supervisor to respawn them
        await asyncio.sleep(2.0)

        # 3. Chaos Injection 2: Flush Redis
        print("\n--- INJECTING CHAOS PHASE 2: REDIS FLUSHALL ---")
        flush_redis_state()

        # Trigger reconciliation to restore Redis queues from Postgres
        await asyncio.sleep(1.0)
        trigger_reconciliation()

        # 4. Monitor until all jobs reach terminal state
        results = await monitor_until_terminal(client, base_url, job_ids, timeout_seconds=90.0)

    # 5. Assertions and reporting
    terminal_count = len(results)
    succeeded_count = sum(1 for d in results.values() if d.get("status") == "SUCCEEDED")
    failed_count = sum(1 for d in results.values() if d.get("status") == "FAILED")
    lost_count = job_count - terminal_count
    retried_count = sum(1 for d in results.values() if d.get("attempts", 0) > 1)

    print("\n" + "=" * 65)
    print("CHAOS TEST RESULTS:")
    print(f"  Total Jobs Submitted:       {job_count}")
    print(f"  Reached Terminal State:     {terminal_count} ({(terminal_count/job_count)*100:.1f}%)")
    print(f"    - SUCCEEDED:              {succeeded_count}")
    print(f"    - FAILED (Max attempts):  {failed_count}")
    print(f"  Jobs Retried (>1 attempt):  {retried_count}")
    print(f"  Lost Jobs:                  {lost_count}")
    print("=" * 65)

    assert lost_count == 0, f"FAILED: {lost_count} jobs were lost during chaos test!"
    assert terminal_count == job_count, f"FAILED: Only {terminal_count}/{job_count} reached terminal state!"
    print("\n>>> ALL ACCEPTANCE CHECKS PASSED: ZERO JOBS LOST UNDER CHAOS <<<\n")


def main():
    parser = argparse.ArgumentParser(description="Pulse chaos engineering test")
    parser.add_argument("--url", default=DEFAULT_BASE_URL, help="Base API URL")
    parser.add_argument("--jobs", type=int, default=500, help="Number of jobs to submit")
    args = parser.parse_args()

    asyncio.run(run_chaos_test(args.url, args.jobs))


if __name__ == "__main__":
    main()
