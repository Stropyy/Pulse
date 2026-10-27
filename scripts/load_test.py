#!/usr/bin/env python3
import argparse
import asyncio
import statistics
import time
from typing import List

import httpx

DEFAULT_BASE_URL = "http://127.0.0.1:8000"


def calculate_percentile(sorted_data: List[float], percentile: float) -> float:
    if not sorted_data:
        return 0.0
    k = (len(sorted_data) - 1) * (percentile / 100.0)
    f = int(k)
    c = f + 1
    if c < len(sorted_data):
        return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])
    else:
        return sorted_data[f]


PAYLOAD_BYTES = b'{"task_name": "data_transform", "payload": {"records": [{"id": 1, "val": 10}, {"id": 2, "val": 20}], "operations": ["scale"], "scale_factor": 1.5}}'
JSON_HEADERS = {"Content-Type": "application/json"}


async def submit_single_job(
    client: httpx.AsyncClient,
    base_url: str,
    sem: asyncio.Semaphore,
    latencies: List[float],
    job_ids: List[str],
) -> None:
    async with sem:
        start = time.perf_counter()
        resp = await client.post(f"{base_url}/jobs", content=PAYLOAD_BYTES, headers=JSON_HEADERS)
        elapsed_ms = (time.perf_counter() - start) * 1000.0

        if resp.status_code == 202:
            latencies.append(elapsed_ms)
            job_ids.append(resp.json()["job_id"])
        else:
            print(f"[ERROR] Failed request: {resp.status_code} {resp.text}")


async def wait_for_jobs_completion(
    client: httpx.AsyncClient,
    base_url: str,
    job_ids: List[str],
    timeout_seconds: float = 60.0,
) -> None:
    print(f"\nMonitoring {len(job_ids)} jobs until all reach terminal state...")
    start = time.time()
    remaining = set(job_ids)

    while remaining and (time.time() - start < timeout_seconds):
        batch = list(remaining)[:50]
        for jid in batch:
            try:
                res = await client.get(f"{base_url}/jobs/{jid}")
                if res.status_code == 200:
                    status = res.json().get("status")
                    if status in ("SUCCEEDED", "FAILED"):
                        remaining.remove(jid)
            except Exception:
                pass
        done_count = len(job_ids) - len(remaining)
        pct = (done_count / len(job_ids)) * 100.0
        print(f"\rProgress: {done_count}/{len(job_ids)} ({pct:.1f}%) completed", end="", flush=True)
        await asyncio.sleep(0.5)

    print()
    if remaining:
        print(f"[Warning] {len(remaining)} jobs did not finish within {timeout_seconds}s timeout.")
    else:
        total_exec_time = time.time() - start
        print(f"[Success] All {len(job_ids)} jobs completed in {total_exec_time:.2f}s!")


async def run_load_test(base_url: str, total_jobs: int, concurrency: int, wait_completion: bool) -> None:
    print("=" * 65)
    print("                PULSE ENQUEUE LOAD BENCHMARK")
    print("=" * 65)
    print(f"Target URL:        {base_url}/jobs")
    print(f"Total Jobs:        {total_jobs}")
    print(f"Concurrency:       {concurrency}")
    print("=" * 65)

    limits = httpx.Limits(max_connections=concurrency * 2, max_keepalive_connections=concurrency)
    timeout = httpx.Timeout(connect=5.0, read=10.0, write=10.0, pool=10.0)

    sem = asyncio.Semaphore(concurrency)
    latencies: List[float] = []
    job_ids: List[str] = []

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        # Pre-flight health check
        health_resp = await client.get(f"{base_url}/healthz")
        if health_resp.status_code != 200:
            print(f"[ERROR] Service healthcheck failed: {health_resp.status_code} {health_resp.text}")
            return

        print("Service is healthy. Warming up connection pool...")
        # Warm-up phase: establish keep-alive connections
        warmup_tasks = [
            client.post(f"{base_url}/jobs", content=PAYLOAD_BYTES, headers=JSON_HEADERS)
            for _ in range(concurrency)
        ]
        await asyncio.gather(*warmup_tasks)
        print("Warm-up complete. Launching benchmark...\n")
        bench_start = time.perf_counter()

        tasks = [
            submit_single_job(client, base_url, sem, latencies, job_ids)
            for _ in range(total_jobs)
        ]
        await asyncio.gather(*tasks)

        total_wall_time = time.perf_counter() - bench_start

    sorted_latencies = sorted(latencies)
    successful_requests = len(sorted_latencies)
    p50 = calculate_percentile(sorted_latencies, 50.0)
    p90 = calculate_percentile(sorted_latencies, 90.0)
    p95 = calculate_percentile(sorted_latencies, 95.0)
    p99 = calculate_percentile(sorted_latencies, 99.0)
    mean_lat = statistics.mean(sorted_latencies) if sorted_latencies else 0.0
    throughput = successful_requests / total_wall_time if total_wall_time > 0 else 0.0

    print("-----------------------------------------------------------------")
    print("BENCHMARK RESULTS:")
    print(f"  Total Jobs Requested:   {total_jobs}")
    print(f"  Successfully Enqueued:  {successful_requests} ({(successful_requests/total_jobs)*100.0:.1f}%)")
    print(f"  Total Duration:         {total_wall_time:.3f} s")
    print(f"  Enqueue Throughput:     {throughput:.1f} req/s")
    print("")
    print("LATENCY DISTRIBUTION (enqueue path):")
    print(f"  Min:                    {sorted_latencies[0]:.2f} ms")
    print(f"  Mean:                   {mean_lat:.2f} ms")
    print(f"  p50 (Median):           {p50:.2f} ms")
    print(f"  p90:                    {p90:.2f} ms")
    print(f"  p95:                    {p95:.2f} ms")
    print(f"  p99:                    {p99:.2f} ms")
    print(f"  Max:                    {sorted_latencies[-1]:.2f} ms")
    print("-----------------------------------------------------------------")

    target_met = p50 <= 15.0
    target_str = "MET (<= 15.0 ms)" if target_met else f"EXCEEDED (> 15.0 ms: {p50:.2f} ms)"
    print(f"p50 Target (15 ms):        {target_str}")
    print("=" * 65)

    if wait_completion and job_ids:
        async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
            await wait_for_jobs_completion(client, base_url, job_ids)


def main():
    parser = argparse.ArgumentParser(description="Pulse HTTP enqueue load test")
    parser.add_argument("--url", default=DEFAULT_BASE_URL, help="Base API URL")
    parser.add_argument("--jobs", type=int, default=1000, help="Number of jobs to submit")
    parser.add_argument("--concurrency", type=int, default=50, help="Concurrent workers")
    parser.add_argument("--wait", action="store_true", default=True, help="Wait for background jobs to finish")
    args = parser.parse_args()

    asyncio.run(run_load_test(args.url, args.jobs, args.concurrency, args.wait))


if __name__ == "__main__":
    main()
