import asyncio
import logging
import multiprocessing
import os
import signal
import sys
import time
from typing import List

from shared.config import get_settings
from worker.process import run_worker_process
from worker.reaper import Reaper, startup_reconciliation

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [WorkerSupervisor] %(message)s",
)
logger = logging.getLogger("WorkerSupervisor")

HEALTH_FILE = "/tmp/pulse_worker_healthy"


def start_worker_proc(index: int) -> multiprocessing.Process:
    worker_id = f"worker-{os.getpid()}-{index}"
    p = multiprocessing.Process(
        target=run_worker_process,
        args=(index, worker_id),
        name=f"WorkerProcess-{index}",
    )
    p.start()
    return p


def run_reaper_process() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [ReaperProcess] %(message)s",
    )
    reaper = Reaper()
    asyncio.run(reaper.start())


def main() -> None:
    multiprocessing.set_start_method("spawn", force=True)
    settings = get_settings()
    concurrency = settings.WORKER_CONCURRENCY

    logger.info(f"Initializing Pulse Worker Pool (Concurrency: {concurrency})...")

    # Step 1: Run startup reconciliation against Postgres & Redis
    try:
        reconciled = asyncio.run(startup_reconciliation())
        logger.info(f"Startup reconciliation complete. {reconciled} missing jobs recovered.")
    except Exception as e:
        logger.error(f"Startup reconciliation encountered error: {e}")

    # Step 2: Start Reaper background process
    reaper_proc = multiprocessing.Process(
        target=run_reaper_process,
        name="ReaperProcess",
    )
    reaper_proc.start()

    # Step 3: Spawn the pool of worker processes
    workers: List[multiprocessing.Process] = []
    for i in range(concurrency):
        p = start_worker_proc(i)
        workers.append(p)

    logger.info(f"Successfully launched {len(workers)} worker processes.")

    # Write healthcheck file
    try:
        with open(HEALTH_FILE, "w") as f:
            f.write(str(time.time()))
    except Exception as e:
        logger.warning(f"Could not write health file: {e}")

    shutdown_initiated = False

    def handle_termination(signum, frame):
        nonlocal shutdown_initiated
        if shutdown_initiated:
            return
        shutdown_initiated = True
        logger.info(f"Supervisor received signal {signum}. Initiating graceful shutdown...")

        # Remove health file immediately
        if os.path.exists(HEALTH_FILE):
            os.remove(HEALTH_FILE)

        # Terminate worker processes
        for p in workers:
            if p.is_alive():
                p.terminate()

        if reaper_proc.is_alive():
            reaper_proc.terminate()

        for p in workers:
            p.join(timeout=3)
        if reaper_proc.is_alive():
            reaper_proc.join(timeout=3)

        logger.info("All worker processes shut down. Exiting supervisor.")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_termination)
    signal.signal(signal.SIGTERM, handle_termination)

    # Step 4: Supervision loop
    while not shutdown_initiated:
        time.sleep(2)

        # Refresh healthcheck timestamp
        try:
            with open(HEALTH_FILE, "w") as f:
                f.write(str(time.time()))
        except Exception:
            pass

        # Supervise workers and restart if any crashed unexpectedly
        for i, p in enumerate(workers):
            if not p.is_alive() and not shutdown_initiated:
                exit_code = p.exitcode
                logger.warning(f"Worker {i} (PID: {p.pid}) died with exit code {exit_code}. Restarting...")
                new_p = start_worker_proc(i)
                workers[i] = new_p

        # Supervise reaper
        if not reaper_proc.is_alive() and not shutdown_initiated:
            logger.warning("Reaper process terminated unexpectedly. Restarting...")
            reaper_proc = multiprocessing.Process(
                target=run_reaper_process,
                name="ReaperProcess",
            )
            reaper_proc.start()


if __name__ == "__main__":
    main()
