import time
from typing import Optional

import redis.asyncio as aioredis
from redis.asyncio.client import Redis
from redis.asyncio.connection import ConnectionPool

from shared.config import get_settings

# Redis Key Names
QUEUE_JOBS = "pulse:queue:jobs"
QUEUE_DELAYED = "pulse:queue:delayed"
PROCESSING_PREFIX = "pulse:processing:"
HEARTBEAT_PREFIX = "pulse:heartbeat:"
ACTIVE_WORKERS = "pulse:active_workers"

_redis_pool: Optional[ConnectionPool] = None
_redis_client: Optional[Redis] = None

PROMOTE_DELAYED_LUA = """
local delayed_queue = KEYS[1]
local jobs_queue = KEYS[2]
local now = tonumber(ARGV[1])
local limit = tonumber(ARGV[2]) or 100

local ready_jobs = redis.call('ZRANGEBYSCORE', delayed_queue, '-inf', now, 'LIMIT', 0, limit)
local promoted = 0
for i, job_id in ipairs(ready_jobs) do
    if redis.call('ZREM', delayed_queue, job_id) == 1 then
        redis.call('LPUSH', jobs_queue, job_id)
        promoted = promoted + 1
    end
end
return promoted
"""


def get_redis_pool() -> ConnectionPool:
    global _redis_pool
    if _redis_pool is None:
        settings = get_settings()
        _redis_pool = ConnectionPool.from_url(
            settings.async_redis_url,
            max_connections=50,
            decode_responses=True,
            socket_timeout=10.0,
            socket_connect_timeout=5.0,
        )
    return _redis_pool


def get_redis_client() -> Redis:
    global _redis_client
    if _redis_client is None:
        pool = get_redis_pool()
        _redis_client = aioredis.Redis(connection_pool=pool)
    return _redis_client


async def check_redis_health() -> bool:
    try:
        client = get_redis_client()
        return bool(await client.ping())
    except Exception:
        return False


async def push_job(job_id: str) -> int:
    client = get_redis_client()
    return await client.lpush(QUEUE_JOBS, str(job_id))


async def pop_job_reliable(worker_id: str, timeout: int = 2) -> Optional[str]:
    """Reliable queue dequeue using BLMOVE.

    Atomically pops a job from the tail of the main queue and pushes it
    to the head of the worker-specific processing queue.
    """
    client = get_redis_client()
    processing_queue = f"{PROCESSING_PREFIX}{worker_id}"
    # In redis-py, blmove: first_list, second_list, timeout, src='LEFT', dest='RIGHT'
    # We pop from tail of jobs queue (RIGHT) and push to head of processing queue (LEFT)
    return await client.blmove(
        QUEUE_JOBS,
        processing_queue,
        timeout,
        "RIGHT",
        "LEFT",
    )


async def ack_job(worker_id: str, job_id: str) -> int:
    """Removes the job from the worker's processing queue after execution."""
    client = get_redis_client()
    processing_queue = f"{PROCESSING_PREFIX}{worker_id}"
    return await client.lrem(processing_queue, 1, str(job_id))


async def schedule_delayed_job(job_id: str, execute_at: float) -> bool:
    """Schedules a job for retry in the delayed sorted set."""
    client = get_redis_client()
    res = await client.zadd(QUEUE_DELAYED, {str(job_id): execute_at})
    return bool(res)


async def promote_delayed_jobs(batch_size: int = 100) -> int:
    """Promotes ready delayed jobs back to the main queue atomically."""
    client = get_redis_client()
    now = time.time()
    promoted = await client.eval(
        PROMOTE_DELAYED_LUA,
        2,
        QUEUE_DELAYED,
        QUEUE_JOBS,
        str(now),
        str(batch_size),
    )
    return int(promoted)


async def set_worker_heartbeat(worker_id: str, ttl_seconds: float) -> bool:
    client = get_redis_client()
    key = f"{HEARTBEAT_PREFIX}{worker_id}"
    res = await client.set(key, "1", ex=int(ttl_seconds))
    await client.sadd(ACTIVE_WORKERS, worker_id)
    return bool(res)


async def is_worker_alive(worker_id: str) -> bool:
    client = get_redis_client()
    key = f"{HEARTBEAT_PREFIX}{worker_id}"
    return bool(await client.exists(key))


async def close_redis() -> None:
    global _redis_pool, _redis_client
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None
    if _redis_pool is not None:
        await _redis_pool.disconnect()
        _redis_pool = None
