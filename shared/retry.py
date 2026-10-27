import random
from typing import Optional


def calculate_backoff_delay(
    attempt: int,
    base: float = 1.0,
    jitter_max: float = 0.5,
    jitter: Optional[float] = None,
) -> float:
    """Calculates exponential backoff delay with jitter.

    Formula: delay = base * 2^attempt + jitter
    where jitter is uniformly distributed in [0, jitter_max].
    """
    if attempt < 0:
        attempt = 0

    exponent = 2 ** attempt
    computed_delay = base * exponent

    if jitter is None:
        jitter_val = random.uniform(0.0, jitter_max)
    else:
        jitter_val = max(0.0, jitter)

    return computed_delay + jitter_val


def should_retry(attempts: int, max_attempts: int) -> bool:
    """Determines if a task that has undergone `attempts` can be retried.

    If attempts is strictly less than max_attempts, another retry is permitted.
    """
    return attempts < max_attempts
