import pytest
from shared.retry import calculate_backoff_delay, should_retry


def test_calculate_backoff_delay_without_jitter():
    # base = 1.0, 2^0 = 1.0
    delay_0 = calculate_backoff_delay(attempt=0, base=1.0, jitter=0.0)
    assert delay_0 == 1.0

    # base = 1.0, 2^1 = 2.0
    delay_1 = calculate_backoff_delay(attempt=1, base=1.0, jitter=0.0)
    assert delay_1 == 2.0

    # base = 1.0, 2^2 = 4.0
    delay_2 = calculate_backoff_delay(attempt=2, base=1.0, jitter=0.0)
    assert delay_2 == 4.0

    # base = 1.0, 2^3 = 8.0
    delay_3 = calculate_backoff_delay(attempt=3, base=1.0, jitter=0.0)
    assert delay_3 == 8.0


def test_calculate_backoff_delay_with_custom_base():
    delay = calculate_backoff_delay(attempt=2, base=2.5, jitter=0.0)
    assert delay == 2.5 * 4.0  # 10.0


def test_calculate_backoff_delay_jitter_bounds():
    base = 1.0
    jitter_max = 0.5
    for attempt in range(5):
        for _ in range(50):
            delay = calculate_backoff_delay(attempt=attempt, base=base, jitter_max=jitter_max)
            expected_base_delay = base * (2 ** attempt)
            assert expected_base_delay <= delay <= expected_base_delay + jitter_max


def test_calculate_backoff_delay_negative_attempt():
    delay = calculate_backoff_delay(attempt=-1, base=1.0, jitter=0.0)
    assert delay == 1.0


def test_should_retry():
    assert should_retry(attempts=1, max_attempts=3) is True
    assert should_retry(attempts=2, max_attempts=3) is True
    assert should_retry(attempts=3, max_attempts=3) is False
    assert should_retry(attempts=4, max_attempts=3) is False
