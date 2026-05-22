"""Tests for retry policies."""

from __future__ import annotations

import pytest
from miniq.retry import (
    ExponentialBackoff,
    FixedDelay,
    JitteredBackoff,
    NoRetry,
    RetryPolicy,
)


class TestRetryPolicy:
    def test_is_abstract(self) -> None:
        with pytest.raises(TypeError):
            RetryPolicy()


class TestNoRetry:
    def test_always_raises(self) -> None:
        policy = NoRetry()
        with pytest.raises(ValueError):
            policy.next_delay(1)


class TestFixedDelay:
    def test_constant_delay(self) -> None:
        policy = FixedDelay(delay_seconds=5.0)
        assert policy.next_delay(1) == 5.0
        assert policy.next_delay(2) == 5.0
        assert policy.next_delay(99) == 5.0

    def test_rejects_negative_delay(self) -> None:
        with pytest.raises(ValueError):
            FixedDelay(delay_seconds=-1.0)

    def test_rejects_invalid_attempt(self) -> None:
        policy = FixedDelay(delay_seconds=5.0)
        with pytest.raises(ValueError):
            policy.next_delay(0)


class TestExponentialBackoff:
    def test_doubles_each_attempt(self) -> None:
        policy = ExponentialBackoff(base_seconds=1.0, factor=2.0, max_delay_seconds=1000)
        assert policy.next_delay(1) == 1.0
        assert policy.next_delay(2) == 2.0
        assert policy.next_delay(3) == 4.0
        assert policy.next_delay(4) == 8.0

    def test_respects_max_delay(self) -> None:
        policy = ExponentialBackoff(base_seconds=1.0, factor=2.0, max_delay_seconds=5.0)
        assert policy.next_delay(10) == 5.0  # would be 512 without cap

    def test_validates_constructor_args(self) -> None:
        with pytest.raises(ValueError):
            ExponentialBackoff(base_seconds=-1.0)
        with pytest.raises(ValueError):
            ExponentialBackoff(factor=0.5)
        with pytest.raises(ValueError):
            ExponentialBackoff(base_seconds=10.0, max_delay_seconds=1.0)


class TestJitteredBackoff:
    def test_wraps_inner_policy(self) -> None:
        # Delay should fall within +/- 20% of the inner's base (which is 4 at attempt 3).
        inner = ExponentialBackoff(base_seconds=1.0, factor=2.0)
        policy = JitteredBackoff(inner=inner, jitter_fraction=0.2)
        for _ in range(100):
            delay = policy.next_delay(3)
            assert 4.0 * 0.8 <= delay <= 4.0 * 1.2

    def test_zero_jitter_returns_inner_value(self) -> None:
        inner = FixedDelay(delay_seconds=7.0)
        policy = JitteredBackoff(inner=inner, jitter_fraction=0.0)
        assert policy.next_delay(1) == 7.0

    def test_rejects_invalid_jitter(self) -> None:
        inner = FixedDelay(delay_seconds=1.0)
        with pytest.raises(ValueError):
            JitteredBackoff(inner=inner, jitter_fraction=1.5)

    def test_delay_never_negative(self) -> None:
        # With very high jitter on a small base, ensure we floor at 0.
        inner = FixedDelay(delay_seconds=0.1)
        policy = JitteredBackoff(inner=inner, jitter_fraction=1.0)
        for _ in range(100):
            assert policy.next_delay(1) >= 0.0
