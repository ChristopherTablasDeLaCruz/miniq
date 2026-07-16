"""Tests for retry policies."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

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
    def test_declines_every_retry(self) -> None:
        policy = NoRetry()
        assert policy.next_delay(1) is None
        assert policy.next_delay(5) is None


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
            delay = policy.next_delay(1)
            assert delay is not None
            assert delay >= 0.0

    def test_propagates_inner_decline(self) -> None:
        # Wrapping NoRetry declines too, rather than jittering None.
        policy = JitteredBackoff(inner=NoRetry())
        assert policy.next_delay(1) is None


class TestExponentialBackoffProperties:
    @given(
        base=st.floats(min_value=0.001, max_value=5.0),
        factor=st.floats(min_value=1.0, max_value=10.0),
        max_delay=st.floats(min_value=10.0, max_value=1000.0),
    )
    def test_monotonically_increasing_until_max(
        self, base: float, factor: float, max_delay: float
    ) -> None:
        policy = ExponentialBackoff(base_seconds=base, factor=factor, max_delay_seconds=max_delay)
        delays = [policy.next_delay(attempt) for attempt in range(1, 10)]
        for i in range(len(delays) - 1):
            if delays[i] < max_delay:
                assert delays[i + 1] >= delays[i]

    @given(
        base=st.floats(min_value=0.001, max_value=5.0),
        factor=st.floats(min_value=1.0, max_value=10.0),
        max_delay=st.floats(min_value=10.0, max_value=1000.0),
        attempt=st.integers(min_value=1, max_value=100),
    )
    def test_bounded_by_max_delay(
        self, base: float, factor: float, max_delay: float, attempt: int
    ) -> None:
        policy = ExponentialBackoff(base_seconds=base, factor=factor, max_delay_seconds=max_delay)
        assert policy.next_delay(attempt) <= max_delay

    @given(
        base=st.floats(min_value=0.1, max_value=5.0),
        attempt=st.integers(min_value=1, max_value=5),
    )
    def test_first_attempt_equals_base(self, base: float, attempt: int) -> None:
        # When factor=1, every delay equals base (regardless of attempt number).
        policy = ExponentialBackoff(base_seconds=base, factor=1.0, max_delay_seconds=1000)
        assert policy.next_delay(attempt) == base


class TestJitteredBackoffProperties:
    @given(
        base=st.floats(min_value=0.1, max_value=10.0),
        jitter=st.floats(min_value=0.0, max_value=1.0),
        attempt=st.integers(min_value=1, max_value=10),
    )
    def test_delay_within_jitter_bounds(self, base: float, jitter: float, attempt: int) -> None:
        inner = FixedDelay(delay_seconds=base)
        policy = JitteredBackoff(inner=inner, jitter_fraction=jitter)
        for _ in range(10):
            delay = policy.next_delay(attempt)
            assert delay >= 0.0
            assert delay <= base * (1.0 + jitter) + 1e-9
