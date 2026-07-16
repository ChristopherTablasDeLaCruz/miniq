"""Retry policies for failed tasks.

A RetryPolicy decides two things when a task fails:
  1. Whether to retry at all (based on attempt count).
  2. How long to wait before the next attempt.

Policies are pluggable via the RetryPolicy ABC. The Worker holds a reference
to a policy and consults it after each failure.
"""

from __future__ import annotations

import random
from abc import ABC, abstractmethod


class RetryPolicy(ABC):
    """Abstract base class for retry policies.

    Subclasses define how long to wait before retry attempt N. The Worker
    is responsible for tracking attempt counts and respecting max_retries.
    A policy returns a delay in seconds, or None to decline the retry
    entirely (the task then goes to the dead-letter queue).
    """

    @abstractmethod
    def next_delay(self, attempt: int) -> float | None:
        """Return the delay in seconds before retry attempt number 'attempt',
        or None to decline the retry.

        'attempt' is 1-indexed: attempt=1 means the first retry (i.e. the
        original execution already failed once). Implementations raise
        ValueError if attempt is < 1; that signals a caller bug, not a
        policy decision.
        """


class NoRetry(RetryPolicy):
    """Never retry. 'next_delay' always declines.

    Use this when a task should fail permanently on first error.
    """

    def next_delay(self, attempt: int) -> float | None:
        return None


class FixedDelay(RetryPolicy):
    """Wait a constant number of seconds between every retry.

    Simple and predictable. Reasonable choice when you have no information
    about how long the failure mode lasts.
    """

    def __init__(self, delay_seconds: float) -> None:
        if delay_seconds < 0:
            raise ValueError("delay_seconds must be non-negative")
        self.delay_seconds = delay_seconds

    def next_delay(self, attempt: int) -> float | None:
        if attempt < 1:
            raise ValueError("attempt must be >= 1")
        return self.delay_seconds


class ExponentialBackoff(RetryPolicy):
    """Wait 'base * (factor ** (attempt - 1))' seconds, capped at 'max_delay'.

    The standard retry policy for transient failures (network blips, rate
    limits, brief outages). Doubling the wait each attempt avoids hammering
    a struggling service while still recovering quickly when the issue is
    brief.
    """

    def __init__(
        self,
        base_seconds: float = 1.0,
        factor: float = 2.0,
        max_delay_seconds: float = 300.0,
    ) -> None:
        if base_seconds < 0:
            raise ValueError("base_seconds must be non-negative")
        if factor < 1:
            raise ValueError("factor must be >= 1 (otherwise delays shrink)")
        if max_delay_seconds < base_seconds:
            raise ValueError("max_delay_seconds must be >= base_seconds")
        self.base_seconds = base_seconds
        self.factor = factor
        self.max_delay_seconds = max_delay_seconds

    def next_delay(self, attempt: int) -> float | None:
        if attempt < 1:
            raise ValueError("attempt must be >= 1")
        delay = self.base_seconds * (self.factor ** (attempt - 1))
        return min(delay, self.max_delay_seconds)


class JitteredBackoff(RetryPolicy):
    """Exponential backoff with random jitter to avoid thundering herd.

    Wraps another RetryPolicy (typically ExponentialBackoff) and randomizes
    the delay within +/- 'jitter_fraction' of the base value. When many
    workers retry the same failing dependency at the same instant, pure
    exponential backoff makes them all retry in lockstep. Jitter spreads
    them out.

    This is a decorator-like composition: a policy that wraps a policy.
    """

    def __init__(
        self,
        inner: RetryPolicy,
        jitter_fraction: float = 0.2,
    ) -> None:
        if not 0 <= jitter_fraction <= 1:
            raise ValueError("jitter_fraction must be between 0 and 1")
        self.inner = inner
        self.jitter_fraction = jitter_fraction

    def next_delay(self, attempt: int) -> float | None:
        base = self.inner.next_delay(attempt)
        if base is None:
            return None
        spread = base * self.jitter_fraction
        return max(0.0, base + random.uniform(-spread, spread))
