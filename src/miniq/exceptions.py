"""Typed exception hierarchy for miniq.

All exceptions raised by miniq inherit from MiniqError. Callers can catch
specific failure modes (TaskTimeout, QueueFull) rather than bare Exception,
which keeps error handling explicit and debuggable.
"""

from __future__ import annotations


class MiniqError(Exception):
    """Base class for all miniq-raised exceptions."""


class TaskError(MiniqError):
    """Base class for errors related to task definition or execution."""


class TaskFailed(TaskError):
    """Raised when a task function raises an unhandled exception during execution.

    The original exception is preserved on the `original_exception` attribute
    so callers can inspect or re-raise it.
    """

    def __init__(
        self,
        message: str,
        original_exception: BaseException | None = None,
    ) -> None:
        super().__init__(message)
        self.original_exception = original_exception


class TaskTimeout(TaskError):
    """Raised when a task does not complete within its configured timeout."""

    def __init__(self, message: str, timeout_seconds: float) -> None:
        super().__init__(message)
        self.timeout_seconds = timeout_seconds

    def __str__(self) -> str:
        return f"{super().__str__()} (timeout={self.timeout_seconds}s)"


class TaskNotRegistered(TaskError):
    """Raised when a worker dequeues a task whose function is not in the registry.

    Usually means the worker process did not import the module that defines
    the task, so the @task decorator never ran for it.
    """


class BackendError(MiniqError):
    """Base class for errors raised by storage backends."""


class QueueFull(BackendError):
    """Raised when a backend rejects an enqueue because it is at capacity."""


class QueueEmpty(BackendError):
    """Raised when dequeue is called with no wait and the queue is empty."""


class SerializationError(MiniqError):
    """Raised when a task or argument cannot be serialized or deserialized."""
