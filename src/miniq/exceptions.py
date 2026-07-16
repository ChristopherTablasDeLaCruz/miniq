"""Typed exception hierarchy for miniq.

All exceptions raised by miniq inherit from MiniqError. Callers can catch
specific failure modes (TaskTimeout, TaskFailed) rather than bare Exception,
which keeps error handling explicit and debuggable.
"""

from __future__ import annotations


class MiniqError(Exception):
    """Base class for all miniq-raised exceptions."""


class TaskError(MiniqError):
    """Base class for errors related to task definition or execution."""


class TaskFailed(TaskError):
    """Raised when a task ended in FAILED status.

    Carries the stored error message (exception type and text) from the
    worker that ran the task. The original exception object is not
    available: the failure happened in another process and only the
    message survives serialization.
    """


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


class SerializationError(MiniqError):
    """Raised when a task or argument cannot be serialized or deserialized."""
