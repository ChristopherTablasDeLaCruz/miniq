"""Result backend abstraction.

A ResultBackend stores the outcomes of completed tasks so that callers
holding an AsyncResult can retrieve them. The result backend is separate
from the queue backend: a queue can be discarded after ack, but a result
must be retrievable until the caller fetches it (or until a TTL expires).
Implementations may share storage with the queue or be entirely separate.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from miniq.task import Task


class ResultBackend(ABC):
    """Abstract base class for result storage backends.

    Implementations store finished tasks (in SUCCESS or FAILED state)
    and let callers retrieve them by task_id, optionally blocking until
    the result is available.
    """

    @abstractmethod
    def store(self, task: Task) -> None:
        """Store a finished task's outcome.

        Called by the Worker after the task has been marked success or
        failed. The full Task object is stored; callers access status,
        result, and error fields via that object.
        """

    @abstractmethod
    def get(self, task_id: str) -> Task | None:
        """Look up a stored task by id, or return None if not available.

        Non-blocking. Returns None if the task has not been stored yet
        (still pending, running, or never enqueued).
        """

    @abstractmethod
    def wait(self, task_id: str, timeout: float | None = None) -> Task:
        """Block until the task's result is available, then return it.

        'timeout' is the maximum time to wait in seconds. 'None'
        means wait indefinitely. '0' means do not block; raise
        immediately if the result is not already stored.

        Raises 'TaskTimeout' if the timeout expires before the result
        becomes available.
        """
