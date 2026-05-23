"""Result backend abstraction.

A ResultBackend stores the outcomes of completed tasks so that callers
holding an AsyncResult can retrieve them. The result backend is separate
from the queue backend: a queue can be discarded after ack, but a result
must be retrievable until the caller fetches it (or until a TTL expires).
Implementations may share storage with the queue or be entirely separate.
"""

from __future__ import annotations

import threading
import time
from abc import ABC, abstractmethod

from miniq.exceptions import TaskTimeout
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


class InMemoryResultBackend(ResultBackend):
    """Thread-safe in-memory implementation of ResultBackend.

    Stores tasks in a dict keyed by task_id. Uses a Condition variable to
    wake blocked callers on store(). All state is lost on process exit.
    """

    def __init__(self) -> None:
        self._results: dict[str, Task] = {}
        self._available = threading.Condition()

    def store(self, task: Task) -> None:
        with self._available:
            self._results[task.id] = task
            self._available.notify_all()

    def get(self, task_id: str) -> Task | None:
        with self._available:
            return self._results.get(task_id)

    def wait(self, task_id: str, timeout: float | None = None) -> Task:
        deadline = time.monotonic() + timeout if timeout is not None else None

        with self._available:
            while True:
                if task_id in self._results:
                    return self._results[task_id]

                if deadline is None:
                    self._available.wait()
                    continue

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TaskTimeout(
                        f"Task {task_id} did not complete within timeout",
                        timeout_seconds=timeout or 0.0,
                    )
                self._available.wait(timeout=remaining)
