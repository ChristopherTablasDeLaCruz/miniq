"""Task definitions and supporting types.

A Task represents a unit of work that has been scheduled for execution.
It carries the identity of the function to run, its arguments, and the
runtime state that workers update as the task moves through the system.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from miniq.exceptions import TaskFailed

if TYPE_CHECKING:
    from miniq.results import ResultBackend


class TaskStatus(StrEnum):
    """Lifecycle states of a task.

    The string base class makes statuses JSON-serializable without a
    custom encoder. The set is intentionally small; v2 may add RETRYING
    and DEAD distinct from FAILED if observability needs it.
    """

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class Task:
    """A unit of work waiting to be, or in the process of being, executed.

    Tasks are identified by 'id' (a UUID generated at creation time) and
    reference the function to run by a fully-qualified import path
    ('func_path'), not by a function object. This lets tasks survive
    serialization and worker restarts.

    Args and kwargs must be JSON-serializable. The serializer layer enforces
    this; the Task itself does not validate.
    """

    func_path: str
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: TaskStatus = TaskStatus.PENDING

    retries: int = 0
    max_retries: int = 0
    priority: int = 0

    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None
    available_at: float | None = None

    result: Any = None
    error: str | None = None

    def mark_running(self) -> None:
        """Transition to RUNNING and record the start time."""
        self.status = TaskStatus.RUNNING
        self.started_at = time.time()

    def mark_success(self, result: Any) -> None:
        """Transition to SUCCESS, store the result, record finish time."""
        self.status = TaskStatus.SUCCESS
        self.result = result
        self.finished_at = time.time()

    def mark_failed(self, error: str) -> None:
        """Transition to FAILED, store the error message, record finish time."""
        self.status = TaskStatus.FAILED
        self.error = error
        self.finished_at = time.time()


class AsyncResult:
    """A handle to a task's eventual outcome.

    Returned by 'Task.delay()'. Callers use 'get()' to
    block until the task finishes and either receive its return value
    or have its exception re-raised as a 'TaskFailed'.
    """

    def __init__(self, task_id: str, backend: ResultBackend) -> None:
        self.task_id = task_id
        self._backend = backend

    def get(self, timeout: float | None = None) -> Any:
        """Block until the task finishes, then return its result.

        Raises 'TaskFailed' if the task ended in FAILED status, with
        the stored error message attached. Raises 'TaskTimeout' if the
        wait exceeded 'timeout' seconds.
        """
        task = self._backend.wait(self.task_id, timeout=timeout)
        if task.status is TaskStatus.SUCCESS:
            return task.result
        if task.status is TaskStatus.FAILED:
            raise TaskFailed(f"Task {self.task_id} failed: {task.error}")
        raise RuntimeError(f"Unexpected task status after wait: {task.status}")
