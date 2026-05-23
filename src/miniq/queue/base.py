"""Queue backend abstraction.

A QueueBackend stores tasks waiting to be executed and tracks which tasks
have been claimed by which workers. It is the source of truth for task
state during the queue lifecycle.

Concurrency contract
--------------------
Implementations must be safe to use from multiple worker processes
concurrently (where the backend supports multi-process access). A task
that has been claimed by one worker via 'dequeue' must not be visible
to another worker until one of the following happens:

  - The claiming worker acks it (success path).
  - The claiming worker nacks it with requeue=True (explicit release).
  - The visibility timeout expires without ack or nack.

Delivery semantics
------------------
The contract implements at-least-once delivery. If a worker crashes
mid-task, the visibility timeout will eventually expire and another
worker will reclaim the task. Tasks must therefore be idempotent;
running the same task twice must not produce incorrect results.

Implementations
---------------
- InMemoryQueue (Phase 2): thread-safe in-memory, for development and tests.
- SQLiteQueue (Phase 4): persistent, survives process restarts, supports
  multiple worker processes on the same host.
- RedisQueue (stretch): for multi-host deployments.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from miniq.task import Task


class QueueBackend(ABC):
    """Abstract base class for queue storage backends."""

    @abstractmethod
    def enqueue(self, task: Task) -> None:
        """Add a task to the queue.

        The task is stored in PENDING state and becomes immediately
        available for any worker to dequeue.

        Raises 'QueueFull' if the backend rejects the enqueue because
        of a capacity limit.
        """

    @abstractmethod
    def dequeue(
        self,
        visibility_timeout: float = 30.0,
        wait_seconds: float = 0.0,
    ) -> Task | None:
        """Claim a task for execution, or return None if none available.

        On a successful claim, the returned task is transitioned to
        RUNNING state and reserved for this caller for
        'visibility_timeout' seconds. If the caller does not ack or
        nack within that window, the task becomes available again for
        any worker to claim.

        'wait_seconds' is the maximum time to block waiting for a task
        to become available. '0' (the default) means return immediately
        if the queue is empty.

        Implementations are responsible for reclaiming tasks whose
        visibility timeout has expired, either as part of dequeue or via
        a background sweep.
        """

    @abstractmethod
    def ack(self, task: Task) -> None:
        """Mark a task as completed and remove it from the active queue.

        Called after a task has finished executing (in SUCCESS or in a
        terminal FAILED state that should not be retried). The caller is
        expected to have updated the task's status (via
        'task.mark_success' or 'task.mark_failed') before calling.

        If the task is no longer claimed by the caller (e.g. the
        visibility timeout already expired), the behavior is
        implementation-defined. Implementations should prefer to log
        and ignore rather than raise.
        """

    @abstractmethod
    def nack(self, task: Task, requeue: bool = True) -> None:
        """Release the caller's claim on a task.

        If 'requeue' is True (the default), the task becomes
        immediately available for another claim. If 'requeue' is
        False, the task is removed from the active queue and treated as
        terminally failed; the backend may persist it elsewhere (e.g. a
        dead-letter store) for later inspection.
        """

    @abstractmethod
    def size(self) -> int:
        """Return the number of tasks currently in the queue.

        Includes both pending (claimable) and in-flight (claimed) tasks.
        Does not include completed or terminally failed tasks.
        """

    @abstractmethod
    def get_task(self, task_id: str) -> Task | None:
        """Look up a task by id, or return None if not found.

        Used by 'AsyncResult' to check task status from outside the
        worker. The task may be in any state including completed or
        failed, if the backend retains them after ack.
        """

    def close(self) -> None:  # noqa: B027
        """Release any resources held by the backend. Default: no-op.

        Backends with persistent connections (e.g. SQLiteQueue) override
        this to close the connection cleanly.
        """
