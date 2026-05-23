"""In-memory queue backend.

Thread-safe single-process implementation of QueueBackend. Tasks live in
Python data structures inside this object; nothing is persisted to disk.
A process restart loses all queue state. Useful for development, testing,
and single-script use cases.

Concurrency model: all public methods acquire a single lock. Blocking
dequeue uses a Condition variable that producers notify on enqueue.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass

from miniq.queue.base import QueueBackend
from miniq.task import Task, TaskStatus


@dataclass
class _Claim:
    """Internal record of an in-flight (claimed) task."""

    task: Task
    expires_at: float


class InMemoryQueue(QueueBackend):
    """Thread-safe in-memory implementation of QueueBackend.

    Suitable for tests and single-process scripts. For persistence and
    multi-process workers, use SQLiteQueue (Phase 4).
    """

    def __init__(self) -> None:
        self._pending: deque[Task] = deque()
        self._claimed: dict[str, _Claim] = {}
        self._completed: dict[str, Task] = {}
        self._dlq: dict[str, Task] = {}
        self._not_empty = threading.Condition()

    def enqueue(self, task: Task) -> None:
        with self._not_empty:
            self._pending.append(task)
            self._not_empty.notify()

    def dequeue(
        self,
        visibility_timeout: float = 30.0,
        wait_seconds: float = 0.0,
    ) -> Task | None:
        deadline = time.monotonic() + wait_seconds if wait_seconds > 0 else None

        with self._not_empty:
            while True:
                self._reclaim_expired_locked()

                if self._pending:
                    task = self._pending.popleft()
                    task.mark_running()
                    self._claimed[task.id] = _Claim(
                        task=task,
                        expires_at=time.monotonic() + visibility_timeout,
                    )
                    return task

                if deadline is None:
                    return None

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None

                self._not_empty.wait(timeout=remaining)

    def ack(self, task: Task) -> None:
        with self._not_empty:
            self._claimed.pop(task.id, None)
            self._completed[task.id] = task

    def nack(self, task: Task, requeue: bool = True) -> None:
        with self._not_empty:
            self._claimed.pop(task.id, None)
            if requeue:
                task.status = TaskStatus.PENDING
                task.started_at = None
                self._pending.append(task)
                self._not_empty.notify()
            else:
                self._dlq[task.id] = task

    def size(self) -> int:
        with self._not_empty:
            return len(self._pending) + len(self._claimed)

    def get_task(self, task_id: str) -> Task | None:
        with self._not_empty:
            if task_id in self._claimed:
                return self._claimed[task_id].task
            if task_id in self._completed:
                return self._completed[task_id]
            if task_id in self._dlq:
                return self._dlq[task_id]
            for task in self._pending:
                if task.id == task_id:
                    return task
            return None

    def _reclaim_expired_locked(self) -> None:
        """Move claims whose visibility timeout has expired back to pending.

        Caller must hold the condition lock.
        """
        now = time.monotonic()
        expired = [tid for tid, claim in self._claimed.items() if claim.expires_at <= now]
        for tid in expired:
            claim = self._claimed.pop(tid)
            claim.task.status = TaskStatus.PENDING
            claim.task.started_at = None
            self._pending.append(claim.task)
        if expired:
            self._not_empty.notify_all()
