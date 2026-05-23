"""Worker: dequeues tasks and executes them.

A Worker pulls tasks one at a time from a QueueBackend, looks up each
function in the registry, executes it, stores the result in a
ResultBackend, and acks the task. On failure, the Worker consults its
RetryPolicy:

  - If the task has retries remaining and the policy permits a delay,
    increment retries, set available_at to now + delay, and nack with
    requeue=True. The task becomes claimable again after the delay.
  - Otherwise (out of retries or policy refuses), mark the task failed,
    store the result, and nack with requeue=False to send to the DLQ.
"""

from __future__ import annotations

import logging
import threading
import time

from miniq.exceptions import TaskNotRegistered
from miniq.queue.base import QueueBackend
from miniq.registry import lookup
from miniq.results import ResultBackend
from miniq.retry import NoRetry, RetryPolicy
from miniq.task import Task

logger = logging.getLogger(__name__)


class Worker:
    """Single-threaded task executor."""

    def __init__(
        self,
        queue: QueueBackend,
        results: ResultBackend,
        retry_policy: RetryPolicy | None = None,
        visibility_timeout: float = 30.0,
        poll_wait_seconds: float = 1.0,
    ) -> None:
        self._queue = queue
        self._results = results
        self._retry_policy: RetryPolicy = retry_policy if retry_policy is not None else NoRetry()
        self._visibility_timeout = visibility_timeout
        self._poll_wait_seconds = poll_wait_seconds
        self._stop_event = threading.Event()

    def run_once(self) -> bool:
        task = self._queue.dequeue(
            visibility_timeout=self._visibility_timeout,
            wait_seconds=self._poll_wait_seconds,
        )
        if task is None:
            return False

        try:
            func = lookup(task.func_path)
        except TaskNotRegistered as e:
            self._dead_letter(task, error_message=str(e))
            return True

        try:
            result = func(*task.args, **task.kwargs)
        except Exception as e:
            self._handle_failure(task, e)
            return True

        task.mark_success(result=result)
        self._results.store(task)
        self._queue.ack(task)
        return True

    def _handle_failure(self, task: Task, exc: Exception) -> None:
        """Either schedule a retry or send the task to the DLQ."""
        error_message = f"{type(exc).__name__}: {exc}"
        logger.warning("Task %s failed: %s", task.id, error_message)

        if task.retries < task.max_retries:
            try:
                delay = self._retry_policy.next_delay(task.retries + 1)
            except ValueError:
                # NoRetry (or any policy that refuses) raises; fall through to DLQ.
                pass
            else:
                task.retries += 1
                task.available_at = time.time() + delay
                # Don't store the result yet; the task isn't finished.
                self._queue.nack(task, requeue=True)
                return

        self._dead_letter(task, error_message=error_message)

    def _dead_letter(self, task: Task, error_message: str) -> None:
        task.mark_failed(error=error_message)
        self._results.store(task)
        self._queue.nack(task, requeue=False)

    def run(self) -> None:
        while not self._stop_event.is_set():
            self.run_once()

    def stop(self) -> None:
        self._stop_event.set()
