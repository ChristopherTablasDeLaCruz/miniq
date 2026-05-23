"""Worker: dequeues tasks and executes them.

A Worker pulls tasks one at a time from a QueueBackend, looks up each
function in the registry, executes it, stores the result in a
ResultBackend, and either acks (on success) or nacks (on failure).

Phase 2 worker: failures go straight to the DLQ via nack(requeue=False).
Phase 3 will add retry handling that re-enqueues failures up to a
configured limit before sending to DLQ.
"""

from __future__ import annotations

import logging
import threading

from miniq.exceptions import TaskNotRegistered
from miniq.queue.base import QueueBackend
from miniq.registry import lookup
from miniq.results import ResultBackend

logger = logging.getLogger(__name__)


class Worker:
    """Single-threaded task executor.

    One Worker processes one task at a time. Multiple Workers can share a
    queue safely; the queue's visibility timeout ensures only one Worker
    runs any given task at a time.
    """

    def __init__(
        self,
        queue: QueueBackend,
        results: ResultBackend,
        visibility_timeout: float = 30.0,
        poll_wait_seconds: float = 1.0,
    ) -> None:
        self._queue = queue
        self._results = results
        self._visibility_timeout = visibility_timeout
        self._poll_wait_seconds = poll_wait_seconds
        self._stop_event = threading.Event()

    def run_once(self) -> bool:
        """Process one task if one is available; return True if so.

        Returns False if no task became available within the poll window.
        Task failures are caught, stored, and acked; they do not propagate.
        """
        task = self._queue.dequeue(
            visibility_timeout=self._visibility_timeout,
            wait_seconds=self._poll_wait_seconds,
        )
        if task is None:
            return False

        try:
            func = lookup(task.func_path)
        except TaskNotRegistered as e:
            logger.error("Task %s: %s", task.id, e)
            task.mark_failed(error=str(e))
            self._results.store(task)
            self._queue.nack(task, requeue=False)
            return True

        try:
            result = func(*task.args, **task.kwargs)
        except Exception as e:
            logger.exception("Task %s raised", task.id)
            task.mark_failed(error=f"{type(e).__name__}: {e}")
            self._results.store(task)
            self._queue.nack(task, requeue=False)
            return True

        task.mark_success(result=result)
        self._results.store(task)
        self._queue.ack(task)
        return True

    def run(self) -> None:
        """Loop processing tasks until stop() is called."""
        while not self._stop_event.is_set():
            self.run_once()

    def stop(self) -> None:
        """Signal the worker to stop after its current task."""
        self._stop_event.set()
