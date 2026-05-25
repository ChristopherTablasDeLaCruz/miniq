"""Worker pool: multi-process task execution.

Spawns N independent worker processes that each consume from the same
queue and store results in the same backend. Provides graceful shutdown
via SIGTERM and as a context manager.

Process model
-------------
Uses the 'spawn' start method (not 'fork') for portability and safety.
Spawn creates a fresh Python interpreter per child, so no file descriptors
or in-memory state are inherited from the parent. This is required for
safe SQLite usage: each worker process opens its own connection inside
its own interpreter, with no shared state to corrupt.

Users provide factory functions (typically 'functools.partial') rather
than backend instances. The factories run inside each child process to
construct backends in that child's context. Lambdas are NOT supported as
factories because the spawn start method requires picklable arguments.

Example::

    from functools import partial
    pool = WorkerPool(
        queue_factory=partial(SQLiteQueue, "./miniq.db"),
        results_factory=partial(SQLiteResultBackend, "./miniq.db"),
        workers=4,
        task_modules=["myapp.tasks"],
    )
    with pool:
        ...  # producers can enqueue from the main process

Shutdown
--------
'stop(timeout=30)' sends SIGTERM to each worker and waits for it to
exit. Workers catch SIGTERM in a signal handler, mark their stop flag,
finish the current task, and exit cleanly. Stragglers that don't exit
within 'timeout' are killed via SIGKILL.
"""

from __future__ import annotations

import logging
import multiprocessing as mp
import signal
import time
from collections.abc import Callable
from types import FrameType
from typing import Any

from miniq.queue.base import QueueBackend
from miniq.results import ResultBackend
from miniq.retry import RetryPolicy
from miniq.worker import Worker

logger = logging.getLogger(__name__)


def _worker_process_entry(
    queue_factory: Callable[[], QueueBackend],
    results_factory: Callable[[], ResultBackend],
    retry_policy: RetryPolicy | None,
    task_modules: list[str],
    worker_kwargs: dict[str, Any],
) -> None:
    """Entry point that runs inside each spawned worker process.

    Imports the task modules to populate the registry, constructs the
    queue and result backend via the factories, sets up signal handlers,
    and runs the worker loop.
    """
    # Import task modules so @task decorators run and register functions.
    for module_name in task_modules:
        __import__(module_name)

    queue = queue_factory()
    results = results_factory()

    worker = Worker(
        queue=queue,
        results=results,
        retry_policy=retry_policy,
        **worker_kwargs,
    )

    def handle_signal(signum: int, _frame: FrameType | None) -> None:
        logger.info("Worker received signal %d, stopping after current task", signum)
        worker.stop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    try:
        worker.run()
    finally:
        queue.close()
        results.close()


class WorkerPool:
    """Multi-process worker pool.

    Spawns N worker processes that share a queue and result backend by
    each opening their own connection to the same underlying storage
    (e.g. the same SQLite file).

    Usable as a context manager; 'with pool:' starts the workers and
    stops them on exit.
    """

    def __init__(
        self,
        queue_factory: Callable[[], QueueBackend],
        results_factory: Callable[[], ResultBackend],
        workers: int = 4,
        retry_policy: RetryPolicy | None = None,
        task_modules: list[str] | None = None,
        worker_kwargs: dict[str, Any] | None = None,
    ) -> None:
        if workers < 1:
            raise ValueError("workers must be >= 1")

        self._queue_factory = queue_factory
        self._results_factory = results_factory
        self._workers_count = workers
        self._retry_policy = retry_policy
        self._task_modules = task_modules if task_modules is not None else []
        self._worker_kwargs = worker_kwargs if worker_kwargs is not None else {}

        self._ctx = mp.get_context("spawn")
        self._processes: list[mp.process.BaseProcess] = []
        self._started = False

    def start(self) -> None:
        """Launch the worker processes."""
        if self._started:
            raise RuntimeError("Pool already started")

        for i in range(self._workers_count):
            process = self._ctx.Process(
                target=_worker_process_entry,
                args=(
                    self._queue_factory,
                    self._results_factory,
                    self._retry_policy,
                    self._task_modules,
                    self._worker_kwargs,
                ),
                name=f"miniq-worker-{i}",
                daemon=False,
            )
            process.start()
            self._processes.append(process)

        self._started = True
        logger.info("Started %d worker(s)", self._workers_count)

    def stop(self, timeout: float = 30.0) -> None:
        """Send SIGTERM to all workers and wait for them to exit.

        Workers that don't exit within 'timeout' seconds are killed
        with SIGKILL. Idempotent: calling stop on a stopped pool is a
        no-op.
        """
        if not self._started:
            return

        logger.info("Stopping %d worker(s)", len(self._processes))

        # Phase 1: gentle shutdown via SIGTERM
        for process in self._processes:
            if process.is_alive():
                process.terminate()

        # Phase 2: wait up to `timeout` total for all workers to exit
        deadline = time.monotonic() + timeout
        for process in self._processes:
            remaining = max(0.0, deadline - time.monotonic())
            process.join(timeout=remaining)

        # Phase 3: force-kill any stragglers
        for process in self._processes:
            if process.is_alive():
                logger.warning(
                    "Worker %s did not exit gracefully; killing with SIGKILL",
                    process.name,
                )
                process.kill()
                process.join(timeout=1.0)

        self._started = False
        self._processes = []

    @property
    def alive_workers(self) -> int:
        """Count of workers currently alive."""
        return sum(1 for p in self._processes if p.is_alive())

    def __enter__(self) -> WorkerPool:
        self.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.stop()
