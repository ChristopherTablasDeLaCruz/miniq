"""Integration tests for WorkerPool."""

from __future__ import annotations

import time
from functools import partial
from pathlib import Path

from miniq.pool import WorkerPool
from miniq.queue.sqlite import SQLiteQueue
from miniq.results import SQLiteResultBackend
from miniq.task import Task, TaskStatus

# Import the task module so the parent process is aware of these tasks
# (workers will import it themselves via task_modules).
from tests.integration import _pool_tasks  # noqa: F401


def _wait_for_completion(
    results: SQLiteResultBackend,
    task_ids: list[str],
    timeout: float = 15.0,
) -> int:
    """Poll until every task id has a stored result, or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        completed = sum(1 for tid in task_ids if results.get(tid) is not None)
        if completed == len(task_ids):
            return completed
        time.sleep(0.1)
    return sum(1 for tid in task_ids if results.get(tid) is not None)


def test_pool_processes_all_enqueued_tasks(tmp_path: Path) -> None:
    """Two workers, 10 tasks: all complete with correct results."""
    db_path = str(tmp_path / "test.db")

    # Enqueue, then close before starting any workers.
    setup = SQLiteQueue(db_path)
    task_ids = []
    for i in range(10):
        task = Task(func_path="tests.integration._pool_tasks.square", args=(i,))
        setup.enqueue(task)
        task_ids.append(task.id)
    setup.close()

    pool = WorkerPool(
        queue_factory=partial(SQLiteQueue, db_path),
        results_factory=partial(SQLiteResultBackend, db_path),
        workers=2,
        task_modules=["tests.integration._pool_tasks"],
        poll_wait_seconds=0.05,
    )

    with pool:
        results = SQLiteResultBackend(db_path)
        try:
            completed = _wait_for_completion(results, task_ids, timeout=15.0)
            assert completed == 10, f"only {completed}/10 tasks completed"

            for i, tid in enumerate(task_ids):
                stored = results.get(tid)
                assert stored is not None
                assert stored.status is TaskStatus.SUCCESS
                assert stored.result == i * i
        finally:
            results.close()


def test_pool_stop_terminates_workers(tmp_path: Path) -> None:
    """stop() causes all workers to exit."""
    db_path = str(tmp_path / "test.db")
    SQLiteQueue(db_path).close()  # initialize schema

    pool = WorkerPool(
        queue_factory=partial(SQLiteQueue, db_path),
        results_factory=partial(SQLiteResultBackend, db_path),
        workers=2,
        task_modules=["tests.integration._pool_tasks"],
        poll_wait_seconds=0.05,
    )

    pool.start()
    time.sleep(0.5)
    assert pool.alive_workers == 2

    pool.stop(timeout=5.0)
    assert pool.alive_workers == 0


def test_pool_context_manager_starts_and_stops(tmp_path: Path) -> None:
    """Using the pool as a context manager starts on enter and stops on exit."""
    db_path = str(tmp_path / "test.db")
    SQLiteQueue(db_path).close()

    pool = WorkerPool(
        queue_factory=partial(SQLiteQueue, db_path),
        results_factory=partial(SQLiteResultBackend, db_path),
        workers=2,
        task_modules=["tests.integration._pool_tasks"],
        poll_wait_seconds=0.05,
    )

    with pool:
        time.sleep(0.5)
        assert pool.alive_workers == 2

    assert pool.alive_workers == 0


def test_no_double_claim_under_load(tmp_path: Path) -> None:
    """4 workers, 50 tasks: each task is processed exactly once with correct result."""
    db_path = str(tmp_path / "test.db")

    setup = SQLiteQueue(db_path)
    n_tasks = 50
    task_ids = []
    for i in range(n_tasks):
        task = Task(func_path="tests.integration._pool_tasks.identity", args=(i,))
        setup.enqueue(task)
        task_ids.append(task.id)
    setup.close()

    pool = WorkerPool(
        queue_factory=partial(SQLiteQueue, db_path),
        results_factory=partial(SQLiteResultBackend, db_path),
        workers=4,
        task_modules=["tests.integration._pool_tasks"],
        poll_wait_seconds=0.05,
    )

    with pool:
        results = SQLiteResultBackend(db_path)
        try:
            completed = _wait_for_completion(results, task_ids, timeout=30.0)
            assert completed == n_tasks, f"only {completed}/{n_tasks} completed"

            for i, tid in enumerate(task_ids):
                stored = results.get(tid)
                assert stored is not None
                assert stored.status is TaskStatus.SUCCESS
                assert stored.result == i
        finally:
            results.close()


def test_invalid_worker_count_raises() -> None:
    """workers=0 is rejected at construction time."""
    import pytest

    with pytest.raises(ValueError):
        WorkerPool(
            queue_factory=partial(SQLiteQueue, ":memory:"),
            results_factory=partial(SQLiteResultBackend, ":memory:"),
            workers=0,
        )
