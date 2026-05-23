"""Crash recovery and concurrent claim tests for SQLite-backed queues.

These tests are SQLite-specific because they exercise behavior that only
matters for persistent, multi-instance scenarios.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

from miniq.queue.sqlite import SQLiteQueue
from miniq.task import Task, TaskStatus


def test_crash_recovery_reclaims_after_visibility_timeout(tmp_path: Path) -> None:
    """A task claimed by a worker that 'crashes' is reclaimable after timeout."""
    db_path = tmp_path / "miniq.db"

    # Worker 1 claims a task and then dies (close without ack)
    q1 = SQLiteQueue(db_path)
    task = Task(func_path="x.y", args=(1, 2))
    q1.enqueue(task)
    claimed = q1.dequeue(visibility_timeout=0.1)
    assert claimed is not None
    assert claimed.id == task.id
    q1.close()

    # Wait for visibility timeout to expire
    time.sleep(0.15)

    # Worker 2 (new connection, simulates a different process) reclaims
    q2 = SQLiteQueue(db_path)
    try:
        reclaimed = q2.dequeue()
        assert reclaimed is not None
        assert reclaimed.id == task.id
        assert reclaimed.status is TaskStatus.RUNNING
    finally:
        q2.close()


def test_concurrent_workers_no_double_claim(tmp_path: Path) -> None:
    """Two workers racing to dequeue the same task: exactly one wins."""
    db_path = tmp_path / "miniq.db"

    setup = SQLiteQueue(db_path)
    task = Task(func_path="x.y")
    setup.enqueue(task)
    setup.close()

    claimed: list[Task] = []
    claimed_lock = threading.Lock()

    def try_claim() -> None:
        q = SQLiteQueue(db_path)
        try:
            result = q.dequeue(wait_seconds=0.5)
            if result is not None:
                with claimed_lock:
                    claimed.append(result)
        finally:
            q.close()

    threads = [threading.Thread(target=try_claim) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=3.0)

    # Exactly one thread should have claimed the task
    assert len(claimed) == 1
    assert claimed[0].id == task.id


def test_many_concurrent_workers_each_claim_unique_task(tmp_path: Path) -> None:
    """N tasks, N workers; each worker claims exactly one unique task."""
    db_path = tmp_path / "miniq.db"
    n_tasks = 10

    setup = SQLiteQueue(db_path)
    tasks = [Task(func_path=f"x.y.{i}") for i in range(n_tasks)]
    for t in tasks:
        setup.enqueue(t)
    setup.close()

    claimed_ids: list[str] = []
    lock = threading.Lock()

    def claim_one() -> None:
        q = SQLiteQueue(db_path)
        try:
            result = q.dequeue(wait_seconds=3.0)
            if result is not None:
                with lock:
                    claimed_ids.append(result.id)
        finally:
            q.close()

    threads = [threading.Thread(target=claim_one) for _ in range(n_tasks)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    # Every worker should have claimed exactly one task; no duplicates
    assert len(claimed_ids) == n_tasks
    assert len(set(claimed_ids)) == n_tasks
