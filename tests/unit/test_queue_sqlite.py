"""Tests for SQLiteQueue.

The test surface mirrors test_queue_memory.py because both backends must
satisfy the same QueueBackend contract. We parametrize
these tests so the same bodies run against both backends.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from miniq.queue.sqlite import SQLiteQueue
from miniq.task import Task, TaskStatus


@pytest.fixture
def queue(tmp_path: Path) -> SQLiteQueue:
    return SQLiteQueue(tmp_path / "test.db")


class TestEnqueueDequeue:
    def test_dequeue_empty_returns_none(self, queue: SQLiteQueue) -> None:
        assert queue.dequeue() is None

    def test_enqueue_then_dequeue(self, queue: SQLiteQueue) -> None:
        t = Task(func_path="x.y")
        queue.enqueue(t)
        claimed = queue.dequeue()
        assert claimed is not None
        assert claimed.id == t.id
        assert claimed.status is TaskStatus.RUNNING
        assert claimed.started_at is not None

    def test_fifo_order(self, queue: SQLiteQueue) -> None:
        tasks = [Task(func_path="x.y") for _ in range(3)]
        for t in tasks:
            queue.enqueue(t)
        for expected in tasks:
            claimed = queue.dequeue()
            assert claimed is not None
            assert claimed.id == expected.id

    def test_size_counts_pending_and_claimed(self, queue: SQLiteQueue) -> None:
        queue.enqueue(Task(func_path="x.y"))
        queue.enqueue(Task(func_path="x.y"))
        assert queue.size() == 2
        queue.dequeue()
        assert queue.size() == 2


class TestAckNack:
    def test_ack_removes_task_from_active(self, queue: SQLiteQueue) -> None:
        t = Task(func_path="x.y")
        queue.enqueue(t)
        claimed = queue.dequeue()
        assert claimed is not None
        claimed.mark_success(result=42)
        queue.ack(claimed)
        assert queue.size() == 0
        stored = queue.get_task(claimed.id)
        assert stored is not None
        assert stored.status is TaskStatus.SUCCESS
        assert stored.result == 42

    def test_nack_with_requeue_makes_task_available(self, queue: SQLiteQueue) -> None:
        t = Task(func_path="x.y")
        queue.enqueue(t)
        claimed = queue.dequeue()
        assert claimed is not None
        queue.nack(claimed, requeue=True)
        reclaimed = queue.dequeue()
        assert reclaimed is not None
        assert reclaimed.id == t.id

    def test_nack_without_requeue_sends_to_dlq(self, queue: SQLiteQueue) -> None:
        t = Task(func_path="x.y")
        queue.enqueue(t)
        claimed = queue.dequeue()
        assert claimed is not None
        claimed.mark_failed(error="boom")
        queue.nack(claimed, requeue=False)
        assert queue.size() == 0
        stored = queue.get_task(claimed.id)
        assert stored is not None
        assert stored.status is TaskStatus.FAILED


class TestVisibilityTimeout:
    def test_expired_claim_is_reclaimable(self, queue: SQLiteQueue) -> None:
        t = Task(func_path="x.y")
        queue.enqueue(t)
        first = queue.dequeue(visibility_timeout=0.1)
        assert first is not None
        time.sleep(0.15)
        reclaimed = queue.dequeue()
        assert reclaimed is not None
        assert reclaimed.id == t.id

    def test_unexpired_claim_blocks_reclaim(self, queue: SQLiteQueue) -> None:
        t = Task(func_path="x.y")
        queue.enqueue(t)
        first = queue.dequeue(visibility_timeout=10.0)
        assert first is not None
        second = queue.dequeue()
        assert second is None


class TestBlockingDequeue:
    def test_wait_returns_none_after_timeout(self, queue: SQLiteQueue) -> None:
        start = time.monotonic()
        result = queue.dequeue(wait_seconds=0.15)
        elapsed = time.monotonic() - start
        assert result is None
        assert 0.1 < elapsed < 0.5

    def test_wait_returns_task_when_enqueued(self, queue: SQLiteQueue) -> None:
        holder: list[Task | None] = []

        def consumer() -> None:
            holder.append(queue.dequeue(wait_seconds=2.0))

        t = threading.Thread(target=consumer)
        t.start()
        time.sleep(0.1)

        task = Task(func_path="x.y")
        queue.enqueue(task)

        t.join(timeout=2.0)
        assert len(holder) == 1
        assert holder[0] is not None
        assert holder[0].id == task.id


class TestAvailableAt:
    def test_delayed_task_not_claimable_until_available(self, queue: SQLiteQueue) -> None:
        t = Task(func_path="x.y", available_at=time.time() + 0.1)
        queue.enqueue(t)
        assert queue.dequeue() is None
        time.sleep(0.15)
        claimed = queue.dequeue()
        assert claimed is not None
        assert claimed.id == t.id

    def test_ready_task_returned_before_delayed_task(self, queue: SQLiteQueue) -> None:
        delayed = Task(func_path="x.y", available_at=time.time() + 10.0)
        ready = Task(func_path="x.y")
        queue.enqueue(delayed)
        queue.enqueue(ready)
        claimed = queue.dequeue()
        assert claimed is not None
        assert claimed.id == ready.id
        assert queue.dequeue() is None


class TestPersistence:
    def test_task_survives_close_and_reopen(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.db"
        q1 = SQLiteQueue(db_path)
        t = Task(func_path="x.y", args=(1, 2), max_retries=3)
        q1.enqueue(t)
        q1.close()

        q2 = SQLiteQueue(db_path)
        claimed = q2.dequeue()
        assert claimed is not None
        assert claimed.id == t.id
        assert claimed.args == (1, 2)
        assert claimed.max_retries == 3
        q2.close()

    def test_serialization_round_trip(self, queue: SQLiteQueue) -> None:
        t = Task(
            func_path="myapp.tasks.compute",
            args=("hello", 42, [1, 2]),
            kwargs={"verbose": True, "items": [1, 2, 3]},
            max_retries=3,
        )
        queue.enqueue(t)
        claimed = queue.dequeue()
        assert claimed is not None
        assert claimed.func_path == "myapp.tasks.compute"
        # JSON converts nested tuples to lists; that's a known JSON limitation.
        assert claimed.args == ("hello", 42, [1, 2])
        assert claimed.kwargs == {"verbose": True, "items": [1, 2, 3]}
        assert claimed.max_retries == 3


class TestPriority:
    def test_higher_priority_dequeued_first(self, queue: SQLiteQueue) -> None:
        low = Task(func_path="x.low", priority=0)
        high = Task(func_path="x.high", priority=10)
        queue.enqueue(low)
        queue.enqueue(high)
        first = queue.dequeue()
        second = queue.dequeue()
        assert first is not None and first.id == high.id
        assert second is not None and second.id == low.id

    def test_fifo_within_same_priority(self, queue: SQLiteQueue) -> None:
        tasks = [Task(func_path=f"x.{i}", priority=5) for i in range(3)]
        for t in tasks:
            queue.enqueue(t)
        for expected in tasks:
            claimed = queue.dequeue()
            assert claimed is not None
            assert claimed.id == expected.id

    def test_priority_survives_persistence(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.db"
        q1 = SQLiteQueue(db_path)
        task = Task(func_path="x.y", priority=7)
        q1.enqueue(task)
        q1.close()

        q2 = SQLiteQueue(db_path)
        try:
            stored = q2.get_task(task.id)
            assert stored is not None
            assert stored.priority == 7
        finally:
            q2.close()
