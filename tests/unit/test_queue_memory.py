"""Tests for InMemoryQueue."""

from __future__ import annotations

import threading
import time

from miniq.queue.memory import InMemoryQueue
from miniq.task import Task, TaskStatus


class TestEnqueueDequeue:
    def test_dequeue_empty_returns_none(self) -> None:
        q = InMemoryQueue()
        assert q.dequeue() is None

    def test_enqueue_then_dequeue(self) -> None:
        q = InMemoryQueue()
        t = Task(func_path="x.y")
        q.enqueue(t)
        claimed = q.dequeue()
        assert claimed is t
        assert claimed.status is TaskStatus.RUNNING
        assert claimed.started_at is not None

    def test_fifo_order(self) -> None:
        q = InMemoryQueue()
        tasks = [Task(func_path="x.y") for _ in range(3)]
        for t in tasks:
            q.enqueue(t)
        for expected in tasks:
            assert q.dequeue() is expected

    def test_size_counts_pending_and_claimed(self) -> None:
        q = InMemoryQueue()
        q.enqueue(Task(func_path="x.y"))
        q.enqueue(Task(func_path="x.y"))
        assert q.size() == 2
        q.dequeue()
        assert q.size() == 2  # one pending + one claimed


class TestAckNack:
    def test_ack_removes_task_from_active(self) -> None:
        q = InMemoryQueue()
        t = Task(func_path="x.y")
        q.enqueue(t)
        claimed = q.dequeue()
        assert claimed is not None
        claimed.mark_success(result=42)
        q.ack(claimed)
        assert q.size() == 0
        assert q.get_task(claimed.id) is claimed  # still retrievable

    def test_nack_with_requeue_makes_task_available(self) -> None:
        q = InMemoryQueue()
        t = Task(func_path="x.y")
        q.enqueue(t)
        claimed = q.dequeue()
        assert claimed is not None
        q.nack(claimed, requeue=True)
        reclaimed = q.dequeue()
        assert reclaimed is claimed
        assert reclaimed.status is TaskStatus.RUNNING

    def test_nack_without_requeue_sends_to_dlq(self) -> None:
        q = InMemoryQueue()
        t = Task(func_path="x.y")
        q.enqueue(t)
        claimed = q.dequeue()
        assert claimed is not None
        claimed.mark_failed(error="boom")
        q.nack(claimed, requeue=False)
        assert q.size() == 0
        assert q.get_task(claimed.id) is claimed  # still retrievable via DLQ


class TestVisibilityTimeout:
    def test_expired_claim_is_reclaimable(self) -> None:
        q = InMemoryQueue()
        t = Task(func_path="x.y")
        q.enqueue(t)
        first = q.dequeue(visibility_timeout=0.05)
        assert first is t
        time.sleep(0.1)
        reclaimed = q.dequeue()
        assert reclaimed is t

    def test_unexpired_claim_blocks_reclaim(self) -> None:
        q = InMemoryQueue()
        t = Task(func_path="x.y")
        q.enqueue(t)
        first = q.dequeue(visibility_timeout=10.0)
        assert first is t
        second = q.dequeue()
        assert second is None


class TestBlockingDequeue:
    def test_wait_returns_none_after_timeout(self) -> None:
        q = InMemoryQueue()
        start = time.monotonic()
        result = q.dequeue(wait_seconds=0.1)
        elapsed = time.monotonic() - start
        assert result is None
        assert 0.05 < elapsed < 0.5

    def test_wait_returns_task_when_enqueued(self) -> None:
        q = InMemoryQueue()
        holder: list[Task | None] = []

        def consumer() -> None:
            holder.append(q.dequeue(wait_seconds=2.0))

        t = threading.Thread(target=consumer)
        t.start()
        time.sleep(0.05)

        task = Task(func_path="x.y")
        q.enqueue(task)

        t.join(timeout=1.0)
        assert holder == [task]
