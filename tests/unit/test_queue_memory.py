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
        assert q.dequeue() is None  # not redelivered

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

    def test_nack_without_requeue_removes_task(self) -> None:
        q = InMemoryQueue()
        t = Task(func_path="x.y")
        q.enqueue(t)
        claimed = q.dequeue()
        assert claimed is not None
        claimed.mark_failed(error="boom")
        q.nack(claimed, requeue=False)
        assert q.size() == 0
        assert q.dequeue() is None  # not redelivered


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


class TestAvailableAt:
    def test_delayed_task_not_claimable_until_available(self) -> None:
        q = InMemoryQueue()
        t = Task(func_path="x.y", available_at=time.time() + 0.1)
        q.enqueue(t)
        assert q.dequeue() is None
        time.sleep(0.15)
        assert q.dequeue() is t

    def test_ready_task_returned_before_delayed_task(self) -> None:
        q = InMemoryQueue()
        delayed = Task(func_path="x.y", available_at=time.time() + 10.0)
        ready = Task(func_path="x.y")
        q.enqueue(delayed)
        q.enqueue(ready)
        assert q.dequeue() is ready
        assert q.dequeue() is None  # delayed still not ready

    def test_unset_available_at_means_immediately_ready(self) -> None:
        q = InMemoryQueue()
        t = Task(func_path="x.y")  # available_at defaults to None
        q.enqueue(t)
        assert q.dequeue() is t


class TestPriority:
    def test_higher_priority_dequeued_first(self) -> None:
        q = InMemoryQueue()
        low = Task(func_path="x.y.low", priority=0)
        high = Task(func_path="x.y.high", priority=10)
        q.enqueue(low)
        q.enqueue(high)
        assert q.dequeue() is high
        assert q.dequeue() is low

    def test_fifo_within_same_priority(self) -> None:
        q = InMemoryQueue()
        tasks = [Task(func_path=f"x.y.{i}", priority=5) for i in range(3)]
        for t in tasks:
            q.enqueue(t)
        for expected in tasks:
            assert q.dequeue() is expected

    def test_mixed_priorities_strictly_ordered(self) -> None:
        q = InMemoryQueue()
        a = Task(func_path="x.a", priority=1)
        b = Task(func_path="x.b", priority=5)
        c = Task(func_path="x.c", priority=3)
        # Enqueue in arbitrary order
        q.enqueue(a)
        q.enqueue(b)
        q.enqueue(c)
        # Dequeue order: b (5), c (3), a (1)
        assert q.dequeue() is b
        assert q.dequeue() is c
        assert q.dequeue() is a
