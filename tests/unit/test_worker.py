"""Tests for Worker."""

from __future__ import annotations

import threading
import time

import pytest

from miniq.queue.memory import InMemoryQueue
from miniq.registry import clear, register
from miniq.results import InMemoryResultBackend
from miniq.retry import FixedDelay
from miniq.task import Task, TaskStatus
from miniq.worker import Worker


@pytest.fixture(autouse=True)
def reset_registry() -> None:
    clear()


def _add(a: int, b: int) -> int:
    return a + b


def _explode(message: str) -> None:
    raise RuntimeError(message)


class TestWorker:
    def test_run_once_returns_false_when_empty(self) -> None:
        queue = InMemoryQueue()
        results = InMemoryResultBackend()
        worker = Worker(queue=queue, results=results, poll_wait_seconds=0.05)
        assert worker.run_once() is False

    def test_run_once_executes_task(self) -> None:
        queue = InMemoryQueue()
        results = InMemoryResultBackend()
        worker = Worker(queue=queue, results=results, poll_wait_seconds=0.5)

        register("test._add", _add)
        task = Task(func_path="test._add", args=(2, 3))
        queue.enqueue(task)

        assert worker.run_once() is True

        stored = results.get(task.id)
        assert stored is not None
        assert stored.status is TaskStatus.SUCCESS
        assert stored.result == 5

    def test_run_once_handles_exception(self) -> None:
        queue = InMemoryQueue()
        results = InMemoryResultBackend()
        worker = Worker(queue=queue, results=results, poll_wait_seconds=0.5)

        register("test._explode", _explode)
        task = Task(func_path="test._explode", args=("boom",))
        queue.enqueue(task)

        assert worker.run_once() is True

        stored = results.get(task.id)
        assert stored is not None
        assert stored.status is TaskStatus.FAILED
        assert "RuntimeError" in (stored.error or "")
        assert "boom" in (stored.error or "")

    def test_run_once_handles_unregistered_task(self) -> None:
        queue = InMemoryQueue()
        results = InMemoryResultBackend()
        worker = Worker(queue=queue, results=results, poll_wait_seconds=0.5)

        task = Task(func_path="nonexistent.func")
        queue.enqueue(task)

        assert worker.run_once() is True

        stored = results.get(task.id)
        assert stored is not None
        assert stored.status is TaskStatus.FAILED

    def test_stop_exits_run_loop(self) -> None:
        queue = InMemoryQueue()
        results = InMemoryResultBackend()
        worker = Worker(queue=queue, results=results, poll_wait_seconds=0.05)

        thread = threading.Thread(target=worker.run)
        thread.start()
        time.sleep(0.1)
        worker.stop()
        thread.join(timeout=1.0)

        assert not thread.is_alive()


class TestWorkerRetries:
    def test_failed_task_is_retried_until_success(self) -> None:
        queue = InMemoryQueue()
        results = InMemoryResultBackend()
        worker = Worker(
            queue=queue,
            results=results,
            retry_policy=FixedDelay(delay_seconds=0.0),
            poll_wait_seconds=0.1,
        )

        attempts = [0]

        def flaky() -> str:
            attempts[0] += 1
            if attempts[0] < 3:
                raise RuntimeError("not yet")
            return "ok"

        register("test.flaky", flaky)
        task = Task(func_path="test.flaky", max_retries=2)
        queue.enqueue(task)

        # Drain: process tasks until none are available.
        for _ in range(10):
            if not worker.run_once():
                break

        stored = results.get(task.id)
        assert stored is not None
        assert stored.status is TaskStatus.SUCCESS
        assert stored.result == "ok"
        assert attempts[0] == 3

    def test_task_goes_to_dlq_after_max_retries(self) -> None:
        queue = InMemoryQueue()
        results = InMemoryResultBackend()
        worker = Worker(
            queue=queue,
            results=results,
            retry_policy=FixedDelay(delay_seconds=0.0),
            poll_wait_seconds=0.1,
        )

        attempts = [0]

        def always_fails() -> None:
            attempts[0] += 1
            raise RuntimeError("nope")

        register("test.always_fails", always_fails)
        task = Task(func_path="test.always_fails", max_retries=2)
        queue.enqueue(task)

        for _ in range(10):
            if not worker.run_once():
                break

        stored = results.get(task.id)
        assert stored is not None
        assert stored.status is TaskStatus.FAILED
        assert attempts[0] == 3  # 1 initial + 2 retries

    def test_no_retries_when_policy_is_noretry(self) -> None:
        queue = InMemoryQueue()
        results = InMemoryResultBackend()
        # default retry_policy is NoRetry
        worker = Worker(queue=queue, results=results, poll_wait_seconds=0.1)

        attempts = [0]

        def fails() -> None:
            attempts[0] += 1
            raise RuntimeError("nope")

        register("test.fails_no_retry", fails)
        task = Task(func_path="test.fails_no_retry", max_retries=5)
        queue.enqueue(task)

        worker.run_once()

        stored = results.get(task.id)
        assert stored is not None
        assert stored.status is TaskStatus.FAILED
        assert attempts[0] == 1  # no retries despite max_retries=5

    def test_retry_respects_delay(self) -> None:
        queue = InMemoryQueue()
        results = InMemoryResultBackend()
        worker = Worker(
            queue=queue,
            results=results,
            retry_policy=FixedDelay(delay_seconds=0.2),
            poll_wait_seconds=0.05,
        )

        attempts = [0]

        def flaky() -> str:
            attempts[0] += 1
            if attempts[0] < 2:
                raise RuntimeError("once")
            return "ok"

        register("test.flaky_delayed", flaky)
        task = Task(func_path="test.flaky_delayed", max_retries=1)
        queue.enqueue(task)

        start = time.monotonic()
        # First attempt fails, retry is scheduled for +0.2s
        worker.run_once()
        # Immediately try again; task not ready yet
        assert worker.run_once() is False
        # Wait past the delay
        time.sleep(0.25)
        worker.run_once()
        elapsed = time.monotonic() - start

        stored = results.get(task.id)
        assert stored is not None
        assert stored.status is TaskStatus.SUCCESS
        assert elapsed >= 0.2
