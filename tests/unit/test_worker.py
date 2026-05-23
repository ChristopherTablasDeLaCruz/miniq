"""Tests for Worker."""

from __future__ import annotations

import threading
import time

import pytest

from miniq.queue.memory import InMemoryQueue
from miniq.registry import clear, register
from miniq.results import InMemoryResultBackend
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
