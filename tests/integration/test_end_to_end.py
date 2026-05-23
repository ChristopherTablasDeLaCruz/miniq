"""End-to-end integration tests for miniq.

These prove the whole stack works together: producer enqueues via .delay(),
worker dequeues and executes, result is retrieved via AsyncResult.get().
"""

from __future__ import annotations

import threading

import pytest

from miniq import Miniq
from miniq.exceptions import TaskFailed, TaskTimeout
from miniq.registry import clear


@pytest.fixture(autouse=True)
def reset_registry() -> None:
    clear()


def test_enqueue_run_and_get_result() -> None:
    """The canonical hello-world flow."""
    app = Miniq()

    @app.task
    def add(a: int, b: int) -> int:
        return a + b

    result = add.delay(2, 3)
    app.worker(poll_wait_seconds=0.5).run_once()

    assert result.get(timeout=1.0) == 5


def test_multiple_tasks_in_order() -> None:
    """Worker processes tasks in FIFO order."""
    app = Miniq()

    @app.task
    def echo(x: int) -> int:
        return x

    handles = [echo.delay(i) for i in range(5)]
    worker = app.worker(poll_wait_seconds=0.5)
    for _ in range(5):
        worker.run_once()

    assert [h.get(timeout=1.0) for h in handles] == [0, 1, 2, 3, 4]


def test_failed_task_raises_on_get() -> None:
    """A failing task's exception surfaces via AsyncResult.get()."""
    app = Miniq()

    @app.task
    def fail(msg: str) -> None:
        raise RuntimeError(msg)

    result = fail.delay("kaboom")
    app.worker(poll_wait_seconds=0.5).run_once()

    with pytest.raises(TaskFailed):
        result.get(timeout=1.0)


def test_worker_in_thread_processes_concurrently() -> None:
    """Worker running in a thread processes tasks as they're enqueued."""
    app = Miniq()

    @app.task
    def double(x: int) -> int:
        return x * 2

    worker = app.worker(poll_wait_seconds=0.05)
    thread = threading.Thread(target=worker.run)
    thread.start()

    try:
        handles = [double.delay(i) for i in range(5)]
        values = [h.get(timeout=2.0) for h in handles]
        assert values == [0, 2, 4, 6, 8]
    finally:
        worker.stop()
        thread.join(timeout=1.0)


def test_get_with_short_timeout_raises() -> None:
    """get() with a short timeout on an unfinished task raises TaskTimeout."""
    app = Miniq()

    @app.task
    def slow() -> int:
        return 42

    # Enqueue but don't run a worker
    result = slow.delay()

    with pytest.raises(TaskTimeout):
        result.get(timeout=0.05)
