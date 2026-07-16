"""End-to-end integration tests for miniq.

These tests run against multiple backend combinations to prove the
QueueBackend and ResultBackend contracts hold identically across
implementations.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Generator
from pathlib import Path

import pytest

from miniq import Miniq, SQLiteQueue, SQLiteResultBackend
from miniq.exceptions import TaskFailed, TaskTimeout
from miniq.registry import clear


@pytest.fixture(autouse=True)
def reset_registry() -> None:
    clear()


@pytest.fixture(params=["memory", "sqlite"])
def app(request: pytest.FixtureRequest, tmp_path: Path) -> Generator[Miniq]:
    if request.param == "memory":
        instance = Miniq()
        yield instance
    else:
        db_path = tmp_path / "miniq.db"
        instance = Miniq(
            queue=SQLiteQueue(db_path),
            results=SQLiteResultBackend(db_path),
        )
        try:
            yield instance
        finally:
            instance.queue.close()
            instance.results.close()


def test_enqueue_run_and_get_result(app: Miniq) -> None:
    @app.task
    def add(a: int, b: int) -> int:
        return a + b

    result = add.delay(2, 3)
    app.worker(poll_wait_seconds=0.1).run_once()

    assert result.get(timeout=2.0) == 5


def test_multiple_tasks_in_order(app: Miniq) -> None:
    @app.task
    def echo(x: int) -> int:
        return x

    handles = [echo.delay(i) for i in range(5)]
    worker = app.worker(poll_wait_seconds=0.1)
    for _ in range(5):
        worker.run_once()

    assert [h.get(timeout=2.0) for h in handles] == [0, 1, 2, 3, 4]


def test_failed_task_raises_on_get(app: Miniq) -> None:
    @app.task
    def fail(msg: str) -> None:
        raise RuntimeError(msg)

    result = fail.delay("kaboom")
    app.worker(poll_wait_seconds=0.1).run_once()

    with pytest.raises(TaskFailed):
        result.get(timeout=2.0)


def test_worker_in_thread_processes_concurrently(app: Miniq) -> None:
    @app.task
    def double(x: int) -> int:
        return x * 2

    worker = app.worker(poll_wait_seconds=0.05)
    thread = threading.Thread(target=worker.run)
    thread.start()

    try:
        handles = [double.delay(i) for i in range(5)]
        values = [h.get(timeout=3.0) for h in handles]
        assert values == [0, 2, 4, 6, 8]
    finally:
        worker.stop()
        thread.join(timeout=2.0)


def test_get_with_short_timeout_raises(app: Miniq) -> None:
    @app.task
    def slow() -> int:
        return 42

    result = slow.delay()

    with pytest.raises(TaskTimeout):
        result.get(timeout=0.1)


def test_flaky_task_succeeds_with_retries(app: Miniq) -> None:
    from miniq.retry import FixedDelay

    attempts = [0]

    @app.task(max_retries=3)
    def flaky() -> str:
        attempts[0] += 1
        if attempts[0] < 3:
            raise RuntimeError("not yet")
        return "finally"

    result = flaky.delay()
    worker = app.worker(
        retry_policy=FixedDelay(delay_seconds=0.0),
        poll_wait_seconds=0.1,
    )
    for _ in range(10):
        if not worker.run_once():
            break

    assert result.get(timeout=2.0) == "finally"
    assert attempts[0] == 3


def test_scheduled_task_runs_after_delay(app: Miniq) -> None:
    """A task scheduled with countdown is not executed until the time arrives."""

    @app.task
    def get_value() -> int:
        return 42

    start = time.monotonic()
    result = get_value.schedule(countdown=0.3)
    worker = app.worker(poll_wait_seconds=0.05)

    # Poll until the task is processed or we time out.
    deadline = time.monotonic() + 3.0
    processed = False
    while time.monotonic() < deadline:
        if worker.run_once():
            processed = True
            break

    elapsed = time.monotonic() - start

    assert processed, "scheduled task never processed"
    assert elapsed >= 0.25, f"task ran too early ({elapsed:.2f}s)"
    assert result.get(timeout=1.0) == 42
