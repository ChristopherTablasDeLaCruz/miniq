"""Tests for Miniq app and TaskWrapper."""

from __future__ import annotations

import time
from datetime import datetime

import pytest

from miniq.app import Miniq, TaskWrapper
from miniq.registry import clear, lookup
from miniq.task import AsyncResult


@pytest.fixture(autouse=True)
def reset_registry() -> None:
    clear()


class TestMiniq:
    def test_default_backends_constructed(self) -> None:
        app = Miniq()
        assert app.queue is not None
        assert app.results is not None

    def test_task_decorator_returns_wrapper(self) -> None:
        app = Miniq()

        @app.task
        def add(a: int, b: int) -> int:
            return a + b

        assert isinstance(add, TaskWrapper)

    def test_task_decorator_registers_function(self) -> None:
        app = Miniq()

        @app.task
        def add(a: int, b: int) -> int:
            return a + b

        assert lookup(add.func_path)(2, 3) == 5

    def test_direct_call_runs_synchronously(self) -> None:
        app = Miniq()

        @app.task
        def add(a: int, b: int) -> int:
            return a + b

        assert add(2, 3) == 5

    def test_delay_returns_async_result(self) -> None:
        app = Miniq()

        @app.task
        def add(a: int, b: int) -> int:
            return a + b

        result = add.delay(2, 3)
        assert isinstance(result, AsyncResult)

    def test_delay_enqueues_task(self) -> None:
        app = Miniq()

        @app.task
        def add(a: int, b: int) -> int:
            return a + b

        assert app.queue.size() == 0
        add.delay(2, 3)
        assert app.queue.size() == 1

    def test_worker_factory_returns_bound_worker(self) -> None:
        app = Miniq()

        @app.task
        def add(a: int, b: int) -> int:
            return a + b

        worker = app.worker(poll_wait_seconds=0.5)
        result = add.delay(2, 3)
        worker.run_once()
        assert result.get(timeout=1.0) == 5


class TestTaskDecoratorArgs:
    def test_bare_decorator_sets_max_retries_zero(self) -> None:
        app = Miniq()

        @app.task
        def f() -> int:
            return 1

        assert f._max_retries == 0  # type: ignore[attr-defined]

    def test_decorator_with_max_retries(self) -> None:
        app = Miniq()

        @app.task(max_retries=3)
        def f() -> int:
            return 1

        assert f._max_retries == 3  # type: ignore[attr-defined]

    def test_max_retries_propagates_to_task(self) -> None:
        app = Miniq()

        @app.task(max_retries=5)
        def f() -> int:
            return 1

        f.delay()
        # The enqueued task should carry max_retries
        enqueued = next(iter(app.queue._pending))  # type: ignore[attr-defined]
        assert enqueued.max_retries == 5


class TestSchedule:
    def test_schedule_with_countdown_sets_future_available_at(self) -> None:
        app = Miniq()

        @app.task
        def add(a: int, b: int) -> int:
            return a + b

        before = time.time()
        add.schedule(args=(2, 3), countdown=60)
        after = time.time()

        enqueued = next(iter(app.queue._pending))  # type: ignore[attr-defined]
        assert enqueued.available_at is not None
        assert before + 60 - 0.1 <= enqueued.available_at <= after + 60 + 0.1

    def test_schedule_with_at_datetime(self) -> None:
        app = Miniq()

        @app.task
        def add(a: int, b: int) -> int:
            return a + b

        target = datetime(2099, 1, 1, 12, 0, 0)
        add.schedule(args=(2, 3), at=target)

        enqueued = next(iter(app.queue._pending))  # type: ignore[attr-defined]
        assert enqueued.available_at == target.timestamp()

    def test_schedule_without_timing_is_immediate(self) -> None:
        app = Miniq()

        @app.task
        def add(a: int, b: int) -> int:
            return a + b

        add.schedule(args=(2, 3))

        enqueued = next(iter(app.queue._pending))  # type: ignore[attr-defined]
        assert enqueued.available_at is None

    def test_schedule_passes_args_and_kwargs_correctly(self) -> None:
        app = Miniq()

        @app.task
        def compute(a: int, b: int, factor: int = 1) -> int:
            return (a + b) * factor

        compute.schedule(args=(2, 3), kwargs={"factor": 10})

        enqueued = next(iter(app.queue._pending))  # type: ignore[attr-defined]
        assert enqueued.args == (2, 3)
        assert enqueued.kwargs == {"factor": 10}

    def test_schedule_with_both_countdown_and_at_raises(self) -> None:
        app = Miniq()

        @app.task
        def add(a: int, b: int) -> int:
            return a + b

        with pytest.raises(ValueError):
            add.schedule(args=(2, 3), countdown=60, at=datetime(2099, 1, 1))

    def test_schedule_returns_async_result(self) -> None:
        app = Miniq()

        @app.task
        def add(a: int, b: int) -> int:
            return a + b

        result = add.schedule(args=(2, 3), countdown=10)
        assert isinstance(result, AsyncResult)


class TestPriorityDecorator:
    def test_default_priority_is_zero(self) -> None:
        app = Miniq()

        @app.task
        def f() -> int:
            return 1

        assert f._priority == 0  # type: ignore[attr-defined]

    def test_decorator_sets_priority(self) -> None:
        app = Miniq()

        @app.task(priority=10)
        def f() -> int:
            return 1

        assert f._priority == 10  # type: ignore[attr-defined]

    def test_delay_uses_decorator_priority(self) -> None:
        app = Miniq()

        @app.task(priority=5)
        def f() -> int:
            return 1

        f.delay()
        enqueued = next(iter(app.queue._pending))  # type: ignore[attr-defined]
        assert enqueued.priority == 5

    def test_schedule_overrides_priority(self) -> None:
        app = Miniq()

        @app.task(priority=5)
        def f() -> int:
            return 1

        f.schedule(priority=10)
        enqueued = next(iter(app.queue._pending))  # type: ignore[attr-defined]
        assert enqueued.priority == 10

    def test_schedule_default_uses_decorator_priority(self) -> None:
        app = Miniq()

        @app.task(priority=5)
        def f() -> int:
            return 1

        f.schedule()
        enqueued = next(iter(app.queue._pending))  # type: ignore[attr-defined]
        assert enqueued.priority == 5
