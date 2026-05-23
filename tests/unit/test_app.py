"""Tests for Miniq app and TaskWrapper."""

from __future__ import annotations

import pytest

from miniq.app import Miniq, TaskWrapper
from miniq.registry import clear, is_registered
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

        assert is_registered(add.func_path)

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
