"""Tests for the ResultBackend ABC."""

from __future__ import annotations

import threading
import time

import pytest

from miniq.exceptions import TaskTimeout
from miniq.results import InMemoryResultBackend, ResultBackend
from miniq.task import Task


class TestResultBackend:
    def test_cannot_instantiate_directly(self) -> None:
        with pytest.raises(TypeError):
            ResultBackend()  # type: ignore[abstract]

    def test_partial_implementation_still_abstract(self) -> None:
        class PartialBackend(ResultBackend):
            def store(self, task: Task) -> None:
                pass

            # get and wait intentionally not implemented

        with pytest.raises(TypeError):
            PartialBackend()  # type: ignore[abstract]

    def test_full_implementation_can_instantiate(self) -> None:
        class FullBackend(ResultBackend):
            def store(self, task: Task) -> None:
                pass

            def get(self, task_id: str) -> Task | None:
                return None

            def wait(self, task_id: str, timeout: float | None = None) -> Task:
                raise NotImplementedError

        FullBackend()  # should not raise


class TestInMemoryResultBackend:
    def test_store_and_get(self) -> None:
        backend = InMemoryResultBackend()
        task = Task(func_path="x.y")
        task.mark_success(result=42)
        backend.store(task)
        assert backend.get(task.id) is task

    def test_get_unknown_returns_none(self) -> None:
        backend = InMemoryResultBackend()
        assert backend.get("nonexistent") is None

    def test_wait_returns_immediately_if_available(self) -> None:
        backend = InMemoryResultBackend()
        task = Task(func_path="x.y")
        task.mark_success(result=42)
        backend.store(task)
        assert backend.wait(task.id) is task

    def test_wait_blocks_until_available(self) -> None:
        backend = InMemoryResultBackend()
        task = Task(func_path="x.y")
        task.mark_success(result=42)

        holder: list[Task | None] = []

        def waiter() -> None:
            holder.append(backend.wait(task.id, timeout=2.0))

        t = threading.Thread(target=waiter)
        t.start()
        time.sleep(0.05)

        backend.store(task)
        t.join(timeout=1.0)
        assert holder == [task]

    def test_wait_raises_on_timeout(self) -> None:
        backend = InMemoryResultBackend()
        with pytest.raises(TaskTimeout):
            backend.wait("nonexistent", timeout=0.05)

    def test_wait_ignores_non_terminal_task(self) -> None:
        """wait() only returns finished tasks, matching SQLiteResultBackend."""
        backend = InMemoryResultBackend()
        task = Task(func_path="x.y")
        task.mark_running()
        backend.store(task)
        with pytest.raises(TaskTimeout):
            backend.wait(task.id, timeout=0.05)
