"""Tests for Task, TaskStatus, and AsyncResult."""

from __future__ import annotations

import time

import pytest

from miniq.exceptions import TaskFailed, TaskTimeout
from miniq.results import ResultBackend
from miniq.task import AsyncResult, Task, TaskStatus


class TestTaskStatus:
    def test_values_are_strings(self) -> None:
        assert TaskStatus.PENDING == "pending"
        assert TaskStatus.SUCCESS == "success"

    def test_membership(self) -> None:
        assert TaskStatus("pending") is TaskStatus.PENDING


class TestTaskDefaults:
    def test_id_is_unique(self) -> None:
        a = Task(func_path="x.y")
        b = Task(func_path="x.y")
        assert a.id != b.id

    def test_starts_pending(self) -> None:
        t = Task(func_path="x.y")
        assert t.status is TaskStatus.PENDING

    def test_args_and_kwargs_default_empty(self) -> None:
        t = Task(func_path="x.y")
        assert t.args == ()
        assert t.kwargs == {}

    def test_created_at_set(self) -> None:
        before = time.time()
        t = Task(func_path="x.y")
        after = time.time()
        assert before <= t.created_at <= after

    def test_no_started_or_finished_yet(self) -> None:
        t = Task(func_path="x.y")
        assert t.started_at is None
        assert t.finished_at is None

    def test_mutable_defaults_not_shared(self) -> None:
        a = Task(func_path="x.y")
        b = Task(func_path="x.y")
        a.kwargs["k"] = 1
        assert b.kwargs == {}


class TestTaskTransitions:
    def test_mark_running(self) -> None:
        t = Task(func_path="x.y")
        t.mark_running()
        assert t.status is TaskStatus.RUNNING
        assert t.started_at is not None

    def test_mark_success_stores_result(self) -> None:
        t = Task(func_path="x.y")
        t.mark_running()
        t.mark_success(result=42)
        assert t.status is TaskStatus.SUCCESS
        assert t.result == 42
        assert t.finished_at is not None

    def test_mark_failed_stores_error(self) -> None:
        t = Task(func_path="x.y")
        t.mark_running()
        t.mark_failed(error="boom")
        assert t.status is TaskStatus.FAILED
        assert t.error == "boom"
        assert t.finished_at is not None

    def test_mark_pending_resets_running_task(self) -> None:
        t = Task(func_path="x.y")
        t.mark_running()
        t.mark_pending()
        assert t.status is TaskStatus.PENDING
        assert t.started_at is None


class _FakeBackend(ResultBackend):
    """Minimal in-memory stand-in for ResultBackend used in AsyncResult tests.

    Avoids pulling in the real in-memory backend
    while still satisfying the ABC. 'wait' returns the preset task or
    raises TaskTimeout if none is set.
    """

    def __init__(self, task: Task | None = None) -> None:
        self._task = task

    def store(self, task: Task) -> None:
        self._task = task

    def get(self, task_id: str) -> Task | None:
        return self._task

    def wait(self, task_id: str, timeout: float | None = None) -> Task:
        if self._task is None:
            raise TaskTimeout("no task stored", timeout_seconds=timeout or 0.0)
        return self._task


class TestAsyncResult:
    def test_stores_task_id(self) -> None:
        r = AsyncResult(task_id="abc", backend=_FakeBackend())
        assert r.task_id == "abc"

    def test_get_returns_result_on_success(self) -> None:
        task = Task(func_path="x.y")
        task.mark_success(result=42)
        r = AsyncResult(task_id=task.id, backend=_FakeBackend(task=task))
        assert r.get() == 42

    def test_get_raises_task_failed_on_failure(self) -> None:
        task = Task(func_path="x.y")
        task.mark_failed(error="something broke")
        r = AsyncResult(task_id=task.id, backend=_FakeBackend(task=task))
        with pytest.raises(TaskFailed):
            r.get()

    def test_get_raises_timeout_if_not_available(self) -> None:
        r = AsyncResult(task_id="abc", backend=_FakeBackend())
        with pytest.raises(TaskTimeout):
            r.get(timeout=0.0)
