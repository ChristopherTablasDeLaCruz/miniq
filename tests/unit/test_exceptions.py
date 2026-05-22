"""Tests for the miniq exception hierarchy."""

from __future__ import annotations

import pytest

from miniq.exceptions import (
    BackendError,
    MiniqError,
    QueueEmpty,
    QueueFull,
    SerializationError,
    TaskError,
    TaskFailed,
    TaskNotRegistered,
    TaskTimeout,
)


class TestHierarchy:
    """Every miniq exception ultimately inherits from MiniqError and Exception."""

    @pytest.mark.parametrize(
        "exc_cls",
        [
            TaskError,
            TaskFailed,
            TaskTimeout,
            TaskNotRegistered,
            BackendError,
            QueueFull,
            QueueEmpty,
            SerializationError,
        ],
    )
    def test_inherits_from_miniq_error(self, exc_cls: type[Exception]) -> None:
        assert issubclass(exc_cls, MiniqError)
        assert issubclass(exc_cls, Exception)

    def test_task_failed_is_task_error(self) -> None:
        assert issubclass(TaskFailed, TaskError)

    def test_queue_full_is_backend_error(self) -> None:
        assert issubclass(QueueFull, BackendError)


class TestTaskFailed:
    def test_stores_original_exception(self) -> None:
        original = ValueError("boom")
        err = TaskFailed("task blew up", original_exception=original)
        assert err.original_exception is original

    def test_original_exception_optional(self) -> None:
        err = TaskFailed("task blew up")
        assert err.original_exception is None

    def test_message_preserved(self) -> None:
        err = TaskFailed("task blew up")
        assert str(err) == "task blew up"


class TestTaskTimeout:
    def test_stores_timeout_seconds(self) -> None:
        err = TaskTimeout("ran too long", timeout_seconds=30.0)
        assert err.timeout_seconds == 30.0

    def test_str_includes_timeout(self) -> None:
        err = TaskTimeout("ran too long", timeout_seconds=30.0)
        assert "30.0" in str(err)
        assert "ran too long" in str(err)
