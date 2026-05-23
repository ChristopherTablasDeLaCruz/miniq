"""Tests for SQLiteResultBackend."""

from __future__ import annotations

import contextlib
import threading
import time
from pathlib import Path

import pytest

from miniq.exceptions import TaskTimeout
from miniq.results import SQLiteResultBackend
from miniq.task import Task, TaskStatus


class TestStoreAndGet:
    def test_store_and_get_success(self, tmp_path: Path) -> None:
        backend = SQLiteResultBackend(tmp_path / "results.db")
        try:
            task = Task(func_path="x.y")
            task.mark_success(result=42)
            backend.store(task)

            stored = backend.get(task.id)
            assert stored is not None
            assert stored.id == task.id
            assert stored.status is TaskStatus.SUCCESS
            assert stored.result == 42
        finally:
            backend.close()

    def test_store_and_get_failure(self, tmp_path: Path) -> None:
        backend = SQLiteResultBackend(tmp_path / "results.db")
        try:
            task = Task(func_path="x.y")
            task.mark_failed(error="boom")
            backend.store(task)

            stored = backend.get(task.id)
            assert stored is not None
            assert stored.status is TaskStatus.FAILED
            assert stored.error == "boom"
        finally:
            backend.close()

    def test_get_unknown_returns_none(self, tmp_path: Path) -> None:
        backend = SQLiteResultBackend(tmp_path / "results.db")
        try:
            assert backend.get("nonexistent") is None
        finally:
            backend.close()

    def test_store_overwrites_previous(self, tmp_path: Path) -> None:
        backend = SQLiteResultBackend(tmp_path / "results.db")
        try:
            task = Task(func_path="x.y")
            task.mark_success(result=1)
            backend.store(task)

            task.mark_success(result=2)
            backend.store(task)

            stored = backend.get(task.id)
            assert stored is not None
            assert stored.result == 2
        finally:
            backend.close()


class TestWait:
    def test_wait_returns_immediately_if_available(self, tmp_path: Path) -> None:
        backend = SQLiteResultBackend(tmp_path / "results.db")
        try:
            task = Task(func_path="x.y")
            task.mark_success(result=42)
            backend.store(task)

            stored = backend.wait(task.id, timeout=1.0)
            assert stored.result == 42
        finally:
            backend.close()

    def test_wait_raises_on_timeout(self, tmp_path: Path) -> None:
        backend = SQLiteResultBackend(tmp_path / "results.db")
        try:
            with pytest.raises(TaskTimeout):
                backend.wait("nonexistent", timeout=0.1)
        finally:
            backend.close()

    def test_wait_blocks_until_stored(self, tmp_path: Path) -> None:
        backend = SQLiteResultBackend(tmp_path / "results.db", poll_interval=0.02)
        task = Task(func_path="x.y")
        task.mark_success(result=42)

        holder: list[Task] = []

        def waiter() -> None:
            with contextlib.suppress(TaskTimeout):
                holder.append(backend.wait(task.id, timeout=2.0))

        thread = threading.Thread(target=waiter)
        thread.start()
        time.sleep(0.1)
        backend.store(task)
        thread.join(timeout=2.0)
        backend.close()

        assert len(holder) == 1
        assert holder[0].result == 42


class TestPersistence:
    def test_result_survives_reopen(self, tmp_path: Path) -> None:
        db_path = tmp_path / "results.db"

        b1 = SQLiteResultBackend(db_path)
        task = Task(func_path="x.y")
        task.mark_success(result="persisted")
        b1.store(task)
        b1.close()

        b2 = SQLiteResultBackend(db_path)
        try:
            stored = b2.get(task.id)
            assert stored is not None
            assert stored.result == "persisted"
        finally:
            b2.close()
