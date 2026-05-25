"""Smoke tests for the miniq CLI."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from miniq.cli import main
from miniq.queue.sqlite import SQLiteQueue
from miniq.task import Task, TaskStatus


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Create a SQLite database populated with tasks in various states."""
    db_path = tmp_path / "test.db"
    q = SQLiteQueue(str(db_path))

    # Success: enqueue then immediately dequeue + ack so we get the right task
    t = Task(func_path="x.success")
    q.enqueue(t)
    claimed = q.dequeue()
    assert claimed is not None
    claimed.mark_success(result=42)
    q.ack(claimed)

    # Failed: enqueue then immediately dequeue + nack to DLQ
    t = Task(func_path="x.failed")
    q.enqueue(t)
    claimed = q.dequeue()
    assert claimed is not None
    claimed.mark_failed(error="something broke")
    q.nack(claimed, requeue=False)

    # Pending tasks: enqueued last so nothing dequeues them
    q.enqueue(Task(func_path="x.a"))
    q.enqueue(Task(func_path="x.b"))

    q.close()
    return db_path


class TestStatus:
    def test_shows_all_status_counts(
        self, db_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rc = main(["status", "--db", str(db_path)])
        assert rc == 0

        out = capsys.readouterr().out
        # All four status names should appear
        for status in TaskStatus:
            assert status.value in out
        assert "total" in out

    def test_missing_db_returns_error(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rc = main(["status", "--db", str(tmp_path / "nope.db")])
        assert rc == 1

        err = capsys.readouterr().err
        assert "not found" in err.lower()


class TestDlqList:
    def test_lists_failed_tasks(self, db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        rc = main(["dlq", "list", "--db", str(db_path)])
        assert rc == 0

        out = capsys.readouterr().out
        assert "x.failed" in out
        assert "something broke" in out

    def test_empty_dlq(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        # Create an empty database
        empty_db = tmp_path / "empty.db"
        SQLiteQueue(str(empty_db)).close()

        rc = main(["dlq", "list", "--db", str(empty_db)])
        assert rc == 0

        out = capsys.readouterr().out
        assert "empty" in out.lower()


class TestDlqReplay:
    def test_replay_moves_failed_to_pending(
        self, db_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # Find the failed task id
        conn = sqlite3.connect(str(db_path))
        try:
            failed_id = conn.execute(
                "SELECT id FROM tasks WHERE status = ? LIMIT 1",
                (TaskStatus.FAILED.value,),
            ).fetchone()[0]
        finally:
            conn.close()

        rc = main(["dlq", "replay", failed_id, "--db", str(db_path)])
        assert rc == 0

        # Verify the task is now pending
        conn = sqlite3.connect(str(db_path))
        try:
            row = conn.execute(
                "SELECT status, error, retries FROM tasks WHERE id = ?",
                (failed_id,),
            ).fetchone()
        finally:
            conn.close()

        assert row[0] == TaskStatus.PENDING.value
        assert row[1] is None  # error cleared
        assert row[2] == 0  # retries reset

    def test_replay_nonexistent_task_returns_error(
        self, db_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rc = main(["dlq", "replay", "no-such-id", "--db", str(db_path)])
        assert rc == 1

        err = capsys.readouterr().err
        assert "no failed task" in err.lower()
