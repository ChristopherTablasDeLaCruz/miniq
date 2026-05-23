"""Result backend abstraction.

A ResultBackend stores the outcomes of completed tasks so that callers
holding an AsyncResult can retrieve them. The result backend is separate
from the queue backend: a queue can be discarded after ack, but a result
must be retrievable until the caller fetches it (or until a TTL expires).
Implementations may share storage with the queue or be entirely separate.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from abc import ABC, abstractmethod
from pathlib import Path

from miniq.exceptions import TaskTimeout
from miniq.serializers import JSONSerializer, Serializer
from miniq.task import Task, TaskStatus


class ResultBackend(ABC):
    """Abstract base class for result storage backends.

    Implementations store finished tasks (in SUCCESS or FAILED state)
    and let callers retrieve them by task_id, optionally blocking until
    the result is available.
    """

    @abstractmethod
    def store(self, task: Task) -> None:
        """Store a finished task's outcome.

        Called by the Worker after the task has been marked success or
        failed. The full Task object is stored; callers access status,
        result, and error fields via that object.
        """

    @abstractmethod
    def get(self, task_id: str) -> Task | None:
        """Look up a stored task by id, or return None if not available.

        Non-blocking. Returns None if the task has not been stored yet
        (still pending, running, or never enqueued).
        """

    @abstractmethod
    def wait(self, task_id: str, timeout: float | None = None) -> Task:
        """Block until the task's result is available, then return it.

        'timeout' is the maximum time to wait in seconds. 'None'
        means wait indefinitely. '0' means do not block; raise
        immediately if the result is not already stored.

        Raises 'TaskTimeout' if the timeout expires before the result
        becomes available.
        """

    def close(self) -> None:  # noqa: B027
        """Release any resources held by the backend. Default: no-op."""


class InMemoryResultBackend(ResultBackend):
    """Thread-safe in-memory implementation of ResultBackend.

    Stores tasks in a dict keyed by task_id. Uses a Condition variable to
    wake blocked callers on store(). All state is lost on process exit.
    """

    def __init__(self) -> None:
        self._results: dict[str, Task] = {}
        self._available = threading.Condition()

    def store(self, task: Task) -> None:
        with self._available:
            self._results[task.id] = task
            self._available.notify_all()

    def get(self, task_id: str) -> Task | None:
        with self._available:
            return self._results.get(task_id)

    def wait(self, task_id: str, timeout: float | None = None) -> Task:
        deadline = time.monotonic() + timeout if timeout is not None else None

        with self._available:
            while True:
                if task_id in self._results:
                    return self._results[task_id]

                if deadline is None:
                    self._available.wait()
                    continue

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TaskTimeout(
                        f"Task {task_id} did not complete within timeout",
                        timeout_seconds=timeout or 0.0,
                    )
                self._available.wait(timeout=remaining)


class SQLiteResultBackend(ResultBackend):
    """Persistent result backend backed by SQLite.

    Stores task outcomes in a ``task_results`` table. Independent of
    SQLiteQueue's schema; can share a database file or use a separate
    file. Polling-based ``wait`` since SQLite has no native signaling
    between connections.
    """

    _RESULTS_SCHEMA = """
    CREATE TABLE IF NOT EXISTS task_results (
        id TEXT PRIMARY KEY,
        func_path TEXT NOT NULL,
        status TEXT NOT NULL,
        result_json BLOB,
        error TEXT,
        finished_at REAL,
        stored_at REAL NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_task_results_stored_at
        ON task_results (stored_at);
    """

    def __init__(
        self,
        db_path: str | Path,
        serializer: Serializer | None = None,
        poll_interval: float = 0.05,
    ) -> None:
        self._db_path = str(db_path)
        self._serializer = serializer if serializer is not None else JSONSerializer()
        self._poll_interval = poll_interval
        self._lock = threading.Lock()
        self._conn = self._open_connection()
        self._initialize_schema()

    def _open_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            isolation_level=None,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def _initialize_schema(self) -> None:
        with self._lock:
            self._conn.executescript(self._RESULTS_SCHEMA)

    def store(self, task: Task) -> None:
        result_blob = (
            self._serializer.serialize(task.result) if task.status is TaskStatus.SUCCESS else None
        )
        with self._lock:
            self._conn.execute("BEGIN")
            try:
                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO task_results (
                        id, func_path, status, result_json, error, finished_at, stored_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        task.id,
                        task.func_path,
                        task.status.value,
                        result_blob,
                        task.error,
                        task.finished_at,
                        time.time(),
                    ),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def get(self, task_id: str) -> Task | None:
        with self._lock:
            cursor = self._conn.execute("SELECT * FROM task_results WHERE id = ?", (task_id,))
            row = cursor.fetchone()
            if row is None:
                return None
            return self._row_to_task(row)

    def wait(self, task_id: str, timeout: float | None = None) -> Task:
        deadline = time.monotonic() + timeout if timeout is not None else None

        while True:
            task = self.get(task_id)
            if task is not None and task.status in (TaskStatus.SUCCESS, TaskStatus.FAILED):
                return task

            if deadline is None:
                time.sleep(self._poll_interval)
                continue

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TaskTimeout(
                    f"Task {task_id} did not complete within timeout",
                    timeout_seconds=timeout or 0.0,
                )
            time.sleep(min(self._poll_interval, remaining))

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _row_to_task(self, row: sqlite3.Row) -> Task:
        result = (
            self._serializer.deserialize(row["result_json"])
            if row["result_json"] is not None
            else None
        )
        return Task(
            func_path=row["func_path"],
            id=row["id"],
            status=TaskStatus(row["status"]),
            result=result,
            error=row["error"],
            finished_at=row["finished_at"],
        )
