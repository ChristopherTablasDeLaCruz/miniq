"""SQLite-backed queue backend.

A persistent, single-host implementation of QueueBackend. Tasks live in a
single SQLite database file that survives process restarts and supports
multiple worker processes claiming concurrently. Concurrency safety comes
from SQLite's own file locking plus BEGIN IMMEDIATE transactions for
atomic claim operations.

Schema:
  - One 'tasks' table holds every task regardless of state.
  - 'status' drives the state machine (pending, running, success, failed).
  - 'available_at' (wall clock) gates when a pending task is claimable.
  - 'claimed_until' (wall clock) records when a claim expires; expired
    claims are reclaimed on the next dequeue.

Connection model:
  - One sqlite3.Connection per SQLiteQueue instance.
  - 'check_same_thread=False' plus an internal Lock allows safe
    multi-threaded access from within one process.
  - 'isolation_level=None' puts the connection in autocommit mode; we
    manage transactions explicitly via BEGIN/COMMIT/ROLLBACK so we can
    choose IMMEDIATE for read-then-write operations.
  - For multi-process operation (Phase 5), each process should construct
    its own SQLiteQueue and never share a connection across fork.
"""

from __future__ import annotations

import contextlib
import sqlite3
import threading
import time
from collections.abc import Iterator
from pathlib import Path

from miniq.queue.base import QueueBackend
from miniq.serializers import JSONSerializer, Serializer
from miniq.task import Task, TaskStatus

_SCHEMA = _SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    func_path TEXT NOT NULL,
    args_json BLOB NOT NULL,
    kwargs_json BLOB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    retries INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 0,
    priority INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    started_at REAL,
    finished_at REAL,
    available_at REAL,
    claimed_until REAL,
    result_json BLOB,
    error TEXT
);

CREATE INDEX IF NOT EXISTS idx_tasks_dequeue
    ON tasks (status, priority, available_at, created_at);

CREATE INDEX IF NOT EXISTS idx_tasks_status_claimed_until
    ON tasks (status, claimed_until);
"""


class SQLiteQueue(QueueBackend):
    """Persistent queue backed by a SQLite database file.

    Suitable for single-host deployments with one or more worker processes.
    """

    def __init__(
        self,
        db_path: str | Path,
        serializer: Serializer | None = None,
    ) -> None:
        self._db_path = str(db_path)
        self._serializer = serializer if serializer is not None else JSONSerializer()
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
            self._conn.executescript(_SCHEMA)
            # Migration: priority column was added after the initial schema.
            # If an existing database doesn't have it, add it now.
            cursor = self._conn.execute("PRAGMA table_info(tasks)")
            columns = {row[1] for row in cursor.fetchall()}
            if "priority" not in columns:
                self._conn.execute(
                    "ALTER TABLE tasks ADD COLUMN priority INTEGER NOT NULL DEFAULT 0"
                )

    @contextlib.contextmanager
    def _transaction(self, immediate: bool = False) -> Iterator[None]:
        """Explicit transaction management.

        'immediate=True' acquires SQLite's write lock at BEGIN time,
        preventing other writers from interleaving. Use for read-then-write
        sequences like the claim in '_try_claim'.
        """
        self._conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
        try:
            yield
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def enqueue(self, task: Task) -> None:
        args_blob = self._serializer.serialize(list(task.args))
        kwargs_blob = self._serializer.serialize(task.kwargs)

        with self._lock, self._transaction():
            self._conn.execute(
                """
                INSERT INTO tasks (
                    id, func_path, args_json, kwargs_json,
                    status, retries, max_retries, priority,
                    created_at, available_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task.id,
                    task.func_path,
                    args_blob,
                    kwargs_blob,
                    task.status.value,
                    task.retries,
                    task.max_retries,
                    task.priority,
                    task.created_at,
                    task.available_at,
                ),
            )

    def dequeue(
        self,
        visibility_timeout: float = 30.0,
        wait_seconds: float = 0.0,
    ) -> Task | None:
        deadline = time.monotonic() + wait_seconds if wait_seconds > 0 else None

        while True:
            task = self._try_claim(visibility_timeout)
            if task is not None:
                return task

            self._reclaim_expired()
            task = self._try_claim(visibility_timeout)
            if task is not None:
                return task

            if deadline is None:
                return None

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None

            time.sleep(min(0.05, remaining))

    def _try_claim(self, visibility_timeout: float) -> Task | None:
        now = time.time()
        claimed_until = now + visibility_timeout

        with self._lock:
            try:
                with self._transaction(immediate=True):
                    cursor = self._conn.execute(
                        """
                        SELECT * FROM tasks
                        WHERE status = ?
                        AND (available_at IS NULL OR available_at <= ?)
                        ORDER BY priority DESC, created_at ASC
                        LIMIT 1
                        """,
                        (TaskStatus.PENDING.value, now),
                    )
                    row = cursor.fetchone()
                    if row is None:
                        return None

                    self._conn.execute(
                        """
                        UPDATE tasks
                        SET status = ?, started_at = ?, claimed_until = ?
                        WHERE id = ?
                        """,
                        (TaskStatus.RUNNING.value, now, claimed_until, row["id"]),
                    )
                    task = self._row_to_task(row)
                    task.status = TaskStatus.RUNNING
                    task.started_at = now
                    return task
            except sqlite3.OperationalError:
                # Database locked by another connection; the dequeue loop
                # will retry after a brief sleep.
                return None

    def _reclaim_expired(self) -> None:
        now = time.time()
        with self._lock, self._transaction():
            self._conn.execute(
                """
                UPDATE tasks
                SET status = ?, started_at = NULL, claimed_until = NULL
                WHERE status = ?
                  AND claimed_until IS NOT NULL
                  AND claimed_until <= ?
                """,
                (TaskStatus.PENDING.value, TaskStatus.RUNNING.value, now),
            )

    def ack(self, task: Task) -> None:
        result_blob = (
            self._serializer.serialize(task.result) if task.status is TaskStatus.SUCCESS else None
        )
        with self._lock, self._transaction():
            self._conn.execute(
                """
                UPDATE tasks
                SET status = ?, finished_at = ?, result_json = ?,
                    error = ?, claimed_until = NULL
                WHERE id = ?
                """,
                (task.status.value, task.finished_at, result_blob, task.error, task.id),
            )

    def nack(self, task: Task, requeue: bool = True) -> None:
        with self._lock, self._transaction():
            if requeue:
                self._conn.execute(
                    """
                    UPDATE tasks
                    SET status = ?, started_at = NULL, claimed_until = NULL,
                        retries = ?, available_at = ?
                    WHERE id = ?
                    """,
                    (
                        TaskStatus.PENDING.value,
                        task.retries,
                        task.available_at,
                        task.id,
                    ),
                )
            else:
                self._conn.execute(
                    """
                    UPDATE tasks
                    SET status = ?, finished_at = ?, error = ?, claimed_until = NULL
                    WHERE id = ?
                    """,
                    (task.status.value, task.finished_at, task.error, task.id),
                )

    def size(self) -> int:
        with self._lock:
            cursor = self._conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE status IN (?, ?)",
                (TaskStatus.PENDING.value, TaskStatus.RUNNING.value),
            )
            return int(cursor.fetchone()[0])

    def get_task(self, task_id: str) -> Task | None:
        with self._lock:
            cursor = self._conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
            row = cursor.fetchone()
            if row is None:
                return None
            return self._row_to_task(row)

    def close(self) -> None:
        """Close the underlying connection. Idempotent."""
        with self._lock:
            self._conn.close()

    def _row_to_task(self, row: sqlite3.Row) -> Task:
        args = tuple(self._serializer.deserialize(row["args_json"]))
        kwargs = self._serializer.deserialize(row["kwargs_json"])
        result = (
            self._serializer.deserialize(row["result_json"])
            if row["result_json"] is not None
            else None
        )
        return Task(
            func_path=row["func_path"],
            args=args,
            kwargs=kwargs,
            id=row["id"],
            status=TaskStatus(row["status"]),
            retries=row["retries"],
            max_retries=row["max_retries"],
            priority=row["priority"],
            created_at=row["created_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            available_at=row["available_at"],
            result=result,
            error=row["error"],
        )
