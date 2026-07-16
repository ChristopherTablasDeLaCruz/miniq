"""Shared SQLite connection and transaction helpers.

Used by SQLiteQueue and SQLiteResultBackend so both open connections
with identical pragmas and manage transactions the same way.
"""

from __future__ import annotations

import contextlib
import sqlite3
from collections.abc import Iterator


def open_connection(db_path: str) -> sqlite3.Connection:
    """Open a connection configured for miniq's concurrency model.

    'check_same_thread=False' plus the caller's own lock allows safe
    multi-threaded access from within one process. 'isolation_level=None'
    puts the connection in autocommit mode; transactions are managed
    explicitly via 'transaction' so callers can choose BEGIN IMMEDIATE
    for read-then-write sequences.
    """
    conn = sqlite3.connect(
        db_path,
        check_same_thread=False,
        isolation_level=None,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


@contextlib.contextmanager
def transaction(conn: sqlite3.Connection, immediate: bool = False) -> Iterator[None]:
    """Explicit transaction management.

    'immediate=True' acquires SQLite's write lock at BEGIN time,
    preventing other writers from interleaving. Use for read-then-write
    sequences like claiming a task.
    """
    conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
