"""Command-line interface for miniq.

Usage examples::

    miniq worker --db ./miniq.db --app myapp.tasks --workers 4
    miniq status --db ./miniq.db
    miniq dlq list --db ./miniq.db
    miniq dlq replay <task_id> --db ./miniq.db

The CLI assumes a SQLite-backed queue. Other backends are programmatic-only
and don't go through the CLI.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sqlite3
import sys
import threading
from functools import partial
from pathlib import Path
from typing import Any

from miniq.task import TaskStatus


def cmd_worker(args: argparse.Namespace) -> int:
    """Run a worker pool against a SQLite-backed queue."""
    # Lazy imports so help/status don't pull in multiprocessing.
    from miniq.pool import WorkerPool
    from miniq.queue.sqlite import SQLiteQueue
    from miniq.results import SQLiteResultBackend

    task_modules: list[str] = []
    if args.app:
        task_modules = [m.strip() for m in args.app.split(",") if m.strip()]

    pool = WorkerPool(
        queue_factory=partial(SQLiteQueue, args.db),
        results_factory=partial(SQLiteResultBackend, args.db),
        workers=args.workers,
        task_modules=task_modules,
        poll_wait_seconds=args.poll_interval,
    )

    print(f"miniq: starting {args.workers} worker(s) against {args.db}")
    if task_modules:
        print(f"miniq: task modules: {', '.join(task_modules)}")
    print("miniq: press Ctrl-C to stop")

    stop_event = threading.Event()

    def handle_signal(signum: int, _frame: Any) -> None:
        if not stop_event.is_set():
            print(f"\nminiq: received signal {signum}, shutting down...")
            stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    pool.start()
    try:
        stop_event.wait()
    finally:
        pool.stop(timeout=args.shutdown_timeout)
        print("miniq: stopped")

    return 0


def cmd_status(args: argparse.Namespace) -> int:
    """Show task counts grouped by status."""
    if not Path(args.db).exists():
        print(f"Error: database not found: {args.db}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(args.db)
    try:
        cursor = conn.execute("SELECT status, COUNT(*) FROM tasks GROUP BY status")
        counts = dict(cursor.fetchall())
    finally:
        conn.close()

    print(f"miniq status: {args.db}")
    print("-" * 40)
    total = 0
    for status in TaskStatus:
        count = counts.get(status.value, 0)
        total += count
        print(f"  {status.value:<10} {count:>6}")
    print("-" * 40)
    print(f"  {'total':<10} {total:>6}")

    return 0


def cmd_dlq_list(args: argparse.Namespace) -> int:
    """List tasks in the dead-letter queue."""
    if not Path(args.db).exists():
        print(f"Error: database not found: {args.db}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.execute(
            """
            SELECT id, func_path, error, retries, max_retries
            FROM tasks
            WHERE status = ?
            ORDER BY finished_at DESC
            """,
            (TaskStatus.FAILED.value,),
        )
        rows = cursor.fetchall()
    finally:
        conn.close()

    if not rows:
        print("Dead-letter queue is empty.")
        return 0

    print(f"Dead-letter queue: {len(rows)} task(s)")
    print()
    for row in rows:
        print(f"  id:       {row['id']}")
        print(f"  func:     {row['func_path']}")
        print(f"  retries:  {row['retries']}/{row['max_retries']}")
        print(f"  error:    {row['error'] or '(none)'}")
        print()

    return 0


def cmd_dlq_replay(args: argparse.Namespace) -> int:
    """Move a failed task back to pending."""
    if not Path(args.db).exists():
        print(f"Error: database not found: {args.db}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(args.db)
    try:
        cursor = conn.execute(
            """
            UPDATE tasks
            SET status = ?,
                retries = 0,
                error = NULL,
                finished_at = NULL,
                started_at = NULL,
                claimed_until = NULL,
                available_at = NULL
            WHERE id = ? AND status = ?
            """,
            (TaskStatus.PENDING.value, args.task_id, TaskStatus.FAILED.value),
        )
        conn.commit()
        affected = cursor.rowcount
    finally:
        conn.close()

    if affected == 0:
        print(
            f"Error: no failed task with id {args.task_id!r}",
            file=sys.stderr,
        )
        return 1

    print(f"Task {args.task_id} replayed (status set to pending).")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="miniq",
        description="Command-line interface for the miniq task queue.",
    )
    parser.add_argument(
        "--log-level",
        default="WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: WARNING)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    p = subparsers.add_parser("worker", help="Run a worker pool")
    p.add_argument("--db", required=True, help="Path to the SQLite database")
    p.add_argument(
        "--app",
        help="Comma-separated module paths to import for task definitions",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of worker processes (default: 4)",
    )
    p.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="Worker poll interval in seconds (default: 1.0)",
    )
    p.add_argument(
        "--shutdown-timeout",
        type=float,
        default=30.0,
        help="Seconds to wait for graceful shutdown before SIGKILL (default: 30)",
    )
    p.set_defaults(func=cmd_worker)

    p = subparsers.add_parser("status", help="Show task counts by status")
    p.add_argument("--db", required=True, help="Path to the SQLite database")
    p.set_defaults(func=cmd_status)

    dlq_parser = subparsers.add_parser(
        "dlq",
        help="Inspect or manage the dead-letter queue",
    )
    dlq_subparsers = dlq_parser.add_subparsers(dest="dlq_command", required=True)

    p = dlq_subparsers.add_parser("list", help="List failed tasks")
    p.add_argument("--db", required=True)
    p.set_defaults(func=cmd_dlq_list)

    p = dlq_subparsers.add_parser("replay", help="Move a failed task back to pending")
    p.add_argument("task_id", help="ID of the failed task to replay")
    p.add_argument("--db", required=True)
    p.set_defaults(func=cmd_dlq_replay)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
