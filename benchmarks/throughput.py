"""Throughput benchmark for miniq backends.

Measures end-to-end tasks/sec for each backend by:
  1. Enqueueing N no-op tasks (measures producer throughput).
  2. Draining the queue with a single worker (measures consumer throughput).

The result is a small table printed to stdout. Numbers vary by hardware;
re-run on your own machine to get representative values for your setup.

Usage:
    uv run python benchmarks/throughput.py [N]

Default N is 1000.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any

from miniq import (
    InMemoryQueue,
    InMemoryResultBackend,
    Miniq,
    SQLiteQueue,
    SQLiteResultBackend,
)


def _build_app(queue_kind: str, db_path: str | None = None) -> tuple[Miniq, Any]:
    """Build a Miniq app with the requested backend pair. Returns (app, task)."""
    if queue_kind == "memory":
        app = Miniq(queue=InMemoryQueue(), results=InMemoryResultBackend())
    elif queue_kind == "sqlite":
        assert db_path is not None
        app = Miniq(
            queue=SQLiteQueue(db_path),
            results=SQLiteResultBackend(db_path),
        )
    else:
        raise ValueError(f"unknown queue_kind: {queue_kind}")

    @app.task
    def noop() -> int:
        return 0

    return app, noop


def benchmark(queue_kind: str, n_tasks: int, db_path: str | None = None) -> dict[str, float]:
    app, noop_task = _build_app(queue_kind, db_path)

    # Enqueue phase
    t0 = time.monotonic()
    for _ in range(n_tasks):
        noop_task.delay()
    enqueue_elapsed = time.monotonic() - t0

    # Drain phase: single worker, run_once until empty
    worker = app.worker(poll_wait_seconds=0.0)
    t0 = time.monotonic()
    processed = 0
    while processed < n_tasks:
        if worker.run_once():
            processed += 1
    drain_elapsed = time.monotonic() - t0

    if queue_kind == "sqlite":
        app.queue.close()
        app.results.close()

    total = enqueue_elapsed + drain_elapsed
    return {
        "n_tasks": float(n_tasks),
        "enqueue_per_sec": n_tasks / enqueue_elapsed,
        "drain_per_sec": n_tasks / drain_elapsed,
        "total_per_sec": n_tasks / total,
    }


def _cleanup_sqlite(path: str) -> None:
    for suffix in ("", "-wal", "-shm"):
        p = Path(path + suffix)
        if p.exists():
            p.unlink()


def main() -> int:
    n_tasks = int(sys.argv[1]) if len(sys.argv) > 1 else 1000

    print(f"Benchmarking miniq with {n_tasks:,} no-op tasks per backend.")
    print()

    print("Running InMemory benchmark...")
    mem = benchmark("memory", n_tasks)

    db_path = "/tmp/miniq_bench.db"
    _cleanup_sqlite(db_path)
    print("Running SQLite benchmark...")
    sqlite = benchmark("sqlite", n_tasks, db_path)
    _cleanup_sqlite(db_path)

    print()
    print(f"{'Backend':<20} {'Enqueue/s':>12} {'Drain/s':>12} {'End-to-end/s':>15}")
    print("-" * 62)
    for name, results in [("InMemory", mem), ("SQLite (WAL)", sqlite)]:
        print(
            f"{name:<20} "
            f"{results['enqueue_per_sec']:>12,.0f} "
            f"{results['drain_per_sec']:>12,.0f} "
            f"{results['total_per_sec']:>15,.0f}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
