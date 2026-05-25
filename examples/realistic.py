"""A more realistic example showing retries, priorities, scheduling, and SQLite.

This demonstrates how miniq would be used in a real app:
  - High-priority tasks (alerts) jump the queue
  - Tasks with retries handle transient failures
  - Scheduled tasks run after a delay
  - Persistence via SQLite

Run with:
    uv run python examples/realistic.py
"""

from __future__ import annotations

import time
from pathlib import Path

from miniq import Miniq, SQLiteQueue, SQLiteResultBackend
from miniq.retry import ExponentialBackoff

DB_PATH = "/tmp/miniq_realistic.db"


def _cleanup() -> None:
    for suffix in ("", "-wal", "-shm"):
        p = Path(DB_PATH + suffix)
        if p.exists():
            p.unlink()


_cleanup()
app = Miniq(
    queue=SQLiteQueue(DB_PATH),
    results=SQLiteResultBackend(DB_PATH),
)


@app.task(max_retries=3)
def send_welcome_email(user_id: int) -> str:
    print(f"  [worker] sending welcome email to user {user_id}")
    time.sleep(0.05)
    return f"welcomed user {user_id}"


@app.task(priority=10, max_retries=5)
def critical_alert(event: str) -> str:
    print(f"  [worker] handling critical alert: {event}")
    return f"alerted: {event}"


def main() -> None:
    print("Producer: enqueueing tasks...")
    # Normal-priority work
    h1 = send_welcome_email.delay(1)
    h2 = send_welcome_email.delay(2)
    # High-priority alert; will jump the queue
    h3 = critical_alert.delay("fraud_detected")
    # Scheduled for 0.3s from now
    h4 = send_welcome_email.schedule(args=(99,), countdown=0.3)
    handles = [h1, h2, h3, h4]

    print("\nWorker: starting (note priority causes alert to run first)...")
    worker = app.worker(
        retry_policy=ExponentialBackoff(base_seconds=0.05),
        poll_wait_seconds=0.05,
    )

    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        worker.run_once()
        if all(app.results.get(h.task_id) is not None for h in handles):
            break

    print("\nResults:")
    for h in handles:
        print(f"  {h.task_id[:8]}... -> {h.get(timeout=0.1)}")

    app.queue.close()
    app.results.close()
    _cleanup()


if __name__ == "__main__":
    main()
