"""Minimal miniq example: enqueue a task, run a worker, get the result.

Run with:
    uv run python examples/quickstart.py
"""

from miniq import Miniq

app = Miniq()


@app.task
def add(a: int, b: int) -> int:
    return a + b


def main() -> None:
    # Enqueue (returns instantly with a handle)
    result = add.delay(2, 3)

    # Process one task in the foreground (in production, the worker would
    # run in a separate process; here we just run one tick for the demo)
    app.worker(poll_wait_seconds=0.1).run_once()

    # Fetch the result
    print(f"2 + 3 = {result.get(timeout=1.0)}")


if __name__ == "__main__":
    main()
