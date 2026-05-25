"""Task functions used by pool integration tests.

Defined at module level so they're importable in worker subprocesses
via the WorkerPool's ``task_modules`` parameter.
"""

from __future__ import annotations

from miniq import Miniq

# Local Miniq instance used purely as a registration vehicle.
# Worker subprocesses construct their own queue and result backend via
# the factories passed to WorkerPool; this _app is just to satisfy the
# decorator machinery and trigger registry population on import.
_app = Miniq()


@_app.task
def square(x: int) -> int:
    return x * x


@_app.task
def identity(x: int) -> int:
    return x
