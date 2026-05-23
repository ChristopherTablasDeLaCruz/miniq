"""miniq: a minimal task queue with pluggable backends.

Quick start:

    from miniq import Miniq

    app = Miniq()

    @app.task
    def add(a, b):
        return a + b

    result = add.delay(2, 3)
    app.worker().run_once()

    print(result.get())  # 5
"""

from miniq.app import Miniq, TaskWrapper
from miniq.exceptions import (
    BackendError,
    MiniqError,
    QueueEmpty,
    QueueFull,
    SerializationError,
    TaskError,
    TaskFailed,
    TaskNotRegistered,
    TaskTimeout,
)
from miniq.queue.base import QueueBackend
from miniq.queue.memory import InMemoryQueue
from miniq.results import InMemoryResultBackend, ResultBackend
from miniq.retry import (
    ExponentialBackoff,
    FixedDelay,
    JitteredBackoff,
    NoRetry,
    RetryPolicy,
)
from miniq.serializers import JSONSerializer, Serializer
from miniq.task import AsyncResult, Task, TaskStatus
from miniq.worker import Worker

__version__ = "0.0.1"

__all__ = [
    "AsyncResult",
    "BackendError",
    "ExponentialBackoff",
    "FixedDelay",
    "InMemoryQueue",
    "InMemoryResultBackend",
    "JSONSerializer",
    "JitteredBackoff",
    "Miniq",
    "MiniqError",
    "NoRetry",
    "QueueBackend",
    "QueueEmpty",
    "QueueFull",
    "ResultBackend",
    "RetryPolicy",
    "SerializationError",
    "Serializer",
    "Task",
    "TaskError",
    "TaskFailed",
    "TaskNotRegistered",
    "TaskStatus",
    "TaskTimeout",
    "TaskWrapper",
    "Worker",
    "__version__",
]
