"""Top-level Miniq application object.

Miniq bundles a queue backend, a result backend, and the @task decorator
into a single user-facing API. Tasks defined via @app.task are bound to
this app's backends; their .delay() calls enqueue on this app's queue.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from miniq.queue.base import QueueBackend
from miniq.queue.memory import InMemoryQueue
from miniq.registry import register
from miniq.results import InMemoryResultBackend, ResultBackend
from miniq.task import AsyncResult, Task
from miniq.worker import Worker


class TaskWrapper:
    """A function wrapped by @app.task.

    Calling directly runs the function synchronously (as if undecorated).
    Calling .delay() enqueues a deferred execution and returns an
    AsyncResult handle.
    """

    def __init__(
        self,
        func: Callable[..., Any],
        app: Miniq,
        max_retries: int = 0,
    ) -> None:
        self._func = func
        self._app = app
        self._max_retries = max_retries
        self.func_path = f"{func.__module__}.{func.__qualname__}"

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._func(*args, **kwargs)

    def delay(self, *args: Any, **kwargs: Any) -> AsyncResult:
        task = Task(
            func_path=self.func_path,
            args=args,
            kwargs=kwargs,
            max_retries=self._max_retries,
        )
        self._app.queue.enqueue(task)
        return AsyncResult(task_id=task.id, backend=self._app.results)


class Miniq:
    """Application object that owns a queue, result backend, and task decorator.

    Construct one Miniq per application. By default uses in-memory backends
    suitable for tests and single-process scripts. Pass other backends for
    persistence (SQLiteQueue arrives in Phase 4) or multi-process operation.
    """

    def __init__(
        self,
        queue: QueueBackend | None = None,
        results: ResultBackend | None = None,
    ) -> None:
        self.queue: QueueBackend = queue if queue is not None else InMemoryQueue()
        self.results: ResultBackend = results if results is not None else InMemoryResultBackend()

    def task(
        self,
        func: Callable[..., Any] | None = None,
        *,
        max_retries: int = 0,
    ) -> TaskWrapper | Callable[[Callable[..., Any]], TaskWrapper]:
        """Decorator that registers a function as a task.

        Usable two ways::

            @app.task
            def f(...): ...

            @app.task(max_retries=3)
            def f(...): ...
        """

        def decorator(f: Callable[..., Any]) -> TaskWrapper:
            wrapper = TaskWrapper(f, self, max_retries=max_retries)
            register(wrapper.func_path, f)
            return wrapper

        if func is None:
            # Called as @app.task(...) with arguments.
            return decorator
        # Called as @app.task without arguments.
        return decorator(func)

    def worker(self, **kwargs: Any) -> Worker:
        """Create a Worker bound to this app's queue and result backend."""
        return Worker(queue=self.queue, results=self.results, **kwargs)
