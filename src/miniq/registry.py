"""Task registry: maps function paths to live callables.

The @task decorator registers each decorated function at import time.
Workers look up functions by path when dequeueing a task. The registry
is process-local; producers and workers must import the same task
modules to share function definitions.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from miniq.exceptions import TaskNotRegistered

_registry: dict[str, Callable[..., Any]] = {}


def register(func_path: str, func: Callable[..., Any]) -> None:
    """Register a function under its import path.

    Subsequent registrations of the same path overwrite the previous one.
    This is intentional: it makes re-importing safe and lets tests redefine
    tasks freely.
    """
    _registry[func_path] = func


def lookup(func_path: str) -> Callable[..., Any]:
    """Find a registered function by path.

    Raises 'TaskNotRegistered' if no function is registered under this
    path. The usual cause is that the worker process did not import the
    module that defines the task.
    """
    if func_path not in _registry:
        raise TaskNotRegistered(
            f"No task registered under path {func_path!r}. "
            "Did you import the module that defines it?"
        )
    return _registry[func_path]


def is_registered(func_path: str) -> bool:
    """Return True if a function is registered under this path."""
    return func_path in _registry


def clear() -> None:
    """Remove all registered functions. Intended for tests."""
    _registry.clear()
