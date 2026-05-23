"""Tests for the task registry."""

from __future__ import annotations

import pytest

from miniq.exceptions import TaskNotRegistered
from miniq.registry import clear, is_registered, lookup, register


def _example_func(x: int) -> int:
    return x + 1


@pytest.fixture(autouse=True)
def reset_registry() -> None:
    """Each test starts with an empty registry."""
    clear()


class TestRegistry:
    def test_register_and_lookup(self) -> None:
        register("test.foo", _example_func)
        assert lookup("test.foo") is _example_func

    def test_lookup_unregistered_raises(self) -> None:
        with pytest.raises(TaskNotRegistered):
            lookup("nonexistent.path")

    def test_is_registered(self) -> None:
        assert not is_registered("test.foo")
        register("test.foo", _example_func)
        assert is_registered("test.foo")

    def test_clear_removes_all(self) -> None:
        register("test.foo", _example_func)
        register("test.bar", _example_func)
        clear()
        assert not is_registered("test.foo")
        assert not is_registered("test.bar")

    def test_re_register_overwrites(self) -> None:
        def other_func(x: int) -> int:
            return x * 2

        register("test.foo", _example_func)
        register("test.foo", other_func)
        assert lookup("test.foo") is other_func
