"""Tests for the ResultBackend ABC."""

from __future__ import annotations

import pytest
from miniq.results import ResultBackend
from miniq.task import Task


class TestResultBackend:
    def test_cannot_instantiate_directly(self) -> None:
        with pytest.raises(TypeError):
            ResultBackend()  # type: ignore[abstract]

    def test_partial_implementation_still_abstract(self) -> None:
        class PartialBackend(ResultBackend):
            def store(self, task: Task) -> None:
                pass

            # get and wait intentionally not implemented

        with pytest.raises(TypeError):
            PartialBackend()  # type: ignore[abstract]

    def test_full_implementation_can_instantiate(self) -> None:
        class FullBackend(ResultBackend):
            def store(self, task: Task) -> None:
                pass

            def get(self, task_id: str) -> Task | None:
                return None

            def wait(self, task_id: str, timeout: float | None = None) -> Task:
                raise NotImplementedError

        FullBackend()  # should not raise
