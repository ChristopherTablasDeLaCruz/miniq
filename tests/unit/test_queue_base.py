"""Tests for the QueueBackend ABC."""

from __future__ import annotations

import pytest

from miniq.queue.base import QueueBackend
from miniq.task import Task


class TestQueueBackend:
    def test_cannot_instantiate_directly(self) -> None:
        with pytest.raises(TypeError):
            QueueBackend()  # type: ignore[abstract]

    def test_partial_implementation_still_abstract(self) -> None:
        """Subclass missing methods cannot be instantiated."""

        class PartialQueue(QueueBackend):
            def enqueue(self, task: Task) -> None:
                pass

            # other abstract methods intentionally not implemented

        with pytest.raises(TypeError):
            PartialQueue()  # type: ignore[abstract]

    def test_full_implementation_can_instantiate(self) -> None:
        """A subclass that implements every abstract method can be created."""

        class FullQueue(QueueBackend):
            def enqueue(self, task: Task) -> None:
                pass

            def dequeue(
                self,
                visibility_timeout: float = 30.0,
                wait_seconds: float = 0.0,
            ) -> Task | None:
                return None

            def ack(self, task: Task) -> None:
                pass

            def nack(self, task: Task, requeue: bool = True) -> None:
                pass

            def size(self) -> int:
                return 0

            def get_task(self, task_id: str) -> Task | None:
                return None

        FullQueue()  # should not raise
