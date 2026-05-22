"""Tests for the Serializer ABC."""

from __future__ import annotations

from typing import Any

import pytest
from miniq.serializers import Serializer


class TestSerializer:
    def test_cannot_instantiate_directly(self) -> None:
        with pytest.raises(TypeError):
            Serializer()  # type: ignore[abstract]

    def test_partial_implementation_still_abstract(self) -> None:
        """A subclass that implements only one method is still abstract."""

        class HalfDone(Serializer):
            def serialize(self, value: Any) -> bytes:
                return b""

            # deserialize intentionally not implemented

        with pytest.raises(TypeError):
            HalfDone()  # type: ignore[abstract]

    def test_full_implementation_can_instantiate(self) -> None:
        """Confirms the abstract contract is satisfied by a complete subclass."""

        class FullyDone(Serializer):
            def serialize(self, value: Any) -> bytes:
                return b""

            def deserialize(self, data: bytes) -> Any:
                return None

        FullyDone()  # should not raise
