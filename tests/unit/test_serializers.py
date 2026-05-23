"""Tests for the Serializer ABC."""

from __future__ import annotations

from typing import Any

import pytest

from miniq.exceptions import SerializationError
from miniq.serializers import JSONSerializer, Serializer


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


class TestJSONSerializer:
    def test_round_trip_simple_types(self) -> None:
        s = JSONSerializer()
        for value in [None, True, False, 0, 1.5, "hello", [], {}, [1, 2, 3], {"a": 1}]:
            assert s.deserialize(s.serialize(value)) == value

    def test_round_trip_nested(self) -> None:
        s = JSONSerializer()
        value = {"users": [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]}
        assert s.deserialize(s.serialize(value)) == value

    def test_returns_bytes(self) -> None:
        s = JSONSerializer()
        assert isinstance(s.serialize({"a": 1}), bytes)

    def test_rejects_non_json_types(self) -> None:
        s = JSONSerializer()
        with pytest.raises(SerializationError):
            s.serialize({1, 2, 3})  # sets are not JSON-serializable

    def test_rejects_invalid_json(self) -> None:
        s = JSONSerializer()
        with pytest.raises(SerializationError):
            s.deserialize(b"not valid json{{{")
