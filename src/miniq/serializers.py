"""Serialization strategies for task arguments, kwargs, and results.

A Serializer converts arbitrary Python values to and from bytes. The queue
backend uses a serializer to encode task payloads before persisting them and
to decode them when handing tasks to workers. The default serializer in
Phase 2 will be JSONSerializer, which constrains task arguments to
JSON-safe types in exchange for safety and portability. PickleSerializer
will exist for users who need richer types and accept the security tradeoffs.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any

from miniq.exceptions import SerializationError


class Serializer(ABC):
    """Abstract base class for value serializers.

    Implementations convert arbitrary Python values to and from bytes.
    Both methods must raise 'SerializationError' (from miniq.exceptions)
    if the operation cannot be completed.
    """

    @abstractmethod
    def serialize(self, value: Any) -> bytes:
        """Convert a value to bytes for storage or transport.

        Raises 'SerializationError' if the value cannot be serialized by
        this implementation (e.g. JSON given a custom class instance).
        """

    @abstractmethod
    def deserialize(self, data: bytes) -> Any:
        """Reconstruct a value from bytes produced by ``serialize``.

        Raises 'SerializationError' if the data is malformed or was
        produced by an incompatible serializer.
        """


class JSONSerializer(Serializer):
    """JSON-based serializer.

    Constrains values to JSON-compatible types: str, int, float, bool, None,
    list, dict. Anything else (custom classes, datetimes, sets, tuples) raises
    SerializationError on serialize.

    This is the recommended default serializer. JSON is safe (no arbitrary
    code execution on deserialization, unlike pickle), portable (any language
    can read it), and debuggable (stored bytes are human-readable).
    """

    def serialize(self, value: Any) -> bytes:
        try:
            return json.dumps(value).encode("utf-8")
        except (TypeError, ValueError) as e:
            raise SerializationError(f"Could not JSON-serialize value: {e}") from e

    def deserialize(self, data: bytes) -> Any:
        try:
            return json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise SerializationError(f"Could not JSON-deserialize bytes: {e}") from e
