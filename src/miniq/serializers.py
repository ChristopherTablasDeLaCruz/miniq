"""Serialization strategies for task arguments, kwargs, and results.

A Serializer converts arbitrary Python values to and from bytes. The queue
backend uses a serializer to encode task payloads before persisting them and
to decode them when handing tasks to workers. The default serializer in
Phase 2 will be JSONSerializer, which constrains task arguments to
JSON-safe types in exchange for safety and portability. PickleSerializer
will exist for users who need richer types and accept the security tradeoffs.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


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
