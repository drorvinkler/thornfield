import json
from abc import ABC, abstractmethod
from time import time
from typing import Optional, Callable, Any

from .volatile_value import VolatileValue
from ..constants import NOT_FOUND
from ..errors import CachingError

try:
    from yasoo import serialize, deserialize
except ImportError:
    serialize = deserialize = None


class Cache(ABC):
    def __init__(
        self,
        serializer: Optional[Callable[[Any], str]] = None,
        deserializer: Optional[Callable[[Optional[str]], Any]] = None,
        serialization_needed: bool = True,
    ) -> None:
        super().__init__()
        if not serialization_needed:
            return
        if serialize is None or deserialize is None:
            if serializer is None or deserializer is None:
                raise CachingError(
                    'Package "yasoo" is not installed and no de/serializer passed'
                )
        self._serialize = self._default_serialize if serializer is None else serializer
        self._deserialize = (
            self._default_deserialize if deserializer is None else deserializer
        )

    @abstractmethod
    def get(self, key):
        pass

    @abstractmethod
    def set(self, key, value, expiration: int):
        pass

    @staticmethod
    def _to_volatile(value, expiration: int) -> VolatileValue:
        return VolatileValue(value, round(time() * 1000) + expiration)

    @staticmethod
    def _from_volatile(value: VolatileValue):
        if time() * 1000 <= value.expiration:
            return value.value
        return NOT_FOUND

    @staticmethod
    def _default_serialize(obj) -> str:
        return json.dumps(serialize(obj, preserve_iterable_types=True))

    @staticmethod
    def _default_deserialize(data: Optional[str]):
        if data is None:
            return data
        return deserialize(json.loads(data))
