import json
from typing import Optional, Callable, Any

from .cache import Cache
from ..errors import CachingError

try:
    from yasoo import serialize, deserialize
except ImportError:
    serialize = deserialize = None


class CacheSerializationDecorator(Cache):
    def __init__(
        self,
        cache: Cache,
        serializer: Optional[Callable[[Any], str]] = ...,
        deserializer: Optional[Callable[[Optional[str]], Any]] = ...,
    ) -> None:
        super().__init__()
        self._cache = cache
        if serializer is None:
            self._serialize = self._noop
        elif serializer is ...:
            if serialize is None:
                raise CachingError(
                    'Package "yasoo" is not installed and no serializer passed'
                )
            self._serialize = self._default_serialize
        else:
            self._serialize = serializer

        if deserializer is None:
            self._deserialize = self._noop
        elif deserializer is ...:
            if deserialize is None:
                raise CachingError(
                    'Package "yasoo" is not installed and no deserializer passed'
                )
            self._deserialize = self._default_deserialize
        else:
            self._deserialize = deserializer

    def get(self, key):
        value = self._cache.get(self._serialize(key))
        return self._deserialize(value)

    def set(self, key, value, expiration: int) -> None:
        self._cache.set(self._serialize(key), self._serialize(value), expiration)

    @staticmethod
    def _noop(x):
        return x

    @staticmethod
    def _default_serialize(obj) -> str:
        return json.dumps(serialize(obj, preserve_iterable_types=True))

    @staticmethod
    def _default_deserialize(data: Optional[str]):
        if data is None:
            return data
        return deserialize(json.loads(data))
