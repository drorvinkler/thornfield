from .cache import Cache
from .volatile_value import VolatileValue
from ..constants import NOT_FOUND


class MemoryCache(Cache):
    def __init__(self) -> None:
        super().__init__(serialization_needed=False)
        self._cache = {}

    def get(self, key):
        if key not in self._cache:
            return NOT_FOUND
        value = self._cache[key]
        if isinstance(value, VolatileValue):
            return self._from_volatile(value)
        return value

    def set(self, key, value, expiration: int):
        if expiration:
            value = self._to_volatile(value, expiration)
        self._cache[key] = value
