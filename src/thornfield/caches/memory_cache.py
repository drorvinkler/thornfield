from .cache import Cache
from ..constants import NOT_FOUND


class MemoryCache(Cache):
    def __init__(self) -> None:
        super().__init__()
        self._cache = {}

    def get(self, key):
        if key not in self._cache:
            return NOT_FOUND
        return self._cache[key]

    def set(self, key, value):
        self._cache[key] = value
