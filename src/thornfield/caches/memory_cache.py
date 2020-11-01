from .cache import Cache


class MemoryCache(Cache):
    def __init__(self) -> None:
        super().__init__()
        self._cache = {}

    def get(self, key):
        return self._cache.get(key)

    def set(self, key, value):
        self._cache[key] = value
