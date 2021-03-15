from types import MethodType, FunctionType
from typing import Union, Optional, Callable

from redis import Redis

from .cache_factory import CacheFactory
from ..caches.cache import Cache
from ..caches.redis_cache import RedisCache


class RedisCacheFactory(CacheFactory):
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        password: Optional[str] = None,
        decorator: Optional[Callable[[Cache], Cache]] = None,
    ) -> None:
        super().__init__(decorator)
        self.host = host
        self.port = port
        self.password = password
        self._index = Redis(host, port, db=0, password=password)

    def _create(self, func: Union[MethodType, FunctionType]) -> RedisCache:
        key = self._func_to_key(func)
        db = self._index.get(key)
        if db is None:
            used = {int(self._index.get(k)) for k in self._index.keys("*")}
            db = min({i + 1 for i in range(len(used) + 1)}.difference(used))
            self._index.set(key, db)
        return RedisCache(
            host=self.host, port=self.port, db=db, password=self.password,
        )
