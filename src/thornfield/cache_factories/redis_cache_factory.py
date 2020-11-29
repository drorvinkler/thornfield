from types import MethodType, FunctionType
from typing import Union, Optional, Callable, Any

from redis import Redis

from thornfield.cache_factories.cache_factory import CacheFactory
from thornfield.caches.redis_cache import RedisCache


class RedisCacheFactory(CacheFactory):
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        password: Optional[str] = None,
        serializer: Optional[Callable[[Any], str]] = None,
        deserializer: Optional[Callable[[Optional[str]], Any]] = None,
    ) -> None:
        super().__init__()
        self.host = host
        self.port = port
        self.password = password
        self.serializer = serializer
        self.deserializer = deserializer
        self._index = Redis(host, port, db=0, password=password)

    def create(self, func: Union[MethodType, FunctionType]) -> RedisCache:
        key = self._func_to_key(func)
        db = self._index.get(key)
        if db is None:
            used = {int(self._index.get(k)) for k in self._index.keys("*")}
            db = min({i + 1 for i in range(len(used) + 1)}.difference(used))
            self._index.set(key, db)
        return RedisCache(
            host=self.host,
            port=self.port,
            db=db,
            password=self.password,
            serializer=self.serializer,
            deserializer=self.deserializer,
        )
