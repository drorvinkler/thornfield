from typing import Optional, Callable, Any

from .cache import Cache
from ..constants import NOT_FOUND
from ..errors import CachingError

try:
    from redis import Redis
except ModuleNotFoundError:
    Redis = None


class RedisCache(Cache):
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        serializer: Optional[Callable[[Any], str]] = None,
        deserializer: Optional[Callable[[Optional[str]], Any]] = None,
        **kwargs,
    ) -> None:
        super().__init__(serializer=serializer, deserializer=deserializer)
        if Redis is None:
            raise CachingError('Package "redis" is not installed')
        self._redis = Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True,
            **kwargs,
        )

    def get(self, key):
        try:
            value = self._redis.get(self._serialize(key))
            if value is None:
                return NOT_FOUND
            return self._deserialize(value)
        except Exception as e:
            raise CachingError(f"Could not get {key}", exc=e)

    def set(self, key, value, expiration: int):
        try:
            self._redis.set(
                self._serialize(key), self._serialize(value), px=expiration or None
            )
        except Exception as e:
            raise CachingError(f"Could not set {key} as {value}", exc=e)
