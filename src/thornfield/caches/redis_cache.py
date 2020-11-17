import json
from typing import Optional, Callable, Any

from thornfield.caches.cache import Cache
from thornfield.errors import CachingError

try:
    from redis import Redis
except ModuleNotFoundError:
    Redis = None
try:
    from yasoo import serialize, deserialize
except ImportError:
    serialize = deserialize = None


class RedisCache(Cache):
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        expiration: Optional[int] = None,
        serializer: Optional[Callable[[Any], str]] = None,
        deserializer: Optional[Callable[[Optional[str]], Any]] = None,
        **kwargs,
    ) -> None:
        super().__init__()
        if Redis is None:
            raise CachingError('Package "redis" is not installed')
        if serialize is None:
            if serializer is None or deserializer is None:
                raise CachingError(
                    'Package "yasoo" is not installed and no de/serializer passed'
                )
        if serializer is not None:
            self._serialize = serializer
        if deserializer is not None:
            self._deserialize = deserializer
        self._redis = Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True,
            **kwargs,
        )
        self._expiration = expiration

    def get(self, key):
        try:
            return self._deserialize(self._redis.get(self._serialize(key)))
        except Exception as e:
            raise CachingError(f"Could not get {key}", exc=e)

    def set(self, key, value):
        try:
            self._redis.set(
                self._serialize(key), self._serialize(value), px=self._expiration
            )
        except Exception as e:
            raise CachingError(f"Could not set f{key} as {value}", exc=e)

    def _serialize(self, obj) -> str:
        return json.dumps(serialize(obj, preserve_iterable_types=True))

    def _deserialize(self, data: Optional[str]):
        if data is None:
            return data
        return deserialize(json.loads(data))
