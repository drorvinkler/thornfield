import json
from typing import Optional, Callable, Any

from .cache import Cache
from ..constants import NOT_FOUND
from ..errors import CachingError

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
        self._serialize = self._default_serialize if serializer is None else serializer
        self._deserialize = (
            self._default_deserialize if deserializer is None else deserializer
        )
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
            value = self._redis.get(self._serialize(key))
            if value is None:
                return NOT_FOUND
            return self._deserialize(value)
        except Exception as e:
            raise CachingError(f"Could not get {key}", exc=e)

    def set(self, key, value):
        try:
            self._redis.set(
                self._serialize(key), self._serialize(value), px=self._expiration
            )
        except Exception as e:
            raise CachingError(f"Could not set f{key} as {value}", exc=e)

    @staticmethod
    def _default_serialize(obj) -> str:
        return json.dumps(serialize(obj, preserve_iterable_types=True))

    @staticmethod
    def _default_deserialize(data: Optional[str]):
        if data is None:
            return data
        return deserialize(json.loads(data))
