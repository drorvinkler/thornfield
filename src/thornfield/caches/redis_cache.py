from typing import Optional, AnyStr

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
        **kwargs,
    ) -> None:
        super().__init__()
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

    def get(self, key: str) -> AnyStr:
        try:
            value = self._redis.get(key)
            if value is None:
                return NOT_FOUND
            return value
        except Exception as e:
            raise CachingError(f"Could not get {key}", exc=e)

    def set(self, key: str, value: AnyStr, expiration: int) -> None:
        try:
            self._redis.set(key, value, px=expiration or None)
        except Exception as e:
            raise CachingError(f"Could not set {key} as {value}", exc=e)
