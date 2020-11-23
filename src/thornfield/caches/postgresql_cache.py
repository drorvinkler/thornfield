from time import time
from typing import Optional, Any, Callable

from .cache import Cache
from ..constants import NOT_FOUND
from ..errors import CachingError
from ..postgresql_key_value_adapter import PostgresqlKeyValueAdapter

try:
    from psycopg2.pool import AbstractConnectionPool
except ImportError:
    AbstractConnectionPool = ""


class PostgresqlCache(Cache):
    def __init__(
        self,
        connection_pool: AbstractConnectionPool,
        table: str,
        serializer: Optional[Callable[[Any], str]] = None,
        deserializer: Optional[Callable[[Optional[str]], Any]] = None,
    ) -> None:
        super().__init__(serializer=serializer, deserializer=deserializer)
        self._adapter = PostgresqlKeyValueAdapter(connection_pool, table)

    def get(self, key):
        t = round(time() * 1000)
        try:
            value = self._adapter.get(self._serialize(key), t)
            if value is None:
                return NOT_FOUND
            return self._deserialize(value)
        except Exception as e:
            raise CachingError(f"Could not get {key}", exc=e)

    def set(self, key, value, expiration: int):
        if expiration:
            expiration = round(time() * 1000) + expiration
        try:
            self._adapter.set(self._serialize(key), self._serialize(value), expiration)
        except Exception as e:
            raise CachingError(f"Could not set {key} as {value}", exc=e)
