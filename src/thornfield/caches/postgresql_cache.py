from time import time
from typing import AnyStr

from .cache import Cache
from ..constants import NOT_FOUND
from ..errors import CachingError
from ..postgresql_key_value_adapter import (
    PostgresqlKeyValueAdapter,
    ConnectionPool,
)


class PostgresqlCache(Cache):
    def __init__(self, connection_pool: ConnectionPool, table: str) -> None:
        super().__init__()
        self._adapter = PostgresqlKeyValueAdapter(connection_pool, table)

    def get(self, key: str):
        t = self._get_curr_time()
        try:
            value = self._adapter.get(key, t)
            if value is None:
                return NOT_FOUND
            return value
        except Exception as e:
            raise CachingError(f"Could not get {key}", exc=e)

    def set(self, key: str, value: AnyStr, expiration: int) -> None:
        if expiration:
            expiration = self._get_curr_time() + expiration
        try:
            self._adapter.set(key, value, expiration)
        except Exception as e:
            raise CachingError(f"Could not set {key} as {value}", exc=e)

    @staticmethod
    def _get_curr_time():
        return round(time() * 1000)
