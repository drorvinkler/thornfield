import re
from types import MethodType, FunctionType
from typing import Union, Optional, Callable, Any

from psycopg2.pool import AbstractConnectionPool

from .cache_factory import CacheFactory
from ..caches.postgresql_cache import PostgresqlCache
from ..postgresql_key_value_adapter import PostgresqlKeyValueAdapter


class PostgresqlCacheFactory(CacheFactory):
    def __init__(
        self,
        connection_pool: AbstractConnectionPool,
        serializer: Optional[Callable[[Any], str]] = None,
        deserializer: Optional[Callable[[Optional[str]], Any]] = None,
        index_table: str = "_index",
    ) -> None:
        super().__init__()
        self.connection_pool = connection_pool
        self.serializer = serializer
        self.deserializer = deserializer
        self._adapter = PostgresqlKeyValueAdapter(
            self.connection_pool, index_table, ts_col=None
        )

    def create(self, func: Union[MethodType, FunctionType]) -> PostgresqlCache:
        table = self._normalize_table_name(self._func_to_key(func))
        return PostgresqlCache(
            connection_pool=self.connection_pool,
            table=table,
            serializer=self.serializer,
            deserializer=self.deserializer,
        )

    def _normalize_table_name(self, table_name: str) -> str:
        if len(table_name) > 63:
            n = self._adapter.get(table_name)
            if not n:
                used = {int(self._adapter.get(k)) for k in self._adapter.keys()}
                n = min({i + 1 for i in range(len(used) + 1)}.difference(used))
                self._adapter.set(table_name, str(n))
            return f"cache_{n}"
        else:
            return re.sub(r"[^\w]", "_", table_name)
