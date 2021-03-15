import re
from types import MethodType, FunctionType
from typing import Union, Optional, Callable

from .cache_factory import CacheFactory
from ..caches.cache import Cache
from ..caches.postgresql_cache import PostgresqlCache
from ..postgresql_key_value_adapter import (
    PostgresqlKeyValueAdapter,
    ConnectionPool,
)


class PostgresqlCacheFactory(CacheFactory):
    def __init__(
        self,
        connection_pool: ConnectionPool,
        index_table: str = "_index",
        decorator: Optional[Callable[[Cache], Cache]] = None,
    ) -> None:
        super().__init__(decorator)
        self.connection_pool = connection_pool
        self.index_table = index_table
        self._pkv_adapter = None

    @property
    def _adapter(self) -> PostgresqlKeyValueAdapter:
        if self._pkv_adapter is None:
            self._pkv_adapter = PostgresqlKeyValueAdapter(
                self.connection_pool, self.index_table, ts_col=None
            )
        return self._pkv_adapter

    def _create(self, func: Union[MethodType, FunctionType]) -> PostgresqlCache:
        table = self._normalize_table_name(self._func_to_key(func))
        return PostgresqlCache(connection_pool=self.connection_pool, table=table)

    def _normalize_table_name(self, table_name: str) -> str:
        table_name = table_name.lower()
        if len(table_name) > 63:
            n = self._adapter.get(table_name)
            if not n:
                used = {int(self._adapter.get(k)) for k in self._adapter.keys()}
                n = min({i + 1 for i in range(len(used) + 1)}.difference(used))
                self._adapter.set(table_name, str(n))
            return f"cache_{n}"
        else:
            return re.sub(r"[^\w]", "_", table_name)
