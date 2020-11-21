from time import time
from typing import Optional, Any, Callable

from .cache import Cache
from ..constants import NOT_FOUND

try:
    from psycopg2.pool import AbstractConnectionPool
except ImportError:
    AbstractConnectionPool = ""


class ConnectionWrapper:
    def __init__(
        self, connection, release_callback: Callable[["ConnectionWrapper"], None]
    ) -> None:
        super().__init__()
        self.connection = connection
        self._callback = release_callback

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._callback(self)


class ConnectionPoolWrapper:
    def __init__(self, pool: AbstractConnectionPool) -> None:
        super().__init__()
        self._pool = pool

    def getconn(self) -> ConnectionWrapper:
        return ConnectionWrapper(self._pool.getconn(), self.putconn)

    def putconn(self, conn: ConnectionWrapper):
        self._pool.putconn(conn)


class PostgresqlCache(Cache):
    def __init__(
        self,
        connection_pool: ConnectionPoolWrapper,
        table: str,
        serializer: Optional[Callable[[Any], str]] = None,
        deserializer: Optional[Callable[[Optional[str]], Any]] = None,
    ) -> None:
        super().__init__(serializer=serializer, deserializer=deserializer)
        self._connection_pool = connection_pool
        self._table = table
        exists = self._execute_query(
            f"select exists(select * from information_schema.tables where table_name=%s)",
            True,
            self._table,
        )
        if not exists[0]:
            self._execute_query(
                f"create table {self._table} (id serial primary key, key text unique, value text, ts bigint)",
                False,
            )

    def get(self, key):
        t = round(time() * 1000)
        result = self._execute_query(
            f"select value from {self._table} where key=%s and (ts=0 or ts<{t})",
            True,
            self._serialize(key),
        )
        if not result:
            return NOT_FOUND
        return self._deserialize(result[0])

    def set(self, key, value, expiration: int):
        if expiration:
            expiration = round(time() * 1000) + expiration
        s_key = self._serialize(key)
        exists = self._execute_query(
            f"select count(*) from {self._table} where key=%s", True, s_key
        )
        if exists[0]:
            self._execute_query(
                f"update {self._table} set (value=%s, ts={expiration}) where key=%s",
                False,
                self._serialize(value),
                s_key,
            )
        else:
            self._execute_query(
                f"insert into {self._table} (key, value, ts) values (%s, %s, {expiration})",
                False,
                s_key,
                self._serialize(value),
            )

    def _execute_query(self, query: str, fetch: bool, *params: str):
        with self._connection_pool.getconn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                if fetch:
                    return cursor.fetchone()
            conn.commit()
