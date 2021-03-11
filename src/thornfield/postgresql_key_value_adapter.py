from enum import Enum, auto
from typing import Callable, Optional, List, Union, AnyStr

try:
    from psycopg2.pool import AbstractConnectionPool
except ImportError:
    AbstractConnectionPool = None

ConnectionPool = Union[AbstractConnectionPool, Callable[[], AbstractConnectionPool]]


class FetchAmount(Enum):
    ONE = auto()
    ALL = auto()
    ZERO = auto()


class ConnectionWrapper:
    def __init__(
        self, connection, release_callback: Callable[["ConnectionWrapper"], None]
    ) -> None:
        super().__init__()
        self._connection = connection
        self._callback = release_callback

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._callback(self._connection)

    def execute_query(self, query: str, fetch: FetchAmount, *params: str):
        with self._connection.cursor() as cursor:
            cursor.execute(query, params)
            if fetch is FetchAmount.ONE:
                return cursor.fetchone() if cursor.rowcount > 0 else None
            elif fetch is FetchAmount.ALL:
                return cursor.fetchall()
        self._connection.commit()


class ConnectionPoolWrapper:
    def __init__(self, pool: ConnectionPool) -> None:
        super().__init__()
        self._raw_pool = pool

    @property
    def _pool(self):
        if isinstance(self._raw_pool, Callable):
            self._raw_pool = self._raw_pool()
        return self._raw_pool

    def getconn(self) -> ConnectionWrapper:
        return ConnectionWrapper(self._pool.getconn(), self.putconn)

    def putconn(self, conn):
        self._pool.putconn(conn)


class PostgresqlKeyValueAdapter:
    def __init__(
        self,
        connection_pool: ConnectionPool,
        table_name: str,
        key_col: str = "key",
        value_col: str = "value",
        ts_col: Optional[str] = "ts",
    ) -> None:
        super().__init__()
        self._pool = ConnectionPoolWrapper(connection_pool)
        self._table = table_name
        self._key_col = key_col
        self._value_col = value_col
        self._ts_col = ts_col
        with self._pool.getconn() as connection:
            self._table_exists = self._exists(
                connection, "information_schema.tables", "table_name", self._table
            )

    def set(self, key: str, value: AnyStr, ts: Optional[int] = None):
        if self._ts_col:
            assert ts is not None
        if not self._table_exists:
            self._create_table_if_not_exists(isinstance(value, bytes))
            self._table_exists = True
        with self._pool.getconn() as connection:
            if self._exists(connection, self._table, self._key_col, key):
                self._update(connection, key, value, ts)
            else:
                self._add(connection, key, value, ts)

    def get(self, key: str, min_ts: Optional[int] = None) -> Optional[AnyStr]:
        if not self._table_exists:
            return None

        query = f"select {self._value_col} from {self._table} where {self._key_col}=%s"
        if min_ts is not None:
            assert self._ts_col
            query += f" and ({self._ts_col}>{min_ts} or {self._ts_col}=0)"
        with self._pool.getconn() as connection:
            result = connection.execute_query(query, FetchAmount.ONE, key)
        return result[0] if result else None

    def keys(self) -> List[str]:
        if not self._table_exists:
            return []

        with self._pool.getconn() as connection:
            result = connection.execute_query(
                f"select {self._key_col} from {self._table}", FetchAmount.ALL
            )
        return [t[0] for t in result]

    def _create_table_if_not_exists(self, binary: bool):
        with self._pool.getconn() as connection:
            exists = self._exists(
                connection, "information_schema.tables", "table_name", self._table
            )
            if exists:
                return
            value_col_type = "bytes" if binary else "text"
            structure = f"id serial primary key, {self._key_col} text unique, {self._value_col} {value_col_type}"
            if self._ts_col:
                structure += f", {self._ts_col} bigint"
            connection.execute_query(
                f"create table {self._table} ({structure})", FetchAmount.ZERO
            )

    def _update(self, connection: ConnectionWrapper, key, value, ts):
        values_str = f"{self._value_col}=%s"
        if self._ts_col:
            values_str += f", {self._ts_col}={ts}"
        connection.execute_query(
            f"update {self._table} set {values_str} where {self._key_col}=%s",
            FetchAmount.ZERO,
            value,
            key,
        )

    def _add(self, connection: ConnectionWrapper, key, value, ts):
        columns = f"{self._key_col}, {self._value_col}"
        values = "%s, %s"
        if self._ts_col:
            columns += f", {self._ts_col}"
            values += f", {ts}"
        connection.execute_query(
            f"insert into {self._table} ({columns}) values ({values})",
            FetchAmount.ZERO,
            key,
            value,
        )

    @classmethod
    def _exists(
        cls, connection: ConnectionWrapper, table: str, column: str, value: str
    ) -> bool:
        exists = connection.execute_query(
            f"select exists(select * from {table} where {column}=%s)",
            FetchAmount.ONE,
            value,
        )
        return exists[0]
