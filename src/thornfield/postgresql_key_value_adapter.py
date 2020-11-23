from typing import Callable, Optional

try:
    from psycopg2.pool import AbstractConnectionPool
except ImportError:
    AbstractConnectionPool = ""


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
        self._callback(self)

    def execute_query(self, query: str, fetch: bool, *params: str):
        with self._connection.cursor() as cursor:
            cursor.execute(query, params)
            if fetch:
                return cursor.fetchone()
        self._connection.commit()


class ConnectionPoolWrapper:
    def __init__(self, pool: AbstractConnectionPool) -> None:
        super().__init__()
        self._pool = pool

    def getconn(self) -> ConnectionWrapper:
        return ConnectionWrapper(self._pool.getconn(), self.putconn)

    def putconn(self, conn: ConnectionWrapper):
        self._pool.putconn(conn)


class PostgresqlKeyValueAdapter:
    def __init__(
        self,
        connection_pool: AbstractConnectionPool,
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

        self._create_table_if_not_exists()

    def set(self, key: str, value: str, ts: Optional[int] = None):
        if self._ts_col:
            assert ts is not None
        with self._pool.getconn() as connection:
            exists = connection.execute_query(
                f"select exists(select * from {self._table} where {self._key_col}=%s)",
                True,
                key,
            )
            if exists[0]:
                self._update(connection, key, value, ts)
            else:
                self._add(connection, key, value, ts)

    def get(self, key: str, max_ts: Optional[int] = None) -> Optional[str]:
        query = f"select {self._value_col} from {self._table} where {self._key_col}=%s"
        if max_ts is not None:
            assert self._ts_col
            query += f" and {self._ts_col}<{max_ts}"
        with self._pool.getconn() as connection:
            result = connection.execute_query(query, True, key)
        return result[0] if result else None

    def _create_table_if_not_exists(self):
        with self._pool.getconn() as connection:
            exists = connection.execute_query(
                f"select exists(select * from information_schema.tables where table_name=%s)",
                True,
                self._table,
            )
            if not exists[0]:
                structure = f"id serial primary key, {self._key_col} text unique, {self._value_col} text"
                if self._ts_col:
                    structure += f", {self._ts_col} bigint"
                connection.execute_query(
                    f"create table {self._table} ({structure})", False
                )

    def _update(self, connection, key, value, ts):
        values_str = f"{self._value_col}=%s"
        if self._ts_col:
            values_str += f", {self._ts_col}={ts}"
        connection.execute_query(
            f"update {self._table} set ({values_str}) where {self._key_col}=%s",
            False,
            value,
            key,
        )

    def _add(self, connection, key, value, ts):
        columns = f"{self._key_col}, {self._value_col}"
        values = "%s, %s"
        if self._ts_col:
            columns += f", {self._ts_col}"
            values += f", {ts}"
        connection.execute_query(
            f"insert into {self._table} ({columns}) values ({values})",
            False,
            key,
            value,
        )
