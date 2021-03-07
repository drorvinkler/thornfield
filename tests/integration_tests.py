from unittest import TestCase
from unittest.mock import create_autospec, MagicMock

from thornfield.cache_factories import PostgresqlCacheFactory
from thornfield.cacher import Cacher

try:
    from psycopg2.pool import SimpleConnectionPool
except ImportError:
    SimpleConnectionPool = None


class IntegrationTests(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.cursor = MagicMock()
        self.cursor.__enter__ = lambda x: x
        self.cursor.execute = MagicMock()
        self.cursor.fetchone = MagicMock()
        self.cursor.rowcount = 1
        self.connection = MagicMock()
        self.connection.cursor = MagicMock(return_value=self.cursor)
        self.connection.commit = MagicMock()
        self.pool = create_autospec(SimpleConnectionPool, instance=True)
        self.pool.getconn = MagicMock(return_value=self.connection)

    def test_delayed_postgres_connection(self):
        def noop(x):
            return x

        def create_pool():
            create_pool.created = True
            return self.pool

        create_pool.created = False
        cacher = Cacher(PostgresqlCacheFactory(create_pool, serializer=noop, deserializer=noop).create)

        @cacher.cached
        def foo():
            pass

        self.assertFalse(create_pool.created)
        foo()
        self.assertTrue(create_pool.created)
