from unittest import TestCase
from unittest.mock import create_autospec, MagicMock

from tests.utils import test_no_yasoo
from thornfield.cacher import Cacher
from thornfield.caches.postgresql_cache import PostgresqlCache
from thornfield.postgresql_key_value_adapter import ConnectionPoolWrapper


class TestPostgresqlCache(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.cursor = MagicMock()
        self.cursor.__enter__ = lambda x: x
        self.cursor.execute = MagicMock()
        self.cursor.fetchone = MagicMock()
        self.connection = MagicMock()
        self.connection.cursor = MagicMock(return_value=self.cursor)
        self.connection.commit = MagicMock()
        self.pool = create_autospec(ConnectionPoolWrapper)
        self.pool.getconn = MagicMock(return_value=self.connection)

    def test_no_yasoo_raises_error_only_on_instantiation_if_no_searializer(self):
        test_no_yasoo(self, PostgresqlCache, True, connection_pool=self.pool, table='')

    def test_key_and_value_serialized_when_set(self):
        serialize = MagicMock(ret_value='x')
        PostgresqlCache(self.pool, '', serializer=serialize).set(1, 2, 0)
        serialize.assert_any_call(1)
        serialize.assert_any_call(2)

    def test_key_serialized_and_value_deserialized_when_get(self):
        serialize = MagicMock(ret_value='x')
        deserialize = MagicMock(ret_value='x')
        self.cursor.fetchone = MagicMock(return_value=[2])
        PostgresqlCache(self.pool, '', serializer=serialize, deserializer=deserialize).get(1)
        serialize.assert_called_once_with(1)
        deserialize.assert_called_once_with(2)

    def test_none_caching(self):
        import json
        serialize = json.dumps
        deserialize = json.loads
        self.cursor.fetchone = MagicMock(return_value=['null'])
        cacher = Cacher(lambda x: PostgresqlCache(self.pool, '', serializer=serialize, deserializer=deserialize))

        @cacher.cached()
        def foo():
            foo.call_count += 1
            return 5

        foo.call_count = 0
        self.assertIsNone(foo())
        self.assertEqual(0, foo.call_count)
