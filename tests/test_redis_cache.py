import builtins
from importlib import reload
from unittest import TestCase
from unittest.mock import patch, create_autospec, MagicMock

from redis import Redis

from tests.utils import mock_import, test_no_yasoo
from thornfield.cacher import Cacher
from thornfield.caches import redis_cache
from thornfield.errors import CachingError

_real_import = builtins.__import__


class TestRedisCache(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.redis_impl = create_autospec(Redis)
        redis_cache.Redis = lambda *args, **kwargs: self.redis_impl

    def test_no_redis_raises_error_only_on_instantiation(self):
        with patch('builtins.__import__', mock_import('redis')):
            reload(redis_cache)
        self.assertRaises(CachingError, redis_cache.RedisCache)
        reload(redis_cache)

    def test_no_yasoo_raises_error_only_on_instantiation_if_no_searializer(self):
        test_no_yasoo(self, redis_cache.RedisCache, True)

    def test_key_and_value_serialized_when_set(self):
        serialize = MagicMock(ret_value='x')
        self.redis_impl.set = MagicMock()
        redis_cache.RedisCache(serializer=serialize).set(1, 2, 0)
        serialize.assert_any_call(1)
        serialize.assert_any_call(2)

    def test_key_serialized_and_value_deserialized_when_get(self):
        serialize = MagicMock(ret_value='x')
        deserialize = MagicMock(ret_value='x')
        self.redis_impl.set = MagicMock()
        self.redis_impl.get = MagicMock(return_value=2)
        redis_cache.RedisCache(serializer=serialize, deserializer=deserialize).get(1)
        serialize.assert_called_once_with(1)
        deserialize.assert_called_once_with(2)

    def test_none_caching(self):
        import json
        serialize = json.dumps
        deserialize = json.loads
        self.redis_impl.set = MagicMock()
        self.redis_impl.get = MagicMock(return_value='null')
        cacher = Cacher(lambda x: redis_cache.RedisCache(serializer=serialize, deserializer=deserialize))

        @cacher.cached()
        def foo():
            foo.call_count += 1
            return 5

        foo.call_count = 0
        self.assertIsNone(foo())
        self.assertEqual(0, foo.call_count)

        self.redis_impl.get = MagicMock(return_value=None)
        self.assertEqual(5, foo())
        self.assertEqual(1, foo.call_count)
