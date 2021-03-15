from functools import partial
from importlib import reload
from unittest import TestCase
from unittest.mock import create_autospec, MagicMock, patch

from tests.utils import mock_import
from thornfield.caches import cache_serialization_decorator
from thornfield.caches.cache import Cache
from thornfield.caches.cache_serialization_decorator import CacheSerializationDecorator
from thornfield.constants import NOT_FOUND
from thornfield.errors import CachingError


class TestCacheSerializationDecorator(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self._cache = create_autospec(Cache)

    def test_no_yasoo_raises_error_only_on_instantiation_if_no_searializer(self):
        with patch('builtins.__import__', mock_import('yasoo')):
            reload(cache_serialization_decorator)
        cache_serialization_decorator.CacheSerializationDecorator(self._cache, serializer=lambda x: '', deserializer=lambda x: '')
        self.assertRaises(CachingError, partial(cache_serialization_decorator.CacheSerializationDecorator, self._cache))
        reload(cache_serialization_decorator)

    def test_key_and_value_serialized_when_set(self):
        serialize = MagicMock(return_value='x')
        self._cache.set = MagicMock()

        CacheSerializationDecorator(self._cache, serializer=serialize).set(1, 2, 0)
        serialize.assert_any_call(1)
        serialize.assert_any_call(2)
        self._cache.set.assert_called_once_with('x', 'x', 0)

    def test_key_serialized_and_value_deserialized_when_get(self):
        serialize = MagicMock(return_value='x')
        deserialize = MagicMock(return_value='y')
        self._cache.get = MagicMock(return_value=2)

        result = CacheSerializationDecorator(self._cache, serializer=serialize, deserializer=deserialize).get(1)
        serialize.assert_called_once_with(1)
        self._cache.get.assert_called_once_with(serialize.return_value)
        deserialize.assert_called_once_with(2)
        self.assertEqual(deserialize.return_value, result)

    def test_no_action_when_init_with_none(self):
        self._cache.set = MagicMock()
        self._cache.get = MagicMock(return_value=2)

        decorator = CacheSerializationDecorator(self._cache, None, None)
        decorator.set(1, 2, 0)
        self._cache.set.assert_called_once_with(1, 2, 0)

        result = decorator.get(1)
        self._cache.get.assert_called_once_with(1)
        self.assertEqual(self._cache.get.return_value, result)

    def test_not_found(self):
        serialize = MagicMock()
        deserialize = MagicMock()
        self._cache.get = MagicMock(return_value=NOT_FOUND)

        result = CacheSerializationDecorator(self._cache, serializer=serialize, deserializer=deserialize).get(1)
        deserialize.assert_not_called()
        self.assertIs(NOT_FOUND, result)
