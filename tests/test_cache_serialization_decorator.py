from functools import partial
from importlib import reload
from unittest import TestCase
from unittest.mock import create_autospec, MagicMock, patch

from tests.utils import mock_import
from thornfield.caches import cache_serialization_decorator
from thornfield.caches.cache import Cache
from thornfield.caches.cache_serialization_decorator import CacheSerializationDecorator
from thornfield.errors import CachingError


class TestCacheSerializationDecorator(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self._dal = create_autospec(Cache)

    def test_no_yasoo_raises_error_only_on_instantiation_if_no_searializer(self):
        with patch('builtins.__import__', mock_import('yasoo')):
            reload(cache_serialization_decorator)
        cache_serialization_decorator.CacheSerializationDecorator(self._dal, serializer=lambda x: '', deserializer=lambda x: '')
        self.assertRaises(CachingError, partial(cache_serialization_decorator.CacheSerializationDecorator, self._dal))
        reload(cache_serialization_decorator)

    def test_key_and_value_serialized_when_set(self):
        serialize = MagicMock(return_value='x')
        self._dal.set = MagicMock()

        CacheSerializationDecorator(self._dal, serializer=serialize).set(1, 2, 0)
        serialize.assert_any_call(1)
        serialize.assert_any_call(2)
        self._dal.set.assert_called_once_with('x', 'x', 0)

    def test_key_serialized_and_value_deserialized_when_get(self):
        serialize = MagicMock(return_value='x')
        deserialize = MagicMock(return_value='y')
        self._dal.get = MagicMock(return_value=2)

        result = CacheSerializationDecorator(self._dal, serializer=serialize, deserializer=deserialize).get(1)
        serialize.assert_called_once_with(1)
        self._dal.get.assert_called_once_with(serialize.return_value)
        deserialize.assert_called_once_with(2)
        self.assertEqual(deserialize.return_value, result)

    def test_no_action_when_init_with_none(self):
        self._dal.set = MagicMock()
        self._dal.get = MagicMock(return_value=2)

        decorator = CacheSerializationDecorator(self._dal, None, None)
        decorator.set(1, 2, 0)
        self._dal.set.assert_called_once_with(1, 2, 0)

        result = decorator.get(1)
        self._dal.get.assert_called_once_with(1)
        self.assertEqual(self._dal.get.return_value, result)
