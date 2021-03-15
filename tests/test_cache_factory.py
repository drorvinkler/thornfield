from types import MethodType, FunctionType
from typing import Union
from unittest import TestCase
from unittest.mock import create_autospec, MagicMock

from thornfield.cache_factories.cache_factory import CacheFactory
from thornfield.caches.cache import Cache


def func():
    pass


class Foo:
    def func(self):
        pass


class TestCacheFactory(TestCase):
    def test_function_key(self):
        self.assertTrue(CacheFactory._func_to_key(func).endswith('test_cache_factory.func'))

    def test_unbound_method_key(self):
        self.assertTrue(CacheFactory._func_to_key(Foo.func).endswith('test_cache_factory.Foo.func'))

    def test_bound_method_key(self):
        self.assertTrue(CacheFactory._func_to_key(Foo().func).endswith('test_cache_factory.Foo.func'))

    def test_decoration(self):
        cache = create_autospec(Cache)

        class MockFactory(CacheFactory):
            def _create(self, func: Union[MethodType, FunctionType]) -> Cache:
                return cache

        decorator = MagicMock(return_value='y')
        result = MockFactory(decorator).create(func)
        decorator.assert_called_once_with(cache)
        self.assertEqual(decorator.return_value, result)
