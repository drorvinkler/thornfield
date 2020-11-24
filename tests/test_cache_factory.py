from unittest import TestCase

from thornfield.cache_factories.cache_factory import CacheFactory


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
