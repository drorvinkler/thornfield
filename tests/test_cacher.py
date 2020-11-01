import asyncio
import inspect
from functools import partial
from unittest import TestCase
from unittest.mock import create_autospec, MagicMock

from thornfield.cacher import Cacher
from thornfield.caches.cache import Cache
from thornfield.typing import Cached, NotCached


class TestCacher(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.cache = {}
        self.cacher = self._create_cacher(self.cache)

    def test_caching_decorator_for_function(self):
        @self.cacher.cached
        def bar(x):
            bar.call_count += 1
            return x

        bar.call_count = 0
        self.assertEqual(1, bar(1))
        self.assertEqual(5, bar(5))
        self.assertEqual(2, bar.call_count)
        self.assertEqual(1, bar(1))
        self.assertEqual(2, bar.call_count)

    def test_caching_decorator_for_instance_method(self):
        class Foo:
            call_count = 0

            @self.cacher.cached
            def bar(self, x):
                Foo.call_count += 1
                return x

        foo = Foo()
        self.assertEqual(1, foo.bar(1))
        self.assertEqual(5, foo.bar(5))
        self.assertEqual(2, Foo.call_count)
        self.assertEqual(1, foo.bar(1))
        self.assertEqual(2, Foo.call_count)
        self.assertTrue(all(len(k) == 1 for k in self.cache.keys()))

    def test_caching_decorator_for_static_method(self):
        class Foo:
            call_count = 0

            @staticmethod
            @self.cacher.cached
            def bar(x):
                Foo.call_count += 1
                return x

        foo = Foo()
        self.assertEqual(1, foo.bar(1))
        self.assertEqual(5, foo.bar(5))
        self.assertEqual(2, Foo.call_count)
        self.assertEqual(1, foo.bar(1))
        self.assertEqual(2, Foo.call_count)

    def test_caching_decorator_multiple_arguments(self):
        @self.cacher.cached
        def bar(x, y):
            bar.call_count += 1
            return x

        bar.call_count = 0
        self.assertEqual(1, bar(1, 1))
        self.assertEqual(1, bar(1, 5))
        self.assertEqual(5, bar(5, 1))
        self.assertEqual(3, bar.call_count)
        self.assertEqual(1, bar(1, 1))
        self.assertEqual(3, bar.call_count)

    def test_caching_decorator_async_function(self):
        @self.cacher.cached
        async def bar(x):
            bar.call_count += 1
            return x

        bar.call_count = 0
        self.assertTrue(inspect.iscoroutinefunction(bar))
        event_loop = asyncio.get_event_loop()
        self.assertEqual(1, event_loop.run_until_complete(bar(1)))
        self.assertEqual(5, event_loop.run_until_complete(bar(5)))
        self.assertEqual(2, bar.call_count)
        self.assertEqual(1, event_loop.run_until_complete(bar(1)))
        self.assertEqual(2, bar.call_count)

    def test_caching_decorator_cached_type_hint(self):
        @self.cacher.cached
        def bar(x: Cached, y):
            bar.call_count += 1
            return x

        bar.call_count = 0
        self.assertEqual(1, bar(1, 1))
        self.assertEqual(5, bar(5, 1))
        self.assertEqual(2, bar.call_count)
        self.assertEqual(1, bar(1, 5))
        self.assertEqual(2, bar.call_count)
        self.assertEqual(1, bar(1, 1))
        self.assertEqual(2, bar.call_count)

    def test_caching_decorator_not_cached_type_hint(self):
        @self.cacher.cached
        def bar(x, y: NotCached):
            bar.call_count += 1
            return x

        bar.call_count = 0
        self.assertEqual(1, bar(1, 1))
        self.assertEqual(5, bar(5, 1))
        self.assertEqual(2, bar.call_count)
        self.assertEqual(1, bar(1, 5))
        self.assertEqual(2, bar.call_count)
        self.assertEqual(1, bar(1, 1))
        self.assertEqual(2, bar.call_count)

    def test_caching_decorator_both_cached_and_not_cached_type_hints(self):
        @self.cacher.cached
        def bar(x: Cached, y: NotCached, z):
            bar.call_count += 1
            return x

        bar.call_count = 0
        self.assertEqual(1, bar(1, 1, 1))
        self.assertEqual(1, bar(1, 1, 5))
        self.assertEqual(2, bar.call_count)
        self.assertEqual(1, bar(1, 5, 1))
        self.assertEqual(2, bar.call_count)
        self.assertEqual(1, bar(1, 1, 1))
        self.assertEqual(2, bar.call_count)

    def test_caching_decorator_with_validator(self):
        @self.cacher.cached(validator=lambda x: bool(x))
        def bar(x):
            bar.call_count += 1
            return x

        bar.call_count = 0
        self.assertEqual(1, bar(1))
        self.assertEqual(0, bar(0))
        self.assertEqual(2, bar.call_count)
        self.assertEqual(0, bar(0))
        self.assertEqual(3, bar.call_count)

    def test_caching_decorator_with_custom_cache(self):
        data = {}

        class CustomCache(Cache):
            def get(self, key):
                return data.get(key)

            def set(self, key, value):
                data[key] = value

        @self.cacher.cached(CustomCache())
        def bar(x):
            bar.call_count += 1
            return x

        bar.call_count = 0
        self.assertEqual(1, bar(1))
        self.assertEqual(5, bar(5))
        self.assertEqual(2, bar.call_count)
        self.assertEqual(1, bar(1))
        self.assertEqual(2, bar.call_count)
        self.assertEqual({}, self.cache)
        self.assertNotEqual({}, data)

    @classmethod
    def _create_cacher(cls, cache: dict):
        get_func = cache.get
        set_func = lambda k, v: cache.update({k: v})

        def create_cache():
            cache = create_autospec(Cache)
            cache.get = MagicMock(wraps=get_func)
            cache.set = MagicMock(wraps=set_func)
            return cache

        return Cacher(create_cache)
