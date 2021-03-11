from time import sleep
from unittest import TestCase

from thornfield.cacher import Cacher
from thornfield.caches.memory_cache import MemoryCache


class TestMemoryDAL(TestCase):
    def test_basic(self):
        cacher = Cacher(self._create_cache)

        @cacher.cached
        def foo(x):
            foo.call_count += 1
            return x

        foo.call_count = 0
        self.assertEqual(5, foo(5))
        self.assertEqual(1, foo.call_count)
        self.assertEqual(5, foo(5))
        self.assertEqual(1, foo.call_count)

    def test_expiration(self):
        cacher = Cacher(self._create_cache)

        @cacher.cached(expiration=100)
        def foo(x):
            foo.call_count += 1
            return x

        foo.call_count = 0
        self.assertEqual(5, foo(5))
        self.assertEqual(1, foo.call_count)
        self.assertEqual(5, foo(5))
        self.assertEqual(1, foo.call_count)
        sleep(0.11)
        self.assertEqual(5, foo(5))
        self.assertEqual(2, foo.call_count)

    def test_none(self):
        cacher = Cacher(self._create_cache)

        @cacher.cached
        def foo(_):
            foo.call_count += 1
            return None

        foo.call_count = 0
        self.assertEqual(None, foo(5))
        self.assertEqual(1, foo.call_count)
        self.assertEqual(None, foo(5))
        self.assertEqual(1, foo.call_count)

    @staticmethod
    def _create_cache(_):
        return MemoryCache()
