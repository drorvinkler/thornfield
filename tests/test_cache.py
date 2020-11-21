from time import time
from typing import Optional, Callable, Any
from unittest import TestCase

from thornfield.caches.cache import Cache
from thornfield.caches.volatile_value import VolatileValue


class TestCache(TestCase):
    def test_supplied_serializer_overrides_default_one(self):
        class MockCache(Cache):
            def __init__(self, serializer: Optional[Callable[[Any], str]] = None,
                         deserializer: Optional[Callable[[Optional[str]], Any]] = None) -> None:
                super().__init__(serializer, deserializer, True)

            def get(self, key):
                pass

            def set(self, key, value, expiration: int):
                pass

        def foo(x):
            return x

        cache = MockCache(serializer=foo, deserializer=foo)
        self.assertEqual(cache._serialize, foo)
        self.assertEqual(cache._deserialize, foo)

    def test_volatile_conversion(self):
        original_value = 5
        expiration = 1000
        before = round(time() * 1000)
        value = Cache._to_volatile(original_value, expiration=expiration)
        after = round(time() * 1000)
        self.assertIsInstance(value, VolatileValue)
        self.assertEqual(original_value, value.value)
        self.assertGreaterEqual(value.expiration, before + expiration)
        self.assertLessEqual(value.expiration, after + expiration)
