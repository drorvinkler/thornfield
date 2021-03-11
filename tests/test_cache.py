from time import time
from unittest import TestCase

from thornfield.caches.cache import Cache
from thornfield.caches.volatile_value import VolatileValue


class TestCache(TestCase):
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
