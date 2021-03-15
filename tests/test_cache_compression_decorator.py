from unittest import TestCase
from unittest.mock import create_autospec, MagicMock

from thornfield.caches.cache import Cache
from thornfield.caches.cache_compression_decorator import CacheCompressionDecorator
from thornfield.constants import NOT_FOUND


class TestCacheCompressionDecorator(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self._cache = create_autospec(Cache)

    def test_value_compressed_when_set(self):
        compress = MagicMock(return_value='x')
        self._cache.set = MagicMock()
        CacheCompressionDecorator(self._cache, compress=compress).set(1, 2, 0)
        compress.assert_called_once_with(2)
        self._cache.set.assert_called_once_with(1, 'x', 0)

    def test_value_decompressed_when_get(self):
        decompress = MagicMock(return_value='x')
        self._cache.get = MagicMock(return_value=2)
        result = CacheCompressionDecorator(self._cache, decompress=decompress).get(1)
        decompress.assert_called_once_with(2)
        self.assertEqual(decompress.return_value, result)

    def test_no_action_when_init_with_none(self):
        self._cache.get = MagicMock(return_value=2)
        self._cache.set = MagicMock()

        decorator = CacheCompressionDecorator(self._cache, None, None)
        decorator.set(1, 2, 0)
        self._cache.set.assert_called_once_with(1, 2, 0)

        result = decorator.get(1)
        self.assertEqual(self._cache.get.return_value, result)

    def test_not_found(self):
        self._cache.get = MagicMock(return_value=NOT_FOUND)
        decompress = MagicMock()

        result = CacheCompressionDecorator(self._cache, decompress=decompress).get(1)
        decompress.assert_not_called()
        self.assertIs(NOT_FOUND, result)
