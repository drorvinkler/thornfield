from unittest import TestCase
from unittest.mock import create_autospec, MagicMock

from thornfield.caches.cache import Cache
from thornfield.caches.cache_compression_decorator import CacheCompressionDecorator


class TestCacheCompressionDecorator(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self._dal = create_autospec(Cache)

    def test_value_compressed_when_set(self):
        compress = MagicMock(return_value='x')
        self._dal.set = MagicMock()
        CacheCompressionDecorator(self._dal, compress=compress).set(1, 2, 0)
        compress.assert_called_once_with(2)
        self._dal.set.assert_called_once_with(1, 'x', 0)

    def test_value_decompressed_when_get(self):
        decompress = MagicMock(return_value='x')
        self._dal.get = MagicMock(return_value=2)
        result = CacheCompressionDecorator(self._dal, decompress=decompress).get(1)
        decompress.assert_called_once_with(2)
        self.assertEqual(decompress.return_value, result)

    def test_no_action_when_init_with_none(self):
        self._dal.get = MagicMock(return_value=2)
        self._dal.set = MagicMock()

        decorator = CacheCompressionDecorator(self._dal, None, None)
        decorator.set(1, 2, 0)
        self._dal.set.assert_called_once_with(1, 2, 0)

        result = decorator.get(1)
        self.assertEqual(self._dal.get.return_value, result)
