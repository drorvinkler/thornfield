from typing import Callable, AnyStr, Optional
from zlib import compress as default_compress, decompress as default_decompress

from .cache import Cache


class CacheCompressionDecorator(Cache):
    def __init__(
        self,
        dal: Cache,
        compress: Optional[Callable[[str], AnyStr]] = ...,
        decompress: Optional[Callable[[AnyStr], str]] = ...,
    ) -> None:
        super().__init__()
        self._dal = dal
        if compress is None:
            self._compress = self._noop
        elif compress is ...:
            self._compress = self._default_compress
        else:
            self._compress = compress

        if decompress is None:
            self._decompress = self._noop
        elif decompress is ...:
            self._decompress = self._default_decompress
        else:
            self._decompress = decompress

    def get(self, key):
        return self._decompress(self._dal.get(key))

    def set(self, key, value, expiration: int) -> None:
        self._dal.set(key, self._compress(value), expiration)

    @staticmethod
    def _noop(x):
        return x

    @staticmethod
    def _default_compress(obj: str) -> bytes:
        return default_compress(obj.encode("UTF-8"))

    @staticmethod
    def _default_decompress(data: bytes) -> str:
        return default_decompress(data).decode("UTF-8")