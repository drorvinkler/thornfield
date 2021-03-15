from abc import ABC, abstractmethod
from types import MethodType, FunctionType
from typing import Union, Optional, Callable

from ..caches.cache import Cache


class CacheFactory(ABC):
    def __init__(self, decorator: Optional[Callable[[Cache], Cache]]) -> None:
        super().__init__()
        self._decorator = decorator

    def create(self, func: Union[MethodType, FunctionType]) -> Cache:
        result = self._create(func)
        if self._decorator is not None:
            result = self._decorator(result)
        return result

    @abstractmethod
    def _create(self, func: Union[MethodType, FunctionType]) -> Cache:
        pass

    @staticmethod
    def _func_to_key(func: Union[MethodType, FunctionType]) -> str:
        return f"{func.__module__}.{func.__qualname__}"
