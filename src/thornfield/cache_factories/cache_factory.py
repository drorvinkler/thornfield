from abc import ABC, abstractmethod
from types import MethodType, FunctionType
from typing import Union

from thornfield.caches.cache import Cache


class CacheFactory(ABC):
    @abstractmethod
    def create(self, func: Union[MethodType, FunctionType]) -> Cache:
        pass

    @staticmethod
    def _func_to_key(func: Union[MethodType, FunctionType]) -> str:
        return f"{func.__module__}.{func.__qualname__}"
