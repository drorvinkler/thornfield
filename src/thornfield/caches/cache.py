from abc import ABC, abstractmethod
from time import time

from .volatile_value import VolatileValue
from ..constants import NOT_FOUND


class Cache(ABC):
    @abstractmethod
    def get(self, key):
        pass

    @abstractmethod
    def set(self, key, value, expiration: int) -> None:
        pass

    @staticmethod
    def _to_volatile(value, expiration: int) -> VolatileValue:
        return VolatileValue(value, round(time() * 1000) + expiration)

    @staticmethod
    def _from_volatile(value: VolatileValue):
        if time() * 1000 <= value.expiration:
            return value.value
        return NOT_FOUND
