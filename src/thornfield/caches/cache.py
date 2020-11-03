from abc import ABC, abstractmethod


class Cache(ABC):
    @abstractmethod
    def get(self, key):
        pass

    @abstractmethod
    def set(self, key, value, expiration: float = 0, overwrite: bool = False):
        pass
