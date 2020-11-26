from types import FunctionType, MethodType
from typing import TypeVar, Type, Union

NormalCallable = Union[FunctionType, MethodType]
T = TypeVar("T")


class _Cached:
    def __getitem__(self, item: Type[T]) -> Type[T]:
        return self


class _NotCached:
    def __getitem__(self, item: Type[T]) -> Type[T]:
        return self


Cached = _Cached()
NotCached = _NotCached()
