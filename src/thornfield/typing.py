from typing import TypeVar, Type

T = TypeVar('T')


class _Cached:
    def __getitem__(self, item: Type[T]) -> Type[T]:
        return TypeVar(f'Cached[{item.__name__}]', item, item)


class _NotCached:
    def __getitem__(self, item: Type[T]) -> Type[T]:
        return TypeVar(f'NotCached[{item.__name__}]', item, item)


Cached = _Cached()
NotCached = _NotCached()
