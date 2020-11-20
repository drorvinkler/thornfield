from typing import Iterable


def not_none(result):
    if isinstance(result, Iterable):
        return all(i is not None for i in result)
    return result is not None
