from builtins import __import__
from functools import partial
from importlib import reload
from typing import Type
from unittest import TestCase
from unittest.mock import patch

from thornfield.caches import cache
from thornfield.errors import CachingError


def mock_import(exclude):
    def inner(name, globals, locals, fromlist, level):
        if name == exclude:
            raise ModuleNotFoundError()
        return __import__(name, globals, locals, fromlist, level)

    return inner


def test_no_yasoo(test: TestCase,
                  cls: Type[cache.Cache],
                  should_raise: bool,
                  **kwargs):
    with patch('builtins.__import__', mock_import('yasoo')):
        reload(cache)
    if should_raise:
        cls(serializer=lambda x: '', deserializer=lambda x: '', **kwargs)
        test.assertRaises(CachingError, partial(cls, **kwargs))
    else:
        cls(**kwargs)
    reload(cache)
