# `thornfield`: Advanced caching in python

[![Build Status](https://travis-ci.com/drorvinkler/thornfield.svg?branch=main)](https://travis-ci.com/drorvinkler/thornfield)
[![codecov](https://codecov.io/gh/drorvinkler/thornfield/branch/main/graph/badge.svg)](https://codecov.io/gh/drorvinkler/thornfield)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## Installation
```pip install thornfield```

## Usage
Choose the cache storage you want to use - in-memory, redis and postgresql are currently implemented.
You can use a different storage by implementing the `Cache` interface.

Then, use the `cached` decorator to annotate the function being cached:
```
cacher = Cacher(cache_factory_func)

@cacher.cached
def foo():
    ...
```

The decorator supports:
* Setting an expiration time for the cached values.
* Caching only values that match a constraint (e.g. not `None`).
* Using only some of the function parameters as keys for the cache.
* Caching async functions.

#### Caching only some parameters
In case you don't want to use all the parameters of the function as cache key,
you can the `Cached` or `NotCached` types:
```
from thornfield.typing import Cached, NotCached

@cached
def request(url: str, token: str, timeout: NotCached[int]):
    ...


@cached
async def request_async(url: Cached[str], timeout: int, callback):
    ...
```

#### Caching abstract methods
In order to avoid adding the same decorator to all implementations of an
abstract method, you can use `cache_method` as follows:
```
class Base(ABC):
    def __init__(self):
        cacher.cache_method(do_something)

    @abstractmethod
    def do_something(self):
        pass
```

## Cache Factories
In the `cache_factories` package you can find cache factories for Redis and PostgreSQL.
They both cache each function to a different table (in PostgreSQL, db in Redis).

Their `create` method can be passed as `cache_impl` to the constructor of `Cacher`.
