from functools import partial, wraps
from inspect import getsource, iscoroutinefunction, getfullargspec
from typing import Optional, Callable, Any, Dict, List

from .caches.cache import Cache
from .errors import CachingError
from .typing import NotCached, Cached


class Cacher:
    def __init__(self, cache_impl: Optional[Callable[[Callable], Cache]]) -> None:
        super().__init__()
        self._cache_impl = cache_impl

    def cached(
        self,
        cache: Optional[Cache] = None,
        validator: Optional[Callable[[Any], bool]] = None,
    ):
        if callable(cache):
            return self._cached(cache, None, None)
        return partial(self._cached, cache=cache, validator=validator)

    def _cached(
        self,
        func: Callable,
        cache: Optional[Cache],
        validator: Optional[Callable[[Any], bool]],
    ):
        if cache is None and self._cache_impl is None:
            raise CachingError("No cache and no cache creator provided.")

        spec = getfullargspec(func)
        func_args = spec.args
        func_annotations = spec.annotations
        if spec.defaults:
            func_defaults = dict(zip(func_args[-len(spec.defaults) :], spec.defaults))
        else:
            func_defaults = {}

        def _x(*args, **kwargs):
            is_instance_func = args and hasattr(args[0], func.__name__)
            key = self._get_key(
                is_instance_func,
                func_args,
                args,
                kwargs,
                func_defaults,
                func_annotations,
            )
            if not hasattr(func, "_cache"):
                real_func = (
                    getattr(args[0], func.__name__) if is_instance_func else func
                )
                func.cache = cache if cache is not None else self._cache_impl(real_func)
            result = func.cache.get(key)
            if result is None:
                result = func(*args, **kwargs)
                if validator is None or validator(result):
                    func.cache.set(key, result)
            return result

        source = self._get_inner_code(getsource(_x), func)
        g = dict(globals())
        g.update(locals())
        exec(compile(source, "", "exec"), g, locals())
        return wraps(func)(locals()["x"])

    @classmethod
    def _get_inner_code(cls, base, func):
        source = base[base.index("(") :]
        if iscoroutinefunction(func):
            source = f"async def x {source}"
            i = source.index("func(")
            source = f"{source[:i]}await {source[i:]}"
        else:
            source = f"def x {source}"
        return source

    @classmethod
    def _get_key(
        cls,
        is_instance_func: bool,
        func_args: List[str],
        args: tuple,
        kwargs: dict,
        defaults: Dict[str, Any],
        annotations: Dict[str, Any],
    ) -> tuple:
        key_args = func_args[1:] if is_instance_func else func_args
        if any(a is NotCached for a in annotations.values()):
            key_args = filter(lambda a: annotations.get(a) is not NotCached, key_args)
        elif any(a is Cached for a in annotations.values()):
            key_args = filter(lambda a: annotations.get(a) is Cached, key_args)
        arg_values = dict(zip(func_args, args))
        arg_values.update(kwargs)
        return tuple(
            arg_values[a] if a in arg_values else defaults[a] for a in key_args
        )
