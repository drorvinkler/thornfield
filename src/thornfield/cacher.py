from functools import partial, wraps
from inspect import getsource, iscoroutinefunction, getfullargspec
from types import MethodType
from typing import Optional, Callable, Any, Dict, List, cast

from thornfield.caches.cache import Cache
from .caching_data import CachingData
from .constants import NOT_FOUND
from .errors import CachingError
from .typing import NotCached, Cached, NormalCallable

_CACHE_ATTR = "cache"
_CACHING_DATA_ATTR = "caching_data"


class Cacher:
    def __init__(self, cache_impl: Optional[Callable[[NormalCallable], Cache]]) -> None:
        """

        :param cache_impl: An optional factory function that gets
            the cached method and returns an implementation of ``Cache``.
        """
        super().__init__()
        self._cache_impl = cache_impl

    def cached(
        self,
        cache: Optional[Cache] = None,
        validator: Optional[Callable[[Any], bool]] = None,
        expiration: int = 0,
    ):
        """
        :param cache: The ``Cache`` to use. If ``None``, ``self.cache_impl`` is called to create one.
        :param validator: A ``callable`` that will be called on the return value of the cached function.
            The value will be cached only if ``validator`` returns ``True``.
        :param expiration: Expiration time for each key, in milliseconds.
        """
        if isinstance(cache, Callable):
            return self._cached(cast(NormalCallable, cache), None, None, 0)
        return partial(
            self._cached, cache=cache, validator=validator, expiration=expiration
        )

    def cache_method(
        self,
        method: MethodType,
        cache: Optional[Cache] = None,
        validator: Optional[Callable[[Any], bool]] = None,
        expiration: int = 0,
        use_base_method: bool = True,
    ):
        """

        :param method: The method to cache.
        :param cache: The ``Cache`` to use. If ``None``, ``self.cache_impl`` is called to create one.
        :param validator: A ``callable`` that will be called on the return value of the cached function.
            The value will be cached only if ``validator`` returns ``True``.
        :param expiration: Expiration time for each key, in milliseconds.
        :param use_base_method: If ``method`` is an implementation of a base method,
            whether to pass to ``self.cache_impl`` it or the base method.
        :return:
        """
        func_passed_to_cache = None
        if use_base_method:
            func_passed_to_cache = next(
                getattr(c, method.__name__)
                for c in reversed(method.__self__.__class__.mro())
                if hasattr(c, method.__name__)
            )
        new_method = self._cached(
            method.__func__,
            cache=cache,
            validator=validator,
            expiration=expiration,
            func_passed_to_cache=func_passed_to_cache,
        )
        setattr(
            method.__self__, method.__name__, MethodType(new_method, method.__self__)
        )

    def get_cached_result(self, func: NormalCallable, *args, **kwargs) -> Any:
        wrapped = getattr(func, "__wrapped__", None)
        caching_data: Optional[CachingData] = getattr(func, _CACHING_DATA_ATTR, None)
        if wrapped is None or caching_data is None:
            return NOT_FOUND

        is_instance_func = args and hasattr(args[0], func.__name__)
        func_args = caching_data.func_args
        if len(args) < len(func_args) and func_args[0] == "self":
            func_args = func_args[1:]
        key = self._get_key(
            is_instance_func,
            func_args,
            args,
            kwargs,
            caching_data.func_defaults,
            caching_data.func_annotations,
        )
        func_cache: Optional[Cache] = getattr(wrapped, _CACHE_ATTR, None)
        if func_cache is None:
            func_cache = caching_data.cache or self._cache_impl(
                caching_data.func_passed_to_cache or func
            )
            setattr(wrapped, _CACHE_ATTR, func_cache)
        return func_cache.get(key)

    def _cached(
        self,
        func: NormalCallable,
        cache: Optional[Cache],
        validator: Optional[Callable[[Any], bool]],
        expiration: int,
        func_passed_to_cache: Optional[NormalCallable] = None,
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
            func_cache = getattr(func, _CACHE_ATTR, None)
            if func_cache is None:
                func_cache = cache or self._cache_impl(func_passed_to_cache or func)
                setattr(func, _CACHE_ATTR, func_cache)
            result = func_cache.get(key)
            if result is NOT_FOUND:
                result = func(*args, **kwargs)
                if validator is None or validator(result):
                    func_cache.set(key, result, expiration)
            return result

        source = self._get_inner_code(getsource(_x), func)
        g = dict(globals())
        g.update(locals())
        exec(compile(source, "", "exec"), g, locals())
        inner = locals()["x"]
        inner.caching_data = CachingData(
            func_args=func_args,
            func_annotations=func_annotations,
            func_defaults=func_defaults,
            cache=cache,
            func_passed_to_cache=func_passed_to_cache,
        )
        return wraps(func)(inner)

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
