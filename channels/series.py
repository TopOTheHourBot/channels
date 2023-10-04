from __future__ import annotations

__all__ = [
    "Series",
    "series",
]

import asyncio
import time
from asyncio import Task
from asyncio import TimeoutError as AsyncTimeoutError
from collections.abc import AsyncIterable, AsyncIterator, Callable
from typing import Optional, ParamSpec, TypeVar, TypeVarTuple, final, overload

T = TypeVar("T")
S = TypeVar("S")

T_co = TypeVar("T_co", covariant=True)
S_co = TypeVar("S_co", covariant=True)

T1 = TypeVar("T1")
T2 = TypeVar("T2")
T3 = TypeVar("T3")
T4 = TypeVar("T4")
T5 = TypeVar("T5")

Ts = TypeVarTuple("Ts")

P = ParamSpec("P")


def identity(value: T, /) -> T:
    """Return ``value``"""
    return value


def series(func: Callable[P, AsyncIterable[T]], /) -> Callable[P, Series[T]]:
    """Convert a function's return type from an ``AsyncIterable[T]`` to
    ``Series[T]``
    """

    def series_wrapper(*args: P.args, **kwargs: P.kwargs) -> Series[T]:
        return Series(func(*args, **kwargs))

    series_wrapper.__name__ = func.__name__
    series_wrapper.__doc__  = func.__doc__

    return series_wrapper


@final
class Series(AsyncIterator[T_co]):
    """A wrapper type for asynchronous iterators that adds common iterable
    operations and waiting utilities
    """

    __slots__ = ("_values")
    _values: AsyncIterator[T_co]

    def __init__(self, values: AsyncIterable[T_co], /) -> None:
        self._values = aiter(values)

    async def __anext__(self) -> T_co:
        value = await anext(self._values)
        return value

    @series
    async def stagger(self, delay: float, *, instant_first: bool = True) -> AsyncIterator[T_co]:
        """Return a sub-series whose yields are staggered by at least ``delay``
        seconds

        If ``instant_first`` is true, the first value is yielded without extra
        delay applied to the underlying iterator.
        """
        loop  = asyncio.get_event_loop()
        delay = max(0, delay)
        prev_yield_time = 0
        if not instant_first:
            curr_yield_time = loop.time()
            prev_yield_time = curr_yield_time + delay
            await asyncio.sleep(delay)
            try:
                value = await anext(self)
            except StopAsyncIteration:
                return
            else:
                yield value
        async for value in self:
            curr_yield_time = loop.time()
            leftover_delay  = max(0, delay - (curr_yield_time - prev_yield_time))
            prev_yield_time = curr_yield_time + leftover_delay
            await asyncio.sleep(leftover_delay)
            yield value

    @series
    async def timeout(self, delay: float, *, infinite_first: bool = True) -> AsyncIterator[T_co]:
        """Return a sub-series whose value retrievals are time restricted by
        ``delay`` seconds

        If ``infinite_first`` is true, the first retrieval is awaited for
        infinite time.
        """
        try:
            if infinite_first:
                yield await anext(self)
            while True:
                yield await asyncio.wait_for(anext(self), delay)
        except (StopAsyncIteration, AsyncTimeoutError):
            return

    @series
    async def global_unique(self, key: Callable[[T_co], object] = identity) -> AsyncIterator[T_co]:
        """Return a sub-series of the values whose call to ``key`` is unique
        among all encountered values
        """
        seen = set[object]()
        async for value in self:
            result = key(value)
            if result not in seen:
                seen.add(result)
                yield value

    @series
    async def local_unique(self, key: Callable[[T_co], object] = identity) -> AsyncIterator[T_co]:
        """Return a sub-series of the values whose call to ``key`` is unique
        as compared to the previously encountered value
        """
        seen = object()
        async for value in self:
            result = key(value)
            if result != seen:
                seen = result
                yield value

    @series
    async def enumerate(self, start: int = 0) -> AsyncIterator[tuple[int, T_co]]:
        """Return a sub-series whose values are enumerated from ``start``"""
        index = start
        async for value in self:
            yield (index, value)
            index += 1

    @series
    async def limit(self, bound: int) -> AsyncIterator[T_co]:
        """Return a sub-series limited to the first ``bound`` values

        Negative values for ``bound`` are treated equivalently to 0.
        """
        if bound <= 0:
            return
        count = 1
        async for value in self:
            yield value
            if count == bound:
                return
            count += 1

    @series
    async def map(self, mapper: Callable[[T_co], S]) -> AsyncIterator[S]:
        """Return a sub-series of the results from passing each value to
        ``mapper``
        """
        async for value in self:
            yield mapper(value)

    @overload
    def zip(self: Series[T1]) -> Series[tuple[T1]]: ...
    @overload
    def zip(self: Series[T1], other2: AsyncIterable[T2], /) -> Series[tuple[T1, T2]]: ...
    @overload
    def zip(self: Series[T1], other2: AsyncIterable[T2], other3: AsyncIterable[T3], /) -> Series[tuple[T1, T2, T3]]: ...
    @overload
    def zip(self: Series[T1], other2: AsyncIterable[T2], other3: AsyncIterable[T3], other4: AsyncIterable[T4], /) -> Series[tuple[T1, T2, T3, T4]]: ...
    @overload
    def zip(self: Series[T1], other2: AsyncIterable[T2], other3: AsyncIterable[T3], other4: AsyncIterable[T4], other5: AsyncIterable[T5], /) -> Series[tuple[T1, T2, T3, T4, T5]]: ...
    @overload
    def zip(self, *others: AsyncIterable) -> Series[tuple]: ...
    @series
    async def zip(self, *others: AsyncIterable) -> AsyncIterator[tuple]:
        """Return a sub-series zipped with other asynchronous iterables

        Iteration stops when the shortest iterable has been exhausted.
        """
        its = (self, *map(aiter, others))
        try:
            while True:
                yield tuple(await asyncio.gather(*map(anext, its)))
        except StopAsyncIteration:
            return

    @series
    async def broadcast(self, *others: *Ts) -> AsyncIterator[tuple[T_co, *Ts]]:
        """Return a sub-series of the values zipped with repeated objects"""
        async for value in self:
            yield (value, *others)

    @overload
    def merge(self: Series[T1]) -> Series[T1]: ...
    @overload
    def merge(self: Series[T1], other2: AsyncIterable[T2], /) -> Series[T1 | T2]: ...
    @overload
    def merge(self: Series[T1], other2: AsyncIterable[T2], other3: AsyncIterable[T3], /) -> Series[T1 | T2 | T3]: ...
    @overload
    def merge(self: Series[T1], other2: AsyncIterable[T2], other3: AsyncIterable[T3], other4: AsyncIterable[T4], /) -> Series[T1 | T2 | T3 | T4]: ...
    @overload
    def merge(self: Series[T1], other2: AsyncIterable[T2], other3: AsyncIterable[T3], other4: AsyncIterable[T4], other5: AsyncIterable[T5], /) -> Series[T1 | T2 | T3 | T4 | T5]: ...
    @overload
    def merge(self, *others: AsyncIterable) -> Series: ...
    @series
    async def merge(self, *others: AsyncIterable) -> AsyncIterator:
        """Return a sub-series merged with other asynchronous iterables

        Iteration stops when the longest iterable has been exhausted.
        """
        its = {
            str(name): aiter(it)
            for name, it in enumerate((self, *others))
        }

        todo = set[Task]()
        for name, it in its.items():
            task = asyncio.create_task(anext(it), name=name)
            todo.add(task)

        while todo:
            done, todo = await asyncio.wait(todo, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                try:
                    result = task.result()
                except StopAsyncIteration:
                    continue
                else:
                    name = task.get_name()
                    it = its[name]
                    task = asyncio.create_task(anext(it), name=name)
                    todo.add(task)
                    yield result

    @series
    async def star_map(self: Series[tuple[*Ts]], mapper: Callable[[*Ts], S]) -> AsyncIterator[S]:
        """Return a sub-series of the results from unpacking and passing each
        value to ``mapper``
        """
        async for values in self:
            yield mapper(*values)

    @series
    async def filter(self, predicate: Callable[[T_co], object]) -> AsyncIterator[T_co]:
        """Return a sub-series of the values whose call to ``predicate``
        evaluates true
        """
        async for value in self:
            if predicate(value):
                yield value

    def truthy(self) -> Series[T_co]:
        """Return a sub-series of the values filtered by their truthyness"""
        return self.filter(lambda value: value)

    def falsy(self) -> Series[T_co]:
        """Return a sub-series of the values filtered by their falsyness"""
        return self.filter(lambda value: not value)

    def not_none(self: Series[Optional[S]]) -> Series[S]:
        """Return a sub-series of the values that are not ``None``"""
        return self.filter(lambda value: value is not None)  # type: ignore

    async def all(self) -> bool:
        """Return true if all values are true, otherwise false"""
        return bool(await anext(self.falsy(), True))

    async def any(self) -> bool:
        """Return true if any value is true, otherwise false"""
        return bool(await anext(self.truthy(), False))

    async def collect(self) -> list[T_co]:
        """Return the values accumulated as a ``list``"""
        result = []
        async for value in self:
            result.append(value)
        return result

    async def reduce(self, initial: S, reducer: Callable[[S, T_co], S]) -> S:
        """Return the values accumulated as one via left-fold"""
        result = initial
        async for value in self:
            result = reducer(result, value)
        return result

    async def count(self) -> int:
        """Return the number of values"""
        return await self.reduce(0, lambda count, _: count + 1)
