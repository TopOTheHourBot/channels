from __future__ import annotations

__all__ = ["Stream", "compose", "call_while"]

import asyncio
from asyncio import Task
from asyncio import TimeoutError as AsyncTimeoutError
from collections.abc import AsyncIterator, Callable, Coroutine
from typing import Any, Optional, Self, TypeGuard, final, overload


def identity[T](value: T, /) -> T:
    """Return ``value``"""
    return value


def not_none[T](value: Optional[T], /) -> TypeGuard[T]:
    """Return true if ``value`` is not ``None``, otherwise false"""
    return value is not None


def compose[**P, T](func: Callable[P, AsyncIterator[T]]) -> Callable[P, Stream[T]]:
    """Convert a function's return type from an asynchronous iterator to a
    ``Stream``
    """

    def compose_wrapper(*args: P.args, **kwargs: P.kwargs) -> Stream[T]:
        return Stream(func(*args, **kwargs))

    compose_wrapper.__name__ = func.__name__
    compose_wrapper.__doc__ = func.__doc__

    return compose_wrapper


@compose
async def call_while[T](
    func: Callable[[], Coroutine[Any, Any, T]],
    /,
    predicate: Callable[[T], object] = lambda _: True,
) -> AsyncIterator[T]:
    """Compose a new ``Stream`` that makes repeated calls to ``func`` while
    its result evaluates true according to ``predicate``
    """
    while predicate(result := await func()):
        yield result


@final
class Stream[T](AsyncIterator[T]):
    """A wrapper type for asynchronous iterators that adds common iterable
    operations and waiting utilities
    """

    __slots__ = ("_values")
    _values: AsyncIterator[T]

    def __init__(self, values: AsyncIterator[T], /) -> None:
        self._values = values

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> T:
        value = await anext(self._values)
        return value

    @compose
    async def finite_timeout(self, delay: float, *, first: bool = False) -> AsyncIterator[T]:
        """Return a sub-stream whose value retrievals are time restricted by
        ``delay`` seconds

        If ``first`` is true, applies the timeout while awaiting the first
        value. False by default.
        """
        try:
            if not first:
                yield await anext(self)
            while True:
                yield await asyncio.wait_for(anext(self), delay)
        except (StopAsyncIteration, AsyncTimeoutError):
            return

    def timeout(self, delay: Optional[float], *, first: bool = False) -> Stream[T]:
        """Return a sub-stream whose value retrievals are optionally time
        restricted by ``delay`` seconds

        If ``delay`` is ``None``, do not apply a timeout.

        If ``first`` is true, applies the timeout while awaiting the first
        value. False by default.
        """
        if delay is None:  # Reduces layers of composition
            return self
        return self.finite_timeout(delay, first=first)

    @compose
    async def global_unique(self, key: Callable[[T], object] = identity) -> AsyncIterator[T]:
        """Return a sub-stream of the values whose call to ``key`` is unique
        among all encountered values
        """
        seen = set[object]()
        async for value in self:
            result = key(value)
            if result not in seen:
                seen.add(result)
                yield value

    @compose
    async def local_unique(self, key: Callable[[T], object] = identity) -> AsyncIterator[T]:
        """Return a sub-stream of the values whose call to ``key`` is unique
        as compared to the previously encountered value
        """
        seen = object()
        async for value in self:
            result = key(value)
            if result != seen:
                seen = result
                yield value

    @compose
    async def enumerate(self, start: int = 0) -> AsyncIterator[tuple[int, T]]:
        """Return a sub-stream whose values are enumerated from ``start``"""
        index = start
        async for value in self:
            yield (index, value)
            index += 1

    @compose
    async def limit(self, bound: int) -> AsyncIterator[T]:
        """Return a sub-stream limited to the first ``bound`` values

        Negative values for ``bound`` are treated equivalently to 0.
        """
        if bound <= 0:
            return
        async for count, value in self.enumerate(1):
            yield value
            if count == bound:
                return

    @compose
    async def map[S](self, mapper: Callable[[T], S]) -> AsyncIterator[S]:
        """Return a sub-stream of the results from passing each value to
        ``mapper``
        """
        async for value in self:
            yield mapper(value)

    @overload
    def zip(self) -> Stream[tuple[T]]: ...
    @overload
    def zip[T1](self, other1: Stream[T1], /) -> Stream[tuple[T, T1]]: ...
    @overload
    def zip[T1, T2](self, other1: Stream[T1], other2: Stream[T2], /) -> Stream[tuple[T, T1, T2]]: ...
    @overload
    def zip[T1, T2, T3](self, other1: Stream[T1], other2: Stream[T2], other3: Stream[T3], /) -> Stream[tuple[T, T1, T2, T3]]: ...
    @overload
    def zip[T1, T2, T3, T4](self, other1: Stream[T1], other2: Stream[T2], other3: Stream[T3], other4: Stream[T4], /) -> Stream[tuple[T, T1, T2, T3, T4]]: ...
    @overload
    def zip(self, *others: Stream) -> Stream[tuple]: ...
    @compose
    async def zip(self, *others: Stream) -> AsyncIterator[tuple]:
        """Return a sub-stream zipped with other streams

        Iteration stops when the shortest stream has been exhausted.
        """
        its = (self, *others)
        try:
            while True:
                yield tuple(await asyncio.gather(*map(anext, its)))
        except StopAsyncIteration:
            return

    @overload
    def merge(self) -> Stream[T]: ...
    @overload
    def merge[T1](self, other1: Stream[T1], /) -> Stream[T | T1]: ...
    @overload
    def merge[T1, T2](self, other1: Stream[T1], other2: Stream[T2], /) -> Stream[T | T1 | T2]: ...
    @overload
    def merge[T1, T2, T3](self, other1: Stream[T1], other2: Stream[T2], other3: Stream[T3], /) -> Stream[T | T1 | T2 | T3]: ...
    @overload
    def merge[T1, T2, T3, T4](self, other1: Stream[T1], other2: Stream[T2], other3: Stream[T3], other4: Stream[T4], /) -> Stream[T | T1 | T2 | T3 | T4]: ...
    @overload
    def merge(self, *others: Stream) -> Stream: ...
    @compose
    async def merge(self, *others: Stream) -> AsyncIterator:
        """Return a sub-stream merged with other streams

        Iteration stops when the longest stream has been exhausted.
        """
        map = dict[str, Stream]()
        map["0"] = self
        for n, stream in enumerate(others, 1):
            map[f"{n}"] = stream

        todo = set[Task]()
        for name, stream in map.items():
            task = asyncio.create_task(anext(stream), name=name)
            todo.add(task)

        while todo:
            done, todo = await asyncio.wait(todo, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                name = task.get_name()
                try:
                    result = task.result()
                except StopAsyncIteration:
                    # Stream has been exhausted: no need to keep a reference at
                    # this point
                    del map[name]
                    continue
                else:
                    # Stream has yielded: await its next value and return the
                    # current one
                    stream = map[name]
                    task = asyncio.create_task(anext(stream), name=name)
                    todo.add(task)
                    yield result

    @compose
    async def star_map[*Ts, S](self: Stream[tuple[*Ts]], mapper: Callable[[*Ts], S]) -> AsyncIterator[S]:
        """Return a sub-stream of the results from unpacking and passing each
        value to ``mapper``
        """
        async for values in self:
            yield mapper(*values)

    @overload
    def filter[S](self, predicate: Callable[[T], TypeGuard[S]]) -> Stream[S]: ...
    @overload
    def filter(self, predicate: Callable[[T], object]) -> Stream[T]: ...
    @compose
    async def filter(self, predicate: Callable[[T], object]) -> AsyncIterator[T]:
        """Return a sub-stream of the values whose call to ``predicate``
        evaluates true
        """
        async for value in self:
            if predicate(value):
                yield value

    def truthy(self) -> Stream[T]:
        """Return a sub-stream of the values filtered by their truthyness"""
        return self.filter(lambda value: value)

    def falsy(self) -> Stream[T]:
        """Return a sub-stream of the values filtered by their falsyness"""
        return self.filter(lambda value: not value)

    def not_none[S](self: Stream[Optional[S]]) -> Stream[S]:
        """Return a sub-stream of the values that are not ``None``"""
        return self.filter(not_none)

    async def all(self) -> bool:
        """Return true if all values are true, otherwise false"""
        return bool(await anext(self.falsy(), True))

    async def any(self) -> bool:
        """Return true if any value is true, otherwise false"""
        return bool(await anext(self.truthy(), False))

    async def collect(self) -> list[T]:
        """Return the values accumulated as a ``list``"""
        result = []
        async for value in self:
            result.append(value)
        return result

    async def reduce[S](self, initial: S, reducer: Callable[[S, T], S]) -> S:
        """Return the values accumulated as one via left-fold"""
        result = initial
        async for value in self:
            result = reducer(result, value)
        return result

    async def count(self) -> int:
        """Return the number of values"""
        return await self.reduce(0, lambda count, _: count + 1)

    @overload
    async def until[S, DefaultT](self, predicate: Callable[[T], TypeGuard[S]], *, default: DefaultT = None) -> S | DefaultT: ...
    @overload
    async def until[DefaultT](self, predicate: Callable[[T], object], *, default: DefaultT = None) -> T | DefaultT: ...

    async def until(self, predicate, *, default=None):
        """Return the first value whose call to ``predicate`` is true,
        otherwise ``default`` if no such value is found
        """
        async for value in self:
            if predicate(value):
                return value
        return default
