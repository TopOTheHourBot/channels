from __future__ import annotations

__all__ = ["Channel"]

import asyncio
from asyncio import Future
from collections import deque as Deque
from typing import override

from .protocols import Closure, SupportsSendAndRecv


class Channel[T](SupportsSendAndRecv[T, T]):

    __slots__ = ("_max_size", "_getters", "_putters", "_values", "_closing")
    _max_size: int
    _getters: Deque[Future[None]]
    _putters: Deque[Future[None]]
    _values: Deque[T]
    _closing: bool

    def __init__(self, max_size: int = 0) -> None:
        self._max_size = max_size
        self._getters  = Deque()
        self._putters  = Deque()
        self._values   = Deque()
        self._closing  = False

    def __len__(self) -> int:
        return len(self._values)

    @property
    def closing(self) -> bool:
        return self._closing

    @property
    def size(self) -> int:
        return len(self._values)

    @property
    def max_size(self) -> int:
        return self._max_size

    def _wake_next(self, waiters: Deque[Future[None]]) -> None:
        while waiters:
            waiter = waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)
                break

    @override
    async def send(self, value: T, /) -> None:
        if self.closing:
            raise Closure
        while self.full():
            putter = asyncio.get_running_loop().create_future()
            self._putters.append(putter)
            try:
                await putter
            except:
                putter.cancel()
                try:
                    self._putters.remove(putter)
                except ValueError:
                    pass
                if not self.full() and not putter.cancelled():
                    self._wake_next(self._putters)
                raise
        self._values.append(value)
        self._wake_next(self._getters)

    @override
    async def recv(self) -> T:
        if self.closing:
            raise Closure
        while self.empty():
            getter = asyncio.get_running_loop().create_future()
            self._getters.append(getter)
            try:
                await getter
            except:
                getter.cancel()
                try:
                    self._getters.remove(getter)
                except ValueError:
                    pass
                if not self.empty() and not getter.cancelled():
                    self._wake_next(self._getters)
                raise
        value = self._values.popleft()
        self._wake_next(self._putters)
        return value

    @override
    def close(self) -> None:
        self._closing = True

    def empty(self) -> bool:
        return not self._values

    def full(self) -> bool:
        if self.max_size <= 0:
            return False
        return self.size >= self.max_size
