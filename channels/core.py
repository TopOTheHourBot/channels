from __future__ import annotations

__all__ = ["Channel"]

import asyncio
from asyncio import Future, InvalidStateError
from collections import deque as Deque
from typing import override

from .protocols import Closure, SupportsSendAndRecv


class Channel[T](SupportsSendAndRecv[T, T]):

    __slots__ = ("_max_size", "_getters", "_putters", "_values", "_closer")
    _max_size: int
    _getters: Deque[Future[None]]
    _putters: Deque[Future[None]]
    _values: Deque[T]
    _closer: Future[None]

    def __init__(self, max_size: int = 0) -> None:
        self._max_size = max_size
        self._getters = Deque()
        self._putters = Deque()
        self._values = Deque()
        self._closer = asyncio.get_running_loop().create_future()

    def __len__(self) -> int:
        return len(self._values)

    @property
    def size(self) -> int:
        """The channel's current size"""
        return len(self._values)

    @property
    def max_size(self) -> int:
        """The channel's maximum possible size"""
        return self._max_size

    def _wake_next(self, waiters: Deque[Future[None]]) -> None:
        while waiters:
            waiter = waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)
                break

    @override
    def close(self) -> bool:
        """Request to close the channel

        Returns true if the channel has been closed, false if the channel has
        already been closed.

        Any outstanding requests to send or receive at the moment of closure
        will have ``Closure`` raised into them.
        """
        try:
            self._closer.set_result(None)
        except InvalidStateError:
            return False
        self._values.clear()  # Help the garbage collector out
        for waiter in self._putters:
            waiter.set_exception(Closure)
        for waiter in self._getters:
            waiter.set_exception(Closure)
        return True

    @override
    async def send(self, value: T, /) -> None:
        """Send a value to the channel

        Raises ``Closure`` if the channel has been closed.
        """
        if self.closed():
            raise Closure
        while self.full():
            putter = asyncio.get_running_loop().create_future()
            self._putters.append(putter)
            try:
                await putter
            except:
                putter.cancel()
                self._putters.remove(putter)
                raise
        self._values.append(value)
        self._wake_next(self._getters)

    @override
    async def recv(self) -> T:
        """Receive a value from the channel

        Raises ``Closure`` if the channel has been closed.
        """
        if self.closed():
            raise Closure
        while self.empty():
            getter = asyncio.get_running_loop().create_future()
            self._getters.append(getter)
            try:
                await getter
            except:
                getter.cancel()
                self._getters.remove(getter)
                raise
        value = self._values.popleft()
        self._wake_next(self._putters)
        return value

    def closed(self) -> bool:
        """Return true if the channel has been closed, otherwise false"""
        return self._closer.done()

    def empty(self) -> bool:
        """Return true if the channel is empty, otherwise false"""
        return not self

    def full(self) -> bool:
        """Return true if the channel is full, otherwise false"""
        if self.max_size <= 0:
            return False
        return self.size >= self.max_size
