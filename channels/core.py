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
    def closed(self) -> bool:
        """True if the channel has been closed, otherwise false"""
        return self._closer.done()

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
    def close(self) -> None:
        """Request to close the channel

        Raises ``RuntimeError`` if the channel has already been closed.

        Sending to the channel after closing will raise ``Closure``. Receiving
        from the channel is permitted for as long as the channel remains
        non-empty, and will raise ``Closure`` otherwise.

        Any outstanding requests to send or receive at the moment of closure
        will have ``Closure`` raised into them.
        """
        try:
            self._closer.set_result(None)
        except InvalidStateError as error:
            raise RuntimeError("channel has already been closed") from error
        for waiter in self._putters:
            waiter.set_exception(Closure)
        for waiter in self._getters:
            waiter.set_exception(Closure)

    @override
    async def send(self, value: T, /) -> None:
        """Send a value to the channel

        Raises ``Closure`` if the channel has been closed.
        """
        if self.closed:
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
        """Receive a value from the channel

        Raises ``Closure`` if the channel has been closed and is empty.
        """
        if self.closed and self.empty():
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

    def empty(self) -> bool:
        """Return true if the channel is empty, otherwise false"""
        return not self

    def full(self) -> bool:
        """Return true if the channel is full, otherwise false"""
        if self.max_size <= 0:
            return False
        return self.size >= self.max_size
