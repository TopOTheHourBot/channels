from __future__ import annotations

__all__ = ["Channel"]

import asyncio
from asyncio import Future
from collections import deque as Deque
from collections.abc import AsyncIterator
from typing import Optional, final

from . import stream
from .exceptions import Closure


@final
class Channel[T]:

    __slots__ = ("_buffer", "_receiver", "_closer")
    _buffer: Deque[T]
    _receiver: Optional[Future[None]]
    _closer: Future[None]

    def __init__(self, max_size: Optional[int] = None) -> None:
        self._buffer = Deque((), max_size)
        self._receiver = None
        self._closer = asyncio.get_running_loop().create_future()

    @stream.compose
    async def __aiter__(self) -> AsyncIterator[T]:
        try:
            while True:
                yield await self.recv()
        except Closure:
            return

    def send(self, value: T, /) -> None:
        """Send a value to the channel

        Discards the oldest value in the channel buffer to insert the incoming
        one when at maximum size.

        Raises ``Closure`` if the channel has been closed.
        """
        if self.is_closed():
            raise Closure
        self._buffer.append(value)
        receiver = self._receiver
        if receiver is None or receiver.done():
            return
        receiver.set_result(None)

    async def recv(self) -> T:
        """Receive a value from the channel

        Raises ``Closure`` if the channel has been, or is closed during
        execution.

        Raises ``RuntimeError`` if the channel is receiving in another
        coroutine (debug only).
        """
        if self.is_closed():
            raise Closure
        if __debug__:
            if self._receiver is not None:
                raise RuntimeError("channel is already receiving")
        buffer = self._buffer
        while not buffer:
            receiver = asyncio.get_running_loop().create_future()
            self._receiver = receiver
            try:
                await receiver
            finally:
                self._receiver = None
        return buffer.popleft()

    def close(self) -> None:
        """Close the channel

        Raises ``Closure`` into the active ``recv()`` call (if one exists).
        """
        closer = self._closer
        if closer.done():
            return
        closer.set_result(None)
        receiver = self._receiver
        if receiver is None or receiver.done():
            return
        receiver.set_exception(Closure)

    def clear(self) -> None:
        """Clear the channel

        Discards all values from the channel buffer.
        """
        self._buffer.clear()

    def is_closed(self) -> bool:
        """Return true if the channel has been closed, otherwise false"""
        return self._closer.done()