from __future__ import annotations

__all__ = ["Channel"]

import asyncio
from asyncio import Future
from collections import deque as Deque
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from typing import Optional, Self, final

from . import stream
from .exceptions import Closure


@final
class Channel[T]:

    __slots__ = ("_buffer", "_receiver", "_closed")
    _buffer: Deque[T]
    _receiver: Optional[Future[None]]
    _closed: bool

    def __init__(self, max_size: Optional[int] = None) -> None:
        self._buffer = Deque((), max_size)
        self._receiver = None
        self._closed = False

    @stream.compose
    async def __aiter__(self) -> AsyncIterator[T]:
        try:
            while True:
                yield await self.recv()
        except Closure:
            return

    @property
    def size(self) -> int:
        """The channel's current size"""
        return len(self._buffer)

    @property
    def max_size(self) -> Optional[int]:
        """The channel's maximum size"""
        return self._buffer.maxlen

    @property
    def full(self) -> bool:
        """The channel's full state"""
        return self.size == self.max_size

    @property
    def empty(self) -> bool:
        """The channel's empty state"""
        return not self.size

    @property
    def closed(self) -> bool:
        """The channel's closed state"""
        return self._closed

    def send(self, value: T, /) -> None:
        """Send a value to the channel

        Discards the oldest value in the channel buffer to insert the incoming
        one when at maximum size.

        Raises ``Closure`` if the channel has been closed.
        """
        if self._closed:
            raise Closure
        self._buffer.append(value)
        receiver = self._receiver
        if not (receiver is None or receiver.done()):
            receiver.set_result(None)

    async def recv(self) -> T:
        """Receive a value from the channel

        Raises ``Closure`` if the channel has been, or is closed during
        execution.

        Raises ``RuntimeError`` if the channel is receiving in another
        coroutine (debug only).
        """
        if self._closed:
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

    def open(self) -> Self:
        """Open the channel"""
        self._closed = False
        return self

    def close(self) -> Self:
        """Close the channel

        Raises ``Closure`` into the active ``recv()`` call (if one exists).
        """
        self._closed = True
        receiver = self._receiver
        if not (receiver is None or receiver.done()):
            receiver.set_exception(Closure)
        return self

    def clear(self) -> Self:
        """Clear the channel

        Discards all values from the channel buffer.
        """
        self._buffer.clear()
        return self

    @contextmanager
    def closure(self) -> Iterator[Self]:
        """Return a context manager that ensures the channel's closure upon
        exit
        """
        try:
            yield self
        finally:
            self.close()
