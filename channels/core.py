from __future__ import annotations

__all__ = ["Channel"]

import asyncio
from collections import deque as Deque
from typing import Generic, TypeVar, Optional

from .protocols import SupportsRecvAndSend

T = TypeVar("T")


class Channel(SupportsRecvAndSend[T, T], Generic[T]):
    """A basic send-and-receive channel with optional buffering"""

    __slots__ = ("_values")
    _values: Deque[T]

    def __init__(self, capacity: Optional[int] = None) -> None:
        self._values = Deque((), capacity)

    def __len__(self) -> int:
        return len(self._values)

    @property
    def capacity(self) -> Optional[int]:
        """The channel's maximum possible size"""
        return self._values.maxlen

    def full(self) -> bool:
        """Return true if the channel has reached its capacity, otherwise false"""
        return len(self) == self.capacity

    def empty(self) -> bool:
        """Return true if the channel has no values, otherwise false"""
        return not self._values

    async def send(self, value: T, /) -> None:
        while self.full():
            await asyncio.sleep(0)
        self._values.append(value)

    async def recv(self) -> T:
        while self.empty():
            await asyncio.sleep(0)
        return self._values.popleft()
