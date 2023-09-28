from __future__ import annotations

__all__ = ["Channel"]

import asyncio
from collections import deque as Deque
from typing import Generic, TypeVar

from .protocols import SupportsRecvAndSend

T = TypeVar("T")


class Channel(SupportsRecvAndSend[T, T], Generic[T]):

    __slots__ = ("_values")
    _values: Deque[T]

    def __init__(self, capacity: int = -1) -> None:
        self._values = Deque((), None if capacity < 0 else capacity)

    def __len__(self) -> int:
        return len(self._values)

    @property
    def capacity(self) -> int:
        """The channel's maximum possible size"""
        capacity = self._values.maxlen
        return -1 if capacity is None else capacity

    def full(self) -> bool:
        """Return true if the channel has reached its capacity, otherwise false"""
        return len(self) == self.capacity

    def empty(self) -> bool:
        """Return true if the channel has no values, otherwise false"""
        return not self._values

    async def send(self, value: T, /) -> None:
        values = self._values
        while self.full():
            await asyncio.sleep(0)
        values.append(value)

    async def recv(self) -> T:
        values = self._values
        while self.empty():
            await asyncio.sleep(0)
        return values.popleft()
