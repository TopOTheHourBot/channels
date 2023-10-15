from __future__ import annotations

__all__ = [
    "Signal",
    "SupportsRecv",
    "SupportsSend",
    "SupportsSendAndRecv",
]

import enum
from abc import abstractmethod
from collections.abc import AsyncIterable, AsyncIterator
from enum import Flag
from typing import Any, Protocol

from .series import Series, series


class Signal(Flag):
    STOP = enum.auto()


class SupportsRecv[T](Protocol):
    """Type supports receiving operations"""

    def __aiter__(self) -> Series[T]:
        return self.recv_each()

    @abstractmethod
    async def recv(self) -> T | Signal:
        """Receive a value, waiting for one to become available"""
        raise NotImplementedError

    @series
    async def recv_each(self) -> AsyncIterator[T]:
        """Return an async iterator that continuously receives values until
        encountering ``Signal.STOP``
        """
        while (value := await self.recv()) is not Signal.STOP:
            yield value


class SupportsSend[T](Protocol):
    """Type supports sending operations"""

    @abstractmethod
    async def send(self, value: T | Signal, /) -> Any:
        """Send a value, waiting for an appropriate time to do so"""
        raise NotImplementedError

    async def send_each(self, values: AsyncIterable[T | Signal], /) -> Any:
        """Send values from an async iterable until exhaustion"""
        async for value in values:
            await self.send(value)
            if value is Signal.STOP:
                return


class SupportsSendAndRecv[T1, T2](SupportsSend[T1], SupportsRecv[T2], Protocol):
    """Type supports both sending and receiving operations"""
