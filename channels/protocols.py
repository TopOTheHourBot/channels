from __future__ import annotations

__all__ = [
    "SupportsRecv",
    "SupportsSend",
    "SupportsSendAndRecv",
]

from abc import abstractmethod
from collections.abc import AsyncIterable, AsyncIterator
from typing import Any, Protocol

from .series import Series, series


class SupportsRecv[T](Protocol):
    """Type supports receiving operations"""

    def __aiter__(self) -> Series[T]:
        return self.recv_each()

    @property
    @abstractmethod
    def closing(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def recv(self) -> T:
        """Receive a value, waiting for one to become available"""
        raise NotImplementedError

    @series
    async def recv_each(self) -> AsyncIterator[T]:
        """Return a ``Series`` that continuously receives values until closure"""
        while not self.closing:
            yield await self.recv()


class SupportsSend[T](Protocol):
    """Type supports sending operations"""

    @property
    @abstractmethod
    def closing(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def send(self, value: T, /) -> Any:
        """Send a value, waiting for an appropriate time to do so"""
        raise NotImplementedError

    async def send_each(self, values: AsyncIterable[T], /) -> Any:
        """Send values from an async iterable until exhaustion"""
        async for value in values:
            await self.send(value)

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError


class SupportsSendAndRecv[T1, T2](SupportsSend[T1], SupportsRecv[T2], Protocol):
    """Type supports both sending and receiving operations"""
