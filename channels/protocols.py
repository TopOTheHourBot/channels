from __future__ import annotations

__all__ = [
    "Closure",
    "SupportsClose",
    "SupportsRecv",
    "SupportsSend",
    "SupportsSendAndRecv",
]

from abc import abstractmethod
from collections.abc import AsyncIterable, AsyncIterator
from typing import Any, Protocol

from .series import Series, series


class Closure(Exception):

    __slots__ = ()


class SupportsClose(Protocol):

    @abstractmethod
    def close(self) -> Any:
        raise NotImplementedError


class SupportsRecv[T](Protocol):

    def __aiter__(self) -> Series[T]:
        return self.recv_each()

    @abstractmethod
    async def recv(self) -> T:
        raise NotImplementedError

    @series
    async def recv_each(self) -> AsyncIterator[T]:
        try:
            while True:
                yield await self.recv()
        except Closure:
            return


class SupportsSend[T](SupportsClose, Protocol):

    @abstractmethod
    async def send(self, value: T, /) -> Any:
        raise NotImplementedError

    async def send_each(self, values: AsyncIterable[T], /) -> Any:
        try:
            async for value in values:
                await self.send(value)
        except Closure:
            return


class SupportsSendAndRecv[T1, T2](SupportsSend[T1], SupportsRecv[T2], Protocol):
    ...
