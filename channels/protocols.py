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
        return self.recv_all()

    @abstractmethod
    async def recv(self) -> T:
        raise NotImplementedError

    async def try_recv[S](self, *, default: S = None) -> T | S:
        try:
            value = await self.recv()
        except Closure:
            return default
        else:
            return value

    @series
    async def recv_all(self) -> AsyncIterator[T]:
        try:
            while True:
                yield await self.recv()
        except Closure:
            return


class SupportsSend[T](SupportsClose, Protocol):

    @abstractmethod
    async def send(self, value: T, /) -> None:
        raise NotImplementedError

    async def try_send(self, value: T, /) -> None:
        try:
            await self.send(value)
        except Closure:
            return

    async def send_all(self, values: AsyncIterable[T], /) -> Any:
        try:
            async for value in values:
                await self.send(value)
        except Closure:
            return


class SupportsSendAndRecv[T1, T2](SupportsSend[T1], SupportsRecv[T2], Protocol):
    ...
