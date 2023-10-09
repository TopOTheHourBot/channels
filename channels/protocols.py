from __future__ import annotations

__all__ = [
    "StopRecv",
    "StopSend",
    "SupportsRecv",
    "SupportsSend",
    "SupportsSendAndRecv",
]

from abc import abstractmethod
from collections.abc import AsyncIterable, AsyncIterator
from typing import Protocol, TypeVar

from .series import Series, series

T_co = TypeVar("T_co", covariant=True)
T_contra = TypeVar("T_contra", contravariant=True)


class StopRecv(Exception):
    """Values can no longer be received"""

    __slots__ = ()


class StopSend(Exception):
    """Values can no longer be sent"""

    __slots__ = ()


class SupportsRecv(Protocol[T_co]):
    """Type supports receiving operations"""

    def __aiter__(self) -> Series[T_co]:
        return self.recv_each()

    @abstractmethod
    async def recv(self) -> T_co:
        """Receive a value, waiting for one to become available

        This method can raise ``StopRecv`` to signal that no further values
        can be received.
        """
        raise NotImplementedError

    @series
    async def recv_each(self) -> AsyncIterator[T_co]:
        """Return an async iterator that continuously receives values until
        ``StopRecv``
        """
        try:
            while True:
                yield await self.recv()
        except StopRecv:
            return

    async def try_recv(self) -> StopRecv | T_co:
        """Wrapper of ``recv()`` that returns captured ``StopRecv`` exceptions
        rather than raising them
        """
        try:
            value = await self.recv()
        except StopRecv as error:
            return error
        else:
            return value


class SupportsSend(Protocol[T_contra]):
    """Type supports sending operations"""

    @abstractmethod
    async def send(self, value: T_contra, /) -> object:
        """Send a value, waiting for an appropriate time to do so

        This method can raise ``StopSend`` to signal that no further values
        can be sent.
        """
        raise NotImplementedError

    async def send_each(self, values: AsyncIterable[T_contra], /) -> object:
        """Send values from an async iterable until exhaustion, or until
        ``StopSend``
        """
        try:
            async for value in values:
                await self.send(value)
        except StopSend:
            return

    async def try_send(self, value: T_contra, /) -> StopSend | object:
        """Wrapper of ``send()`` that returns captured ``StopSend`` exceptions
        rather than raising them
        """
        try:
            result = await self.send(value)
        except StopSend as error:
            return error
        else:
            return result


class SupportsSendAndRecv(SupportsSend[T_contra], SupportsRecv[T_co], Protocol[T_contra, T_co]):
    """Type supports both sending and receiving operations"""
