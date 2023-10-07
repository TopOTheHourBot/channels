from __future__ import annotations

__all__ = [
    "StopRecv",
    "StopSend",
    "SupportsRecv",
    "SupportsSend",
    "SupportsRecvAndSend",
]

from abc import abstractmethod
from collections.abc import AsyncIterable, AsyncIterator
from typing import Protocol, TypeVar

from .series import series

T_co = TypeVar("T_co", covariant=True)
T_contra = TypeVar("T_contra", contravariant=True)


class StopRecv(Exception):
    """Values can no longer be received"""

    __slots__ = ()


class StopSend(Exception):
    """Values can no longer be sent"""

    __slots__ = ()


class SupportsRecv(Protocol[T_co]):
    """Type supports the ``recv()`` and ``recv_each()`` operations"""

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


class SupportsSend(Protocol[T_contra]):
    """Type supports the ``send()`` and ``send_each()`` operations"""

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


class SupportsRecvAndSend(SupportsRecv[T_co], SupportsSend[T_contra], Protocol[T_co, T_contra]):
    """Type supports both sending and receiving operations"""
