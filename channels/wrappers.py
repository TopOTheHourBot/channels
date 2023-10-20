from __future__ import annotations

__all__ = [
    "SendOnly",
    "RecvOnly",
    "SendOnlyLimiter",
]

import asyncio
from collections.abc import Coroutine
from typing import Any, NamedTuple

from .protocols import SupportsRecv, SupportsSend


class Timespan(NamedTuple):

    start: float
    stop: float
    step: float


class SendOnly[T](SupportsSend[T]):

    __slots__ = ("_channel")
    _channel: SupportsSend[T]

    def __init__(self, channel: SupportsSend[T]) -> None:
        self._channel = channel

    def send(self, value: T, /) -> Coroutine[Any, Any, Any]:
        return self._channel.send(value)

    def close(self) -> Any:
        return self._channel.close()


class RecvOnly[T](SupportsRecv[T]):

    __slots__ = ("_channel")
    _channel: SupportsRecv[T]

    def __init__(self, channel: SupportsRecv[T]) -> None:
        self._channel = channel

    def recv(self) -> Coroutine[Any, Any, T]:
        return self._channel.recv()


class SendOnlyLimiter[T](SendOnly[T]):

    __slots__ = ("_prev_send_time", "_cooldown")
    _prev_send_time: float
    _cooldown: float

    def __init__(self, channel: SupportsSend[T], *, cooldown: float = 0) -> None:
        super().__init__(channel)
        self._prev_send_time = 0
        self.cooldown = cooldown

    @property
    def cooldown(self) -> float:
        return self._cooldown

    @cooldown.setter
    def cooldown(self, value: float) -> None:
        self._cooldown = max(value, 0)

    async def send(self, value: T, /) -> Any:
        _, next_send_time, delay = self.wait_span()
        self._prev_send_time = next_send_time
        if delay:
            await asyncio.sleep(delay)
        return await super().send(value)

    def wait_span(self) -> Timespan:
        """Return a ``(start, stop, step)`` tuple, where ``start`` is the
        current event loop time, ``stop`` is the next available time to send,
        and ``step`` is the amount of time to delay before reaching ``stop``
        """
        curr_send_time = asyncio.get_running_loop().time()
        prev_send_time = self._prev_send_time
        cooldown = self._cooldown if prev_send_time else curr_send_time
        delay = max(cooldown - (curr_send_time - prev_send_time), 0)
        next_send_time = curr_send_time + delay
        return Timespan(curr_send_time, next_send_time, delay)
