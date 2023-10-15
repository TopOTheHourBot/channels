from __future__ import annotations

__all__ = ["Limiter"]

import asyncio
from typing import NamedTuple

from .protocols import SupportsSend


class Timespan(NamedTuple):

    start: float
    stop: float
    step: float


class Limiter[T](SupportsSend[T]):

    __slots__ = ("_channel", "_prev_send_time", "_cooldown")
    _channel: SupportsSend[T]
    _prev_send_time: float
    _cooldown: float

    def __init__(self, channel: SupportsSend[T], *, cooldown: float = 0) -> None:
        self._channel = channel
        self._prev_send_time = 0
        self.cooldown = cooldown

    @property
    def cooldown(self) -> float:
        return self._cooldown

    @cooldown.setter
    def cooldown(self, value: float) -> None:
        self._cooldown = max(value, 0)

    async def send(self, value: T, /) -> None:
        _, next_send_time, delay = self.wait_span()
        self._prev_send_time = next_send_time
        await asyncio.sleep(delay)
        await self._channel.send(value)

    def wait_span(self) -> Timespan:
        """Return a ``(start, stop, step)`` tuple, where ``start`` is the
        current event loop time, ``stop`` is the next available time to send,
        and ``step`` is the amount of time to delay before reaching ``stop``
        """
        curr_send_time = asyncio.get_event_loop().time()
        prev_send_time = self._prev_send_time or curr_send_time
        delay = max(self._cooldown - (curr_send_time - prev_send_time), 0)
        next_send_time = curr_send_time + delay
        return Timespan(curr_send_time, next_send_time, delay)
