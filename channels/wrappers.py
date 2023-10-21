from __future__ import annotations

__all__ = [
    "SendOnly",
    "RecvOnly",
    "SendOnlyLimiter",
]

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any, NamedTuple, override

from .protocols import SupportsRecv, SupportsSend


class SendOnly[T](SupportsSend[T]):

    __slots__ = ("_channel")
    _channel: SupportsSend[T]

    def __init__(self, channel: SupportsSend[T]) -> None:
        self._channel = channel

    @override
    def send(self, value: T, /) -> Coroutine[Any, Any, None]:
        return self._channel.send(value)

    @override
    def close(self) -> Any:
        return self._channel.close()


class RecvOnly[T](SupportsRecv[T]):

    __slots__ = ("_channel")
    _channel: SupportsRecv[T]

    def __init__(self, channel: SupportsRecv[T]) -> None:
        self._channel = channel

    @override
    def recv(self) -> Coroutine[Any, Any, T]:
        return self._channel.recv()


class Timespan(NamedTuple):

    start: float
    stop: float
    step: float


class SendOnlyLimiter[T](SendOnly[T]):

    __slots__ = ("_prev_send_time", "_cooldown", "_predicate")
    _prev_send_time: float
    _cooldown: float
    _predicate: Callable[[T], object]

    def __init__(
        self,
        channel: SupportsSend[T],
        *,
        cooldown: float = 0,
        predicate: Callable[[T], object] = lambda value: True,
    ) -> None:
        super().__init__(channel)
        self._prev_send_time = 0
        self.cooldown = cooldown
        self.predicate = predicate

    @property
    def cooldown(self) -> float:
        """The amount of time, in seconds, to delay before sending subsequent
        messages to the wrapped channel
        """
        return self._cooldown

    @cooldown.setter
    def cooldown(self, cooldown: float) -> None:
        self._cooldown = max(cooldown, 0)

    @property
    def predicate(self) -> Callable[[T], object]:
        """A function of the sending object that returns true when the object
        should be considered for delay, otherwise false
        """
        return self._predicate

    @predicate.setter
    def predicate(self, predicate: Callable[[T], object]) -> None:
        self._predicate = predicate

    @override
    async def send(self, value: T, /) -> None:
        if self.predicate(value):
            curr_send_time, next_send_time, delay = self.wait_span()
            self._prev_send_time = next_send_time
            if delay:
                await asyncio.sleep(delay)
        await super().send(value)

    def wait_span(self) -> Timespan:
        """Return a ``(start, stop, step)`` tuple, where ``start`` is the
        current event loop time, ``stop`` is the next available time to send,
        and ``step`` is the amount of time to delay before reaching ``stop``
        """
        curr_send_time = asyncio.get_running_loop().time()
        prev_send_time = self._prev_send_time
        cooldown = self.cooldown if prev_send_time else curr_send_time
        delay = max(cooldown - (curr_send_time - prev_send_time), 0)
        next_send_time = curr_send_time + delay
        return Timespan(curr_send_time, next_send_time, delay)
