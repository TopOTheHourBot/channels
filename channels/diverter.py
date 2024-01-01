from __future__ import annotations

__all__ = ["Diverter"]

import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Optional, Self
from uuid import UUID

from .channel import Channel
from .exceptions import Closure


class Diverter[T]:

    __slots__ = ("_channels")
    _channels: dict[UUID, Channel[T]]

    def __init__(self) -> None:
        self._channels = {}

    def channels(self) -> Mapping[UUID, Channel[T]]:
        """Return a view of the currently-attached channels"""
        return self._channels

    def attach(self, channel: Channel[T]) -> UUID:
        """Attach ``channel`` and return its assigned token"""
        token = uuid.uuid4()
        self._channels[token] = channel
        return token

    def detach(self, token: UUID) -> bool:
        """Detach the channel assigned to ``token``, returning true if a
        corresponding channel was found, otherwise false
        """
        try:
            del self._channels[token]
        except KeyError:
            return False
        else:
            return True

    @contextmanager
    def attachment(self, channel: Optional[Channel[T]] = None) -> Iterator[Channel[T]]:
        """Return a context manager that safely attaches and detaches
        ``channel``

        Default-constructs a ``Channel`` instance if ``channel`` is ``None``.
        """
        if channel is None:
            channel = Channel()
        token = self.attach(channel)
        try:
            yield channel
        finally:
            self.detach(token)

    def send(self, value: T, /) -> None:
        """Send a value to all attached channels

        Channels that are closed but still attached at the moment of sending
        are detached.
        """
        tokens = []
        for token, channel in self.channels().items():
            try:
                channel.send(value)
            except Closure:
                tokens.append(token)
        for token in tokens:
            self.detach(token)

    def close(self) -> None:
        """Close and detach all attached channels"""
        tokens = list(self.channels())
        for channel in self.channels().values():
            channel.close()
        for token in tokens:
            self.detach(token)

    def clear(self) -> None:
        """Clear all attached channels"""
        for channel in self.channels().values():
            channel.clear()

    @contextmanager
    def closure(self) -> Iterator[Self]:
        """Return a context manager that ensures the diverter's closure upon
        exit
        """
        try:
            yield self
        finally:
            self.close()
