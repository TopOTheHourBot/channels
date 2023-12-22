from __future__ import annotations

__all__ = ["Assembly"]

import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Optional
from uuid import UUID

from .channel import Channel


class Assembly[T]:

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
