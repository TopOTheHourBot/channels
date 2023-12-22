from __future__ import annotations

__all__ = ["Diverter"]

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Self

from .assembly import Assembly
from .exceptions import Closure


class Diverter[T](Assembly[T]):

    __slots__ = ()

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
