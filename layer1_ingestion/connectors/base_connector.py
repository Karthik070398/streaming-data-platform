"""
This file defines the "contract" every source and every sink must follow.

Why bother with this? Because the engine (engine.py) doesn't want to know
whether it's talking to a fake data generator, a real Postgres database, or
something else entirely. It just wants to call `.get_event()` on ANY source,
and `.send(event)` on ANY sink. This is called an "interface" -- it's a
promise that every connector will have these methods, so they're interchangeable.

This is exactly how you can later swap "fake generator" for "real Postgres"
without touching engine.py at all -- that's the whole point.
"""

from abc import ABC, abstractmethod


class BaseSource(ABC):
    """Any data SOURCE (where events come FROM) must implement this."""

    @abstractmethod
    def get_event(self) -> dict:
        """Return one event as a Python dictionary."""
        raise NotImplementedError


class BaseSink(ABC):
    """Any data SINK (where events go TO) must implement this."""

    @abstractmethod
    def send(self, event: dict) -> None:
        """Send one event dictionary to the destination."""
        raise NotImplementedError

    def close(self) -> None:
        """Optional cleanup when the engine shuts down. Override if needed."""
        pass
