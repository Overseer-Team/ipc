from __future__ import annotations


class MDPError(Exception):
    """Base class for transport protocol errors."""


class InvalidHeader(MDPError):
    """A message has an invalid envelope or command."""


class InvalidPayload(MDPError):
    """A request or reply body cannot be decoded as MessagePack."""


class WorkerNotConnected(MDPError):
    """The worker has not connected to the broker."""
