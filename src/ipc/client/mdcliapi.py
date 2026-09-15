from __future__ import annotations

import asyncio
import logging
from types import TracebackType
from typing import Any

import zmq
import zmq.asyncio

from ..core.codec import pack, unpack
from ..core.models.errors import InvalidHeader, InvalidPayload
from ..core.models.mdp import C_CLIENT

__all__ = ('MDClient',)
log: logging.Logger = logging.getLogger('ipc.client')


class MDClient:
    """A serial, retrying REQ client. Use one instance per independent request stream.

    Timeouts return None; retries can execute a handler more than once. Cancellation
    propagates and resets the socket before another caller can acquire the lock.
    """

    TIMEOUT: int = 2500
    RETRIES: int = 3

    def __init__(self, broker_ip: str, broker_port: int, *, log_level: int = logging.INFO) -> None:
        self.broker: str = f'tcp://{broker_ip}:{broker_port}'
        self.client: zmq.asyncio.Socket | None = None
        self.ctx: zmq.asyncio.Context = zmq.asyncio.Context()
        self.poller: zmq.asyncio.Poller = zmq.asyncio.Poller()
        self.lock: asyncio.Lock = asyncio.Lock()
        self._closed = False
        log.setLevel(log_level)
        try:
            self.connect_to_broker()
        except BaseException:
            self.close()
            raise

    def connect_to_broker(self) -> None:
        """Reset the REQ socket. External callers must exclude active requests."""
        if self._closed:
            raise RuntimeError('Client is closed')
        reconnect = self.client is not None
        if self.client is not None:
            if self.client in self.poller:
                self.poller.unregister(self.client)
            self.client.close(linger=0)

        self.client = self.ctx.socket(zmq.REQ)
        self.client.linger = 0
        self.client.connect(self.broker)
        self.poller.register(self.client, zmq.POLLIN)
        log.debug('%s to broker at %s', 'Reconnected' if reconnect else 'Connected', self.broker)

    def close(self) -> None:
        """Close this client's socket and context. Safe to call more than once."""
        if self._closed:
            return
        self._closed = True
        if self.client is not None:
            if self.client in self.poller:
                self.poller.unregister(self.client)
            self.client.close(linger=0)
            self.client = None
        self.ctx.term()

    async def __aenter__(self) -> MDClient:
        if self._closed:
            raise RuntimeError('Client is closed')
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        self.close()

    async def request(self, service: str | bytes, request: object) -> Any:
        """Return the decoded reply, or None after RETRIES failed attempts.

        Request encoding errors and use after close raise immediately. The caller
        owns the payload schema: replies are dynamically typed MessagePack values.
        """
        if self._closed:
            raise RuntimeError('Client is closed')
        service = service.encode() if isinstance(service, str) else service
        frames = [C_CLIENT, service, pack(request)]

        async with self.lock:
            for attempt in range(1, self.RETRIES + 1):
                if self.client is None:
                    raise RuntimeError('Client is closed')
                completed = False
                try:
                    # PyZMQ's annotation omits the element type of Sequence.
                    await self.client.send_multipart(frames)  # pyright: ignore[reportUnknownMemberType]
                    if await self.poller.poll(self.TIMEOUT):
                        reply = await self.client.recv_multipart()
                        if len(reply) != 4 or reply[:3] != [C_CLIENT, service, b'']:
                            raise InvalidHeader('Invalid client reply envelope')
                        result = unpack(reply[3], strict_map_key=False)
                        completed = True
                        return result
                    log.warning('No reply for service %r (attempt %s/%s)', service, attempt, self.RETRIES)
                except (InvalidHeader, InvalidPayload, zmq.ZMQError):
                    log.warning(
                        'Request to service %r failed (attempt %s/%s)', service, attempt, self.RETRIES, exc_info=True
                    )
                finally:
                    # Even the final timeout or a cancelled send/poll leaves a REQ
                    # socket awaiting a reply. Recover while still holding the lock.
                    if not completed and not self._closed:
                        self.connect_to_broker()

            log.warning('Retry limit exhausted for service %r', service)
            return None
