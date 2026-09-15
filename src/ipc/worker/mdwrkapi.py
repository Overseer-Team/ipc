from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import zmq
import zmq.asyncio

from ..core.codec import pack, unpack
from ..core.models import mdp
from ..core.models.errors import InvalidHeader, WorkerNotConnected

log: logging.Logger = logging.getLogger('ipc.worker')


class MDWorker:
    """One serial DEALER service; every received request must be replied to."""

    HEARTBEAT_LIVENESS: int = 3
    HEARTBEAT: int = 2500
    RECONNECT: int = 2500
    TIMEOUT: int = 2500

    def __init__(
        self,
        service_name: bytes,
        broker_ip: str,
        broker_port: int,
        *,
        log_level: int = logging.INFO,
        context: zmq.asyncio.Context | None = None,
    ) -> None:
        self.service = service_name
        self.broker: str = f'tcp://{broker_ip}:{broker_port}'
        self.ctx: zmq.asyncio.Context = context if context is not None else zmq.asyncio.Context()
        self._owns_context = context is None
        self.poller: zmq.asyncio.Poller = zmq.asyncio.Poller()
        self.liveness: int = self.HEARTBEAT_LIVENESS
        self.heartbeat_at: float = 0.0
        self.expect_reply: bool = False
        self.reply_to: bytes | None = None
        self.worker: zmq.asyncio.Socket | None = None
        self._closed = False
        log.setLevel(log_level)

    async def connect_to_broker(self) -> None:
        if self._closed:
            raise WorkerNotConnected('Worker is closed')
        if self.worker is not None:
            await self._disconnect()
            if self.worker in self.poller:
                self.poller.unregister(self.worker)
            self.worker.close(linger=0)

        self.worker = self.ctx.socket(zmq.DEALER)
        self.worker.linger = 0
        self.worker.connect(self.broker)
        self.poller.register(self.worker, zmq.POLLIN)
        self.expect_reply = False
        self.reply_to = None
        self.liveness = self.HEARTBEAT_LIVENESS
        await self.send_to_broker(mdp.W_READY, self.service)
        self.heartbeat_at = time.monotonic() + self.HEARTBEAT / 1000
        log.debug('Registered service %r at %s', self.service, self.broker)

    def close(self) -> None:
        """Close the socket and an owned context; borrowed contexts remain open."""
        if self._closed:
            return
        self._closed = True
        if self.worker is not None:
            if self.worker in self.poller:
                self.poller.unregister(self.worker)
            self.worker.close(linger=0)
            self.worker = None
        self.expect_reply = False
        self.reply_to = None
        if self._owns_context:
            self.ctx.term()

    async def _disconnect(self) -> None:
        if self.worker is not None and not self.worker.closed:
            try:
                # Best effort: shutdown must not wait on a disconnected/full peer.
                await self.worker.send_multipart(  # pyright: ignore[reportUnknownMemberType]
                    [b'', mdp.W_WORKER, mdp.W_DISCONNECT], flags=zmq.DONTWAIT
                )
            except zmq.ZMQError:
                log.debug('Could not disconnect service %r before closing', self.service, exc_info=True)

    async def aclose(self) -> None:
        """Notify the broker when possible, then unconditionally close resources."""
        try:
            await self._disconnect()
        finally:
            self.close()

    async def send_to_broker(
        self, command: bytes, option: bytes | None = None, msg: list[bytes] | bytes | None = None
    ) -> None:
        if self.worker is None:
            raise WorkerNotConnected('connect_to_broker() must be called prior to sending')
        frames = [b'', mdp.W_WORKER, command]
        if option is not None:
            frames.append(option)
        if isinstance(msg, bytes):
            frames.append(msg)
        elif msg is not None:
            frames.extend(msg)
        # PyZMQ's annotation omits the element type of Sequence.
        await self.worker.send_multipart(frames)  # pyright: ignore[reportUnknownMemberType]

    async def reply(self, message: object) -> None:
        if self.worker is None:
            raise WorkerNotConnected('connect_to_broker() must be called prior to replying')
        if not self.expect_reply or self.reply_to is None:
            raise RuntimeError('recv() must receive a request before reply()')

        # Encode before changing state: a bad result can still receive an error reply.
        payload = pack(message)
        await self.send_to_broker(mdp.W_REPLY, msg=[self.reply_to, b'', payload])
        self.expect_reply = False
        self.reply_to = None

    async def recv(self) -> Any:
        """Receive one decoded request. None is a valid MessagePack request.

        InvalidPayload leaves the reply address intact so the caller can reply with
        an error. InvalidHeader requires reconnecting to recover the conversation.
        """
        if self.worker is None:
            raise WorkerNotConnected('connect_to_broker() must be called prior to receiving')
        if self.expect_reply:
            raise RuntimeError('reply() must be called before receiving another request')

        while True:
            if await self.poller.poll(self.TIMEOUT):
                msg = await self.worker.recv_multipart()
                if len(msg) < 3 or msg[:2] != [b'', mdp.W_WORKER]:
                    raise InvalidHeader('Invalid worker envelope')
                command = msg[2]
                self.liveness = self.HEARTBEAT_LIVENESS
                if command == mdp.W_REQUEST:
                    if len(msg) != 6 or msg[4] != b'':
                        raise InvalidHeader('Invalid worker request envelope')
                    self.reply_to = msg[3]
                    self.expect_reply = True
                    return unpack(msg[5])
                if command == mdp.W_DISCONNECT and len(msg) == 3:
                    await self.connect_to_broker()
                elif command != mdp.W_HEARTBEAT or len(msg) != 3:
                    raise InvalidHeader(f'Invalid worker command: {command!r}')
            else:
                self.liveness -= 1
                if self.liveness <= 0:
                    log.warning('Disconnected from broker; reconnecting service %r', self.service)
                    await asyncio.sleep(self.RECONNECT / 1000)
                    await self.connect_to_broker()

            if time.monotonic() > self.heartbeat_at:
                await self.send_to_broker(mdp.W_HEARTBEAT)
                self.heartbeat_at = time.monotonic() + self.HEARTBEAT / 1000
