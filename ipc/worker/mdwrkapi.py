import asyncio
import logging
import time
from typing import Any

import zmq
import zmq.asyncio
import msgpack

from ..core.models import mdp
from ..core.models.errors import InvalidHeader, WorkerNotConnected

log = logging.getLogger('ipc.worker')


class MDWorker:
    HEARTBEAT_LIVENESS = 3  # 3-5 is reasonable
    HEARTBEAT = 2500
    RECONNECT = 2500
    TIMEOUT = 2500

    def __init__(self, service_name: bytes, broker_ip: str, broker_port: int, *, log_level=logging.INFO):
        self.service = service_name
        self.broker = f'tcp://{broker_ip}:{broker_port}'
        self.ctx = zmq.asyncio.Context()
        self.poller = zmq.asyncio.Poller()
        self.liveness = self.HEARTBEAT_LIVENESS
        self.expect_reply: bool = False
        self.reply_to = None
        self.worker = None

    async def connect_to_broker(self):
        reconnect = False
        if self.worker:
            self.poller.unregister(self.worker)
            self.worker.close()
            reconnect = True

        self.worker = self.ctx.socket(zmq.DEALER)
        self.worker.linger = 0
        self.worker.connect(self.broker)
        self.poller.register(self.worker, zmq.POLLIN)
        if reconnect:
            log.debug('Reconnecting to broker at %s', self.broker)
        else:
            log.info('Connecting to broker at %s', self.broker)

        # Register the service with the broker
        await self.send_to_broker(mdp.W_READY, self.service, [])

        self.heartbeat_at = time.time() + 1e-3 * self.HEARTBEAT

    async def send_to_broker(self, command, option=None, msg=None):
        """Sends a message to the broker."""
        if self.worker is None:
            raise WorkerNotConnected('connect_to_broker() must be called prior to sending')

        if msg is None:
            msg = []
        elif not isinstance(msg, list):
            msg = [msg]

        if option:
            msg = [option] + msg

        msg = [b'', mdp.W_WORKER, command] + msg
        log.debug('Sending %r to broker', command)
        await self.worker.send_multipart(msg)

    async def reply(self, message: Any):
        if self.worker is None:
            raise WorkerNotConnected('connect_to_broker() must be called prior to receiving')
        assert self.expect_reply
        assert self.reply_to is not None

        reply = [self.reply_to, b''] + [msgpack.packb(message, use_bin_type=True)]
        await self.send_to_broker(mdp.W_REPLY, msg=reply)
        self.expect_reply = False

    async def recv(self):
        if self.worker is None:
            raise WorkerNotConnected('connect_to_broker() must be called prior to receiving')

        self.expect_reply = True
        while True:
            try:
                items = await self.poller.poll(self.TIMEOUT)
            except KeyboardInterrupt:
                break

            if items:
                msg = await self.worker.recv_multipart()
                log.debug('Received message from broker: %s', msg)

                self.liveness = self.HEARTBEAT_LIVENESS
                assert len(msg) >= 3

                empty = msg.pop(0)
                assert empty == b''
                header = msg.pop(0)
                assert header == mdp.W_WORKER

                command = msg.pop(0)
                if command == mdp.W_REQUEST:
                    self.reply_to = msg.pop(0)
                    empty = msg.pop(0)
                    assert empty == b''

                    return msgpack.unpackb(msg[0], raw=False)
                elif command == mdp.W_HEARTBEAT:
                    # do nothing for heartbeats
                    pass
                elif command == mdp.W_DISCONNECT:
                    await self.connect_to_broker()
                else:
                    raise InvalidHeader(f'Message received with invalid header value of {header}; must be 0 or 1.')
            else:
                self.liveness -= 1
                if self.liveness == 0:
                    log.warn('Disconnected from broker - retrying...')
                    try:
                        await asyncio.sleep(1e-3 * self.RECONNECT)
                    except KeyboardInterrupt:
                        break
                    await self.connect_to_broker()

            # Send HEARTBEAT if it's time
            if time.time() > self.heartbeat_at:
                await self.send_to_broker(mdp.W_HEARTBEAT)
                self.heartbeat_at = time.time() + 1e-3 * self.HEARTBEAT

        return None
