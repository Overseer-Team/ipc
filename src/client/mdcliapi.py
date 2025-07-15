import asyncio
import logging
from typing import Any

import zmq
import zmq.asyncio
import msgpack

from ..core.models.mdp import C_CLIENT

log = logging.getLogger('ipc.client')


class MDClient:
    TIMEOUT = 2500
    RETRIES = 3

    def __init__(self, broker_ip: str, broker_port: int, *, log_level=logging.INFO):
        self.broker = f'tcp://{broker_ip}:{broker_port}'
        self.client = None
        self.ctx = zmq.asyncio.Context()
        self.poller = zmq.asyncio.Poller()
        self.lock = asyncio.Lock()
        log.setLevel(log_level)
        self.connect_to_broker()

    def connect_to_broker(self):
        reconnect = False
        if self.client:
            self.poller.unregister(self.client)
            self.client.close()
            reconnect = True

        self.client = self.ctx.socket(zmq.REQ)
        self.client.linger = 0
        self.client.connect(self.broker)
        self.poller.register(self.client, zmq.POLLIN)
        if reconnect:
            log.info('Reconnected to broker at %s', self.broker)
        else:
            log.info('Connected to broker at %s', self.broker)

    async def request(self, service: str | bytes, request: Any):
        """Send a request to the broker and receive a reply"""
        assert self.client is not None

        if isinstance(service, str):
            service = service.encode()
        request = [C_CLIENT, service] + [msgpack.packb(request, use_bin_type=True)]

        async with self.lock:
            retries = 1
            while retries <= self.RETRIES:
                log.debug('Sending multipart request: %s', request)
                await self.client.send_multipart(request)
                try:
                    items = await self.poller.poll(self.TIMEOUT)
                except KeyboardInterrupt:
                    break

                if items:
                    msg = await self.client.recv_multipart()
                    log.debug('Received reply: %s', msg)

                    assert len(msg) >= 3
                    header = msg.pop(0)
                    assert C_CLIENT == header
                    reply_service = msg.pop(0)
                    assert service == reply_service

                    reply = msgpack.unpackb(msg[1], raw=False)
                    break
                else:
                    if retries <= self.RETRIES:
                        log.warning('No reply received, reconnecting...')
                        self.connect_to_broker()
                    else:
                        log.warning('Retry limit exhausted (%s/%s), aborting...', retries - 1, self.RETRIES)
                        break
                    retries -= 1

            return reply
