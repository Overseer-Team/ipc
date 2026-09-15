from __future__ import annotations

import asyncio
import logging
import os
import time
from binascii import hexlify
from collections import OrderedDict, deque
from logging.handlers import RotatingFileHandler
from pathlib import Path
from types import TracebackType

import zmq
import zmq.asyncio

from ..core.models import mdp
from ..core.models.errors import InvalidHeader

log: logging.Logger = logging.getLogger('ipc.broker')


class Service:
    def __init__(self, name: bytes) -> None:
        self.name = name
        self.requests: deque[list[bytes]] = deque()
        self.waiting: OrderedDict[bytes, Worker] = OrderedDict()


class Worker:
    """An idle or active worker; identity is the hex-encoded ROUTER address."""

    def __init__(self, identity: bytes, address: bytes, lifetime: int) -> None:
        self.identity = identity
        self.address = address
        self.service: Service | None = None
        self.busy = False
        self.expiry: float = time.time() + lifetime / 1000


class MDBroker:
    INTERNAL_SERVICE_PREFIX: bytes = b'mmi.'
    HEARTBEAT_LIVENESS: int = 3
    HEARTBEAT_INTERVAL: int = 2500
    # Deployment-sensitive legacy timing: change separately with broker validation.
    HEARTBEAT_EXPIRY: int = HEARTBEAT_INTERVAL + HEARTBEAT_LIVENESS

    def __init__(self, host: str, port: int) -> None:
        self.endpoint: str = f'tcp://{host}:{port}'
        self.heartbeat_at: float = time.time() + self.HEARTBEAT_INTERVAL / 1000
        self.ctx: zmq.asyncio.Context = zmq.asyncio.Context()
        self.socket: zmq.asyncio.Socket = self.ctx.socket(zmq.ROUTER)
        self.socket.linger = 0
        self.poller: zmq.asyncio.Poller = zmq.asyncio.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        self.services: dict[bytes, Service] = {}
        self.workers: dict[bytes, Worker] = {}
        self.waiting: OrderedDict[bytes, Worker] = OrderedDict()

    async def mediate(self) -> None:
        errors = 0
        max_errors = 3
        while True:
            try:
                # Intentionally preserve the current traffic-driven maintenance
                # cadence. Awaiting this poll is a separate broker timing change.
                items = self.poller.poll(self.HEARTBEAT_INTERVAL)
                if items:
                    msg = await self.socket.recv_multipart()
                    if len(msg) < 3 or msg[1] != b'':
                        raise InvalidHeader('Invalid broker envelope')
                    sender, _, header, *body = msg
                    if header == mdp.C_CLIENT:
                        await self.process_client(sender, body)
                    elif header == mdp.W_WORKER:
                        await self.process_worker(sender, body)
                    else:
                        raise InvalidHeader(f'Unknown protocol header: {header!r}')
            except zmq.ZMQError as exc:
                errors += 1
                if errors >= max_errors:
                    log.critical('ZMQ error threshold reached after %s attempts', errors, exc_info=True)
                    raise
                wait_time = min(2**errors, 30)
                log.exception('ZMQ error (attempt %s/%s), retrying in %ss', errors, max_errors, wait_time)
                await asyncio.sleep(wait_time)
                if exc.errno in (zmq.ETERM, zmq.ENOTSOCK):
                    await self._recreate_socket()
            except InvalidHeader as exc:
                # Malformed peer input must not disable validation under python -OO
                # or force all other peers through the exception backoff.
                log.warning('Rejected message: %s', exc)
            except Exception:
                errors += 1
                if errors >= max_errors:
                    log.critical('Error threshold reached after %s attempts', errors, exc_info=True)
                    raise
                wait_time = min(2**errors, 30)
                log.exception('Unexpected error (attempt %s/%s), retrying in %ss', errors, max_errors, wait_time)
                await asyncio.sleep(wait_time)
            else:
                errors = 0

            await self.purge_workers()
            await self.send_heartbeats()

    async def _recreate_socket(self) -> None:
        log.info('Recreating socket')
        self.poller.unregister(self.socket)
        self.socket.close(linger=0)
        if self.ctx.closed:
            self.ctx = zmq.asyncio.Context()
        self.socket = self.ctx.socket(zmq.ROUTER)
        self.socket.linger = 0
        self.socket.bind(self.endpoint)
        self.poller.register(self.socket, zmq.POLLIN)
        # A new ROUTER cannot rely on the old connection/worker inventory.
        self.workers.clear()
        self.waiting.clear()
        for service in self.services.values():
            service.waiting.clear()

    async def send_heartbeats(self) -> None:
        if time.time() > self.heartbeat_at:
            for worker in self.waiting.values():
                await self.send_to_worker(worker, mdp.W_HEARTBEAT, None, None)
            self.heartbeat_at = time.time() + self.HEARTBEAT_INTERVAL / 1000

    def require_service(self, name: bytes) -> Service:
        service = self.services.get(name)
        if service is None:
            service = Service(name)
            self.services[name] = service
        return service

    async def delete_worker(self, worker: Worker, disconnect: bool) -> None:
        if disconnect:
            await self.send_to_worker(worker, mdp.W_DISCONNECT, None, None)
        self.waiting.pop(worker.identity, None)
        if worker.service is not None:
            worker.service.waiting.pop(worker.identity, None)
        self.workers.pop(worker.identity, None)

    async def purge_workers(self) -> None:
        """Preserve oldest-first expiry checks and stop at the first live worker."""
        while self.waiting:
            worker = next(iter(self.waiting.values()))
            if worker.expiry >= time.time():
                break
            log.debug('Deleting expired worker: %s', worker.identity)
            await self.delete_worker(worker, False)

    async def send_to_worker(
        self, worker: Worker, command: bytes, option: bytes | None, msg: list[bytes] | bytes | None
    ) -> None:
        frames = [worker.address, b'', mdp.W_WORKER, command]
        if option is not None:
            frames.append(option)
        if isinstance(msg, bytes):
            frames.append(msg)
        elif msg is not None:
            frames.extend(msg)
        # Preserve the legacy enqueue-without-await behavior in this review.
        # PyZMQ's annotation omits the element type of Sequence.
        self.socket.send_multipart(frames)  # pyright: ignore[reportUnknownMemberType]

    async def dispatch(self, service: Service, msg: list[bytes] | None) -> None:
        if msg is not None:
            service.requests.append(msg)
        await self.purge_workers()
        while service.waiting and service.requests:
            request = service.requests.popleft()
            _, worker = service.waiting.popitem(last=False)
            self.waiting.pop(worker.identity, None)
            worker.busy = True
            await self.send_to_worker(worker, mdp.W_REQUEST, None, request)

    async def process_client(self, sender: bytes, msg: list[bytes]) -> None:
        if len(msg) < 2:
            raise InvalidHeader('Client request requires a service and payload')
        service, *payload = msg
        await self.dispatch(self.require_service(service), [sender, b'', *payload])

    def require_worker(self, address: bytes) -> Worker:
        identity = hexlify(address)
        worker = self.workers.get(identity)
        if worker is None:
            worker = Worker(identity, address, self.HEARTBEAT_EXPIRY)
            self.workers[identity] = worker
            log.debug('Registered a new worker: %s', identity)
        return worker

    async def process_worker(self, sender: bytes, msg: list[bytes]) -> None:
        if not msg:
            raise InvalidHeader('Worker message requires a command')
        command, *body = msg
        if command == mdp.W_READY:
            valid = len(body) == 1
        elif command == mdp.W_REPLY:
            valid = len(body) >= 3 and body[1] == b''
        elif command in (mdp.W_HEARTBEAT, mdp.W_DISCONNECT):
            valid = not body
        else:
            raise InvalidHeader(f'Unknown worker command: {command!r}')
        if not valid:
            raise InvalidHeader(f'Invalid envelope for worker command: {command!r}')

        worker_ready = hexlify(sender) in self.workers
        worker = self.require_worker(sender)
        if command == mdp.W_READY:
            service = body[0]
            if worker_ready or service.startswith(self.INTERNAL_SERVICE_PREFIX):
                await self.delete_worker(worker, True)
            else:
                worker.service = self.require_service(service)
                await self.worker_waiting(worker)
        elif command == mdp.W_REPLY:
            if worker_ready and worker.service is not None and worker.busy:
                client, *payload = body
                await self.socket.send_multipart(  # pyright: ignore[reportUnknownMemberType]
                    [client, b'', mdp.C_CLIENT, worker.service.name, *payload]
                )
                await self.worker_waiting(worker)
            else:
                await self.delete_worker(worker, True)
        elif command == mdp.W_HEARTBEAT:
            if worker_ready:
                worker.expiry = time.time() + self.HEARTBEAT_EXPIRY / 1000
                # Keep the idle list ordered by expiry, or purge_workers stops early
                # at this refreshed worker and never reaches expired ones behind it.
                if worker.identity in self.waiting:
                    self.waiting.move_to_end(worker.identity)
            else:
                await self.delete_worker(worker, True)
        else:  # W_DISCONNECT
            await self.delete_worker(worker, False)

    async def worker_waiting(self, worker: Worker) -> None:
        if worker.service is None:
            raise InvalidHeader('Worker has not registered a service')
        worker.busy = False
        self.waiting[worker.identity] = worker
        worker.service.waiting[worker.identity] = worker
        worker.expiry = time.time() + self.HEARTBEAT_EXPIRY / 1000
        await self.dispatch(worker.service, None)

    def bind(self) -> None:
        self.socket.bind(self.endpoint)
        log.info('MDP broker is active at %s', self.endpoint)

    def close(self) -> None:
        """Release the broker socket and context after mediate() has stopped."""
        if not self.socket.closed:
            self.poller.unregister(self.socket)
            self.socket.close(linger=0)
        self.ctx.term()


class SetupLogging:
    def __init__(self, *, stream: bool = True) -> None:
        self.log: logging.Logger = logging.getLogger()
        self.max_bytes = 32 * 1024
        self.logging_path: Path = Path('./logs/')
        self.stream = stream
        self.handlers: list[logging.Handler] = []
        self.previous_level: int = self.log.level

    def __enter__(self) -> SetupLogging:
        self.logging_path.mkdir(exist_ok=True)
        self.previous_level = self.log.level
        self.log.setLevel(logging.INFO)
        handler = RotatingFileHandler(
            filename=self.logging_path / 'broker.log', encoding='utf-8', maxBytes=self.max_bytes, backupCount=5
        )
        self.handlers = [handler]
        if self.stream:
            self.handlers.append(logging.StreamHandler())
        fmt = logging.Formatter('[{asctime}] [{levelname}] {name}: {message}', '%Y-%m-%d %H:%M:%S', style='{')
        for handler in self.handlers:
            handler.setFormatter(fmt)
            self.log.addHandler(handler)
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        for handler in self.handlers:
            self.log.removeHandler(handler)
            handler.close()
        self.handlers.clear()
        self.log.setLevel(self.previous_level)


async def main() -> None:
    broker = MDBroker(host=os.getenv('BROKER_HOST', '127.0.0.1'), port=int(os.getenv('BROKER_PORT', '5555')))
    try:
        broker.bind()
        await broker.mediate()
    finally:
        broker.close()


if __name__ == '__main__':
    with SetupLogging():
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            pass
