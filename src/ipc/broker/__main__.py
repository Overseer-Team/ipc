import asyncio
import os
import time
import logging
import pathlib
from logging.handlers import RotatingFileHandler
from binascii import hexlify
from typing import Any

import zmq
import zmq.asyncio

from ..core.models import mdp
from ..core.models.errors import InvalidHeader

log = logging.getLogger('ipc.broker')


class Service:
    def __init__(self, name: str):
        self.name = name
        self.requests = []
        self.waiting = []


class Worker:
    """An idle or active worker"""
    service = None

    def __init__(self, identity: bytes, address: str, lifetime: int):
        self.identity = identity
        self.address = address
        self.expiry = time.time() + 1e-3*lifetime


class MDBroker:
    INTERNAL_SERVICE_PREFIX = b"mmi."
    HEARTBEAT_LIVENESS = 3  # 3-5 is reasonable
    HEARTBEAT_INTERVAL = 2500
    HEARTBEAT_EXPIRY = HEARTBEAT_INTERVAL + HEARTBEAT_LIVENESS
    ROUTES: dict[str, Any] = {}

    def __init__(self, host: str, port: int):
        self.endpoint = f'tcp://{host}:{port}'
        self.heartbeat_at = time.time() + 1e-3*self.HEARTBEAT_INTERVAL
        self.ctx = zmq.asyncio.Context()
        self.socket = self.ctx.socket(zmq.ROUTER)
        self.socket.linger = 0
        self.poller = zmq.asyncio.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        self.services: dict[str, Service] = {}
        self.workers: dict[bytes, Worker] = {}
        self.waiting = []

    async def mediate(self):
        errors = 0
        max_errors = 3

        while True:
            try:
                items = self.poller.poll(self.HEARTBEAT_INTERVAL)

                if items:
                    msg = await self.socket.recv_multipart()
                    log.debug('Received message %s', msg)

                    # ZMQ ROUTER prepends a unique identifier for the sender for every send
                    # this is followed by a null byte: b''
                    sender = msg.pop(0)
                    empty = msg.pop(0)
                    assert empty == b''
                    header = msg.pop(0)

                    if mdp.C_CLIENT == header:
                        await self.process_client(sender, msg)
                    elif mdp.W_WORKER == header:
                        await self.process_worker(sender, msg)
                    else:
                        raise InvalidHeader(f'Message received with invalid header value of {header}; must be 0 or 1.')
            except KeyboardInterrupt:
                break
            except zmq.ZMQError as e:
                errors += 1
                if errors >= max_errors:
                    log.critical(f"ZMQ error threshold reached after {errors} attempts: {e}")
                    break
                
                wait_time = min(2 ** errors, 30)  # exponential backoff, max 30s
                log.exception(f"ZMQ error (attempt {errors}/{max_errors}), retrying in {wait_time}s: {e}")
                await asyncio.sleep(wait_time)
                
                if e.errno in (zmq.ETERM, zmq.ENOTSOCK):
                    await self._recreate_socket()
            except InvalidHeader:
                log.warning('Received unknown header with no associated worker: %s', header)  # pyright: ignore[reportPossiblyUnboundVariable] 
            except Exception:
                if errors >= max_errors:
                    log.critical(f"Error threshold reached after {errors} attempts", exc_info=True)
                    break
                    
                wait_time = min(2 ** errors, 30)
                log.exception(f"Unexpected error (attempt {errors}/{max_errors}), retrying in {wait_time}s")
                await asyncio.sleep(wait_time)
            else:
                errors = 0

            try:
                await self.purge_workers()
                await self.send_heartbeats()
            except Exception as e:
                log.error(f"Error in maintenance tasks: {e}")

    async def _recreate_socket(self):
        log.info('Recreating socket')
        
        self.poller.unregister(self.socket)
        self.socket.close()
        
        self.socket = self.ctx.socket(zmq.ROUTER)
        self.socket.linger = 0
        self.socket.bind(self.endpoint)  # store endpoint in __init__
        
        self.poller.register(self.socket, zmq.POLLIN)

    async def send_heartbeats(self):
        if time.time() > self.heartbeat_at:
            for worker in self.waiting:
                await self.send_to_worker(worker, mdp.W_HEARTBEAT, None, None)

            self.heartbeat_at = time.time() + 1e-3*self.HEARTBEAT_INTERVAL

    def require_service(self, name: str):
        """Locates a service (or creates one if necessary)"""
        assert name is not None
        service = self.services.get(name)
        if service is None:
            service = Service(name)
            self.services[name] = service

        return service

    async def delete_worker(self, worker: Worker, disconnect: bool):
        """Deletes worker from all data structures, and deletes worker."""
        if disconnect:
            await self.send_to_worker(worker, mdp.W_DISCONNECT, None, None)

        if worker.service is not None:
            worker.service.waiting.remove(worker)
        self.workers.pop(worker.identity)

    async def purge_workers(self):
        """Look for and kill expired workers.

        Workers are oldest to most recent, so we stop at the first alive worker.
        """
        while self.waiting:
            w = self.waiting[0]
            if w.expiry < time.time():
                log.debug("Deleting expired worker: %s", w.identity)
                await self.delete_worker(w, False)
                self.waiting.pop(0)
            else:
                break

    async def send_to_worker(self, worker: Worker, command: Any | None, option: Any | None, msg):
        """Sends a message to a worker"""
        if msg is None:
            msg = []
        elif not isinstance(msg, list):
            msg = [msg]

        if option is not None:
            msg = [option] + msg
        msg = [worker.address, b'', mdp.W_WORKER, command] + msg

        log.debug('Sending %r to worker', command)
        self.socket.send_multipart(msg)

    async def dispatch(self, service: Service, msg):
        if msg is not None:
            service.requests.append(msg)

        await self.purge_workers()
        while service.waiting and service.requests:
            msg = service.requests.pop(0)
            worker = service.waiting.pop(0)
            self.waiting.remove(worker)
            await self.send_to_worker(worker, mdp.W_REQUEST, None, msg)

    async def process_client(self, sender, msg):
        """Processes a request coming from a client"""
        assert len(msg) >= 2
        service = msg.pop(0)
        msg = [sender, b''] + msg

        await self.dispatch(self.require_service(service), msg)

    def require_worker(self, address):
        """Locates a worker (or creates one if necessary)"""
        identity = hexlify(address)
        worker = self.workers.get(identity)
        if worker is None:
            worker = Worker(identity, address, self.HEARTBEAT_EXPIRY)
            self.workers[identity] = worker
            log.debug('Registered a new worker: %s', identity)

        return worker

    async def process_worker(self, sender, msg):
        """Processes a message sent from a worker"""
        command = msg.pop(0)
        worker_ready = hexlify(sender) in self.workers
        worker = self.require_worker(sender)

        if mdp.W_READY == command:
            service = msg.pop(0)
            if worker_ready or service.startswith(self.INTERNAL_SERVICE_PREFIX):
                await self.delete_worker(worker, True)
            else:
                worker.service = self.require_service(service)
                await self.worker_waiting(worker)
        elif mdp.W_REPLY == command:
            if worker_ready:
                client = msg.pop(0)
                msg = [client, b'', mdp.C_CLIENT, worker.service.name] + msg
                await self.socket.send_multipart(msg)
                await self.worker_waiting(worker)
            else:
                await self.delete_worker(worker, True)
        elif mdp.W_HEARTBEAT == command:
            if worker_ready:
                worker.expiry = time.time() + 1e-3*self.HEARTBEAT_EXPIRY
            else:
                await self.delete_worker(worker, True)
        elif mdp.W_DISCONNECT == command:
            await self.delete_worker(worker, False)
        else:
            raise InvalidHeader(f'Command does not match any existing worker actions: {command}')

    async def worker_waiting(self, worker: Worker):
        self.waiting.append(worker)
        worker.service.waiting.append(worker)
        worker.expiry = time.time() + 1e-3*self.HEARTBEAT_EXPIRY
        await self.dispatch(worker.service, None)

    def bind(self):
        self.socket.bind(self.endpoint)
        log.info('MDP broker is active at %s', self.endpoint)


class SetupLogging:
    def __init__(self, *, stream: bool = True) -> None:
        self.log: logging.Logger = logging.getLogger()
        self.max_bytes: int = 32 * 1024
        self.logging_path = pathlib.Path("./logs/")
        self.logging_path.mkdir(exist_ok=True)
        self.stream: bool = stream

    def __enter__(self):
        self.log.setLevel(logging.INFO)
        handler = RotatingFileHandler(
            filename=self.logging_path / "broker.log", encoding="utf-8", mode="w", maxBytes=self.max_bytes, backupCount=5
        )
        dt_fmt = "%Y-%m-%d %H:%M:%S"
        fmt = logging.Formatter("[{asctime}] [{levelname}] {name}: {message}", dt_fmt, style="{")
        handler.setFormatter(fmt)
        self.log.addHandler(handler)

        if self.stream:
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(fmt)
            self.log.addHandler(stream_handler)

        return self

    def __exit__(self, *args: Any) -> None:
        handlers = self.log.handlers[:]
        for hdlr in handlers:
            hdlr.close()
            self.log.removeHandler(hdlr)


if __name__ == '__main__':
    with SetupLogging():
        broker = MDBroker(host=os.getenv('BROKER_HOST', '0.0.0.0'), port=int(os.getenv('BROKER_PORT', 5555)))
        broker.bind()
        asyncio.run(broker.mediate())
