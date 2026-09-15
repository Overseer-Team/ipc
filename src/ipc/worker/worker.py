from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import Any, ClassVar, TypeVar

import zmq
import zmq.asyncio

from ..core.models.errors import InvalidHeader, InvalidPayload, WorkerNotConnected
from .mdwrkapi import MDWorker

__all__ = ('route', 'IPC')
log: logging.Logger = logging.getLogger('ipc.worker')

# Each route owns its application schema and bot type. Keep that heterogeneity at
# the registry boundary, and preserve the exact callable type through the decorator.
Route = Callable[[Any, Any], Awaitable[object]]
RouteT = TypeVar('RouteT', bound=Callable[..., Awaitable[object]])


def route(name: str | None = None) -> Callable[[RouteT], RouteT]:
    """Register a coroutine before IPC.start(), preserving its signature.

    Re-registration replaces the previous function, allowing module reloads. An
    already running worker keeps its captured function until IPC is restarted.
    """

    def decorator(func: RouteT) -> RouteT:
        service = name or func.__name__
        previous = IPC.ROUTES.get(service)
        if previous is not None and previous is not func:
            # A reloaded module re-registers the same definition; a second definition
            # elsewhere is a name collision that would silently shadow the first.
            level = logging.DEBUG if _origin(previous) == _origin(func) else logging.WARNING
            log.log(level, 'Replacing registered IPC route %r', service)
        IPC.ROUTES[service] = func
        return func

    return decorator


def _origin(func: object) -> tuple[object, object]:
    """Identify where a route was defined; staticmethod objects carry these since 3.10."""
    return getattr(func, '__module__', None), getattr(func, '__qualname__', None)


def _error_reply(exc: Exception) -> dict[str, str]:
    return {'error': f'{type(exc).__name__}: {exc}'}


async def _serve(worker: MDWorker, bot: object, func: Route) -> None:
    while True:
        try:
            request = await worker.recv()
        except InvalidPayload as exc:
            log.warning('Invalid request for route %r: %s', worker.service, exc)
            result = _error_reply(exc)
        else:
            try:
                result = await func(bot, request)
            except Exception as exc:
                log.exception('Route handler %r raised an exception', worker.service)
                result = _error_reply(exc)
        try:
            await worker.reply(result)
        except (TypeError, ValueError, OverflowError, RecursionError) as exc:
            log.exception('Route %r returned an unencodable reply', worker.service)
            await worker.reply(_error_reply(exc))


async def _worker(
    service_name: str,
    broker_ip: str,
    broker_port: int,
    bot: object,
    func: Route,
    *,
    log_level: int = logging.INFO,
    context: zmq.asyncio.Context | None = None,
) -> None:
    worker = MDWorker(service_name.encode(), broker_ip, broker_port, log_level=log_level, context=context)
    try:
        while True:
            try:
                await worker.connect_to_broker()
                await _serve(worker, bot, func)
            except (InvalidHeader, WorkerNotConnected, zmq.ZMQError):
                log.exception('Transport failed for route %r; reconnecting', service_name)
                await asyncio.sleep(worker.RECONNECT / 1000)
    finally:
        await worker.aclose()


def log_errors(task: asyncio.Task[None]) -> None:
    if not task.cancelled():
        exc = task.exception()
        if exc is not None:
            log.error('IPC worker %s died with an exception', task.get_name(), exc_info=exc)


class IPC:
    """Own a snapshot of the process-wide route registry and its worker tasks."""

    ROUTES: ClassVar[dict[str, Route]] = {}

    def __init__(self, bot: object, *, broker_ip: str = '127.0.0.1', broker_port: int = 5555) -> None:
        self.bot = bot
        self.ip = broker_ip
        self.port = broker_port
        self.tasks: set[asyncio.Task[None]] = set()
        self._lifecycle_lock = asyncio.Lock()
        self._context: zmq.asyncio.Context | None = None

    async def start(self) -> None:
        """Start one task per registered route. Repeated calls are harmless."""
        async with self._lifecycle_lock:
            if self.tasks or not self.ROUTES:
                return
            # Share the I/O thread across route sockets, with explicit ownership.
            if self._context is None:
                self._context = zmq.asyncio.Context()
            for name, func in tuple(self.ROUTES.items()):
                task = asyncio.create_task(
                    _worker(name, self.ip, self.port, self.bot, func, context=self._context), name=f'ipc__{name}'
                )
                task.add_done_callback(log_errors)
                task.add_done_callback(self.tasks.discard)
                self.tasks.add(task)

    async def stop(self) -> None:
        """Cancel handlers and wait until every worker has closed its resources."""
        async with self._lifecycle_lock:
            tasks = tuple(self.tasks)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.tasks.difference_update(tasks)
            if self._context is not None:
                self._context.term()
                self._context = None

    async def __aenter__(self) -> IPC:
        await self.start()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        await self.stop()
