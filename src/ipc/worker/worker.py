from __future__ import annotations

import logging
import asyncio
from typing import TYPE_CHECKING, Callable, Any

from .mdwrkapi import MDWorker

if TYPE_CHECKING:
    from discord.ext.commands import Bot

__all__ = ('route', 'IPC')
log = logging.getLogger('ipc.worker')


def route(name: str | None = None):
    def decorator(func: Callable):
        IPC.ROUTES[name or func.__name__] = func

        return func

    return decorator


async def _worker(service_name: str, broker_ip: str, broker_port: int, bot: Bot, func: Callable, *, log_level=logging.INFO):
    worker_ = MDWorker(service_name.encode(), broker_ip, broker_port, log_level=log_level)
    await worker_.connect_to_broker()

    while True:
        request = await worker_.recv()
        log.debug('WORKER [%s] received message %s', service_name, request)
        if request is None:
            break

        await worker_.reply(await func(bot, request))


def log_errors(task: asyncio.Task):
    log.error('An exception has been raised in an IPC worker (%s)', task.get_name(), exc_info=task.exception())


class IPC:
    ROUTES: dict[str, Any] = {}

    def __init__(self, bot: Bot, *, broker_ip: str = '127.0.0.1', broker_port: int = 5555):
        self.bot = bot
        self.ip = broker_ip
        self.port = broker_port
        self.tasks = set()

    async def start(self):
        for route, func in self.ROUTES.items():
            task = asyncio.create_task(_worker(route, self.ip, self.port, self.bot, func), name=f'ipc__{route}')
            task.add_done_callback(log_errors)
            self.tasks.add(task)
