"""Static regressions: route registration must preserve signatures and descriptors."""

from __future__ import annotations

from typing_extensions import assert_type

from src.ipc.worker import route


class Application:
    pass


@route()
async def add(app: Application, data: dict[str, int]) -> int:
    return data['x'] + data['y']


class Handlers:
    @route('static_route')
    @staticmethod
    async def encode(app: Application, data: str) -> bytes:
        return data.encode()


async def check_signatures() -> None:
    assert_type(await add(Application(), {'x': 1, 'y': 2}), int)
    assert_type(await Handlers.encode(Application(), 'text'), bytes)
