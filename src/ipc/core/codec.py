"""MessagePack boundary; application schemas deliberately remain with callers."""

from __future__ import annotations

from typing import Any, cast

import msgpack  # pyright: ignore[reportMissingTypeStubs]

from .models.errors import InvalidPayload


def pack(value: object) -> bytes:
    # Preserve the underlying encoding exception for callers and route error replies.
    # msgpack's C extension does not ship types; contain that gap at this boundary.
    return cast(bytes, msgpack.packb(value, use_bin_type=True))  # pyright: ignore[reportUnknownMemberType]


def unpack(payload: bytes, *, strict_map_key: bool = True) -> Any:
    try:
        return cast(Any, msgpack.unpackb(payload, raw=False, strict_map_key=strict_map_key))  # pyright: ignore[reportUnknownMemberType]
    except (ValueError, TypeError, msgpack.UnpackException) as exc:
        raise InvalidPayload(f'{type(exc).__name__}: {exc}') from exc
