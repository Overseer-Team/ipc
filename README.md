# IPC — asynchronous Majordomo transport

A Python 3.10+ transport built on ZeroMQ and MessagePack. A ROUTER broker connects
REQ clients to DEALER workers. Each route has one socket and handles one request
at a time; different routes run independently.

The broker forwards opaque payload bytes. Application schemas, authorization, and
idempotency belong to the services using this package.

## Installation and development

```bash
pip install 'git+https://github.com/Overseer-Team/ipc.git'
```

The `worker` extra remains available for existing installations, but the transport
itself does not import or require Discord. `IPC` accepts any application object,
including `None`.

From a checkout, install the locked development tools and run the checks:

```bash
pdm install -dG dev
pdm run ruff check src tests
pdm run ruff format --check src tests
pdm run pyright
pdm run python -m unittest discover -s tests -v
pdm run python -OO -m unittest discover -s tests -v
```

Tests use local TCP sockets on dynamically allocated ports. They need no running
broker or consumer application. Source annotations are checked in strict mode for
Python 3.10; the installed package includes `py.typed`.

## Running the broker

```bash
pdm run python -m src.ipc.broker  # from this checkout
python -m ipc.broker            # when installed
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `BROKER_HOST` | `127.0.0.1` | Interface to bind |
| `BROKER_PORT` | `5555` | TCP port |

The broker logs to a stream and rotating `./logs/broker.log`. For Docker Compose:

```yaml
services:
  broker:
    build: https://github.com/Overseer-Team/ipc.git
    restart: unless-stopped
    environment:
      BROKER_HOST: 0.0.0.0
      BROKER_PORT: 5555
```

The Compose build follows GitHub's default branch; consumer lockfiles pin their
own revisions. Local edits do not update deployed services. The wire format must
remain compatible when broker and consumers run different revisions.

## Client

```python
import asyncio
from ipc.client import MDClient

async def main() -> None:
    async with MDClient('127.0.0.1', 5555) as client:
        result = await client.request('add', {'x': 6, 'y': 7})
        print(result)  # 13 when the worker below is running

asyncio.run(main())
```

A client serializes all calls behind an `asyncio.Lock`. Use separate instances for
request streams that must not wait for each other. The default is three attempts,
with a 2500 ms reply poll per attempt. The client returns `None` when attempts are
exhausted. A handler can also return `None`, so applications requiring an explicit
success result should use a different payload.

Retries can deliver the same request more than once, including after the original
caller has stopped waiting. Handlers must be idempotent. Cancellation propagates
and resets the REQ socket while holding the lock, so subsequent calls can proceed.
For an overall deadline, wrap `request()` in `asyncio.wait_for()`; the per-attempt
poll timeout does not include time waiting for the lock or sending frames.

Encoding errors and use after `close()` raise. Replies have type `Any` because the
transport cannot validate an application's schema. Decode or validate them at the
consumer boundary. Request map keys must be strings or bytes; integer keys remain
supported in replies for existing consumers.

Use the async context manager or call the idempotent `client.close()` explicitly.
The public `connect_to_broker()` reset remains available; external calls must not
race an active request. Objects and sockets belong to one event loop/thread.

## Workers

```python
import asyncio
from ipc.worker import IPC, route

@route()
async def add(bot: object, data: dict[str, int]) -> int:
    return data['x'] + data['y']

async def main() -> None:
    async with IPC(bot=None, broker_ip='127.0.0.1', broker_port=5555):
        await asyncio.Event().wait()

asyncio.run(main())
```

`@route()` preserves the decorated function's type and registers it globally at
import time. Import every route module before calling `await ipc.start()`. A
repeated name replaces the registered function: quietly when the same definition
is re-imported (a module reload), with a warning when a different definition takes
the name. Existing worker tasks keep their captured function until stopped and
restarted. Stacking `@route(...)` above `@staticmethod` remains supported on
Python 3.10+.

`start()` creates background tasks and returns immediately. Repeated starts are
harmless while running. `await ipc.stop()` cancels handlers, waits for socket
cleanup, and terminates the shared ZeroMQ context. The same `IPC` can then be
started again. Stopping may interrupt in-flight work; it is not a queue drain.

Handler exceptions, invalid MessagePack requests, and unencodable results produce
`{'error': '<Type>: <message>'}` replies without terminating the route. A malformed
transport envelope or ZeroMQ error triggers a delayed worker reconnect.
Cancellation always propagates. Unexpected programming errors outside these
boundaries are logged as task failures.

Each `IPC` shares one context across its workers, with one DEALER socket per
route. Direct `MDWorker` users must connect before receiving, reply before the
next receive, and use `await worker.aclose()` when finished. Async cleanup and
reconnect send a best-effort disconnect notification; `close()` provides immediate
local cleanup. A supplied `context=` is
borrowed and remains open when that worker closes.

## Wire format

This is the project's existing Majordomo-style protocol, with ASCII command bytes
`b'0'` through `b'6'`; it is not an implementation of the standard MDP signatures.
The following frames are shown at each endpoint's socket API. ROUTER sockets
add/remove a routing address outside these frames.

| Direction | Multipart frames |
| --- | --- |
| Client → broker | `[C_CLIENT, service, packed_request]` |
| Broker → client | `[C_CLIENT, service, b'', packed_reply]` |
| Worker → broker, registration | `[b'', W_WORKER, W_READY, service]` |
| Broker → worker, request | `[b'', W_WORKER, W_REQUEST, client_address, b'', packed_request]` |
| Worker → broker, reply | `[b'', W_WORKER, W_REPLY, client_address, b'', packed_reply]` |
| Heartbeat / disconnect | `[b'', W_WORKER, command]` |

Client REQ sockets supply an additional empty delimiter on the ROUTER side.
Bodies use `msgpack.packb(..., use_bin_type=True)` and `raw=False` on decode.
Service names and routing addresses are bytes. Envelope validation uses explicit
exceptions and remains active under `python -OO`.

## Operational limits

The broker is unauthenticated and has no durable request storage. Applications
must authenticate sensitive routes themselves. Queues for absent services have
no cap or expiry, and expired client requests can be delivered to a later worker.

The broker's legacy unawaited poll/send and 2503 ms worker expiry are deliberately
preserved in this review because deployment timing changes require separate
validation. Maintenance still runs when traffic arrives. See the
[review record](docs/transport-review.md) for the changes, evidence, and remaining
work.

## License

MIT; see [LICENSE](LICENSE).
