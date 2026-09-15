# Transport review — 2026-09-10

The review preserves the existing frames and application contract while repairing
failure boundaries and resource ownership. The broker is built from an unpinned
GitHub branch while consumers pin revisions, so changes must work across mixed
versions. This work changes no route schemas or authentication policy.

## Findings and changes

| Priority | Finding | Change |
| --- | --- | --- |
| High | Cancellation after a REQ send leaves the socket waiting for a reply, poisoning the next caller. | Reset failed/cancelled attempts in `finally`, under the request lock, including the final timeout. Cancellation still propagates. |
| High | Bad request decoding or result encoding permanently terminates a route. | Preserve the reply address across decoding/encoding errors, send the existing error-map shape, and continue serving. Protocol/socket failures reconnect with a delay. |
| High | `assert` is used for protocol validation, but production uses `-OO`. | Validate frame count, delimiter, protocol and command explicitly at each edge and at the broker. Reject malformed broker input without sleeping the entire broker loop. |
| High | Worker deletion can fail for busy workers and can leave stale entries in the global idle list. Duplicate replies can put workers in the idle pool repeatedly. | Keep both ordered idle indexes consistent, track busy state, and reject unsolicited replies. |
| Medium | Every route allocates its own context and I/O thread, and nothing explicitly closes resources. | Share one context per `IPC`; add client/worker/broker cleanup and `IPC.stop()`, plus async context managers for the client and route manager. |
| Medium | FIFO requests use `list.pop(0)` and idle-worker removal scans lists. | Use `deque.popleft()` for requests and ordered dictionaries for idle workers. Queue head/removal operations become O(1) while preserving FIFO order. |
| Medium | Worker liveness is not reset after reconnect; after the first outage it can become negative and cease reconnecting. | Reset liveness and conversation state on each connection; test repeated outages. Worker deadlines use a monotonic clock. |
| Medium | Broad, incomplete annotations hide actual bytes, task ownership, callable signatures and optional state. | Enable strict Pyright, preserve decorator types, annotate lifecycle/state, export symbols explicitly, and ship `py.typed`. |
| Low | A `None` request is interpreted as shutdown. | Treat it as an ordinary MessagePack value; task cancellation is shutdown. |
| Low | Logging eagerly formats failures, logs full successful payloads, ignores worker log level, and closes handlers owned by the host process. | Use parameterized metadata logs, apply worker log level, and remove only the logging context's own handlers. |
| Medium | Broker unexpected-error accounting never increments and terminal failures can look like a successful exit. | Count consecutive unexpected failures and propagate terminal errors; the entrypoint always closes its resources. Recreated ROUTER sockets discard stale worker inventories. |

A registered function still survives independently of cog instances. Duplicate
route registration warns but continues to replace the function for reload
compatibility; it does not silently change an already running worker. Repeated
`start()` does not accidentally create a second worker per route. Async worker
cleanup and reconnect send a nonblocking, best-effort `W_DISCONNECT` before
closing, so a healthy broker can remove the old registration immediately.

## Typing and API choices

The bot argument is an application-owned object, not a dependency on Discord.
The heterogeneous registry uses `Any` only for the bot and request argument
boundary. The decorator returns the exact input callable type, so annotations
remain useful for direct calls, including decorated static methods.

The transport accepts `object` for encoding and returns `Any` for decoded values.
Inventing `request[T]()` would promise a result type without validating it. A
recursive union of MessagePack values would also misrepresent existing typed
payload dictionaries and extensions. The consumer should perform schema
validation, as it already does with msgspec.

There are narrow type-checker suppressions for missing MessagePack C-extension
stubs and PyZMQ's unparameterized `Sequence` annotation. They are confined to the
codec and multipart sends. Project-level checking remains strict. Python 3.10 is
still the minimum; no `Self`, `TaskGroup`, or Python 3.12 type-parameter syntax is
required.

The package explicitly declares its setuptools build backend and includes the
PEP 561 marker. Development tools are locked with PDM without upgrading the
existing runtime dependencies. The historical `worker` extra is retained for
install compatibility, although the transport no longer needs Discord types.

## Validation

The 21 local regression tests use real TCP sockets with dynamically assigned ports.
It covers cancellation both inside a request and while waiting for the lock,
retry exhaustion followed by reuse, malformed client replies and peer frames,
invalid request keys and MessagePack, integer reply keys, null requests, handler
and encoding failures, worker recovery, route serialization and independence,
registration replacement, repeated start/stop, and context ownership.

Mixed-version round trips were also exercised against the pre-review implementation
from git revision `ca6464f`, across all eight combinations of old/new broker,
client and worker, with a fresh event loop per combination. They passed for
structured values, Unicode and opaque binary payloads. Repeated broker/context
teardown in one shared event loop intermittently timed out even with all three
unchanged components; the cause of that inherited behavior is not resolved here.
This is a compatibility smoke test, not a production load or outage soak test.

Ruff lint/format and strict Pyright are required checks. The regression suite is
run in ordinary and optimized Python modes, including Python 3.10 with the minimum
declared PyZMQ 26.4.0 and MessagePack 1.1.0. The built wheel includes `py.typed` and
passes `pyright --verifytypes ipc --ignoreexternal` with all 122 public symbols
known (100% type completeness). This measures public annotations, not application
schema validation or external dependency typing.

On this Linux/Python 3.12/PyZMQ 27.1.0 environment, starting 16 routes added 32
background threads with the original implementation and 2 with the shared-context
implementation. Context count fell from 16 to 1, measured with identical idle
routes and `/proc/self/task`. This is a resource measurement; no end-to-end
throughput or latency improvement is claimed.

## Deliberately deferred

- The broker does not await its poll Future. Maintenance is driven by received
  traffic, not a periodic idle tick. Awaiting it changes production behavior.
- Broker-to-worker sends remain unawaited. Send failures and backpressure cannot
  be treated as handled by this patch. Awaiting sends needs a decision about how
  a slow peer may delay the broker and what to do with a failed dispatch.
- Broker expiry remains `2500 + 3` ms, rather than `2500 * 3` ms. Its wall clock
  and the oldest-first early exit in expiry scanning are also unchanged. (Heartbeat
  refreshes could make expiry order differ from waiting order; the second pass
  below fixes that ordering without touching the constants.)
- Unknown-service queues and service cardinality remain unbounded; no deadlines,
  authentication, delivery acknowledgments or durable recovery have been added.
  Limits require defined overload replies and retry semantics. Request expiry or
  cancellation propagation requires a versioned client/worker contract.
- Calls through one client and handlers on one route remain serial. Sharing a
  context does not increase handler concurrency. Multiple processes can register
  the same route when the application is safe to run concurrently.

Those are substantive remaining reliability limits. They should be addressed as
an explicit transport behavior change with broker restart, idle-liveness,
backpressure, late-reply and overload tests. No deployment or consumer relock is
part of this local review.

## Second pass — 2026-09-10

A review of the first pass against the consumers (`../bot.py`, `../cogs/observer.py`,
`../../backend/client/client.py`, `../../observer/observer/relay.py`) and against
the wire format. The first pass holds up: the failure-boundary changes are correct,
the tests pin what they claim, and the public surface the consumers reach into is
intact. Two things needed fixing, one in code the first pass introduced and one it
carried forward. The rest is documentation drift.

| Priority | Finding | Change |
| --- | --- | --- |
| Medium | **Introduced by the first pass.** `route()` warns whenever a name is re-registered with a different function object. The bot's `-reload` re-imports `cogs/observer.py`, which re-runs its module-level `@route('observer_ingest')` and would now log a WARNING on every routine reload, while a real collision (two modules claiming one name) looks identical. | Compare the previous and new function's `__module__` and `__qualname__`. Same origin is a reload and logs at DEBUG; a different origin is a collision and keeps the WARNING. Verified on CPython 3.10.20 that `staticmethod` objects expose both attributes (bpo-43682), the same change that made them callable. `RouteRegistryTests` pins both branches. |
| Medium | **Carried forward from the original.** `purge_workers` stops at the first live worker, which is only correct if `self.waiting` is ordered by expiry. A `W_HEARTBEAT` refreshed `expiry` in place, so a refreshed old registration could shadow an expired newer one behind it; that worker then stayed in the idle pool and could be handed a request. In production the bot is the only worker process and all its routes die together, so the exposure is small, but the invariant the docstring claims was not maintained. | `move_to_end` on heartbeat, O(1), restoring the invariant exactly: both append and refresh set `expiry = now + EXPIRY`, so insertion order is expiry order again. It cannot purge a live worker; it only lets purge reach dead ones it previously skipped. `test_purge_reaches_expired_worker_behind_refreshed_one` fails on the previous code and passes now. This is a broker behaviour change, though not to any of the three timing constants; it ships with the next broker build from `master`. |
| Low | README said a repeated route name "emits a warning" unconditionally. | Reworded to the reload/collision distinction. |

Reviewed and left as they are, with the reasoning:

- The client's `completed`-flag `finally` reads slightly indirectly but is the
  shortest form that resets on timeout, transport error and cancellation while
  never resetting on success. Splitting it did not get shorter.
- `worker_waiting` raises `InvalidHeader` for an internal invariant that both
  callers already exclude. It is unreachable; fixing the misnomer properly means
  making `Worker.service` non-optional, which means constructing workers only on
  `W_READY` and answering unknown senders by bare address. That is a rewrite of
  `process_worker` for no behaviour change. Deferred.
- Unknown senders on `W_HEARTBEAT` / `W_REPLY` / `W_DISCONNECT` still create and
  immediately delete a transient `Worker`, logging "Registered a new worker" at
  DEBUG. Same rewrite, same deferral.
- The best-effort `W_DISCONNECT` before closing a worker socket is usually dropped
  (`DONTWAIT` and `linger=0`); the supervisor test already accepts both outcomes.
  Harmless, and cheap when it does land.
- The unawaited broker poll was traced through PyZMQ: the stale Future is parked on
  the ROUTER socket's own recv queue and resolved by the next message or by its
  2500 ms timer, then skipped as done. It is garbage per loop iteration, bounded,
  not a leak. Still deliberately untouched, as are the unawaited `send_to_worker`
  and the 2503 ms expiry.
- The worker's strict six-frame check on `W_REQUEST` is stricter than the original
  (which decoded only the first payload frame). A non-conforming client sending
  extra frames now triggers a worker reconnect instead of a silent truncation.
  No consumer does this; kept.
- `build/`, `src/ipc.egg-info/` and `.ruff_cache/` are leftovers of the first
  pass's wheel verification. All three are gitignored.

Verification, all from this working tree: 23 tests pass on Python 3.12.13 with
PyZMQ 27.1.0 and MessagePack 1.1.2, in normal and `-OO` mode; on Python 3.11 (the
Dockerfile's interpreter line) with current PyZMQ and MessagePack under `-OO`; and
on CPython 3.10.20 with the minimum declared `pyzmq==26.4.0` and `msgpack==1.1.0`
under `-OO`. Ruff lint and format are clean, strict Pyright reports nothing, and
`pdm lock --check` matches `pyproject.toml`. Not exercised: the Docker image build
(`pdm install --check --prod` inside the image) and the consumer test suites.
Nothing is committed.

## References

- [PyZMQ asyncio API](https://pyzmq.readthedocs.io/en/stable/api/zmq.asyncio.html):
  async poll, receive and send operations return awaitables.
- [PyZMQ context API](https://pyzmq.readthedocs.io/en/stable/api/zmq.html):
  close owned sockets before terminating their context.
- [MessagePack API](https://msgpack-python.readthedocs.io/en/stable/api.html):
  decoding failures and strict map-key behavior.
- [Typing Python libraries](https://typing.python.org/en/latest/guides/libraries.html):
  public type information and decorator typing.
