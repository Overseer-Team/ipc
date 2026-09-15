"""Real TCP regressions for transport failures, serial delivery, and legacy framing.

Run from the checkout with: python -m unittest discover -s tests -v
No consumer application, Discord connection, or fixed port is required.

RouteRegistryTests pins the import-time registry alone: a module reload re-registers
quietly, a second definition under the same name warns.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import unittest
from unittest.mock import patch

import zmq
import zmq.asyncio

from src.ipc.broker.__main__ import MDBroker
from src.ipc.client import MDClient
from src.ipc.core.codec import pack, unpack
from src.ipc.core.models import mdp
from src.ipc.core.models.errors import InvalidHeader
from src.ipc.worker import IPC, route
from src.ipc.worker.mdwrkapi import MDWorker


class TransportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.addCleanup(logging.disable, logging.root.manager.disable)
        logging.disable(logging.CRITICAL)
        self.routes = IPC.ROUTES.copy()
        IPC.ROUTES.clear()
        self.broker = MDBroker('127.0.0.1', 0)
        self.port = self.broker.socket.bind_to_random_port('tcp://127.0.0.1')
        self.broker.endpoint = f'tcp://127.0.0.1:{self.port}'
        self.broker_task = asyncio.create_task(self.broker.mediate())
        self.ipc = IPC(None, broker_port=self.port)
        self.clients: list[MDClient] = []
        self.sockets: list[zmq.asyncio.Socket] = []
        self.background: list[asyncio.Task] = []

    async def asyncTearDown(self) -> None:
        for task in self.background:
            task.cancel()
        await asyncio.gather(*self.background, return_exceptions=True)
        await self.ipc.stop()
        self.broker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.broker_task
        for client in self.clients:
            client.close()
        for socket in self.sockets:
            socket.close(linger=0)
        self.broker.close()
        IPC.ROUTES.clear()
        IPC.ROUTES.update(self.routes)

    def client(self) -> MDClient:
        client = MDClient('127.0.0.1', self.port)
        client.TIMEOUT = 500
        self.clients.append(client)
        return client

    def socket(self, kind: int) -> zmq.asyncio.Socket:
        socket = self.broker.ctx.socket(kind)
        socket.connect(self.broker.endpoint)
        self.sockets.append(socket)
        return socket

    async def request(self, client: MDClient, data: object, service: str = 'echo'):
        return await asyncio.wait_for(client.request(service, data), 3)

    def task(self, coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self.background.append(task)
        return task

    async def start_echo(self) -> None:
        @route()
        async def echo(bot, request):
            return request

        await self.ipc.start()

    async def test_roundtrip_values_and_null_request(self) -> None:
        await self.start_echo()
        client = self.client()
        for value in (None, False, 0, '', b'\x00\xff', [], {'nested': ['text', b'bytes', 42]}):
            with self.subTest(value=value):
                self.assertEqual(await self.request(client, value), value)
        self.assertEqual(await self.request(client, 'still alive'), 'still alive')

    async def test_integer_reply_keys_remain_supported(self) -> None:
        @route('echo')
        async def resolve(bot, request):
            return {123456789012345678: 'entity'}

        await self.ipc.start()
        self.assertEqual(await self.request(self.client(), {}), {123456789012345678: 'entity'})

    async def test_invalid_request_keys_do_not_kill_route(self) -> None:
        await self.start_echo()
        client = self.client()
        error = await self.request(client, {1: 'invalid'})
        self.assertIn('strict_map_key', error['error'])
        self.assertEqual(await self.request(client, 'recovered'), 'recovered')

    async def test_handler_and_encoding_errors_do_not_kill_route(self) -> None:
        @route('echo')
        async def sometimes_bad(bot, request):
            if request == 'raise':
                raise ValueError('handler failed')
            if request == 'object':
                return object()
            if request == 'overflow':
                return 2**100
            return request

        await self.ipc.start()
        client = self.client()
        for value, error_type in [('raise', 'ValueError'), ('object', 'TypeError'), ('overflow', 'OverflowError')]:
            with self.subTest(value=value):
                self.assertTrue((await self.request(client, value))['error'].startswith(error_type))
                self.assertEqual(await self.request(client, 'ok'), 'ok')

    async def test_malformed_msgpack_gets_error_then_route_recovers(self) -> None:
        await self.start_echo()
        raw = self.socket(zmq.REQ)
        await raw.send_multipart([mdp.C_CLIENT, b'echo', b'\xc1'])
        reply = await asyncio.wait_for(raw.recv_multipart(), 2)
        self.assertEqual(reply[:3], [mdp.C_CLIENT, b'echo', b''])
        self.assertIn('error', unpack(reply[3]))
        self.assertEqual(await self.request(self.client(), 'ok'), 'ok')

    async def test_cancellation_releases_req_socket_for_waiting_caller(self) -> None:
        entered = asyncio.Event()
        release = asyncio.Event()

        @route('echo')
        async def slow(bot, request):
            if request == 'slow':
                entered.set()
                await release.wait()
            return request

        await self.ipc.start()
        client = self.client()
        pending = self.task(client.request('echo', 'slow'))
        await asyncio.wait_for(entered.wait(), 2)
        following = self.task(client.request('echo', 'next'))
        pending.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await pending
        release.set()
        self.assertEqual(await asyncio.wait_for(following, 2), 'next')

    async def test_cancellation_while_waiting_for_lock_does_not_reset_owner(self) -> None:
        entered = asyncio.Event()
        release = asyncio.Event()

        @route('echo')
        async def slow(bot, request):
            entered.set()
            await release.wait()
            return request

        await self.ipc.start()
        client = self.client()
        pending = self.task(client.request('echo', 'first'))
        await asyncio.wait_for(entered.wait(), 2)
        original_socket = client.client
        following = self.task(client.request('echo', 'cancelled'))
        await asyncio.sleep(0)
        following.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await following
        self.assertIs(client.client, original_socket)
        release.set()
        self.assertEqual(await asyncio.wait_for(pending, 2), 'first')

    async def test_exhausted_retries_leave_client_usable(self) -> None:
        await self.start_echo()
        client = self.client()
        client.TIMEOUT = 30
        self.assertIsNone(await self.request(client, 'missing', service='absent'))
        self.assertEqual(len(self.broker.services[b'absent'].requests), client.RETRIES)
        self.assertEqual(await self.request(client, 'ok'), 'ok')

    async def test_invalid_outbound_payload_does_not_poison_socket(self) -> None:
        await self.start_echo()
        client = self.client()
        with self.assertRaises(TypeError):
            await client.request('echo', object())
        self.assertEqual(await self.request(client, 'ok'), 'ok')

    async def test_routes_remain_serial_but_independent(self) -> None:
        entered = asyncio.Event()
        release = asyncio.Event()
        calls = []

        @route('slow')
        async def slow(bot, request):
            calls.append(request)
            entered.set()
            await release.wait()
            return request

        await self.start_echo()
        first = self.task(self.client().request('slow', 1))
        await asyncio.wait_for(entered.wait(), 2)
        second = self.task(self.client().request('slow', 2))
        self.assertEqual(await self.request(self.client(), 'independent'), 'independent')
        self.assertEqual(calls, [1])
        release.set()
        self.assertEqual(await asyncio.wait_for(asyncio.gather(first, second), 2), [1, 2])
        self.assertEqual(calls, [1, 2])

    async def test_start_stop_and_restart(self) -> None:
        await self.start_echo()
        context = self.ipc._context
        self.assertIsNotNone(context)
        tasks = self.ipc.tasks.copy()
        await self.ipc.start()
        self.assertEqual(self.ipc.tasks, tasks)
        self.assertEqual(await self.request(self.client(), 'ready'), 'ready')
        await self.ipc.stop()
        await self.ipc.stop()
        self.assertFalse(self.ipc.tasks)
        self.assertTrue(context.closed)
        self.assertTrue(all(task.done() for task in tasks))
        await self.ipc.start()
        client = self.client()
        client.RETRIES = 1
        self.assertEqual(await self.request(client, 'restarted'), 'restarted')

    async def test_staticmethod_decorator_and_replacement(self) -> None:
        class Handlers:
            @route('echo')
            @staticmethod
            async def echo(bot: object, request: str) -> str:
                return request

        self.assertEqual(await Handlers.echo(None, 'direct'), 'direct')
        await self.ipc.start()
        self.assertEqual(await self.request(self.client(), 'routed'), 'routed')
        await self.ipc.stop()

        @route('echo')
        async def replacement(bot, request):
            return 'replacement'

        await self.ipc.start()
        self.assertEqual(await self.request(self.client(), 'ignored'), 'replacement')

    async def test_malformed_peer_frames_do_not_crash_broker(self) -> None:
        await self.start_echo()
        raw = self.socket(zmq.DEALER)
        for frames in (
            [b''],
            [b'bad', mdp.C_CLIENT],
            [b'', b'unknown'],
            [b'', mdp.W_WORKER],
            [b'', mdp.W_WORKER, mdp.W_READY],
            [b'', mdp.W_WORKER, b'unknown'],
        ):
            await raw.send_multipart(frames)
        self.assertEqual(await self.request(self.client(), 'ok'), 'ok')
        self.assertFalse(self.broker_task.done())
        self.assertEqual(len(self.broker.workers), 1)

    async def test_delete_idle_and_busy_workers_cleans_both_queues(self) -> None:
        for busy in (False, True):
            with self.subTest(busy=busy):
                worker = self.broker.require_worker(b'test')
                worker.service = self.broker.require_service(b'test-service')
                await self.broker.worker_waiting(worker)
                if busy:
                    await self.broker.dispatch(worker.service, [b'client', b'', pack('data')])
                await self.broker.delete_worker(worker, False)
                self.assertNotIn(worker.identity, self.broker.waiting)
                self.assertNotIn(worker.identity, worker.service.waiting)
                self.assertNotIn(worker.identity, self.broker.workers)

    async def test_purge_reaches_expired_worker_behind_refreshed_one(self) -> None:
        service = self.broker.require_service(b'test-service')
        stale, fresh = self.broker.require_worker(b'stale'), self.broker.require_worker(b'fresh')
        for worker in (stale, fresh):
            worker.service = service
            await self.broker.worker_waiting(worker)
        # A heartbeat refreshes the older registration, so the newer one now expires first.
        await self.broker.process_worker(b'stale', [mdp.W_HEARTBEAT])
        self.assertEqual(list(self.broker.waiting), [fresh.identity, stale.identity])
        fresh.expiry = time.time() - 1
        await self.broker.purge_workers()
        self.assertNotIn(fresh.identity, self.broker.workers)
        self.assertIn(stale.identity, self.broker.waiting)

    async def test_duplicate_reply_cannot_register_worker_twice(self) -> None:
        worker = self.broker.require_worker(b'test')
        worker.service = self.broker.require_service(b'test-service')
        await self.broker.worker_waiting(worker)
        await self.broker.process_worker(b'test', [mdp.W_REPLY, b'client', b'', pack('unsolicited')])
        self.assertNotIn(worker.identity, self.broker.waiting)
        self.assertFalse(worker.service.waiting)

    async def test_invalid_worker_command_does_not_create_worker(self) -> None:
        with self.assertRaises(InvalidHeader):
            await self.broker.process_worker(b'unknown', [b'bad-command'])
        self.assertFalse(self.broker.workers)

    async def test_client_context_manager_closes_resources(self) -> None:
        await self.start_echo()
        async with self.client() as client:
            self.assertEqual(await self.request(client, 'ok'), 'ok')
        self.assertTrue(client.ctx.closed)
        self.assertIsNone(client.client)
        client.close()
        with self.assertRaises(RuntimeError):
            await client.request('echo', 'closed')

    async def test_worker_reconnect_resets_liveness_each_time(self) -> None:
        # An unused port ensures no broker heartbeat resets the counter for us.
        worker = MDWorker(b'unserved', '127.0.0.1', 1)
        worker.TIMEOUT = 10
        worker.RECONNECT = 10
        self.addCleanup(worker.close)
        registrations = 0
        original_connect = worker.connect_to_broker
        reconnected = asyncio.Event()

        async def connect():
            nonlocal registrations
            await original_connect()
            registrations += 1
            if registrations == 3:
                reconnected.set()

        with patch.object(worker, 'connect_to_broker', connect):
            await connect()
            pending = self.task(worker.recv())
            await asyncio.wait_for(reconnected.wait(), 2)
            self.assertEqual(worker.liveness, worker.HEARTBEAT_LIVENESS)
            pending.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await pending

    async def test_malformed_replies_leave_client_usable(self) -> None:
        server = self.broker.ctx.socket(zmq.ROUTER)
        port = server.bind_to_random_port('tcp://127.0.0.1')
        self.sockets.append(server)
        client = MDClient('127.0.0.1', port)
        client.TIMEOUT = 500
        client.RETRIES = 1
        self.clients.append(client)
        invalid = [
            [b'wrong-header', b'echo', b'', pack('bad')],
            [mdp.C_CLIENT, b'wrong-service', b'', pack('bad')],
            [mdp.C_CLIENT, b'echo', b'wrong-delimiter', pack('bad')],
            [mdp.C_CLIENT, b'echo', pack('missing-delimiter')],
            [mdp.C_CLIENT, b'echo', b'', b'\xc1'],
        ]

        async def serve():
            for bad in invalid:
                frames = await server.recv_multipart()
                await server.send_multipart([frames[0], b'', *bad])
                frames = await server.recv_multipart()
                await server.send_multipart([frames[0], b'', mdp.C_CLIENT, b'echo', b'', pack('ok')])

        serving = self.task(serve())
        for frames in invalid:
            with self.subTest(frames=frames):
                self.assertIsNone(await self.request(client, 'request'))
                self.assertEqual(await self.request(client, 'request'), 'ok')
        await asyncio.wait_for(serving, 2)

    async def test_worker_supervisor_recovers_from_malformed_envelope(self) -> None:
        server = self.broker.ctx.socket(zmq.ROUTER)
        self.ipc.port = server.bind_to_random_port('tcp://127.0.0.1')
        self.sockets.append(server)
        with patch.object(MDWorker, 'RECONNECT', 10):
            await self.start_echo()
            ready = await asyncio.wait_for(server.recv_multipart(), 2)
            await server.send_multipart([ready[0], b'bad-delimiter', mdp.W_WORKER, mdp.W_HEARTBEAT])
            recovered = await asyncio.wait_for(server.recv_multipart(), 2)
            if recovered[1:] == [b'', mdp.W_WORKER, mdp.W_DISCONNECT]:
                self.assertEqual(recovered[0], ready[0])
                recovered = await asyncio.wait_for(server.recv_multipart(), 2)
            self.assertEqual(recovered[1:], [b'', mdp.W_WORKER, mdp.W_READY, b'echo'])
            self.assertNotEqual(recovered[0], ready[0])
            await server.send_multipart([recovered[0], b'', mdp.W_WORKER, mdp.W_REQUEST, b'client', b'', pack('ok')])
            reply = await asyncio.wait_for(server.recv_multipart(), 2)
            self.assertEqual(reply[1:], [b'', mdp.W_WORKER, mdp.W_REPLY, b'client', b'', pack('ok')])

    async def test_closing_worker_does_not_terminate_borrowed_context(self) -> None:
        context = zmq.asyncio.Context()
        self.addCleanup(context.term)
        worker = MDWorker(b'borrowed', '127.0.0.1', self.port, context=context)
        try:
            await worker.connect_to_broker()
        finally:
            worker.close()
        self.assertFalse(context.closed)


class RouteRegistryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.routes = IPC.ROUTES.copy()
        IPC.ROUTES.clear()

    def tearDown(self) -> None:
        IPC.ROUTES.clear()
        IPC.ROUTES.update(self.routes)

    def test_reload_replaces_quietly_and_collision_warns(self) -> None:
        def define():
            @route('echo')
            async def echo(bot, request):
                return request

            return echo

        first = define()
        with self.assertNoLogs('ipc.worker', logging.WARNING):
            second = define()
        self.assertIsNot(first, second)
        self.assertIs(IPC.ROUTES['echo'], second)

        with self.assertLogs('ipc.worker', logging.WARNING):

            @route('echo')
            async def shadowing(bot, request):
                return request

        self.assertIs(IPC.ROUTES['echo'], shadowing)


if __name__ == '__main__':
    unittest.main()
