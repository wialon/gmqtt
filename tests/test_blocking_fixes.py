"""Regressions for the crash/hang/loop defects in the handler-split refactor.

One section per defect; each docstring states the failure mode.
"""

import asyncio
import struct

import pytest

import gmqtt
from gmqtt.mqtt.connection import MQTTConnection
from gmqtt.mqtt.constants import MQTTCommands
from gmqtt.mqtt.utils import IdGenerator


def _connack(session_present, reason_code, props=b""):
    return struct.pack("!BB", session_present, reason_code) + bytes([len(props)]) + props


def _handler(name="test"):
    client = gmqtt.Client(name)
    handler = client._package_handler
    calls = []

    async def fake_reconnect(delay=False):
        calls.append(delay)

    handler._reconnect_callback = fake_reconnect
    return client, handler, calls


# ---------------------------------------------------------------------------
# HANG: the handler's _connection was never assigned after the Client split
# ---------------------------------------------------------------------------

class _FakeTransport:
    def write(self, data):
        pass

    def close(self):
        pass

    def is_closing(self):
        return False


class _FakeProtocol:
    def set_connection(self, connection):
        pass


def _connection(handler, keepalive=60):
    return MQTTConnection(
        _FakeTransport(), _FakeProtocol(), True, keepalive, package_handler=handler
    )


@pytest.mark.asyncio
async def test_connection_registers_itself_with_the_handler():
    client, handler, _ = _handler("wiring")
    assert handler._connection is None

    conn = _connection(handler)
    try:
        assert handler._connection is conn
    finally:
        conn._keep_connection_callback.cancel()


@pytest.mark.asyncio
async def test_server_keep_alive_reaches_the_connection():
    """Without the wiring this raised AttributeError inside the blanket except
    in __call__ — before _connack_received.set() — so connect() hung forever."""
    client, handler, _ = _handler("keepalive")
    conn = _connection(handler, keepalive=60)
    try:
        props = struct.pack("!BH", 19, 30)  # server_keep_alive = 30
        handler._handle_connack_packet(0x20, _connack(0, 0, props))
        await asyncio.sleep(0)

        assert conn.keepalive == 30
        assert handler._connack_received.is_set()
    finally:
        if conn._keep_connection_callback:
            conn._keep_connection_callback.cancel()


@pytest.mark.asyncio
async def test_server_keep_alive_without_a_connection_still_unblocks_connect():
    _, handler, _ = _handler("keepalive-none")
    handler._connection = None

    handler._handle_connack_packet(0x20, _connack(0, 0, struct.pack("!BH", 19, 30)))
    await asyncio.sleep(0)

    assert handler._connack_received.is_set()


# ---------------------------------------------------------------------------
# CRASH: (None, None) from _parse_properties dereferenced before the guard
# ---------------------------------------------------------------------------

def test_publish_with_malformed_properties_does_not_crash():
    client = gmqtt.Client("bad-publish")
    client._package_handler.on_message = lambda *a, **k: None
    client._package_handler._send_command_with_mid = lambda *a, **k: None

    topic = b"t"
    raw = struct.pack("!H", len(topic)) + topic + struct.pack("!H", 5)
    raw += b"\x01\xff"  # property block holding invalid id 0xFF
    raw += b"payload"

    client._package_handler._handle_publish_packet(0x32, raw)  # must not raise


def test_suback_with_malformed_properties_does_not_crash():
    client = gmqtt.Client("bad-suback")
    raw = struct.pack("!H", 1) + b"\x01\xff" + bytes([1])

    client._package_handler._handle_suback_packet(0x90, raw)  # must not raise


# ---------------------------------------------------------------------------
# CRASH: packet ids climbing past 65535 into struct.pack("!H", mid)
# ---------------------------------------------------------------------------

def test_ids_stay_within_16_bit_after_max_is_lowered():
    """receive_maximum lowers _max below the counter. Wrapping on == meant the
    bound was never hit again and the id ran away past 65535."""
    gen = IdGenerator()
    gen._last_used_id = 5000
    gen._max = 100

    for _ in range(300):
        mid = gen.next_id()
        assert 1 <= mid < 100, f"mid {mid} escaped the lowered id space"
        struct.pack("!H", mid)  # must never raise
        gen.free_id(mid)


@pytest.mark.asyncio
async def test_invalid_receive_maximum_leaves_the_allocator_usable():
    """_max < 2 makes every allocation raise OverflowError, so the client could
    never publish again."""
    _, handler, _ = _handler("rm-zero")

    handler._handle_connack_packet(0x20, _connack(0, 0, struct.pack("!BH", 33, 0)))
    await asyncio.sleep(0)

    assert handler._connack_received.is_set()
    assert handler.id_generator.next_id() == 1


# ---------------------------------------------------------------------------
# INFINITE LOOP: reconnecting on a refusal that retrying cannot fix
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("code", [134, 135, 138, 133, 156, 157])
async def test_permanent_refusal_is_retried_with_backoff(code):
    """0.8/0.9 gave up for good, yet a ban is lifted, a token renewed, a server back."""
    _, handler, calls = _handler("perm")

    handler._handle_connack_packet(0x20, _connack(0, code))
    await asyncio.sleep(0)

    assert calls == [True]
    assert handler.permanently_refused
    assert handler._connack_received.is_set(), "connect() must still unblock"


@pytest.mark.asyncio
async def test_permanent_refusal_flag_survives_the_broker_closing_the_socket():
    """The server closes the connection after a non-zero CONNACK, which arrives
    as a synthetic DISCONNECT; its reconnect is refused by reconnecting_now."""
    _, handler, calls = _handler("perm-disconnect")

    handler._handle_connack_packet(0x20, _connack(0, 134))
    await asyncio.sleep(0)
    handler._handle_disconnect_packet(MQTTCommands.DISCONNECT, b"")
    await asyncio.sleep(0)

    assert handler.permanently_refused
    assert calls == [True, True]


@pytest.mark.asyncio
async def test_permanent_refusal_is_visible_on_the_client():
    """on_disconnect gets no error, so callers need a public way to tell a
    dead client from one that will reconnect by itself."""
    client, handler, _ = _handler("perm-public")
    seen = []
    client.on_disconnect = lambda c, packet, exc=None: seen.append(
        (c.permanently_refused, c.connect_error)
    )

    handler._handle_connack_packet(0x20, _connack(0, 135))  # not authorized
    await asyncio.sleep(0)
    handler._handle_disconnect_packet(MQTTCommands.DISCONNECT, b"")

    [(refused, error)] = seen
    assert refused
    assert isinstance(error, gmqtt.mqtt.handler.MQTTConnectError)
    assert "135" in str(error)


@pytest.mark.asyncio
async def test_transient_refusal_is_not_permanent():
    client, handler, _ = _handler("transient-public")

    handler._handle_connack_packet(0x20, _connack(0, 137))  # server busy
    await asyncio.sleep(0)

    assert not client.permanently_refused
    assert client.connect_error is not None

    handler._handle_connack_packet(0x20, _connack(0, 0))
    assert client.connect_error is None


@pytest.mark.asyncio
async def test_transient_refusal_after_permanent_one_clears_the_flag():
    """135 then 137 (e.g. app called reconnect() instead of connect()) must
    not stay "permanent"."""
    client, handler, _ = _handler("perm-then-transient")

    handler._handle_connack_packet(0x20, _connack(0, 135))  # not authorized
    await asyncio.sleep(0)
    assert client.permanently_refused

    handler._handle_connack_packet(0x20, _connack(0, 137))  # server busy
    await asyncio.sleep(0)
    assert not client.permanently_refused


@pytest.mark.asyncio
async def test_disconnect_log_names_the_permanent_refusal(caplog):
    """The CONNACK log never showed up in Sentry while this one did, so the
    refusal reason was lost; carry it here too."""
    _, handler, _ = _handler("perm-log")

    handler._handle_connack_packet(0x20, _connack(0, 135))
    await asyncio.sleep(0)
    caplog.clear()
    handler._handle_disconnect_packet(MQTTCommands.DISCONNECT, b"")

    [record] = [r for r in caplog.records if "permanent CONNACK refusal" in r.getMessage()]
    assert "135" in record.getMessage() and record.levelname == "ERROR"


@pytest.mark.asyncio
async def test_transient_refusal_still_reconnects():
    _, handler, calls = _handler("transient")

    handler._handle_connack_packet(0x20, _connack(0, 137))  # server busy
    await asyncio.sleep(0)
    handler._handle_disconnect_packet(MQTTCommands.DISCONNECT, b"")
    await asyncio.sleep(0)

    assert len(calls) == 2


@pytest.mark.asyncio
async def test_error_cleared_on_successful_connack():
    """_error survived a later success, so has_error stayed True forever and
    the next connect() raised a stale MQTTConnectError."""
    _, handler, _ = _handler("stale-error")

    handler._handle_connack_packet(0x20, _connack(0, 136))  # server unavailable
    await asyncio.sleep(0)
    assert handler.has_error

    handler._handle_connack_packet(0x20, _connack(0, 0))
    await asyncio.sleep(0)
    assert not handler.has_error


@pytest.mark.asyncio
async def test_new_connection_waits_for_its_own_connack():
    """Suppressing the reconnect also suppressed the _disconnect() that cleared
    _connack_received, so the next connect() returned with no handshake."""
    client, handler, _ = _handler("stale-connack")

    handler._handle_connack_packet(0x20, _connack(0, 135))
    await asyncio.sleep(0)
    assert client._connack_received.is_set()

    import gmqtt.mqtt.connection as conn_mod

    async def fake_create(*a, **k):
        return object()

    original = conn_mod.MQTTConnection.create_connection
    conn_mod.MQTTConnection.create_connection = fake_create
    try:
        await client._create_connection("h", 1883, False, True, 60)
    finally:
        conn_mod.MQTTConnection.create_connection = original

    assert not client._connack_received.is_set()


@pytest.mark.asyncio
async def test_reset_error_state_clears_the_permanent_flag():
    """Fixing your credentials and calling connect() again must work."""
    _, handler, calls = _handler("reset")

    handler._handle_connack_packet(0x20, _connack(0, 134))
    await asyncio.sleep(0)
    assert handler._permanent_failure

    handler.reset_error_state()

    assert not handler.permanently_refused and not handler.has_error


@pytest.mark.asyncio
async def test_malformed_connack_reconnects_with_backoff():
    """0.9 disconnected for good on code 10; the counter also reset before the
    properties were parsed, so it never backed off."""
    _, handler, calls = _handler("malformed")
    reasons = []
    handler.on_disconnect = reasons.append
    bad = struct.pack("!BB", 0, 0) + bytes([1, 0xFF])  # one property byte with an unknown id

    for _ in range(3):
        handler._handle_connack_packet(0x20, bad)
    await asyncio.sleep(0)

    assert calls == [True] * 3
    assert handler._connection_state.failed_connections == 3
    assert [r.code for r in reasons] == [10] * 3


@pytest.mark.asyncio
async def test_cancelled_reconnect_logs_no_traceback(caplog):
    """e.g. asyncio.run() cancels a reconnect still in its backoff at shutdown."""
    client = gmqtt.Client("cancelled")
    handler = client._package_handler

    async def in_backoff(delay=False):
        await asyncio.sleep(3600)

    handler._reconnect_callback = in_backoff
    handler._handle_disconnect_packet(MQTTCommands.DISCONNECT, b"")
    await asyncio.sleep(0)
    [task] = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    task.cancel()
    await asyncio.sleep(0.01)  # the task ends, then its done-callbacks run

    assert task.cancelled()
    assert not [r for r in caplog.records if r.levelname in ("ERROR", "WARNING")]
