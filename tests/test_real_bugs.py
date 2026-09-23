"""Non-crashing correctness bugs: silent message loss, spec violations, stale state."""

import asyncio
import struct

import pytest

import gmqtt
from gmqtt.mqtt.constants import MQTTCommands


def _connack(session_present, reason_code, props=b""):
    return struct.pack("!BB", session_present, reason_code) + bytes([len(props)]) + props


# ---------------------------------------------------------------------------
# publish() keyed off the qos argument instead of the message's own qos
# ---------------------------------------------------------------------------

class _PublishConnection:
    def __init__(self):
        self.sent = []

    def publish(self, message):
        self.sent.append(message)
        return (7, b"raw-publish-bytes") if message.qos else (None, b"raw")


def test_prebuilt_message_qos1_is_stored_for_replay():
    """client.publish(Message(..., qos=1)) leaves the argument at its 0 default,
    so the message was never stored, never replayed, and its mid never freed."""
    client = gmqtt.Client("prebuilt-qos")
    client._connection = _PublishConnection()

    client.publish(gmqtt.Message("t", b"payload", qos=1))

    assert client._persistent_storage.get_all() == [(7, b"raw-publish-bytes")], (
        "A prebuilt QoS 1 message must be queued for replay."
    )


def test_prebuilt_message_qos0_is_not_stored():
    client = gmqtt.Client("prebuilt-qos0")
    client._connection = _PublishConnection()

    client.publish(gmqtt.Message("t", b"payload", qos=0))

    assert client._persistent_storage.is_empty


# ---------------------------------------------------------------------------
# QoS 2 identifier lifecycle (§4.3.3) and replay content (MQTT-4.4.0-1)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_qos2_id_held_until_pubcomp_and_pubrel_stored_for_replay():
    client = gmqtt.Client("qos2-lifecycle")
    sent = []

    class _Conn:
        def send_command_with_mid(self, cmd, mid, dup, reason_code=0):
            sent.append(cmd)

    client._connection = _Conn()
    gen = client._package_handler.id_generator
    mid = gen.next_id()
    client._persistent_storage.push_message(mid, b"\x34original-publish")

    client._package_handler._handle_pubrec_packet(0x50, struct.pack("!H", mid))

    assert mid in gen._used_ids, (
        "PUBREC released the id; it must be held until PUBCOMP or it can be "
        "reissued while PUBREL/PUBCOMP are in flight."
    )
    stored = dict(client._persistent_storage.get_all())[mid]
    assert stored[0] & 0xF0 == MQTTCommands.PUBREL, (
        f"Replay must resend a PUBREL, not the acknowledged PUBLISH; "
        f"stored cmd {hex(stored[0])}"
    )
    assert struct.unpack("!H", stored[2:4])[0] == mid

    client._package_handler._handle_pubcomp_packet(0x70, struct.pack("!H", mid))
    assert mid not in gen._used_ids
    assert client._persistent_storage.is_empty


def test_inbound_pubrel_does_not_free_an_outbound_mid():
    """PUBREL belongs to the inbound QoS 2 flow, so its mid is the broker's."""
    client = gmqtt.Client("inbound-pubrel")
    client._package_handler._send_command_with_mid = lambda *a, **k: None
    gen = client._package_handler.id_generator

    gen._used_ids.add(99)
    client._package_handler._handle_pubrel_packet(0x62, struct.pack("!H", 99))

    assert 99 in gen._used_ids, "An inbound PUBREL freed an outbound mid."


# ---------------------------------------------------------------------------
# Replay must not report the queue drained before anything is acknowledged
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_replay_does_not_prematurely_release_wait_empty():
    """_resend_qos_messages cleared the store before re-pushing, firing every
    wait_empty() waiter, so connect() returned claiming the queue had drained
    while every replayed message was still unacknowledged."""
    client = gmqtt.Client("replay")
    sent = []

    class _Conn:
        def is_closing(self):
            return False

        def send_package(self, package):
            sent.append(package)

    client._connection = _Conn()
    client._persistent_storage.push_message(1, b"one")
    client._persistent_storage.push_message(2, b"two")
    client._connack_received.set()

    drain = asyncio.ensure_future(client._wait_qos_queue_drained())
    await asyncio.sleep(0)
    await client._resend_qos_messages()
    await asyncio.sleep(0)

    assert sent == [b"one", b"two"], "Both messages must replay, in order."
    assert not drain.done(), "The queue is not drained until the broker acks."

    client._persistent_storage.remove_message_by_mid(1)
    client._persistent_storage.remove_message_by_mid(2)
    await asyncio.wait_for(drain, timeout=1)


@pytest.mark.asyncio
async def test_drain_gives_up_when_the_connection_is_lost():
    """The queue can only drain while the connection is up; without a bound
    connect() would block forever (there is no timeout anywhere)."""
    client = gmqtt.Client("drain-lost")
    client._persistent_storage.push_message(1, b"one")
    client._connection_lost.set()

    await asyncio.wait_for(client._wait_qos_queue_drained(), timeout=1)
    assert client._persistent_storage.get_all() == [(1, b"one")], (
        "Messages stay queued for the next reconnect."
    )


@pytest.mark.asyncio
async def test_drain_bound_is_race_free_against_a_late_waiter():
    """The connection can drop before connect() reaches the drain step. An
    Event is level-triggered, so a late caller still sees it."""
    client = gmqtt.Client("drain-late")
    client._persistent_storage.push_message(1, b"one")

    client._package_handler._handle_disconnect_packet(MQTTCommands.DISCONNECT, b"")
    await asyncio.sleep(0)

    await asyncio.wait_for(client._wait_qos_queue_drained(), timeout=1)


@pytest.mark.asyncio
async def test_drain_surfaces_storage_errors():
    client = gmqtt.Client("drain-error")

    class _BrokenStorage:
        is_empty = False

        async def wait_empty(self):
            raise RuntimeError("redis is down")

    client._persistent_storage = _BrokenStorage()

    with pytest.raises(RuntimeError, match="redis is down"):
        await client._wait_qos_queue_drained()


@pytest.mark.asyncio
async def test_cancelled_waiter_does_not_strand_the_others():
    from gmqtt.storage import PersistentStorage

    storage = PersistentStorage()
    storage.push_message(1, b"a")

    cancelled = asyncio.ensure_future(storage.wait_empty())
    survivor = asyncio.ensure_future(storage.wait_empty())
    await asyncio.sleep(0)
    cancelled.cancel()
    await asyncio.sleep(0)

    storage.remove_message_by_mid(1)  # must not raise InvalidStateError
    await asyncio.wait_for(survivor, timeout=1)
    assert storage._empty_waiters == set()


# ---------------------------------------------------------------------------
# CONNACK state must not survive into the next connection
# ---------------------------------------------------------------------------

def _handler():
    client = gmqtt.Client("connack-state")
    handler = client._package_handler

    async def noop(*a, **k):
        pass

    handler._reconnect_callback = noop
    handler._disconnect_callback = noop
    return client, handler


@pytest.mark.asyncio
async def test_receive_maximum_resets_when_absent_from_a_later_connack():
    """§3.2.2.3.3: absent Receive Maximum means the default. Keeping the
    previous broker's window throttled the client permanently."""
    _, handler = _handler()
    default = handler.id_generator._max

    handler._handle_connack_packet(0x20, _connack(0, 0, struct.pack("!BH", 33, 3)))
    await asyncio.sleep(0)
    assert handler.id_generator._max == 3

    handler._handle_connack_packet(0x20, _connack(0, 0))
    await asyncio.sleep(0)
    assert handler.id_generator._max == default, (
        "An absent receive_maximum must restore the default window."
    )


@pytest.mark.asyncio
async def test_connack_properties_do_not_leak_across_connections():
    _, handler = _handler()

    handler._handle_connack_packet(0x20, _connack(0, 0, struct.pack("!BH", 19, 30)))
    await asyncio.sleep(0)
    assert "server_keep_alive" in handler._connack_properties

    handler._handle_connack_packet(0x20, _connack(0, 0))
    await asyncio.sleep(0)
    assert handler._connack_properties == {}


# ---------------------------------------------------------------------------
# PUBREC failure reason codes end the QoS 2 flow (§4.3.3)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_refused_pubrec_ends_the_flow_without_pubrel():
    """A PUBREC reason code >= 0x80 terminates the exchange. Sending PUBREL
    anyway is a protocol error, and the broker need not answer it — pinning the
    mid and the stored packet until the connection drops."""
    client = gmqtt.Client("pubrec-refused")
    sent = []

    class _Conn:
        def send_command_with_mid(self, cmd, mid, dup, reason_code=0):
            sent.append(cmd)

    client._connection = _Conn()
    gen = client._package_handler.id_generator
    mid = gen.next_id()
    client._persistent_storage.push_message(mid, b"\x34publish")

    # 0x87 Not authorized
    client._package_handler._handle_pubrec_packet(
        0x50, struct.pack("!HB", mid, 0x87)
    )

    assert sent == [], "No PUBREL may be sent after a refused PUBREC."
    assert mid not in gen._used_ids, "The identifier must be released."
    assert client._persistent_storage.is_empty, "The message must be dropped."


@pytest.mark.asyncio
async def test_successful_pubrec_with_explicit_reason_code_still_sends_pubrel():
    client = gmqtt.Client("pubrec-ok")
    sent = []

    class _Conn:
        def send_command_with_mid(self, cmd, mid, dup, reason_code=0):
            sent.append(cmd)

    client._connection = _Conn()
    mid = client._package_handler.id_generator.next_id()
    client._persistent_storage.push_message(mid, b"\x34publish")

    client._package_handler._handle_pubrec_packet(0x50, struct.pack("!HB", mid, 0x00))

    assert sent == [MQTTCommands.PUBREL | 2]


# ---------------------------------------------------------------------------
# A malformed SUBACK/UNSUBACK must still release the identifier
# ---------------------------------------------------------------------------

def test_malformed_suback_releases_the_mid():
    """The early return added for the crash skipped free_id(), leaking the mid
    and pinning the Subscription to a stale one."""
    client = gmqtt.Client("suback-leak")
    gen = client._package_handler.id_generator

    sub = gmqtt.Subscription("t")
    sub.mid = 1
    client.subscriptions.append(sub)
    gen._used_ids.add(1)

    raw = struct.pack("!H", 1) + b"\x01\xff" + bytes([1])
    client._package_handler._handle_suback_packet(0x90, raw)

    assert 1 not in gen._used_ids, "Malformed SUBACK leaked the subscribe mid."
    assert sub.mid is None, "The Subscription stayed pinned to a dead mid."


def test_unsuback_parses_v5_properties():
    """Without property parsing the property bytes reached on_unsubscribe as
    reason codes."""
    client = gmqtt.Client("unsuback-props")
    seen = []
    client._package_handler.on_unsubscribe = lambda mid, codes: seen.append(codes)

    # mid=1, property block len 0, then one reason code 0x00
    raw = struct.pack("!H", 1) + b"\x00" + bytes([0x00])
    client._package_handler._handle_unsuback_packet(0xB0, raw)

    assert seen == [(0,)], f"Reason codes must exclude the property block; got {seen}"


# ---------------------------------------------------------------------------
# A superseded connection must not drive the live one
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stale_connection_packets_are_dropped():
    """The old transport's synthetic DISCONNECT would otherwise abort the new
    connection's drain and schedule a reconnect against a healthy link."""
    from gmqtt.mqtt.connection import MQTTConnection
    from gmqtt.mqtt.package import Package

    client = gmqtt.Client("stale")
    handler = client._package_handler
    calls = []

    async def fake_reconnect(delay=False):
        calls.append(delay)

    handler._reconnect_callback = fake_reconnect

    class _T:
        def write(self, d): pass
        def close(self): pass
        def is_closing(self): return False

    class _P:
        def set_connection(self, c): pass

    old = MQTTConnection(_T(), _P(), True, 60, package_handler=handler)
    new = MQTTConnection(_T(), _P(), True, 60, package_handler=handler)
    try:
        assert handler.current_connection is new

        old.put_package(Package(MQTTCommands.DISCONNECT, b""))
        await asyncio.sleep(0)

        assert calls == [], "A superseded connection triggered a reconnect."
        assert not client._connection_lost.is_set(), (
            "A superseded connection aborted the live connection's drain."
        )
    finally:
        old._keep_connection_callback.cancel()
        new._keep_connection_callback.cancel()
