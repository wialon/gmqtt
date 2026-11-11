"""
Offline tests for _handle_connack_packet — covers four bugs:
"""

import asyncio
import struct

import pytest

import gmqtt
from gmqtt.mqtt.constants import MQTTv50, MQTTv311


# ---------------------------------------------------------------------------
# Packet-building helpers
# ---------------------------------------------------------------------------

def _pack_vbi(value: int) -> bytes:
    """Minimal MQTT variable-byte integer encoder."""
    result = bytearray()
    while True:
        value, b = divmod(value, 128)
        if value > 0:
            b |= 0x80
        result.append(b)
        if value == 0:
            break
    return bytes(result)


def _build_connack_v5(
    session_present: int, reason_code: int, props: bytes = b""
) -> bytes:
    """Return the packet bytes fed to _handle_connack_packet (MQTT v5 layout)."""
    return struct.pack("!BB", session_present, reason_code) + _pack_vbi(len(props)) + props


def _receive_maximum_prop(value: int) -> bytes:
    """MQTT v5 Receive Maximum property (id=33 / 0x21, uint16)."""
    return struct.pack("!BH", 33, value)


# ---------------------------------------------------------------------------
# Handler factory
# ---------------------------------------------------------------------------

class _FakeConnection:
    """Minimal stand-in for MQTTConnection so _update_keepalive_if_needed works."""
    keepalive = 60


def _make_handler():
    """Build an offline Client whose CONNACK callbacks are replaced by list-appending stubs.

    Returns (client, handler, resend_calls, clear_calls, reconnect_calls).
    """
    client = gmqtt.Client("test-connack")
    handler = client._package_handler
    handler._connection = _FakeConnection()

    resend_calls: list = []
    clear_calls: list = []
    reconnect_calls: list = []

    async def fake_resend():
        resend_calls.append("resend")

    def fake_clear():
        clear_calls.append("clear")

    async def fake_reconnect(delay=False):
        reconnect_calls.append(delay)

    handler._resend_qos_callback = fake_resend
    handler._clear_qos_callback = fake_clear
    handler._reconnect_callback = fake_reconnect

    return client, handler, resend_calls, clear_calls, reconnect_calls


# ---------------------------------------------------------------------------
# _connack_received must NOT be set on the v5→v3.1.1 downgrade path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_connected_not_set_on_protocol_downgrade():
    """CONNACK result=1 (Unacceptable Protocol Version) while running
    MQTT v5 triggers the v3.1.1 downgrade.  _connack_received must NOT be set
    at this point — Client.connect() must keep waiting for the successful
    CONNACK that will arrive after the reconnect with v3.1.1."""
    _, handler, _, _, _ = _make_handler()
    assert handler._connection_state.protocol_version == MQTTv50

    packet = _build_connack_v5(session_present=0, reason_code=1)
    handler._handle_connack_packet(0x20, packet)
    await asyncio.sleep(0)

    assert not handler._connack_received.is_set(), (
        "_connack_received must not be set during the v5→v3.1.1 downgrade — "
        "connect() should keep waiting for the next CONNACK."
    )
    assert handler._connection_state.protocol_version == MQTTv311, (
        "Protocol version must be downgraded to v3.1.1 after result=1."
    )


@pytest.mark.asyncio
async def test_connected_set_on_non_retriable_failure():
    """A permanent CONNACK failure (e.g. 135 Not Authorized) must set
    _connack_received so that an awaiting Client.connect() can unblock and propagate
    the error via propagate_error()."""
    _, handler, _, _, _ = _make_handler()

    packet = _build_connack_v5(session_present=0, reason_code=135)
    handler._handle_connack_packet(0x20, packet)
    await asyncio.sleep(0)

    assert handler._connack_received.is_set(), (
        "_connack_received must be set on a permanent refusal so connect() can unblock "
        "and surface the error."
    )
    assert handler._error is not None, "_error must be set on a refused CONNACK."


# ---------------------------------------------------------------------------
# session callbacks must not fire on a refused CONNACK
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_qos_callbacks_not_called_on_refused_connack():
    """On result != 0 neither _resend_qos_callback nor _clear_qos_callback
    must be scheduled."""
    _, handler, resend_calls, clear_calls, _ = _make_handler()

    packet = _build_connack_v5(session_present=0, reason_code=135)
    handler._handle_connack_packet(0x20, packet)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert resend_calls == [], "resend_qos_callback must not run on a refused CONNACK"
    assert clear_calls == [], "clear_qos_callback must not run on a refused CONNACK"


@pytest.mark.asyncio
async def test_qos_callbacks_not_called_on_downgrade():
    """Same guarantee for the v5→v3.1.1 downgrade result code."""
    _, handler, resend_calls, clear_calls, _ = _make_handler()

    packet = _build_connack_v5(session_present=0, reason_code=1)
    handler._handle_connack_packet(0x20, packet)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert resend_calls == [], "resend_qos_callback must not run during protocol downgrade"
    assert clear_calls == [], "clear_qos_callback must not run during protocol downgrade"


@pytest.mark.asyncio
async def test_resend_called_on_success_with_session():
    """On successful CONNACK with session_present=1, only
    _resend_qos_callback must be scheduled."""
    _, handler, resend_calls, clear_calls, _ = _make_handler()

    packet = _build_connack_v5(session_present=1, reason_code=0)
    handler._handle_connack_packet(0x20, packet)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert resend_calls == ["resend"], (
        "_resend_qos_callback must fire on success with session_present=1"
    )
    assert clear_calls == [], (
        "_clear_qos_callback must NOT fire when session_present=1"
    )


@pytest.mark.asyncio
async def test_clear_called_on_success_without_session():
    """On successful CONNACK with session_present=0, only
    _clear_qos_callback must be scheduled."""
    _, handler, resend_calls, clear_calls, _ = _make_handler()

    packet = _build_connack_v5(session_present=0, reason_code=0)
    handler._handle_connack_packet(0x20, packet)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert resend_calls == [], (
        "_resend_qos_callback must NOT fire when session_present=0"
    )
    assert clear_calls == ["clear"], (
        "_clear_qos_callback must fire on success with session_present=0"
    )


# ---------------------------------------------------------------------------
# receive_maximum from CONNACK must update id_generator._max
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_receive_maximum_updates_id_generator():
    """A successful CONNACK carrying receive_maximum=20 must cap
    id_generator._max to 20."""
    _, handler, _, _, _ = _make_handler()
    assert handler.id_generator._max != 20  # sanity: default is much larger

    props = _receive_maximum_prop(20)
    packet = _build_connack_v5(session_present=0, reason_code=0, props=props)
    handler._handle_connack_packet(0x20, packet)
    await asyncio.sleep(0)

    assert handler.id_generator._max == 20, (
        f"id_generator._max must be updated to broker's receive_maximum=20; "
        f"got {handler.id_generator._max}"
    )


@pytest.mark.asyncio
async def test_receive_maximum_not_applied_on_failed_connack():
    """receive_maximum in a refused CONNACK packet must NOT change
    id_generator._max (properties live after result, which is non-zero)."""
    _, handler, _, _, _ = _make_handler()
    original_max = handler.id_generator._max

    # Build a refused CONNACK that still has properties bytes (edge case)
    props = _receive_maximum_prop(10)
    packet = _build_connack_v5(session_present=0, reason_code=135, props=props)
    handler._handle_connack_packet(0x20, packet)
    await asyncio.sleep(0)

    assert handler.id_generator._max == original_max, (
        f"id_generator._max must not change on a refused CONNACK; "
        f"expected {original_max}, got {handler.id_generator._max}"
    )


@pytest.mark.asyncio
async def test_no_receive_maximum_keeps_default():
    """A CONNACK with no receive_maximum property must leave
    id_generator._max unchanged (65535 default)."""
    _, handler, _, _, _ = _make_handler()
    default_max = handler.id_generator._max

    packet = _build_connack_v5(session_present=0, reason_code=0)
    handler._handle_connack_packet(0x20, packet)
    await asyncio.sleep(0)

    assert handler.id_generator._max == default_max, (
        "id_generator._max must not change when receive_maximum is absent"
    )


# ---------------------------------------------------------------------------
# CRASH — malformed properties must not set _connack_properties to None
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_crash_malformed_properties_does_not_assign_none():
    """CRASH: A successful-looking CONNACK with an invalid property id must
    not crash and must not leave _connack_properties as None (which would
    cause AttributeError on any subsequent .get() call)."""
    _, handler, _, _, _ = _make_handler()

    # 0xFF is not a valid MQTT v5 property id.
    bad_prop = struct.pack("!B", 0xFF)
    props_bytes = _pack_vbi(len(bad_prop)) + bad_prop
    packet = struct.pack("!BB", 0, 0) + props_bytes  # session_present=0, result=0

    handler._handle_connack_packet(0x20, packet)  # must not raise
    await asyncio.sleep(0)

    assert handler._connack_properties is not None, (
        "_connack_properties must never be set to None; "
        "a malformed properties block should be rejected cleanly."
    )
    # The error should be surfaced and a disconnect triggered.
    assert handler._error is not None, (
        "_error must be set when CONNACK properties cannot be parsed."
    )


@pytest.mark.asyncio
async def test_crash_malformed_properties_connected_still_set():
    """CRASH: Even when CONNACK properties are malformed, _connack_received must be
    set so that Client.connect() can unblock and propagate the error."""
    _, handler, _, _, _ = _make_handler()

    bad_prop = struct.pack("!B", 0xFF)
    props_bytes = _pack_vbi(len(bad_prop)) + bad_prop
    packet = struct.pack("!BB", 0, 0) + props_bytes

    handler._handle_connack_packet(0x20, packet)
    await asyncio.sleep(0)

    assert handler._connack_received.is_set(), (
        "_connack_received must be set even on malformed properties so connect() "
        "can surface the error."
    )


@pytest.mark.asyncio
async def test_crash_malformed_properties_fires_on_disconnect():
    """CRASH: When CONNACK properties are malformed the on_disconnect callback
    must fire so the application knows the connection was terminated and why."""
    _, handler, _, _, _ = _make_handler()

    disconnect_calls: list = []
    handler.on_disconnect = lambda exc: disconnect_calls.append(exc)

    bad_prop = struct.pack("!B", 0xFF)
    props_bytes = _pack_vbi(len(bad_prop)) + bad_prop
    packet = struct.pack("!BB", 0, 0) + props_bytes

    handler._handle_connack_packet(0x20, packet)
    await asyncio.sleep(0)

    assert len(disconnect_calls) == 1, (
        "on_disconnect must be called once when CONNACK properties are malformed"
    )
    assert isinstance(disconnect_calls[0], Exception), (
        "on_disconnect should receive the MQTTConnectError as the second argument"
    )


