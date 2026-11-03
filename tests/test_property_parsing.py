"""MQTT v5 property-list parsing (incl. O(n^2) regression, CWE-407)."""

import time

import gmqtt
from gmqtt.mqtt.property import Property
from gmqtt.mqtt.utils import pack_variable_byte_integer, pack_utf8


def _handler():
    return gmqtt.Client("test")._package_handler


def _property_block(raw):
    return bytes(pack_variable_byte_integer(len(raw))) + raw


def test_parses_every_property_type_and_returns_remainder_as_bytes():
    raw = b"".join(
        bytes(Property.factory(name=name).dumps(value))
        for name, value in [
            ("payload_format_id", 1),  # !B
            ("topic_alias", 7),  # !H
            ("message_expiry_interval", 3600),  # !L
            ("content_type", "application/json"),  # u8
            ("correlation_data", b"\x00\x01corr"),  # b
            ("subscription_identifier", 268435455),  # vbi, 4 bytes
            ("user_property", ("ключ", "значення")),  # u8x2, non-ASCII
            ("user_property", ("k2", "v2")),
        ]
    )
    props, left = _handler()._parse_properties(_property_block(raw) + b"payload")

    assert props == {
        "payload_format_id": [1],
        "topic_alias": [7],
        "message_expiry_interval": [3600],
        "content_type": ["application/json"],
        "correlation_data": [b"\x00\x01corr"],
        "subscription_identifier": [268435455],
        "user_property": [("ключ", "значення"), ("k2", "v2")],
    }
    # No memoryviews leak out.
    assert type(props["correlation_data"][0]) is bytes
    assert type(left) is bytes and left == b"payload"


def test_invalid_property_id_still_rejected():
    props, left = _handler()._parse_properties(_property_block(b"\xff\x00"))
    assert (props, left) == (None, None)


def _parse_time(n, repeats=3):
    raw = (b"\x26" + bytes(pack_utf8("k")) + bytes(pack_utf8("v"))) * n
    packet = _property_block(raw) + b"tail"
    best = float("inf")
    for _ in range(repeats):
        start = time.perf_counter()
        props, left = _handler()._parse_properties(packet)
        best = min(best, time.perf_counter() - start)
    assert len(props["user_property"]) == n
    assert left == b"tail"
    return best


def test_many_user_properties_parse_in_linear_time():
    # Ratio, not wall-clock: 4x input is ~4x time if linear, ~16x if quadratic.
    small, large = _parse_time(50_000), _parse_time(200_000)
    ratio = large / small
    assert ratio < 8, f"4x properties took {ratio:.1f}x longer"
