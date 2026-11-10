"""credentials_provider: credentials are asked for before every (re)connect,
and auth refusals stay retryable because the next credentials may differ."""

import asyncio
import struct

import pytest

import gmqtt
from gmqtt.mqtt.constants import MQTTCommands, MQTTv311
from gmqtt.mqtt.handler import MQTTConnectError

BAD_CREDENTIALS = 134
NOT_AUTHORIZED = 135
BANNED = 138


def _connack(session_present, reason_code):
    return struct.pack("!BB", session_present, reason_code) + b"\x00"


class _Provider:
    def __init__(self, *tokens, fail=0):
        self.tokens = list(tokens)
        self.fail = fail
        self.seen = []

    async def __call__(self, client, error):
        self.seen.append(error)
        if self.fail:
            self.fail -= 1
            raise RuntimeError("issuer unreachable")
        return self.tokens.pop(0), None


class _FakeConnection:
    def __init__(self, auths):
        self.auths = auths

    async def auth(self, client_id, username, password, **kwargs):
        self.auths.append(username)


def _client(provider=None, monkeypatch=None):
    client = gmqtt.Client("cred", credentials_provider=provider)
    client.set_auth_credentials("initial")
    client._host, client._port, client._ssl = "broker", 1883, False
    client._is_active = True
    client.reconnect_delay = 0
    reconnects = []
    auths = []

    async def fake_reconnect(delay=False):
        reconnects.append(delay)

    async def fake_create_connection(*args, **kwargs):
        client._exit_reconnecting_state()
        return _FakeConnection(auths)

    async def fake_disconnect(*args, **kwargs):
        pass

    client._package_handler._reconnect_callback = fake_reconnect
    if monkeypatch is not None:
        monkeypatch.setattr(client, "_create_connection", fake_create_connection)
        monkeypatch.setattr(client, "_disconnect", fake_disconnect)
    return client, reconnects, auths


@pytest.mark.asyncio
async def test_without_provider_auth_refusal_stays_permanent():
    client, reconnects, _ = _client()

    client._package_handler._handle_connack_packet(0x20, _connack(0, NOT_AUTHORIZED))
    await asyncio.sleep(0)

    assert client.permanently_refused
    assert reconnects == []


@pytest.mark.asyncio
@pytest.mark.parametrize("code", [BAD_CREDENTIALS, NOT_AUTHORIZED])
async def test_with_provider_auth_refusal_is_retried(code):
    client, reconnects, _ = _client(_Provider("fresh"))

    client._package_handler._handle_connack_packet(0x20, _connack(0, code))
    await asyncio.sleep(0)

    assert not client.permanently_refused
    assert reconnects == [True]


@pytest.mark.asyncio
async def test_with_provider_v311_auth_refusal_is_retried():
    client, reconnects, _ = _client(_Provider("fresh"))
    client._connection_state.protocol_version = MQTTv311

    client._package_handler._handle_connack_packet(0x20, _connack(0, 5))
    await asyncio.sleep(0)

    assert reconnects == [True]


@pytest.mark.asyncio
async def test_with_provider_other_permanent_refusals_still_stop():
    """New credentials cannot lift a ban, so retrying would only hammer the broker."""
    client, reconnects, _ = _client(_Provider("fresh"))

    client._package_handler._handle_connack_packet(0x20, _connack(0, BANNED))
    await asyncio.sleep(0)
    client._package_handler._handle_disconnect_packet(MQTTCommands.DISCONNECT, b"")
    await asyncio.sleep(0)

    assert client.permanently_refused
    assert reconnects == []


@pytest.mark.asyncio
async def test_reconnect_authenticates_with_the_provided_credentials(monkeypatch):
    provider = _Provider("fresh")
    client, _, auths = _client(provider, monkeypatch)
    client._package_handler._handle_connack_packet(0x20, _connack(0, NOT_AUTHORIZED))
    await asyncio.sleep(0)

    await client.reconnect(delay=False)

    # the provider sees the refusal, so it knows the old credentials are dead
    [error] = provider.seen
    assert isinstance(error, MQTTConnectError) and error.code == NOT_AUTHORIZED
    assert auths == [b"fresh"]


@pytest.mark.asyncio
async def test_plain_connection_loss_passes_no_error(monkeypatch):
    provider = _Provider("same")
    client, _, auths = _client(provider, monkeypatch)

    await client.reconnect(delay=False)

    assert provider.seen == [None]
    assert auths == [b"same"]


@pytest.mark.asyncio
async def test_failing_provider_schedules_another_reconnect(monkeypatch):
    provider = _Provider("fresh", fail=1)
    client, _, auths = _client(provider, monkeypatch)
    retries = []

    original = client.reconnect

    async def reconnect(delay=False):
        retries.append(delay)

    monkeypatch.setattr(client, "reconnect", reconnect)

    await original(delay=False)
    await asyncio.sleep(0)

    assert auths == []
    assert retries == [True]
    # otherwise _allow_reconnect() would refuse every later attempt
    assert not client._connection_state.reconnecting_now


@pytest.mark.parametrize(
    "failed, expected",
    [(0, 6), (1, 6), (2, 12), (3, 24), (6, 192), (7, 300), (50, 300)],
)
def test_failed_attempts_back_off_exponentially(failed, expected):
    client, _, _ = _client(_Provider())
    client.reconnect_delay = 6
    client._connection_state.failed_connections = failed

    assert client._next_reconnect_delay() == expected


def test_backoff_cap_is_configurable():
    client, _, _ = _client()
    client.set_config({"reconnect_delay": 6, "reconnect_delay_max": 20})
    client._connection_state.failed_connections = 10

    assert client._next_reconnect_delay() == 20


def test_backoff_cap_never_shortens_reconnect_delay():
    client, _, _ = _client()
    client.set_config({"reconnect_delay": 600})
    client._connection_state.failed_connections = 3

    assert client._next_reconnect_delay() == 600


def test_backoff_does_not_overflow_with_a_float_delay():
    client, _, _ = _client()
    client.set_config({"reconnect_delay": 0.5})
    client._connection_state.failed_connections = 5000

    assert client._next_reconnect_delay() == 300


def test_zero_reconnect_delay_reconnects_at_once_then_backs_off():
    client, _, _ = _client()
    client.reconnect_delay = 0

    assert client._next_reconnect_delay() == 0  # plain connection loss
    client._connection_state.failed_connections = 4
    assert client._next_reconnect_delay() == 8


def _refuse(client, code):
    client._package_handler._handle_connack_packet(0x20, _connack(0, code))


@pytest.mark.asyncio
async def test_successful_connack_resets_the_backoff():
    client, _, _ = _client(_Provider())
    client.reconnect_delay = 6
    for _ in range(5):
        _refuse(client, NOT_AUTHORIZED)
    _refuse(client, 0)
    await asyncio.sleep(0)

    assert client._next_reconnect_delay() == 6


@pytest.mark.asyncio
async def test_connect_asks_the_provider_before_forgetting_the_refusal():
    provider = _Provider(fail=1)
    client, _, _ = _client(provider)
    client._package_handler._error = MQTTConnectError(NOT_AUTHORIZED)

    with pytest.raises(RuntimeError):
        await client.connect("broker")

    [error] = provider.seen
    assert error.code == NOT_AUTHORIZED


@pytest.mark.asyncio
async def test_explicit_connect_raises_auth_refusal_without_retrying(monkeypatch):
    """A background reconnect next to the caller's own retry sent two CONNECTs
    with the same client id."""
    client, reconnects, _ = _client(_Provider("stale"), monkeypatch)

    class _Refusing:
        async def auth(self, *args, **kwargs):
            _refuse(client, NOT_AUTHORIZED)

    async def create_connection(*args, **kwargs):
        client._exit_reconnecting_state()
        return _Refusing()

    monkeypatch.setattr(client, "_create_connection", create_connection)

    with pytest.raises(MQTTConnectError):
        await client.connect("broker")
    await asyncio.sleep(0)

    assert reconnects == []
    assert client.permanently_refused
    # later connection losses still retry auth refusals
    assert client._package_handler.retry_auth_refusals


@pytest.mark.asyncio
async def test_provider_gets_each_refusal_once_but_connect_error_keeps_it(monkeypatch):
    """Refused -> new token -> OSError: the next retry must not issue a token
    again for the old refusal, yet the refusal stays visible to monitoring."""
    provider = _Provider("fresh", "same")
    client, _, _ = _client(provider, monkeypatch)
    _refuse(client, NOT_AUTHORIZED)
    await asyncio.sleep(0)

    async def unreachable(*args, **kwargs):
        client._exit_reconnecting_state()
        raise OSError("broker down")

    monkeypatch.setattr(client, "_create_connection", unreachable)
    monkeypatch.setattr(asyncio, "ensure_future", lambda coro: coro.close())
    await client.reconnect(delay=False)
    await client.reconnect(delay=False)

    first, second = provider.seen
    assert first.code == NOT_AUTHORIZED and second is None
    assert client.connect_error.code == NOT_AUTHORIZED


@pytest.mark.asyncio
async def test_failing_provider_sees_the_refusal_again(monkeypatch):
    provider = _Provider("fresh", fail=1)
    client, _, auths = _client(provider, monkeypatch)
    _refuse(client, NOT_AUTHORIZED)
    await asyncio.sleep(0)
    monkeypatch.setattr(asyncio, "ensure_future", lambda coro: coro.close())

    await client.reconnect(delay=False)  # provider raises
    await client.reconnect(delay=False)

    assert [e.code for e in provider.seen] == [NOT_AUTHORIZED, NOT_AUTHORIZED]
    assert auths == [b"fresh"]


@pytest.mark.asyncio
async def test_disconnect_while_the_provider_runs_cancels_the_reconnect(monkeypatch):
    async def provider(client, error):
        await client.disconnect()  # the app shuts down meanwhile
        return "token", None

    client, _, auths = _client(provider, monkeypatch)

    await client.reconnect(delay=False)

    assert auths == []
    assert not client._connection_state.reconnecting_now


@pytest.mark.asyncio
async def test_failing_provider_backs_off_even_with_zero_delay(monkeypatch):
    client, _, _ = _client(_Provider(fail=3), monkeypatch)
    client.reconnect_delay = 0
    monkeypatch.setattr(asyncio, "ensure_future", lambda coro: coro.close())

    await client.reconnect(delay=False)
    await client.reconnect(delay=False)

    assert client._next_reconnect_delay() == 2


@pytest.mark.parametrize("username, password", [(12345, None), ("user", 42)])
def test_set_auth_credentials_rejects_other_types(username, password):
    client, _, _ = _client()
    with pytest.raises(TypeError, match="must be str, bytes-like or None"):
        client.set_auth_credentials(username, password)


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", [None, "token", ("a", "b", "c")])
async def test_provider_must_return_a_pair(bad):
    async def provider(client, error):
        return bad

    client, _, _ = _client(provider)
    with pytest.raises(TypeError, match="credentials_provider must return"):
        await client._refresh_credentials()


@pytest.mark.asyncio
async def test_provider_may_be_sync_and_anonymous():
    client, _, _ = _client(lambda client, error: (None, None))

    await client._refresh_credentials()

    assert client._username is None and client._password is None


@pytest.mark.asyncio
async def test_cancelling_reconnect_during_the_provider_is_not_a_provider_failure(monkeypatch):
    started = asyncio.Event()

    async def provider(client, error):
        started.set()
        await asyncio.sleep(3600)

    client, _, _ = _client(provider, monkeypatch)
    task = asyncio.ensure_future(client.reconnect(delay=False))
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert client._connection_state.failed_connections == 0


@pytest.mark.asyncio
async def test_cancelled_provider_does_not_block_later_reconnects(monkeypatch):
    async def provider(client, error):
        raise asyncio.CancelledError()  # e.g. a shared refresh task was cancelled

    client, _, _ = _client(provider, monkeypatch)

    with pytest.raises(asyncio.CancelledError):
        await client.reconnect(delay=False)

    assert not client._connection_state.reconnecting_now
    assert client._allow_reconnect()


@pytest.mark.asyncio
async def test_refusal_arriving_while_the_provider_runs_is_not_marked_seen():
    seen = []

    async def provider(client, error):
        seen.append(error)
        if len(seen) == 1:
            _refuse(client, BAD_CREDENTIALS)  # e.g. a concurrent attempt was refused
        return "token", None

    client, _, _ = _client(provider)

    await client._refresh_credentials()
    await client._refresh_credentials()

    assert seen[0] is None
    assert seen[1].code == BAD_CREDENTIALS


@pytest.mark.parametrize("password", [bytearray(b"secret"), memoryview(b"secret")])
def test_set_auth_credentials_keeps_accepting_buffers(password):
    client, _, _ = _client()
    client.set_auth_credentials("user", password)

    assert client._password is password
