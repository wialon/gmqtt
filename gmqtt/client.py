import asyncio
import json
import logging
import uuid
from functools import partial

from .mqtt.connection import MQTTConnection
from .mqtt.constants import UNLIMITED_RECONNECTS, MQTTv50
from .mqtt.handler import MqttPackageHandler
from .mqtt.utils import ConnectionState
from .storage import PersistentStorage
from .subscription import SubscriptionsHandlerMixin


class Message:
    def __init__(self, topic, payload, qos=0, retain=False, **kwargs):
        self.topic = (
            topic.encode("utf-8", errors="replace") if isinstance(topic, str) else topic
        )
        self.qos = qos
        self.retain = retain
        self.dup = False
        self.properties = kwargs

        if isinstance(payload, (list, tuple, dict)):
            payload = json.dumps(payload, ensure_ascii=False)

        if isinstance(payload, (int, float)):
            self.payload = str(payload).encode("ascii")
        elif isinstance(payload, str):
            self.payload = payload.encode("utf-8", errors="replace")
        elif payload is None:
            self.payload = b""
        else:
            self.payload = payload

        self.payload_size = len(self.payload)

        if self.payload_size > 268435455:
            raise ValueError("Payload too large.")


class Client(SubscriptionsHandlerMixin):
    def __init__(
        self,
        client_id,
        clean_session=True,
        optimistic_acknowledgement=True,
        will_message=None,
        persistent_storage=None,
        logger=None,
        **kwargs
    ):

        super().__init__()

        self._client_id = client_id or uuid.uuid4().hex
        self._connection_state = ConnectionState()

        # TOD0: Move to a dedicated init/conn properties validator
        # MQTT 5.0 §3.1.2.11.2 — Session Expiry Interval is a Four Byte Integer (seconds).
        # 0 (or absent) = session ends when the Network Connection is closed.
        # 0xFFFFFFFF = session never expires.
        # The server MAY override the requested value in CONNACK (§3.2.2.3.2); read it back
        # via the `session_expiry_interval` property after connect.
        session_expiry_interval = kwargs.get("session_expiry_interval")
        if session_expiry_interval is not None:
            if not isinstance(session_expiry_interval, int) or isinstance(session_expiry_interval, bool):
                raise TypeError("session_expiry_interval must be an int (seconds, 0..0xFFFFFFFF)")
            if not 0 <= session_expiry_interval <= 0xFFFFFFFF:
                raise ValueError(
                    "session_expiry_interval must be in [0, 0xFFFFFFFF]; "
                    "0 = end on disconnect, 0xFFFFFFFF = never expires"
                )

        self._connect_properties = kwargs

        self._connack_received = asyncio.Event()
        self._connection_lost = asyncio.Event()
        self._package_handler = MqttPackageHandler(
            connack_event=self._connack_received,
            connection_lost_event=self._connection_lost,
            connection_state=self._connection_state,
            reconnect_callback=self.reconnect,
            disconnect_callback=self.disconnect,
            resend_qos_callback=self._resend_qos_messages,
            clear_qos_callback=self._clear_resend_qos_queue,
            remove_message_callback=self._remove_message_from_queue,
            store_message_callback=self._store_message_in_queue,
            send_command_with_mid_callback=self._send_command_with_mid,
            connect_properties=self._connect_properties,
            subscriptions_getter=lambda: self.subscriptions,
            optimistic_acknowledgement=optimistic_acknowledgement,
            logger=logger,
        )

        # in MQTT 5.0 this is clean start flag
        self._clean_session = clean_session

        # this flag should be True after connect and False when disconnect was called
        self._is_active = False

        self._connection = None
        self._keepalive = 60

        self._username = None
        self._password = None

        self._host = None
        self._port = None
        self._ssl = None

        self._will_message = will_message

        self._persistent_storage = persistent_storage or PersistentStorage()

        self._topic_alias_maximum = kwargs.get("topic_alias_maximum", 0)

        self._logger = logger or logging.getLogger(__name__)

    @property
    def session_expiry_interval(self):
        """Effective Session Expiry Interval (seconds), per MQTT 5.0 §3.1.2.11.2.

        Returns the value advertised by the server in CONNACK if present
        (§3.2.2.3.2 — the server's value overrides the client's request),
        otherwise the value the client requested, otherwise 0 (spec default:
        session ends when the Network Connection is closed).
        A value of 0xFFFFFFFF means the session never expires.
        """
        server_value = self._package_handler._connack_properties.get(
            "session_expiry_interval"
        )
        if server_value is not None:
            # _parse_properties always stores values as lists via defaultdict(list).
            return server_value[0] if isinstance(server_value, list) else server_value

        return self._connect_properties.get("session_expiry_interval", 0)

    def get_subscription_by_identifier(self, subscription_identifier):
        return next(
            (
                sub
                for sub in self.subscriptions
                if sub.subscription_identifier == subscription_identifier
            ),
            None,
        )

    def _handle_exception_in_future(self, future, msg=None):
        """Done-callback for fire-and-forget futures.

        msg: string to be added to the log message with the exception itself
        """
        exc = future.exception()
        log_string = "[Client]: %s"
        if msg:
            log_string = "[Client] " + msg + ": %s"

        if exc:
            self._logger.warning(log_string, exc)

    # ------------------------------------------------------------------
    # Event callback delegation — properties live on _package_handler;
    # expose them on Client so callers can do client.on_message = fn.
    # ------------------------------------------------------------------

    @property
    def on_connect(self):
        return self._package_handler.on_connect

    @on_connect.setter
    def on_connect(self, cb):
        """
        Called when the broker accepts the connection

        Signature: on_connect(client, flags, rc, properties)
        """
        self._package_handler.on_connect = partial(cb, self)

    @property
    def on_message(self):
        return self._package_handler.on_message

    @on_message.setter
    def on_message(self, cb):
        """
        Called for every inbound packet

        Signature: on_message(client, topic, payload, qos, properties)
        """
        self._package_handler.on_message = partial(cb, self)

    @property
    def on_disconnect(self):
        return self._package_handler.on_disconnect

    @on_disconnect.setter
    def on_disconnect(self, cb):
        """
        Called when the connection is lost or the broker disconnects

        Signature: on_disconnect(client, packet, exc)
        """
        self._package_handler.on_disconnect = partial(cb, self)

    @property
    def on_subscribe(self):
        return self._package_handler.on_subscribe

    @on_subscribe.setter
    def on_subscribe(self, cb):
        """
        Called when a subscription request is received

        Signature: on_subscribe(client, mid, qos, properties)
        """
        self._package_handler.on_subscribe = partial(cb, self)

    @property
    def on_unsubscribe(self):
        return self._package_handler.on_unsubscribe

    @on_unsubscribe.setter
    def on_unsubscribe(self, cb):
        """
        Called when an unsubscription request is received

        Signature: on_unsubscribe(client, mid, qos)
        """
        self._package_handler.on_unsubscribe = partial(cb, self)

    def set_config(self, config: dict):
        self._connection_state.config.update(config)

    def stop_reconnect(self):
        self._package_handler.stop_reconnect()

    @property
    def reconnect_delay(self):
        return self._package_handler.reconnect_delay

    @reconnect_delay.setter
    def reconnect_delay(self, value):
        self._package_handler.reconnect_delay = value

    @property
    def reconnect_retries(self):
        return self._package_handler.reconnect_retries

    @reconnect_retries.setter
    def reconnect_retries(self, value):
        self._package_handler.reconnect_retries = value

    def _store_message_in_queue(self, mid, raw_package):
        # Also used to swap a PUBLISH for its PUBREL once PUBREC arrives.
        self._persistent_storage.push_message(mid, raw_package)

    def _remove_message_from_queue(self, mid):
        self._logger.debug("[Client] remove message. mid: %s", mid)
        self._persistent_storage.remove_message_by_mid(mid)

    @property
    def is_connected(self):
        # tells if connection is alive and CONNACK was received
        return self._connack_received.is_set() and not self._connection.is_closing()

    async def _resend_qos_messages(self):
        await self._connack_received.wait()

        if self._persistent_storage.is_empty:
            self._logger.debug("[Client] QoS queue is empty, nothing to replay")
            return
        elif self._connection.is_closing():
            self._logger.warning(
                "[Client] transport already closing, skipping replay of %s message(s) — "
                "next reconnect will retry",
                len(self._persistent_storage.get_all()),
            )
            return
        else:
            msgs = self._persistent_storage.get_all()
            self._logger.debug(
                "[Client] replaying %s inflight message(s)", len(msgs)
            )

            # Messages stay stored for the whole replay: clearing and
            # re-pushing would fire every wait_empty() waiter, reporting the
            # queue drained while nothing has been acked yet.
            for mid, package in msgs:
                try:
                    self._connection.send_package(package)
                except Exception as exc:
                    self._logger.error(
                        "[Client] failed to resend mid=%s, kept in queue for next reconnect",
                        mid, exc_info=exc
                    )

    def _clear_resend_qos_queue(self):
        self._persistent_storage.clear()

    def set_auth_credentials(self, username, password=None):
        self._username = username.encode()
        self._password = password
        if isinstance(self._password, str):
            self._password = password.encode()

    async def connect(
        self, host, port=1883, ssl=False, keepalive=60, version=MQTTv50, raise_exc=True
    ):
        # Init connection
        self._host = host
        self._port = port
        self._ssl = ssl
        self._keepalive = keepalive
        self._is_active = True

        # Fresh attempt: drop a previous refusal, which would suppress
        # reconnects and raise a stale error below.
        self._package_handler.reset_error_state()

        self._connection_state.protocol_version = version

        self._connection = await self._create_connection(
            host,
            port=self._port,
            ssl=self._ssl,
            clean_session=self._clean_session,
            keepalive=keepalive,
        )

        await self._connection.auth(
            self._client_id,
            self._username,
            self._password,
            will_message=self._will_message,
            **self._connect_properties
        )
        await self._connack_received.wait()

        await self._wait_qos_queue_drained()

        if raise_exc and self._package_handler.has_error:
            raise self._package_handler.propagate_error()

    async def _wait_qos_queue_drained(self):
        """Wait for replayed QoS messages to be acked, bounded by the connection.

        The queue can only drain while this connection is up, so give up if it
        drops; otherwise connect() blocks forever. Racing an Event rather than
        poking the waiters is deliberate — a one-shot notification can fire
        before connect() registers its waiter, reintroducing the hang.
        """
        drained = asyncio.ensure_future(self._persistent_storage.wait_empty())
        lost = asyncio.ensure_future(self._connection_lost.wait())
        try:
            await asyncio.wait({drained, lost}, return_when=asyncio.FIRST_COMPLETED)
            # asyncio.wait never raises; surface storage failures instead of
            # reporting a phantom drain.
            if drained.done() and not drained.cancelled():
                drained.result()
            elif lost.done():
                self._logger.warning(
                    "[Client] connection lost before the QoS queue drained; "
                    "%s message(s) kept for the next reconnect",
                    len(self._persistent_storage.get_all()),
                )
        finally:
            for task in (drained, lost):
                if not task.done():
                    task.cancel()

    def _exit_reconnecting_state(self):
        self._connection_state.reconnecting_now = False

    async def _create_connection(self, host, port, ssl, clean_session, keepalive):
        # important for reconnects! Make sure u know what u're doing if you wanna change it :(
        self._exit_reconnecting_state()
        self._package_handler.clear_topics_aliases()
        # A CONNACK left set by a refused attempt would otherwise make
        # connect() return before this connection's CONNACK arrives, and the
        # previous loss must not short-circuit the drain on this connection.
        self._connack_received.clear()
        self._connection_lost.clear()
        connection = await MQTTConnection.create_connection(
            host,
            port,
            ssl,
            clean_session,
            keepalive,
            connection_state=self._connection_state,
            package_handler=self._package_handler,
            logger=self._logger,
        )
        return connection

    def _temporarily_stop_reconnect(self):
        self._connection_state.reconnecting_now = True

    def _allow_reconnect(self):
        if self._connection_state.reconnecting_now or not self._is_active:
            return False
        if self._connection_state.config["reconnect_retries"] == UNLIMITED_RECONNECTS:
            return True
        if self._connection_state.failed_connections <= self._connection_state.config["reconnect_retries"]:
            return True
        self._logger.error(
            "[Client] max number of failed connection attempts achieved"
        )
        return False

    async def reconnect(self, delay=False):
        if not self._allow_reconnect():
            return

        # stopping auto-reconnects during reconnect procedure is important, better do not touch :(
        self._temporarily_stop_reconnect()

        try:
            await self._disconnect()
        except Exception:
            self._logger.info(
                "[Client] ignored error while disconnecting, trying to reconnect anyway"
            )

        if delay:
            await asyncio.sleep(self._connection_state.config["reconnect_delay"])

        try:
            self._connection = await self._create_connection(
                self._host,
                self._port,
                ssl=self._ssl,
                clean_session=False,
                keepalive=self._keepalive,
            )
        except OSError as exc:
            self._connection_state.failed_connections += 1
            self._logger.warning(
                "[Client] failed to reconnect. Number of retries: %s. Reason: %s",
                self._connection_state.failed_connections, exc
            )
            asyncio.ensure_future(self.reconnect(delay=True))
            return

        await self._connection.auth(
            self._client_id,
            self._username,
            self._password,
            will_message=self._will_message,
            **self._connect_properties
        )

    async def disconnect(self, reason_code=0, **properties):
        self._is_active = False
        await self._disconnect(reason_code=reason_code, **properties)

    async def _disconnect(self, reason_code=0, **properties):
        self._package_handler.clear_topics_aliases()

        self._connack_received.clear()
        # This connection will not acknowledge anything else.
        self._connection_lost.set()
        if self._connection:
            self._connection.send_disconnect(reason_code=reason_code, **properties)
            await self._connection.close()

    def publish(self, message_or_topic, payload=None, qos=0, retain=False, **kwargs):
        if isinstance(message_or_topic, Message):
            message = message_or_topic
        else:
            message = Message(
                message_or_topic, payload, qos=qos, retain=retain, **kwargs
            )

        mid, package = self._connection.publish(message)

        # message.qos, not the argument: a prebuilt Message leaves the
        # argument at 0, so QoS 1/2 would never be stored or replayed.
        if message.qos > 0:
            self._persistent_storage.push_message(mid, package)

    def _send_simple_command(self, cmd):
        self._connection.send_simple_command(cmd)

    def _send_command_with_mid(self, cmd, mid, dup, reason_code=0):
        self._connection.send_command_with_mid(cmd, mid, dup, reason_code=reason_code)
