# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.10.0] - 2026-11-11

### Changed
- Auto-reconnect retries every refusal with the exponential backoff, as 0.7.0 did, instead of
  giving up on refusals that retrying alone cannot fix (bad credentials, banned, server moved,
  ...): their cause may go away. `client.permanently_refused` still tells them apart, now as
  information to alert on, and the error logs say "reconnecting with backoff". With a
  `credentials_provider`, auth refusals (`134`/`135`, `4`/`5` on MQTT 3.1.1) are routine and not
  flagged: the provider renews the credentials for the next attempt.
- A CONNACK with malformed properties (code 10) reconnects with backoff instead of disconnecting
  for good. `on_disconnect` still receives the `MQTTConnectError(10)`.

### Removed
- The special retry of auth refusals for clients with a `credentials_provider`: all refusals are
  retried now, and an explicit `connect()` raises a refusal while auto-reconnect carries on, as
  in 0.7.0.

### Fixed
- A malformed CONNACK reset the failed-attempt counter, so its reconnects never backed off.
- A reconnect cancelled in its backoff (e.g. when the event loop shuts down) logged
  "Exception in callback" with a `CancelledError` traceback.

## [0.9.0] - 2026-11-10

### Added
- `credentials_provider` argument of `Client`: an `async (client, error) -> (username, password)`
  callable awaited before every connect and reconnect, for credentials that expire (e.g. tokens).
  `error` is the last `MQTTConnectError`, or `None` after a plain connection loss. Sync callables
  and `(None, None)` for anonymous access are accepted too.
- With a provider set, auto-reconnect retries auth refusals (`134`/`135`, `4`/`5` on MQTT 3.1.1)
  instead of giving up; an explicit `connect()` still raises them. Other permanent refusals still
  stop the reconnects.
- `reconnect_delay_max` config option (default `300` seconds).
- `MQTTConnectError.code` property.
- `set_auth_credentials()` accepts `None` (anonymous) and `bytes` user names.

### Changed
- Reconnects back off exponentially: the delay starts at `reconnect_delay` and doubles after each
  failed attempt in a row (refusal, network error, ...), up to `reconnect_delay_max`. Previously
  every attempt waited `reconnect_delay`, and `reconnect_delay=0` retried in a tight loop.
- `set_auth_credentials()` raises `TypeError` for a user name or password that is not `str`,
  `bytes` or `None`.

### Fixed
- `reconnect()` no longer connects after `disconnect()` was called during its delay.

## [0.8.1] - 2026-11-09

### Added
- `Client.permanently_refused`: `True` after a CONNACK refusal that auto-reconnect will not retry.
- `Client.connect_error`: the last CONNACK refusal (`MQTTConnectError`).

### Changed
- The "not reconnecting after a permanent CONNACK refusal" error log now includes the refusal
  code and reason.

### Fixed
- A transient refusal (e.g. `137` Server busy) that followed a permanent one stayed flagged as
  permanent, so the client never reconnected.
- MQTT 5.0 property parsing was quadratic in the number of properties (#166).

## [0.8.0] - 2026-10-08

### Added
- `Client.session_expiry_interval`, with validation of the Session Expiry Interval
  (`0..0xFFFFFFFF`) on `Client(...)` and `disconnect(...)`.

### Changed
- Packet handling moved from `Client` into a separate `MqttPackageHandler`, and the message-id
  manager was reworked (#161, #162).
- Refusals that retrying cannot fix (bad credentials, banned, invalid client id) no longer
  reconnect forever (#164).

### Removed
- `Client.protocol_version` property.

### Fixed
- `connect()` could hang forever when the CONNACK carried `server_keep_alive`, or while waiting
  for replayed QoS messages after the connection dropped (#164).
- Crashes on unknown property ids in PUBLISH and SUBACK, and on packet ids past 65535 (#164).
- QoS 1/2 messages passed as prebuilt `Message` objects were never stored or replayed (#164).
- QoS 2 flow: the packet id is released at PUBCOMP instead of PUBREC, PUBREL is replayed instead
  of the acknowledged PUBLISH, and failed PUBRECs (reason code >= `0x80`) end the flow (#164).
- An inbound PUBREL no longer frees an outbound packet id (#164).
- UNSUBACK parses MQTT 5.0 properties instead of passing them to `on_unsubscribe` as reason
  codes (#164).
- CONNACK properties and `receive_maximum` are reset on reconnect instead of carrying over
  from the previous connection (#164).

## [0.7.0] - 2024-11-22

### Changed
- QoS messages are resent only on reconnect, not periodically.

## Earlier versions

See the [tags](https://github.com/wialon/gmqtt/tags) and commit history.

[Unreleased]: https://github.com/wialon/gmqtt/compare/v0.10.0...HEAD
[0.10.0]: https://github.com/wialon/gmqtt/compare/85357a1...v0.10.0
[0.9.0]: https://github.com/wialon/gmqtt/compare/v0.8.1...85357a1
[0.8.1]: https://github.com/wialon/gmqtt/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/wialon/gmqtt/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/wialon/gmqtt/releases/tag/v0.7.0
