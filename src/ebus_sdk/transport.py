"""Structural types for the MQTT transport the SDK is given.

Their own module because they are public API rather than an internal detail: a consumer who
cannot name the type gains nothing from the widening, so they are re-exported from
``ebus_sdk`` beside ``MqttClient``. Keeping small public types out of a ~2,900-line module is
the only reason they are not in ``homie.py``; nothing else imports them today.
"""

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class MqttTransport(Protocol):
    """The surface every caller-supplied MQTT client provides, whatever role it serves.

    Two members, deliberately — not the full ``MqttClient`` surface. Role-specific contracts
    derive from this and add only what their own call sites reach: ``MqttControllerTransport``
    adds ``unsubscribe``; a ``Device``-side contract would instead add ``is_connected`` and
    ``is_running``. Neither inherits the other's members.

    **The SDK never starts or stops a client it did not build.** Widening this to the full
    client surface would type an injection point as *something the SDK may start and stop* —
    the opposite of that guarantee — and would oblige every consumer to implement lifecycle
    methods the SDK provably never calls on their object; for a host supplying a connection
    whose lifecycle it already manages elsewhere, those stubs are pure ceremony.

    Signatures mirror ``ebus_mqtt_client.MqttClient`` exactly, including the ``Any`` on
    ``subscribe``'s callback, so that ``MqttClient`` satisfies every protocol here unchanged.
    Returns are ``object`` because every call site in the SDK discards them.
    """

    def publish(self, topic: str, data: str, qos: int = 1, retain: bool = False) -> object: ...

    def subscribe(self, sub: str, param: Any, qos: int = 1) -> object: ...


@runtime_checkable
class MqttControllerTransport(MqttTransport, Protocol):
    """What the SDK calls on a client injected into ``Controller``.

    ``MqttTransport`` plus ``unsubscribe`` (``Controller._unsubscribe_and_drop``), the only
    member beyond the shared base that the consumer path reaches.

    The base's no-start/no-stop guarantee is enforced here rather than merely documented:

    * ``Controller._connect_broker`` returns immediately when ``self.mqttc`` is already set,
      so the ``start()`` beside ``MqttClient.from_config(...)`` is unreachable for an
      injected client.
    * ``Controller.stop`` calls ``stop()`` only behind ``if self._owns_client``, which is
      ``mqttc is None`` fixed at construction.

    ``is_connected``, ``is_running`` and ``publish_and_flush`` are absent because nothing on
    the ``Controller`` path calls them — they belong to the ``Device`` / ``Property`` path,
    which types its own injection point with ``MqttDeviceTransport`` below.

    ``Controller`` is unpaced on an injected transport unless given
    ``subscription_batch_size``. Pacing requires that subscribing a filter again
    delivers its retained messages again, as a broker connection does: the end of a
    device's burst is the retained ``$description`` resubscribed behind its property
    filters. A ``subscribe`` that takes a keyword-only ``with_retain`` (ebus-mqtt-client
    0.7.0) is called with ``with_retain=True`` for ``$description`` and must then pass
    the retain flag as a third callback argument, so a live republish is not taken for
    that marker.
    """

    def unsubscribe(self, sub: str) -> object: ...


@runtime_checkable
class MqttDeviceTransport(MqttTransport, Protocol):
    """What the SDK calls on a client injected into a root ``Device``.

    ``MqttTransport`` plus ``is_connected()`` and the ``is_running`` attribute, which the
    device publish path reads to gate publishing on connectivity. Like
    ``MqttControllerTransport`` it omits ``start`` / ``stop`` (and ``publish_and_flush``):
    those are owned-only and resolve on the concrete client the SDK builds
    (``Device._owned_client``), never on an injected one, so the no-start/no-stop guarantee
    is a property of the types rather than a promise in a comment.

    This protocol has a data member (``is_running``), so use ``isinstance`` for runtime
    checks; ``issubclass`` is unsupported for protocols with non-method members.

    Optional: a ``subscribe`` that takes a keyword-only ``with_retain`` (as
    ebus-mqtt-client 0.7.0 does) is called with ``with_retain=True`` for ``/set``
    subscriptions and must then pass the message's retain flag as a third callback
    argument, so the device can ignore a retained ``/set``. Without the parameter
    the SDK subscribes as before and cannot detect one.
    """

    def is_connected(self) -> bool: ...

    is_running: bool
