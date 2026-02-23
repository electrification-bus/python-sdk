import logging
import os
import ssl
import paho.mqtt.client as mqtt
import paho.mqtt.matcher as matcher
from typing import Any, Callable, Optional, Union

# Default broker configuration (can be overridden via environment variables)
MQTT_DEFAULT_HOST = os.environ.get("EBUS_MQTT_HOST", "127.0.0.1")
MQTT_DEFAULT_PORT = int(os.environ.get("EBUS_MQTT_PORT", "1883"))

# Authentication types
AUTH_TYPE_USER_PASS = "USER_PASS"


"""
TODO: Re-think the way a client/user can specify callbacks
  Consider:
  https://eclipse.dev/paho/files/paho.mqtt.python/html/client.html#paho.mqtt.client.Client.message_callback_add
"""


class MqttClient:
    def __init__(
        self,
        client_id: str,
        endpoint: str,
        port: int,
        callback: Callable[[Union[bytes, bytearray], Any], None] = None,
        username=None,
        password=None,
        use_tls: Optional[bool] = False,
        tls_ca_cert: Optional[str] = None,
        tls_ca_data: Optional[Union[str, bytes]] = None,
        tls_insecure: Optional[bool] = True,
        v5: Optional[bool] = False,
        lwt: Optional[dict] = {},
        on_connect_callback: Optional[Callable] = None,
    ):
        self.client_id = client_id
        try:
            if v5:
                self.mqttc = mqtt.Client(
                    client_id=self.client_id, protocol=mqtt.MQTTv5
                )  # TODO FIXME
            else:
                self.mqttc = mqtt.Client(client_id=self.client_id)
        except Exception as e:  # TODO: should this be an exception?
            logging.exception("reason=mqttClientInstantationException")

        # Last Will and Testament
        """
        lwt = {'topic': str,
               'payload': str | bytes | bytearray | int | float | None = None,
               'retain': bool,
               'qos': int}
        """
        if lwt:
            self.lwt_topic = lwt.get("topic", None)
            self.lwt_payload = lwt.get("payload", None)
            self.lwt_retain = lwt.get("retain", True)
            self.lwt_qos = lwt.get("qos", 0)
            if self.lwt_topic and self.lwt_payload:
                self.mqttc.will_set(
                    topic=self.lwt_topic,
                    payload=self.lwt_payload,
                    retain=self.lwt_retain,
                    qos=self.lwt_qos,
                )

        self.mqttc.reconnect_delay_set(min_delay=1, max_delay=30)
        self.mqttc.on_connect = self._on_connect
        self.mqttc.on_disconnect = self._on_disconnect
        self.mqttc.on_message = self._on_message
        self.mqttc.user_data_set(callback)
        self.sub_callbacks = {}
        self.sub_matcher = matcher.MQTTMatcher()
        self.on_connect_callback = on_connect_callback

        self.is_running = False
        if username and password:
            self.mqttc.username_pw_set(username, password)
        if use_tls:
            if (tls_ca_cert or tls_ca_data) and not tls_insecure:
                # Verify server certificate against provided CA cert
                if tls_ca_data:
                    logging.info("reason=mqttClientTlsSecure,ca_data=provided")
                    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                    context.load_verify_locations(cadata=tls_ca_data)
                    self.mqttc.tls_set_context(context)
                else:
                    logging.info(f"reason=mqttClientTlsSecure,ca_cert={tls_ca_cert}")
                    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                    context.load_verify_locations(cafile=tls_ca_cert)
                    self.mqttc.tls_set_context(context)
                self.mqttc.tls_insecure_set(False)
            else:
                # Insecure mode - skip certificate verification
                logging.info("reason=mqttClientTlsInsecure")
                context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                self.mqttc.tls_set_context(context)
                self.mqttc.tls_insecure_set(True)
        self.mqttc.connect(endpoint, port, keepalive=60)

    @classmethod
    def from_config(
        cls,
        mqtt_cfg: dict,
        client_id: str,
        callback: Callable[[Union[bytes, bytearray], Any], None] = None,
        lwt: Optional[dict] = None,
        on_connect_callback: Optional[Callable] = None,
    ) -> "MqttClient":
        """
        Factory method to create MqttClient from a configuration dictionary.

        Args:
            mqtt_cfg: Configuration dictionary with keys:
                - host: Broker hostname/IP (default: from EBUS_MQTT_HOST env or '127.0.0.1')
                - port: Broker port (default: from EBUS_MQTT_PORT env or 1883)
                - use_tls: Enable TLS (default: False)
                - tls_ca_cert: Path to CA certificate file (optional)
                - tls_ca_data: CA certificate content as PEM string or DER bytes (optional)
                - tls_insecure: Skip certificate verification (default: True)
                - authentication: Dict with 'type', 'username', 'password' (optional)
            client_id: MQTT client identifier
            callback: Message callback function (optional)
            lwt: Last Will and Testament dict (optional)
            on_connect_callback: Callback invoked on successful connection (optional)

        Returns:
            Configured MqttClient instance
        """
        endpoint = mqtt_cfg.get("host", MQTT_DEFAULT_HOST)
        port = mqtt_cfg.get("port", MQTT_DEFAULT_PORT)
        use_tls = mqtt_cfg.get("use_tls", False)
        tls_ca_cert = mqtt_cfg.get("tls_ca_cert")
        tls_ca_data = mqtt_cfg.get("tls_ca_data")
        tls_insecure = mqtt_cfg.get("tls_insecure", True)

        # Extract authentication credentials
        username = None
        password = None
        auth = mqtt_cfg.get("authentication", {})
        if auth.get("type") == AUTH_TYPE_USER_PASS:
            username = auth.get("username")
            password = auth.get("password")

        logging.info(
            f"reason=mqttClientFromConfig,host={endpoint},port={port},useTls={use_tls},clientID={client_id}"
        )

        return cls(
            client_id=client_id,
            endpoint=endpoint,
            port=port,
            callback=callback,
            username=username,
            password=password,
            use_tls=use_tls,
            tls_ca_cert=tls_ca_cert,
            tls_ca_data=tls_ca_data,
            tls_insecure=tls_insecure,
            lwt=lwt or {},
            on_connect_callback=on_connect_callback,
        )

    def is_connected(self):
        """Check if MQTT client is connected"""
        return self.mqttc.is_connected() if hasattr(self, "mqttc") else False

    def start(self, blocking=False):
        self.is_running = True
        if blocking:
            self.mqttc.loop_forever()
        else:
            self.mqttc.loop_start()

    def stop(self):
        self.is_running = False
        self.mqttc.disconnect()
        self.mqttc.loop_stop()
        # Release subscription callbacks and matcher to free memory
        self.sub_callbacks.clear()
        self.sub_matcher = matcher.MQTTMatcher()
        self.on_connect_callback = None

    def publish(self, topic: str, data: str, qos: int = 1, retain: bool = False):
        if not hasattr(self, "mqttc"):
            logging.error(
                f"reason=mqttPublishNoClient,client={self.client_id},topic={topic}"
            )
            return
        msg_info = self.mqttc.publish(topic, data, qos, retain)

        if msg_info.rc != mqtt.MQTT_ERR_SUCCESS:
            logging.warning(
                f"reason=mqttPublishFail,client={self.client_id},topic={topic}"
            )

    def subscribe(self, sub: str, param: Any, qos: int = 1):
        if not hasattr(self, "mqttc"):
            logging.error(
                f"reason=mqttSubscribeNoClient,client={self.client_id},sub={sub}"
            )
            return
        self.sub_callbacks[sub] = (param, qos)
        self.sub_matcher[sub] = sub
        self.mqttc.subscribe(sub, qos)

    def _on_connect(self, mqttc: mqtt.Client, userdata: Any, flags: int, rc: int):
        logging.info(f"reason=mqttBrokerConnected,client={self.client_id}")

        # Loop over a shallow copy of the dictionary keys, so it will not crash if the dict size changes on the fly.
        for sub, (_, qos) in list(self.sub_callbacks.items()):
            result, msg_id = self.mqttc.subscribe(sub, qos)
            if result == mqtt.MQTT_ERR_SUCCESS:
                logging.info(
                    f"reason=mqttSubscribeSuccess,client={self.client_id},sub={sub}"
                )
            else:
                logging.warning(
                    f"reason=mqttSubscribeFail,client={self.client_id},sub={sub}"
                )
        # Invoke supplied on_connect_callback if provided
        if self.on_connect_callback:
            self.on_connect_callback()

    def _on_disconnect(self, mqttc: mqtt.Client, userdata: Any, rc: int):
        if self.is_running and rc != mqtt.MQTT_ERR_SUCCESS:
            logging.warning(
                f"reason=mqttBrokerConnectionLost,rc={rc},client={self.client_id}"
            )
        else:
            logging.info(f"reason=mqttBrokerDisconnected,client={self.client_id}")

    def _find_matching_sub(self, topic):
        try:
            return next(self.sub_matcher.iter_match(topic))
        except StopIteration:
            return None

    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage):
        try:
            sub = self._find_matching_sub(msg.topic)
        except:
            logging.warning(
                f"reason=onMessageFindMatchingSubException,topic={msg.topic}",
                exc_info=True,
            )
            return

        if sub is None:
            logging.warning(f"reason=onMessageNoMatchingSubscription,topic={msg.topic}")
            return

        try:
            if userdata:
                userdata(msg.topic, msg.payload, self.sub_callbacks[sub][0])
            else:
                self.sub_callbacks[sub][0](msg.topic, msg.payload)
        except:
            logging.warning(
                f"reason=onMessageClientCallbackException,topic={msg.topic}",
                exc_info=True,
            )
