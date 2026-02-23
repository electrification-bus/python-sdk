"""Tests for ebus_sdk.mqtt.MqttClient."""

from unittest.mock import MagicMock, patch, call

import paho.mqtt.matcher as matcher
import pytest

from ebus_sdk.mqtt import MqttClient, AUTH_TYPE_USER_PASS


class TestMqttClientInit:
    """Test MqttClient construction."""

    def test_basic_init(self, mock_paho):
        client = MqttClient(
            client_id="test-client",
            endpoint="127.0.0.1",
            port=1883,
        )
        assert client.client_id == "test-client"
        assert client.is_running is False
        assert client.sub_callbacks == {}
        mock_paho.connect.assert_called_once_with("127.0.0.1", 1883, keepalive=60)

    def test_with_credentials(self, mock_paho):
        MqttClient(
            client_id="test",
            endpoint="localhost",
            port=1883,
            username="user",
            password="pass",
        )
        mock_paho.username_pw_set.assert_called_once_with("user", "pass")

    def test_lwt_configured(self, mock_paho):
        lwt = {"topic": "device/$state", "payload": "lost", "retain": True, "qos": 1}
        MqttClient(
            client_id="test",
            endpoint="localhost",
            port=1883,
            lwt=lwt,
        )
        mock_paho.will_set.assert_called_once_with(
            topic="device/$state", payload="lost", retain=True, qos=1
        )

    def test_on_connect_callback_stored(self, mock_paho):
        cb = MagicMock()
        client = MqttClient(
            client_id="test",
            endpoint="localhost",
            port=1883,
            on_connect_callback=cb,
        )
        assert client.on_connect_callback is cb


class TestMqttClientFromConfig:
    """Test the from_config factory method."""

    def test_minimal_config(self, mock_paho):
        client = MqttClient.from_config(
            mqtt_cfg={"host": "broker.local", "port": 8883},
            client_id="from-cfg",
        )
        assert client.client_id == "from-cfg"
        mock_paho.connect.assert_called_once_with("broker.local", 8883, keepalive=60)

    def test_with_auth(self, mock_paho):
        cfg = {
            "host": "broker.local",
            "port": 1883,
            "authentication": {
                "type": AUTH_TYPE_USER_PASS,
                "username": "admin",
                "password": "secret",
            },
        }
        MqttClient.from_config(mqtt_cfg=cfg, client_id="auth-test")
        mock_paho.username_pw_set.assert_called_once_with("admin", "secret")

    def test_tls_insecure(self, mock_paho):
        cfg = {"host": "broker.local", "port": 8883, "use_tls": True}
        MqttClient.from_config(mqtt_cfg=cfg, client_id="tls-test")
        mock_paho.tls_insecure_set.assert_called_once_with(True)


class TestMqttClientOperations:
    """Test start, stop, publish, subscribe."""

    def test_start_non_blocking(self, mock_paho):
        client = MqttClient(
            client_id="test", endpoint="localhost", port=1883
        )
        client.start(blocking=False)
        assert client.is_running is True
        mock_paho.loop_start.assert_called_once()

    def test_stop_cleans_up(self, mock_paho):
        client = MqttClient(
            client_id="test", endpoint="localhost", port=1883
        )
        cb = MagicMock()
        client.on_connect_callback = cb
        client.start()
        client.subscribe("test/topic", param=MagicMock(), qos=1)

        client.stop()

        assert client.is_running is False
        mock_paho.disconnect.assert_called_once()
        mock_paho.loop_stop.assert_called_once()
        assert client.sub_callbacks == {}
        assert client.on_connect_callback is None

    def test_subscribe_registers_callback(self, mock_paho):
        client = MqttClient(
            client_id="test", endpoint="localhost", port=1883
        )
        cb = MagicMock()
        client.subscribe("ebus/5/+/$state", param=cb, qos=2)

        assert "ebus/5/+/$state" in client.sub_callbacks
        assert client.sub_callbacks["ebus/5/+/$state"] == (cb, 2)
        mock_paho.subscribe.assert_called_with("ebus/5/+/$state", 2)

    def test_publish(self, mock_paho):
        client = MqttClient(
            client_id="test", endpoint="localhost", port=1883
        )
        client.publish("ebus/5/dev1/$state", "ready", qos=2, retain=True)
        mock_paho.publish.assert_called_once_with(
            "ebus/5/dev1/$state", "ready", 2, True
        )

    def test_on_connect_resubscribes(self, mock_paho):
        client = MqttClient(
            client_id="test", endpoint="localhost", port=1883
        )
        cb = MagicMock()
        client.sub_callbacks["ebus/5/+/$state"] = (cb, 2)

        # Simulate broker connection
        client._on_connect(mock_paho, None, {}, 0)

        # Should resubscribe
        mock_paho.subscribe.assert_called_with("ebus/5/+/$state", 2)

    def test_on_connect_fires_callback(self, mock_paho):
        on_connect = MagicMock()
        client = MqttClient(
            client_id="test",
            endpoint="localhost",
            port=1883,
            on_connect_callback=on_connect,
        )
        client._on_connect(mock_paho, None, {}, 0)
        on_connect.assert_called_once()

    def test_on_message_dispatches_to_subscriber(self, mock_paho):
        client = MqttClient(
            client_id="test", endpoint="localhost", port=1883
        )
        cb = MagicMock()
        client.subscribe("ebus/5/+/$state", param=cb, qos=2)

        msg = MagicMock()
        msg.topic = "ebus/5/my-device/$state"
        msg.payload = b"ready"

        # userdata is None (no global callback)
        client._on_message(mock_paho, None, msg)
        cb.assert_called_once_with("ebus/5/my-device/$state", b"ready")

    def test_on_message_no_matching_sub(self, mock_paho):
        client = MqttClient(
            client_id="test", endpoint="localhost", port=1883
        )
        msg = MagicMock()
        msg.topic = "unmatched/topic"
        msg.payload = b"data"

        # Should not raise
        client._on_message(mock_paho, None, msg)
