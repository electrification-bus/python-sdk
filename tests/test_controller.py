"""Tests for ebus_sdk.homie.Controller and DiscoveredDevice."""

import json
from unittest.mock import MagicMock, patch


from ebus_sdk.homie import (
    Controller,
    DiscoveredDevice,
    EBUS_HOMIE_DOMAIN,
    EBUS_HOMIE_VERSION_MAJOR,
    EBUS_HOMIE_MQTT_QOS,
)

# ── DiscoveredDevice ─────────────────────────────────────────────────────


class TestDiscoveredDevice:
    def test_init_defaults(self):
        dev = DiscoveredDevice("panel-1")
        assert dev.device_id == "panel-1"
        assert dev.homie_domain == EBUS_HOMIE_DOMAIN
        assert dev.state is None
        assert dev.description is None
        assert dev.properties == {}
        assert dev.property_targets == {}
        assert dev.last_seen is None

    def test_update_state(self):
        dev = DiscoveredDevice("panel-1")
        dev.update_state("ready")
        assert dev.state == "ready"
        assert dev.last_seen is not None

    def test_update_description(self):
        dev = DiscoveredDevice("panel-1")
        desc = {"homie": "5.0", "nodes": {"core": {"name": "Core"}}}
        dev.update_description(json.dumps(desc))
        assert dev.description == desc
        assert dev.last_seen is not None

    def test_update_description_invalid_json(self):
        dev = DiscoveredDevice("panel-1")
        dev.update_description("not-json{{{")
        assert dev.description is None

    def test_update_and_get_property(self):
        dev = DiscoveredDevice("panel-1")
        dev.update_property("core", "active-power", "-500")
        assert dev.get_property("core", "active-power") == "-500"

    def test_get_property_missing(self):
        dev = DiscoveredDevice("panel-1")
        assert dev.get_property("nonexistent", "prop") is None

    def test_update_and_get_property_target(self):
        dev = DiscoveredDevice("panel-1")
        dev.update_property_target("breaker", "state", "CLOSED")
        assert dev.get_property_target("breaker", "state") == "CLOSED"

    def test_get_nodes_from_description(self):
        dev = DiscoveredDevice("panel-1")
        desc = {
            "nodes": {
                "core": {"name": "Core"},
                "circuit-1": {"name": "Kitchen"},
            }
        }
        dev.update_description(json.dumps(desc))
        nodes = dev.get_nodes()
        assert set(nodes) == {"core", "circuit-1"}

    def test_get_nodes_no_description(self):
        dev = DiscoveredDevice("panel-1")
        assert dev.get_nodes() == []

    def test_get_node_properties(self):
        dev = DiscoveredDevice("panel-1")
        desc = {
            "nodes": {
                "core": {
                    "name": "Core",
                    "properties": {"active-power": {"datatype": "float", "unit": "W"}},
                }
            }
        }
        dev.update_description(json.dumps(desc))
        props = dev.get_node_properties("core")
        assert "active-power" in props

    def test_get_node_properties_missing_node(self):
        dev = DiscoveredDevice("panel-1")
        dev.update_description(json.dumps({"nodes": {}}))
        assert dev.get_node_properties("missing") == {}


# ── Controller ───────────────────────────────────────────────────────────


def _make_controller(mock_paho, device_id=None, auto_start=False):
    """Helper to create a Controller with mocked MQTT."""
    with patch("ebus_sdk.homie.MqttClient.from_config") as mock_from_config:
        mock_client = MagicMock()
        mock_client.sub_callbacks = {}
        mock_from_config.return_value = mock_client

        ctrl = Controller(
            mqtt_cfg={"host": "localhost", "port": 1883},
            auto_start=auto_start,
            device_id=device_id,
        )
        return ctrl, mock_client


class TestControllerInit:
    def test_default_init(self, mock_paho):
        ctrl, mock_client = _make_controller(mock_paho)
        assert ctrl.homie_domain == EBUS_HOMIE_DOMAIN
        assert ctrl.device_id is None
        assert ctrl.devices == {}

    def test_device_id_stored(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho, device_id="panel-1")
        assert ctrl.device_id == "panel-1"

    def test_callbacks_initially_none(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        assert ctrl._on_device_discovered is None
        assert ctrl._on_device_state_changed is None
        assert ctrl._on_device_removed is None
        assert ctrl._on_property_changed is None
        assert ctrl._on_description_received is None


class TestControllerDiscoveryWildcard:
    """Test wildcard (multi-device) discovery mode."""

    def test_start_discovery_subscribes_wildcard(self, mock_paho):
        ctrl, mock_client = _make_controller(mock_paho)
        ctrl.start_discovery()

        mock_client.subscribe.assert_called_once()
        args = mock_client.subscribe.call_args
        assert args[0][0] == f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/+/$state"

    def test_state_message_discovers_new_device(self, mock_paho):
        ctrl, mock_client = _make_controller(mock_paho)
        discovered = []
        ctrl.set_on_device_discovered_callback(lambda dev: discovered.append(dev))
        ctrl.start_discovery()

        # Simulate $state message
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )

        assert "panel-1" in ctrl.devices
        assert len(discovered) == 1
        assert discovered[0].device_id == "panel-1"
        assert discovered[0].state == "ready"

    def test_state_change_fires_callback(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        changes = []
        ctrl.set_on_device_state_changed_callback(lambda dev, old, new: changes.append((old, new)))
        ctrl.start_discovery()

        # First message — discovery
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"init",
        )
        # Second message — state change
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )

        assert len(changes) == 1
        assert changes[0] == ("init", "ready")

    def test_same_state_does_not_fire_callback(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        changes = []
        ctrl.set_on_device_state_changed_callback(lambda dev, old, new: changes.append((old, new)))
        ctrl.start_discovery()

        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )

        assert len(changes) == 0

    def test_empty_payload_removes_device(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        removed = []
        ctrl.set_on_device_removed_callback(lambda dev: removed.append(dev))
        ctrl.start_discovery()

        # Discover first
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )
        # Then remove
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"",
        )

        assert "panel-1" not in ctrl.devices
        assert len(removed) == 1


class TestControllerDiscoverySingleDevice:
    """Test single-device (device_id) discovery mode."""

    def test_start_discovery_subscribes_exact_topics(self, mock_paho):
        ctrl, mock_client = _make_controller(mock_paho, device_id="panel-1")
        ctrl.start_discovery()

        # Should subscribe to 4 exact topics (no wildcard in device-id position)
        assert mock_client.subscribe.call_count == 4
        topics = [c[0][0] for c in mock_client.subscribe.call_args_list]
        base = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1"
        assert f"{base}/$state" in topics
        assert f"{base}/$description" in topics
        assert f"{base}/+/+" in topics
        assert f"{base}/+/+/$target" in topics

    def test_pre_creates_device_entry(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho, device_id="panel-1")
        ctrl.start_discovery()

        assert "panel-1" in ctrl.devices
        assert ctrl.devices["panel-1"].state is None  # Pre-created, no state yet

    def test_first_state_fires_discovered(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho, device_id="panel-1")
        discovered = []
        ctrl.set_on_device_discovered_callback(lambda dev: discovered.append(dev))
        ctrl.start_discovery()

        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )

        assert len(discovered) == 1
        assert discovered[0].state == "ready"

    def test_no_wildcard_in_device_id_position(self, mock_paho):
        """Verify there is no '+' in the device-id segment of any subscription."""
        ctrl, mock_client = _make_controller(mock_paho, device_id="panel-1")
        ctrl.start_discovery()

        for c in mock_client.subscribe.call_args_list:
            topic = c[0][0]
            parts = topic.split("/")
            # parts[2] is the device-id position
            assert parts[2] == "panel-1", f"Wildcard found in device-id position: {topic}"


class TestControllerPropertyMessages:
    """Test property and description message handling."""

    def test_description_received(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        descriptions = []
        ctrl.set_on_description_received_callback(lambda dev: descriptions.append(dev))
        ctrl.start_discovery()

        # Discover device
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )

        desc = {"homie": "5.0", "nodes": {"core": {"name": "Core"}}}
        ctrl._on_description_message(
            "panel-1",
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$description",
            json.dumps(desc).encode(),
        )

        assert len(descriptions) == 1
        assert descriptions[0].description == desc

    def test_property_changed(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        changes = []
        ctrl.set_on_property_changed_callback(
            lambda dev_id, node, prop, val, old: changes.append((dev_id, node, prop, val, old))
        )
        ctrl.start_discovery()

        # Discover
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )

        ctrl._on_property_message(
            "panel-1",
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/core/active-power",
            b"-500",
        )

        assert len(changes) == 1
        assert changes[0] == ("panel-1", "core", "active-power", "-500", None)

    def test_property_skips_dollar_attributes(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        changes = []
        ctrl.set_on_property_changed_callback(lambda *args: changes.append(args))
        ctrl.start_discovery()

        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )

        # $description should be skipped
        ctrl._on_property_message(
            "panel-1",
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/core/$description",
            b"{}",
        )

        assert len(changes) == 0

    def test_target_message(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        ctrl.start_discovery()

        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )

        ctrl._on_target_message(
            "panel-1",
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/breaker/state/$target",
            b"CLOSED",
        )

        dev = ctrl.devices["panel-1"]
        assert dev.get_property_target("breaker", "state") == "CLOSED"


class TestControllerSetProperty:
    def test_set_property_publishes(self, mock_paho):
        ctrl, mock_client = _make_controller(mock_paho)

        result = ctrl.set_property("panel-1", "breaker", "state", "CLOSED")

        assert result is True
        expected_topic = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/breaker/state/set"
        mock_client.publish.assert_called_once_with(expected_topic, "CLOSED", qos=EBUS_HOMIE_MQTT_QOS, retain=False)

    def test_set_property_no_connection(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        ctrl.mqttc = None

        result = ctrl.set_property("panel-1", "breaker", "state", "CLOSED")
        assert result is False


class TestControllerBroadcast:
    def test_broadcast(self, mock_paho):
        ctrl, mock_client = _make_controller(mock_paho)

        result = ctrl.broadcast("alert", "test-message")

        assert result is True
        expected_topic = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/$broadcast/alert"
        mock_client.publish.assert_called_once_with(
            expected_topic, "test-message", qos=EBUS_HOMIE_MQTT_QOS, retain=False
        )


class TestControllerStop:
    def test_stop_clears_devices(self, mock_paho):
        ctrl, mock_client = _make_controller(mock_paho)
        ctrl.start_discovery()

        # Discover a device
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )
        assert len(ctrl.devices) == 1

        ctrl.stop()

        assert ctrl.devices == {}
        assert ctrl.mqttc is None
        mock_client.stop.assert_called_once()

    def test_stop_clears_callbacks(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        ctrl.set_on_device_discovered_callback(lambda d: None)
        ctrl.set_on_property_changed_callback(lambda *a: None)
        ctrl.set_on_description_received_callback(lambda d: None)
        ctrl.set_on_device_state_changed_callback(lambda *a: None)
        ctrl.set_on_device_removed_callback(lambda d: None)

        ctrl.stop()

        assert ctrl._on_device_discovered is None
        assert ctrl._on_device_state_changed is None
        assert ctrl._on_device_removed is None
        assert ctrl._on_property_changed is None
        assert ctrl._on_description_received is None

    def test_stop_without_mqttc(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        ctrl.mqttc = None
        # Should not raise
        ctrl.stop()

    def test_get_device(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        ctrl.start_discovery()
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )

        dev = ctrl.get_device("panel-1")
        assert dev is not None
        assert dev.device_id == "panel-1"

        assert ctrl.get_device("nonexistent") is None

    def test_get_all_devices_returns_copy(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        ctrl.start_discovery()
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )

        all_devs = ctrl.get_all_devices()
        assert "panel-1" in all_devs
        # Mutating the copy shouldn't affect the controller
        all_devs.pop("panel-1")
        assert "panel-1" in ctrl.devices


# ── Controller QoS ────────────────────────────────────────────────────────


def _make_controller_with_qos(mock_paho, qos, device_id=None):
    """Helper to create a Controller with a custom QoS and mocked MQTT."""
    with patch("ebus_sdk.homie.MqttClient.from_config") as mock_from_config:
        mock_client = MagicMock()
        mock_client.sub_callbacks = {}
        mock_from_config.return_value = mock_client

        ctrl = Controller(
            mqtt_cfg={"host": "localhost", "port": 1883},
            device_id=device_id,
            qos=qos,
        )
        return ctrl, mock_client


class TestControllerQoS:
    """Test client-settable QoS on Controller."""

    def test_qos_defaults_to_global(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        assert ctrl.qos == EBUS_HOMIE_MQTT_QOS

    def test_qos_property_returns_custom_value(self, mock_paho):
        ctrl, _ = _make_controller_with_qos(mock_paho, qos=1)
        assert ctrl.qos == 1

    def test_wildcard_subscribe_uses_custom_qos(self, mock_paho):
        ctrl, mock_client = _make_controller_with_qos(mock_paho, qos=0)
        ctrl.start_discovery()

        mock_client.subscribe.assert_called_once()
        _, kwargs = mock_client.subscribe.call_args
        assert kwargs["qos"] == 0

    def test_single_device_subscribe_uses_custom_qos(self, mock_paho):
        ctrl, mock_client = _make_controller_with_qos(mock_paho, qos=1, device_id="panel-1")
        ctrl.start_discovery()

        assert mock_client.subscribe.call_count == 4
        for c in mock_client.subscribe.call_args_list:
            _, kwargs = c
            assert kwargs["qos"] == 1

    def test_set_property_uses_controller_qos_by_default(self, mock_paho):
        ctrl, mock_client = _make_controller_with_qos(mock_paho, qos=1)

        ctrl.set_property("panel-1", "breaker", "state", "CLOSED")

        _, kwargs = mock_client.publish.call_args
        assert kwargs["qos"] == 1

    def test_set_property_allows_qos_override(self, mock_paho):
        ctrl, mock_client = _make_controller_with_qos(mock_paho, qos=1)

        ctrl.set_property("panel-1", "breaker", "state", "CLOSED", qos=0)

        _, kwargs = mock_client.publish.call_args
        assert kwargs["qos"] == 0

    def test_broadcast_uses_controller_qos_by_default(self, mock_paho):
        ctrl, mock_client = _make_controller_with_qos(mock_paho, qos=1)

        ctrl.broadcast("alert", "test-message")

        _, kwargs = mock_client.publish.call_args
        assert kwargs["qos"] == 1

    def test_broadcast_allows_qos_override(self, mock_paho):
        ctrl, mock_client = _make_controller_with_qos(mock_paho, qos=1)

        ctrl.broadcast("alert", "test-message", qos=0)

        _, kwargs = mock_client.publish.call_args
        assert kwargs["qos"] == 0

    def test_subscribe_to_device_uses_custom_qos(self, mock_paho):
        """Verify _subscribe_to_device (wildcard re-subscribe on new device) uses controller QoS."""
        ctrl, mock_client = _make_controller_with_qos(mock_paho, qos=1)
        ctrl.start_discovery()
        mock_client.subscribe.reset_mock()

        # Simulate discovering a new device in wildcard mode
        ctrl._subscribe_to_device("panel-2")

        assert mock_client.subscribe.call_count == 3
        for c in mock_client.subscribe.call_args_list:
            _, kwargs = c
            assert kwargs["qos"] == 1
