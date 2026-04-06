"""Tests for ebus_sdk.homie device-role classes: Property, Node, Device, and helpers."""

import json
from enum import Enum
from functools import partial
from unittest.mock import MagicMock, patch, call

import pytest

from ebus_sdk.homie import (
    Property,
    PropertyDatatype,
    Node,
    Device,
    DeviceState,
    StateTransitionContext,
    Unit,
    datatype_from_type,
    ebus_cfg_add_auth,
    EBUS_HOMIE_DOMAIN,
    EBUS_HOMIE_MQTT_QOS,
    EBUS_HOMIE_VERSION_MAJOR,
)


# ── Helpers ──────────────────────────────────────────────────────────────


def _mock_mqtt_client():
    """Create a MagicMock that behaves like MqttClient."""
    mock = MagicMock()
    mock.is_running = True
    mock.publish.return_value = MagicMock(rc=0)
    mock.subscribe.return_value = (0, 1)
    return mock


def _make_device(mock_paho, device_id="test-device", **kwargs):
    """Create a Device with mocked MQTT."""
    with patch("ebus_sdk.homie.MqttClient.from_config") as mock_from_config:
        mock_client = _mock_mqtt_client()
        mock_from_config.return_value = mock_client
        device = Device(
            id=device_id,
            mqtt_cfg={"host": "localhost", "port": 1883},
            **kwargs,
        )
        return device, mock_client


def _make_wired_property(mock_client, device_id="dev1", node_id="node1", **prop_kwargs):
    """Create a Property wired to a mock Node and Device for publish testing."""
    mock_device = MagicMock()
    mock_device.id.return_value = device_id
    mock_device.get_mqtt_client.return_value = mock_client
    mock_device._qos = EBUS_HOMIE_MQTT_QOS

    node = Node(id=node_id, device=mock_device)
    defaults = dict(id="temperature", value=72.5, datatype=PropertyDatatype.FLOAT)
    defaults.update(prop_kwargs)
    prop = Property(**defaults)
    prop.set_node(node)
    return prop


# ── datatype_from_type ───────────────────────────────────────────────────


class TestDatatypeFromType:
    def test_int(self):
        assert datatype_from_type(int) == PropertyDatatype.INTEGER

    def test_float(self):
        assert datatype_from_type(float) == PropertyDatatype.FLOAT

    def test_bool(self):
        assert datatype_from_type(bool) == PropertyDatatype.BOOLEAN

    def test_str(self):
        assert datatype_from_type(str) == PropertyDatatype.STRING

    def test_color_string(self):
        assert datatype_from_type("color") == PropertyDatatype.COLOR

    def test_datetime_string(self):
        assert datatype_from_type("datetime") == PropertyDatatype.DATETIME

    def test_duration_string(self):
        assert datatype_from_type("duration") == PropertyDatatype.DURATION

    def test_json_string(self):
        assert datatype_from_type("json") == PropertyDatatype.JSON

    def test_unknown_returns_none(self):
        assert datatype_from_type(list) is None


# ── Unit enum ────────────────────────────────────────────────────────────


class TestUnit:
    def test_watt(self):
        assert Unit.WATT == "W"

    def test_kilowatt_hour(self):
        assert Unit.KILOWATT_HOUR == "kWh"

    def test_percent(self):
        assert Unit.PERCENT == "%"


# ── Homie Property ──────────────────────────────────────────────────────


class TestHomieProperty:
    def test_basic_init(self):
        p = Property(id="temp", value=72.5, name="Temperature",
                     datatype=PropertyDatatype.FLOAT, unit="°C")
        assert p.id() == "temp"
        assert p.name() == "Temperature"
        assert p.value() == 72.5
        assert p.datatype() == PropertyDatatype.FLOAT
        assert p.settable() is False
        assert p.retained() is True

    def test_name_defaults_to_id(self):
        p = Property(id="temp")
        assert p.name() == "temp"

    def test_from_dict(self):
        d = {
            "id": "mode",
            "value": "auto",
            "name": "Mode",
            "datatype": PropertyDatatype.ENUM,
            "format": "auto,manual,off",
            "settable": True,
            "set_callback": lambda x: None,
        }
        p = Property(from_dict=d)
        assert p.id() == "mode"
        assert p.format() == "auto,manual,off"
        assert p.settable() is True
        assert p.get_set_callback() is not None

    def test_set_callback_ignored_when_not_settable(self):
        cb = MagicMock()
        p = Property(id="temp", settable=False, set_callback=cb)
        assert p.get_set_callback() is None

    def test_set_callback_stored_when_settable(self):
        cb = MagicMock()
        p = Property(id="temp", settable=True, set_callback=cb)
        assert p.get_set_callback() is cb

    def test_set_set_callback(self):
        p = Property(id="temp", settable=True)
        cb = MagicMock()
        p.set_set_callback(cb)
        assert p.get_set_callback() is cb

    def test_round_to(self):
        p = Property(id="temp", value=72.456, datatype=PropertyDatatype.FLOAT, round_to=1)
        assert p.value() == 72.5
        assert p.round() == 1

    def test_no_round(self):
        p = Property(id="temp", value=72.456)
        assert p.value() == 72.456

    def test_ever_published_initially_false(self):
        p = Property(id="temp", value=72)
        assert p.was_ever_published() is False

    def test_skip_initial_publish_from_dict(self):
        p = Property(from_dict={"id": "temp", "skip_initial_publish": True})
        assert p._skip_initial_publish is True

    def test_supports_target(self):
        p = Property(id="temp", supports_target=True)
        assert p.supports_target() is True

    def test_is_json_datatype(self):
        p = Property(id="data", datatype=PropertyDatatype.JSON)
        assert p.is_json_datatype() is True

        p2 = Property(id="data", datatype=PropertyDatatype.STRING)
        assert p2.is_json_datatype() is False

    def test_set_settable_from_false_to_true(self):
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(mock_client, settable=False)
        prop.set_settable(True)
        assert prop.settable() is True

    def test_set_settable_noop_same_value(self):
        p = Property(id="temp", settable=False)
        p.set_settable(False)
        assert p.settable() is False


class TestHomiePropertyDescription:
    def test_basic_description(self):
        p = Property(id="temp", name="Temperature",
                     datatype=PropertyDatatype.FLOAT, unit="°C")
        desc = p.description()
        assert desc["name"] == "Temperature"
        assert desc["datatype"] == PropertyDatatype.FLOAT
        assert desc["unit"] == "°C"
        assert "settable" not in desc  # only included if True
        assert "retained" not in desc  # only included if False

    def test_description_includes_settable(self):
        p = Property(id="mode", settable=True, datatype=PropertyDatatype.ENUM,
                     format="auto,manual")
        desc = p.description()
        assert desc["settable"] is True
        assert desc["format"] == "auto,manual"

    def test_description_includes_retained_false(self):
        p = Property(id="event", retained=False, datatype=PropertyDatatype.STRING)
        desc = p.description()
        assert desc["retained"] is False

    def test_as_dict(self):
        p = Property(id="temp", name="Temperature", value=72.5,
                     datatype=PropertyDatatype.FLOAT, settable=False)
        d = p.as_dict()
        assert d["id"] == "temp"
        assert d["value"] == 72.5
        assert d["settable"] is False


class TestHomiePropertyCoercion:
    def test_coerced_value_string(self):
        p = Property(id="name", value="hello", datatype=PropertyDatatype.STRING)
        assert p.coerced_value() == "hello"

    def test_coerced_value_int(self):
        p = Property(id="count", value=42, datatype=PropertyDatatype.INTEGER)
        assert p.coerced_value() == "42"

    def test_coerced_value_float(self):
        p = Property(id="temp", value=72.5, datatype=PropertyDatatype.FLOAT)
        assert p.coerced_value() == "72.5"

    def test_coerced_value_boolean_true(self):
        p = Property(id="active", value=True, datatype=PropertyDatatype.BOOLEAN)
        assert p.coerced_value() == "true"

    def test_coerced_value_boolean_false(self):
        p = Property(id="active", value=False, datatype=PropertyDatatype.BOOLEAN)
        assert p.coerced_value() == "false"

    def test_coerced_value_boolean_invalid(self):
        p = Property(id="active", value="yes", datatype=PropertyDatatype.BOOLEAN)
        assert p.coerced_value() is None

    def test_coerced_value_none(self):
        p = Property(id="temp", value=None, datatype=PropertyDatatype.FLOAT)
        assert p.coerced_value() is None

    def test_coerced_value_enum(self):
        class Color(Enum):
            RED = "red"
        p = Property(id="color", value=Color.RED, datatype=PropertyDatatype.ENUM)
        assert p.coerced_value() == "red"


class TestHomiePropertyPublish:
    def test_publish_value_success(self):
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(mock_client)

        result = prop.publish_value()

        assert result is True
        assert prop.was_ever_published() is True
        mock_client.publish.assert_called_once()
        call_args = mock_client.publish.call_args
        assert "dev1" in call_args[0][0]
        assert "node1" in call_args[0][0]
        assert "temperature" in call_args[0][0]

    def test_publish_value_no_mqtt_client(self):
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(mock_client)
        # Break the mqtt client
        prop.node().device().get_mqtt_client.return_value = None

        result = prop.publish_value()
        assert result is False

    def test_publish_value_mqtt_not_running(self):
        mock_client = _mock_mqtt_client()
        mock_client.is_running = False
        prop = _make_wired_property(mock_client)

        result = prop.publish_value()
        assert result is False

    def test_publish_skips_none_value_never_published(self):
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(mock_client, value=None)

        result = prop.publish_value()
        assert result is True  # returns True but doesn't actually publish
        mock_client.publish.assert_not_called()
        assert prop.was_ever_published() is False

    def test_publish_skip_initial_publish_flag(self):
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(mock_client, value=None)
        prop._skip_initial_publish = True

        result = prop.publish_value()
        assert result is True
        mock_client.publish.assert_not_called()

    def test_set_value_publishes(self):
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(mock_client)

        result = prop.set_value(99.0)
        assert result is True
        assert prop.value() == 99.0
        mock_client.publish.assert_called_once()

    def test_publish_boolean_coerced(self):
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(mock_client, value=True,
                                     datatype=PropertyDatatype.BOOLEAN, id="active")

        prop.publish_value()
        call_args = mock_client.publish.call_args
        assert call_args[0][1] == "true"

    def test_publish_exception_returns_false(self):
        mock_client = _mock_mqtt_client()
        mock_client.publish.side_effect = Exception("connection lost")
        prop = _make_wired_property(mock_client)

        result = prop.publish_value()
        assert result is False


class TestHomiePropertyClearValue:
    def test_clear_value_never_published_skips(self):
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(mock_client)
        assert prop.was_ever_published() is False

        result = prop.clear_value()
        assert result is True
        mock_client.publish.assert_not_called()

    def test_clear_value_after_publish(self):
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(mock_client)
        prop.publish_value()  # mark as published
        mock_client.publish.reset_mock()

        result = prop.clear_value()
        assert result is True
        assert prop.was_ever_published() is False
        call_args = mock_client.publish.call_args
        assert call_args[0][1] == ""  # empty payload
        assert call_args[1]["retain"] is True

    def test_clear_value_no_mqtt_client(self):
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(mock_client)
        prop._ever_published = True
        prop.node().device().get_mqtt_client.return_value = None

        result = prop.clear_value()
        assert result is False

    def test_clear_value_exception(self):
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(mock_client)
        prop._ever_published = True
        mock_client.publish.side_effect = Exception("fail")

        result = prop.clear_value()
        assert result is False


class TestHomiePropertySettableCallback:
    def test_settable_callback_invokes_set_callback(self):
        cb = MagicMock()
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(mock_client, id="mode", settable=True,
                                     set_callback=cb, datatype=PropertyDatatype.STRING)

        topic = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/dev1/node1/mode/set"
        prop._settable_callback(topic, b"manual")
        cb.assert_called_once_with("manual")

    def test_settable_callback_json_datatype(self):
        cb = MagicMock()
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(mock_client, id="config", settable=True,
                                     set_callback=cb, datatype=PropertyDatatype.JSON)

        topic = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/dev1/node1/config/set"
        payload = json.dumps({"key": "value"}).encode()
        prop._settable_callback(topic, payload)
        cb.assert_called_once_with({"key": "value"})

    def test_settable_callback_not_settable_noop(self):
        cb = MagicMock()
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(mock_client, id="temp", settable=False,
                                     set_callback=cb, datatype=PropertyDatatype.FLOAT)

        topic = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/dev1/node1/temp/set"
        prop._settable_callback(topic, b"99")
        cb.assert_not_called()

    def test_settable_callback_invalid_topic_noop(self):
        cb = MagicMock()
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(mock_client, id="mode", settable=True,
                                     set_callback=cb, datatype=PropertyDatatype.STRING)

        # Wrong domain
        topic = "wrong/5/dev1/node1/mode/set"
        prop._settable_callback(topic, b"manual")
        cb.assert_not_called()


class TestHomiePropertySetSubscribe:
    def test_set_subscribe_settable(self):
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(mock_client, id="mode", settable=True,
                                     datatype=PropertyDatatype.STRING)

        prop.set_subscribe()
        mock_client.subscribe.assert_called_once()
        topic = mock_client.subscribe.call_args[0][0]
        assert topic.endswith("/mode/set")

    def test_set_subscribe_not_settable(self):
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(mock_client, id="temp", settable=False)

        prop.set_subscribe()
        mock_client.subscribe.assert_not_called()

    def test_set_subscribe_no_mqtt(self):
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(mock_client, id="mode", settable=True)
        prop.node().device().get_mqtt_client.return_value = None

        prop.set_subscribe()
        mock_client.subscribe.assert_not_called()


# ── Node ─────────────────────────────────────────────────────────────────


class TestNode:
    def test_basic_init(self):
        n = Node(id="core", name="Core Node", type="sensor")
        assert n.id() == "core"
        assert n.name() == "Core Node"
        assert n.type() == "sensor"
        assert n.properties() == {}

    def test_name_defaults_to_id(self):
        n = Node(id="core")
        assert n.name() == "core"

    def test_from_dict(self):
        n = Node(from_dict={"id": "core", "name": "Core", "type": "ctrl"})
        assert n.id() == "core"
        assert n.name() == "Core"
        assert n.type() == "ctrl"

    def test_from_dict_name_defaults_to_id(self):
        n = Node(from_dict={"id": "core"})
        assert n.name() == "core"

    def test_set_and_get_device(self):
        mock_device = MagicMock()
        n = Node(id="core")
        n.set_device(mock_device)
        assert n.device() is mock_device

    def test_get_mqtt_client_via_device(self):
        mock_device = MagicMock()
        mock_mqttc = MagicMock()
        mock_device.get_mqtt_client.return_value = mock_mqttc
        n = Node(id="core", device=mock_device)

        assert n.get_mqtt_client() is mock_mqttc

    def test_get_mqtt_client_no_device(self):
        n = Node(id="core")
        assert n.get_mqtt_client() is None

    def test_get_property(self):
        n = Node(id="core")
        p = Property(id="temp", value=72)
        n._properties["temp"] = p
        assert n.get_property("temp") is p

    def test_get_property_missing(self):
        n = Node(id="core")
        assert n.get_property("missing") is None


class TestNodeAddProperty:
    def test_add_property(self, mock_paho):
        device, mock_client = _make_device(mock_paho)
        node = device.new_node("core", "Core", "sensor")
        device.add_node(node)
        mock_client.publish.reset_mock()

        prop = Property(id="temp", value=72.5, datatype=PropertyDatatype.FLOAT)
        result = node.add_property(prop)

        assert result is prop
        assert "temp" in node.properties()
        assert prop.node() is node

    def test_add_property_from_dict(self, mock_paho):
        device, mock_client = _make_device(mock_paho)
        node = device.new_node("core")
        device.add_node(node)

        prop = node.add_property_from_dict({
            "id": "humidity",
            "value": 50.0,
            "datatype": PropertyDatatype.FLOAT,
        })
        assert prop.id() == "humidity"
        assert "humidity" in node.properties()

    def test_add_property_propagates_qos(self, mock_paho):
        device, _ = _make_device(mock_paho, qos=1)
        node = device.new_node("core")
        device.add_node(node)

        prop = Property(id="temp", value=72, datatype=PropertyDatatype.FLOAT)
        node.add_property(prop)
        assert prop._qos == 1


class TestNodeDeleteProperty:
    def test_delete_existing_property(self):
        mock_client = _mock_mqtt_client()
        mock_device = MagicMock()
        mock_device.get_mqtt_client.return_value = mock_client
        mock_device.id.return_value = "dev1"
        mock_device._qos = EBUS_HOMIE_MQTT_QOS

        node = Node(id="core", device=mock_device)
        prop = Property(id="temp", value=72, datatype=PropertyDatatype.FLOAT)
        prop.set_node(node)
        node._properties["temp"] = prop

        result = node.delete_property("temp")
        assert result is True
        assert "temp" not in node.properties()

    def test_delete_missing_property(self):
        n = Node(id="core")
        assert n.delete_property("missing") is False


class TestNodeDescription:
    def test_description(self):
        n = Node(id="core", name="Core", type="sensor")
        p = Property(id="temp", name="Temperature", datatype=PropertyDatatype.FLOAT)
        n._properties["temp"] = p

        desc = n.description()
        assert desc["name"] == "Core"
        assert desc["type"] == "sensor"
        assert "temp" in desc["properties"]
        assert desc["properties"]["temp"]["name"] == "Temperature"

    def test_as_dict(self):
        n = Node(id="core", name="Core", type="sensor")
        p = Property(id="temp", name="Temperature", value=72,
                     datatype=PropertyDatatype.FLOAT)
        n._properties["temp"] = p

        d = n.as_dict()
        assert d["id"] == "core"
        assert d["name"] == "Core"
        assert "temp" in d["properties"]


class TestNodeClearAllProperties:
    def test_clear_all_properties(self):
        mock_client = _mock_mqtt_client()
        mock_device = MagicMock()
        mock_device.get_mqtt_client.return_value = mock_client
        mock_device.id.return_value = "dev1"
        mock_device._qos = EBUS_HOMIE_MQTT_QOS

        node = Node(id="core", device=mock_device)

        # published property
        p1 = Property(id="temp", value=72, datatype=PropertyDatatype.FLOAT)
        p1.set_node(node)
        p1._ever_published = True
        node._properties["temp"] = p1

        # never-published property
        p2 = Property(id="humidity", value=50, datatype=PropertyDatatype.FLOAT)
        p2.set_node(node)
        node._properties["humidity"] = p2

        node.clear_all_properties()
        assert node.properties() == {}
        # Only the published property should have been cleared
        assert mock_client.publish.call_count == 1


class TestNodePublish:
    def test_publish_calls_property_publish(self):
        n = Node(id="core")
        p1 = MagicMock()
        p2 = MagicMock()
        n._properties = {"a": p1, "b": p2}

        n.publish()
        p1.publish_value.assert_called_once()
        p2.publish_value.assert_called_once()


# ── Device ───────────────────────────────────────────────────────────────


class TestDeviceInit:
    def test_basic_init(self, mock_paho):
        device, mock_client = _make_device(mock_paho, device_id="panel-1")
        assert device.id() == "panel-1"
        assert device.name() == "panel-1"  # defaults to id
        assert device.state() == DeviceState.READY  # after state_transition
        assert device.nodes() == {}

    def test_with_name(self, mock_paho):
        device, _ = _make_device(mock_paho, name="My Panel")
        assert device.name() == "My Panel"

    def test_with_type(self, mock_paho):
        device, _ = _make_device(mock_paho, type="electrical-panel")
        assert device.type() == "electrical-panel"

    def test_qos_stored(self, mock_paho):
        device, _ = _make_device(mock_paho, qos=1)
        assert device.qos == 1

    def test_lwt_configured(self, mock_paho):
        """Device should configure LWT as DeviceState.LOST on its $state topic."""
        with patch("ebus_sdk.homie.MqttClient.from_config") as mock_from_config:
            mock_client = _mock_mqtt_client()
            mock_from_config.return_value = mock_client
            Device(id="panel-1", mqtt_cfg={"host": "localhost", "port": 1883})

            lwt = mock_from_config.call_args[1]["lwt"]
            assert lwt["payload"] == DeviceState.LOST.value
            assert "$state" in lwt["topic"]

    def test_on_connect_callback_set(self, mock_paho):
        with patch("ebus_sdk.homie.MqttClient.from_config") as mock_from_config:
            mock_client = _mock_mqtt_client()
            mock_from_config.return_value = mock_client
            Device(id="panel-1", mqtt_cfg={"host": "localhost", "port": 1883})

            assert mock_from_config.call_args[1]["on_connect_callback"] is not None

    def test_nodes_passed_in_constructor(self, mock_paho):
        with patch("ebus_sdk.homie.MqttClient.from_config") as mock_from_config:
            mock_client = _mock_mqtt_client()
            mock_from_config.return_value = mock_client
            node = Node(id="core", name="Core", type="sensor")
            device = Device(
                id="panel-1",
                mqtt_cfg={"host": "localhost", "port": 1883},
                nodes=[node],
            )
            assert "core" in device.nodes()


class TestDeviceState:
    def test_set_state(self, mock_paho):
        device, mock_client = _make_device(mock_paho)
        mock_client.publish.reset_mock()

        result = device.set_state(DeviceState.DISCONNECTED)
        assert result is True
        assert device.state() == DeviceState.DISCONNECTED

    def test_set_state_same_noop(self, mock_paho):
        device, mock_client = _make_device(mock_paho)
        mock_client.publish.reset_mock()

        result = device.set_state(DeviceState.READY)
        assert result is False
        mock_client.publish.assert_not_called()


class TestDeviceStateTransition:
    def test_state_transition_context(self, mock_paho):
        device, mock_client = _make_device(mock_paho)
        mock_client.publish.reset_mock()

        with device.state_transition():
            assert device.state() == DeviceState.INIT

        assert device.state() == DeviceState.READY

    def test_state_transition_on_exception(self, mock_paho):
        device, _ = _make_device(mock_paho)

        with pytest.raises(ValueError):
            with device.state_transition():
                raise ValueError("test error")

        # Should still end in READY despite exception
        assert device.state() == DeviceState.READY


class TestDeviceNodes:
    def test_new_node(self, mock_paho):
        device, _ = _make_device(mock_paho)
        node = device.new_node("core", "Core Node", "sensor")
        assert node.id() == "core"
        assert node.device() is device

    def test_add_node(self, mock_paho):
        device, _ = _make_device(mock_paho)
        node = device.new_node("core")
        device.add_node(node)
        assert "core" in device.nodes()

    def test_add_node_from_dict(self, mock_paho):
        device, _ = _make_device(mock_paho)
        node = device.add_node_from_dict({"id": "core", "name": "Core"})
        assert "core" in device.nodes()
        assert node.id() == "core"

    def test_get_node(self, mock_paho):
        device, _ = _make_device(mock_paho)
        node = device.new_node("core")
        device.add_node(node)
        assert device.get_node("core") is node
        assert device.get_node("missing") is None

    def test_remove_node(self, mock_paho):
        device, _ = _make_device(mock_paho)
        node = device.new_node("core")
        device.add_node(node)

        assert device.remove_node("core") is True
        assert "core" not in device.nodes()

    def test_remove_node_missing(self, mock_paho):
        device, _ = _make_device(mock_paho)
        assert device.remove_node("missing") is False

    def test_delete_node(self, mock_paho):
        device, mock_client = _make_device(mock_paho)
        node = device.new_node("core")
        device.add_node(node)

        result = device.delete_node("core")
        assert result is True
        assert "core" not in device.nodes()

    def test_delete_node_missing(self, mock_paho):
        device, _ = _make_device(mock_paho)
        assert device.delete_node("missing") is False

    def test_add_node_propagates_qos(self, mock_paho):
        device, _ = _make_device(mock_paho, qos=1)
        node = Node(id="core")
        prop = Property(id="temp", value=72, datatype=PropertyDatatype.FLOAT)
        node._properties["temp"] = prop

        device.add_node(node)
        assert prop._qos == 1


class TestDeviceChildren:
    def test_add_child(self, mock_paho):
        device, _ = _make_device(mock_paho)
        assert device.add_child("child-1") is True
        assert "child-1" in device.children_ids()

    def test_add_child_duplicate(self, mock_paho):
        device, _ = _make_device(mock_paho)
        device.add_child("child-1")
        assert device.add_child("child-1") is False

    def test_remove_child(self, mock_paho):
        device, _ = _make_device(mock_paho)
        device.add_child("child-1")
        assert device.remove_child("child-1") is True
        assert "child-1" not in device.children_ids()

    def test_remove_child_missing(self, mock_paho):
        device, _ = _make_device(mock_paho)
        assert device.remove_child("missing") is False


class TestDeviceParent:
    def test_set_parent(self, mock_paho):
        device, _ = _make_device(mock_paho)
        device.set_parent("root-1")
        assert device.parent_id() == "root-1"

    def test_unset_parent(self, mock_paho):
        device, _ = _make_device(mock_paho)
        device.set_parent("root-1")
        assert device.unset_parent() is True
        assert device.parent_id() is None

    def test_unset_parent_already_none(self, mock_paho):
        device, _ = _make_device(mock_paho)
        assert device.unset_parent() is False


class TestDeviceDescription:
    def test_description_structure(self, mock_paho):
        device, _ = _make_device(mock_paho, device_id="panel-1",
                                  name="Panel", type="electrical-panel")
        desc = device.description()
        assert desc["name"] == "Panel"
        assert desc["type"] == "electrical-panel"
        assert "homie" in desc
        assert "version" in desc
        assert "nodes" in desc
        assert desc["children"] == []
        assert desc["extensions"] == []

    def test_description_with_root_and_parent(self, mock_paho):
        with patch("ebus_sdk.homie.MqttClient.from_config") as mock_from_config:
            mock_client = _mock_mqtt_client()
            mock_from_config.return_value = mock_client
            device = Device(
                id="child-1",
                mqtt_cfg={"host": "localhost", "port": 1883},
                root_id="root-1",
                parent_id="root-1",
            )
            desc = device.description()
            assert desc["root"] == "root-1"
            assert desc["parent"] == "root-1"

    def test_description_omits_root_parent_for_root_device(self, mock_paho):
        device, _ = _make_device(mock_paho)
        desc = device.description()
        assert "root" not in desc
        assert "parent" not in desc


class TestDevicePublish:
    def test_publish_state(self, mock_paho):
        device, mock_client = _make_device(mock_paho)
        mock_client.publish.reset_mock()

        device.publish_state(DeviceState.SLEEPING)
        mock_client.publish.assert_called_once()
        call_args = mock_client.publish.call_args
        assert "$state" in call_args[0][0]
        assert call_args[0][1] == DeviceState.SLEEPING

    def test_publish_description(self, mock_paho):
        device, mock_client = _make_device(mock_paho)
        mock_client.publish.reset_mock()

        device.publish_description(republish=True)
        # Should publish $description
        topics = [c[0][0] for c in mock_client.publish.call_args_list]
        assert any("$description" in t for t in topics)

    def test_publish_no_mqtt_client(self, mock_paho):
        device, _ = _make_device(mock_paho)
        device.mqttc = None
        # Should not raise
        device.publish("$state")

    def test_publish_no_device_id(self, mock_paho):
        device, _ = _make_device(mock_paho)
        device._id = None
        # Should not raise
        device.publish("$state")

    def test_publish_nodes(self, mock_paho):
        device, _ = _make_device(mock_paho)
        mock_node = MagicMock()
        device._nodes = {"core": mock_node}

        device.publish_nodes()
        mock_node.publish.assert_called_once()


class TestDeviceOnConnect:
    def test_initial_connection(self, mock_paho):
        device, mock_client = _make_device(mock_paho)
        # After construction, initial_broker_connection is already set to False
        # by the state_transition in __init__. Let's reset it.
        device.initial_broker_connection = True
        mock_node = MagicMock()
        device._nodes = {"core": mock_node}

        device.on_connect()

        assert device.initial_broker_connection is False
        mock_node.publish.assert_called_once()

    def test_reconnection(self, mock_paho):
        device, mock_client = _make_device(mock_paho)
        # Add a real node so description() can be JSON-serialized
        node = device.new_node("core", "Core", "sensor")
        device.add_node(node)
        device.initial_broker_connection = False
        mock_client.publish.reset_mock()

        device.on_connect()

        # Should republish description, nodes, and state
        topics = [c[0][0] for c in mock_client.publish.call_args_list]
        assert any("$description" in t for t in topics)
        assert any("$state" in t for t in topics)


class TestDeviceDeleteAllFromMqtt:
    def test_delete_all(self, mock_paho):
        device, mock_client = _make_device(mock_paho)
        node = device.new_node("core")
        device.add_node(node)

        # Add a published property
        prop = Property(id="temp", value=72, datatype=PropertyDatatype.FLOAT)
        prop.set_node(node)
        prop._ever_published = True
        node._properties["temp"] = prop

        mock_client.publish.reset_mock()

        device.delete_all_from_mqtt()

        assert device.nodes() == {}
        # Should have cleared the property topic and the description topic
        assert mock_client.publish.call_count >= 2

    def test_delete_all_no_mqtt_client(self, mock_paho):
        device, _ = _make_device(mock_paho)
        device.mqttc = None
        # Should not raise
        device.delete_all_from_mqtt()

    def test_delete_all_skips_unpublished_properties(self, mock_paho):
        device, mock_client = _make_device(mock_paho)
        node = device.new_node("core")
        device.add_node(node)

        prop = Property(id="temp", value=72, datatype=PropertyDatatype.FLOAT)
        prop.set_node(node)
        prop._ever_published = False
        node._properties["temp"] = prop

        mock_client.publish.reset_mock()

        device.delete_all_from_mqtt()

        # Only the $description should be cleared, not the property
        topics = [c[0][0] for c in mock_client.publish.call_args_list]
        assert len([t for t in topics if "temp" in t]) == 0
        assert len([t for t in topics if "$description" in t]) == 1


class TestDeviceClearRetainedTopic:
    def test_clear_retained(self, mock_paho):
        device, mock_client = _make_device(mock_paho)
        mock_client.publish.reset_mock()

        result = device.clear_retained_topic("ebus/5/panel-1/core/temp")
        assert result is True
        mock_client.publish.assert_called_once_with(
            "ebus/5/panel-1/core/temp", "", retain=True, qos=device.qos
        )

    def test_clear_retained_no_mqtt(self, mock_paho):
        device, _ = _make_device(mock_paho)
        device.mqttc = None
        assert device.clear_retained_topic("some/topic") is False

    def test_clear_retained_exception(self, mock_paho):
        device, mock_client = _make_device(mock_paho)
        mock_client.publish.side_effect = Exception("fail")
        assert device.clear_retained_topic("some/topic") is False


class TestDeviceConnectBroker:
    def test_connect_broker_noop_if_already_connected(self, mock_paho):
        device, mock_client = _make_device(mock_paho)
        # mqttc is already set
        with patch("ebus_sdk.homie.MqttClient.from_config") as mock_from_config:
            device.connect_broker()
            mock_from_config.assert_not_called()


class TestDeviceRefreshAllNodes:
    def test_refresh_all_nodes(self, mock_paho):
        device, mock_client = _make_device(mock_paho)
        mock_client.publish.reset_mock()

        device.refresh_all_nodes()

        topics = [c[0][0] for c in mock_client.publish.call_args_list]
        assert any("$description" in t for t in topics)
        assert any("$state" in t for t in topics)


class TestDeviceNowEms:
    def test_now_ems_returns_int(self):
        result = Device.now_ems()
        assert isinstance(result, int)
        assert result > 0


# ── ebus_cfg_add_auth ────────────────────────────────────────────────────


class TestEbusCfgAddAuth:
    def test_adds_auth(self):
        cfg = {"host": "localhost", "port": 1883}
        result = ebus_cfg_add_auth(cfg, "user", "pass")
        assert result["authentication"]["username"] == "user"
        assert result["authentication"]["password"] == "pass"
        assert result is cfg  # mutates in place


# ── DeviceState enum ────────────────────────────────────────────────────


class TestDeviceStateEnum:
    def test_values(self):
        assert DeviceState.INIT.value == "init"
        assert DeviceState.READY.value == "ready"
        assert DeviceState.DISCONNECTED.value == "disconnected"
        assert DeviceState.SLEEPING.value == "sleeping"
        assert DeviceState.LOST.value == "lost"


# ── PropertyDatatype enum ──────────────────────────────────────────────


class TestPropertyDatatypeEnum:
    def test_values(self):
        assert PropertyDatatype.INTEGER.value == "integer"
        assert PropertyDatatype.FLOAT.value == "float"
        assert PropertyDatatype.BOOLEAN.value == "boolean"
        assert PropertyDatatype.STRING.value == "string"
        assert PropertyDatatype.ENUM.value == "enum"
        assert PropertyDatatype.JSON.value == "json"
