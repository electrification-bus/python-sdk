"""Tests for ebus_sdk.homie device-role classes: Property, Node, Device, and helpers."""

import json
import logging
from enum import Enum
from unittest.mock import MagicMock, patch

import pytest

from ebus_sdk.homie import (
    Property,
    PropertyDatatype,
    Node,
    Device,
    DeviceState,
    Unit,
    datatype_from_type,
    ebus_cfg_add_auth,
    sanitize_homie_id,
    encode_empty_string,
    decode_empty_string,
    HOMIE_EMPTY_STRING_PAYLOAD,
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


# ── sanitize_homie_id ────────────────────────────────────────────────────


class TestSanitizeHomieId:
    def test_empty_string(self):
        assert sanitize_homie_id("") == ""

    def test_none(self):
        assert sanitize_homie_id(None) == ""

    def test_already_legal(self):
        assert sanitize_homie_id("device-1") == "device-1"

    def test_lowercases(self):
        # Tesla Powerwall serial — the real bug from G3P-23496
        assert sanitize_homie_id("TG121153003K7G") == "tg121153003k7g"

    def test_underscore_to_hyphen(self):
        assert sanitize_homie_id("my_device_id") == "my-device-id"

    def test_whitespace_to_hyphen(self):
        assert sanitize_homie_id("my device id") == "my-device-id"

    def test_dot_to_hyphen(self):
        assert sanitize_homie_id("v1.2.3") == "v1-2-3"

    def test_drops_illegal_chars(self):
        # Slashes, plus signs, etc. drop out entirely
        assert sanitize_homie_id("a/b+c") == "abc"

    def test_collapses_runs_of_hyphens(self):
        assert sanitize_homie_id("a---b") == "a-b"

    def test_collapses_mixed_separators(self):
        # Underscore + space + dot all become hyphens, then collapsed
        assert sanitize_homie_id("a_ .b") == "a-b"

    def test_strips_leading_trailing_hyphens(self):
        assert sanitize_homie_id("-abc-") == "abc"

    def test_strips_leading_trailing_from_separators(self):
        assert sanitize_homie_id(" abc ") == "abc"

    def test_all_illegal_collapses_to_empty(self):
        # Caller is responsible for handling empty result
        assert sanitize_homie_id("///+++") == ""

    def test_only_separators_collapses_to_empty(self):
        assert sanitize_homie_id("___") == ""

    def test_complex_composition(self):
        # Vendor-supplied composite: capitals, underscore, dot, illegal char
        assert sanitize_homie_id("My_Device.v1/2") == "my-device-v12"


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
        p = Property(id="temp", value=72.5, name="Temperature", datatype=PropertyDatatype.FLOAT, unit="°C")
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

    def test_set_format_updates_format_and_description(self):
        p = Property(id="mode", datatype=PropertyDatatype.ENUM, format="auto,manual")
        assert p.format() == "auto,manual"
        assert p.description()["format"] == "auto,manual"
        # A dynamic format update (e.g. an EVSE's advertised current range changing).
        p.set_format("auto,manual,off")
        assert p.format() == "auto,manual,off"
        assert p.description()["format"] == "auto,manual,off"

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
        p = Property(id="temp", name="Temperature", datatype=PropertyDatatype.FLOAT, unit="°C")
        desc = p.description()
        assert desc["name"] == "Temperature"
        assert desc["datatype"] == PropertyDatatype.FLOAT
        assert desc["unit"] == "°C"
        assert "settable" not in desc  # only included if True
        assert "retained" not in desc  # only included if False

    def test_description_includes_settable(self):
        p = Property(id="mode", settable=True, datatype=PropertyDatatype.ENUM, format="auto,manual")
        desc = p.description()
        assert desc["settable"] is True
        assert desc["format"] == "auto,manual"

    def test_description_includes_retained_false(self):
        p = Property(id="event", retained=False, datatype=PropertyDatatype.STRING)
        desc = p.description()
        assert desc["retained"] is False

    def test_as_dict(self):
        p = Property(id="temp", name="Temperature", value=72.5, datatype=PropertyDatatype.FLOAT, settable=False)
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

    def test_coerced_value_json_dict(self):
        # A dict on a json property must serialize to valid JSON (json.dumps),
        # not Python repr (str() would emit single quotes). SDK-3c8 / GH #4.
        value = {"mode": "SHED", "duration": 3600, "active": True, "note": None}
        p = Property(id="event", value=value, datatype=PropertyDatatype.JSON)
        coerced = p.coerced_value()
        assert "'" not in coerced  # no Python-repr single quotes
        assert json.loads(coerced) == value  # round-trips back to the dict

    def test_coerced_value_json_list(self):
        # The utility-meter doe model uses json arrays of envelope objects.
        value = [{"power-limit": 30000, "source": "GRID"}, {"power-limit": 0}]
        p = Property(id="import-limit", value=value, datatype=PropertyDatatype.JSON)
        assert json.loads(p.coerced_value()) == value

    def test_coerced_value_json_string_passthrough(self):
        # An already-serialized JSON string must not be double-encoded.
        text = '{"mode":"SHED","duration":3600}'
        p = Property(id="event", value=text, datatype=PropertyDatatype.JSON)
        assert p.coerced_value() == text
        assert json.loads(p.coerced_value()) == {"mode": "SHED", "duration": 3600}

    def test_coerced_value_json_roundtrip_via_settable(self):
        # A value received on /set (parsed to a dict by the inbound path) must
        # re-publish as valid JSON: inbound json.loads and outbound json.dumps
        # agree on the stored Python type (dict). SDK-3c8 / GH #4.
        received = json.loads('{"mode":"LOAD_UP","duration":1800}')  # inbound /set path
        p = Property(id="event", value=received, datatype=PropertyDatatype.JSON)
        assert json.loads(p.coerced_value()) == received


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
        mock_client.is_connected.return_value = False  # fully down: not started and not connected
        prop = _make_wired_property(mock_client)

        result = prop.publish_value()
        assert result is False

    def test_publish_value_publishes_when_connected_but_not_running(self):
        """Bring-your-own-transport: a caller-driven client is connected but never gets
        the SDK's is_running set (the caller owns the loop). Values must still publish (#14)."""
        mock_client = _mock_mqtt_client()
        mock_client.is_running = False
        mock_client.is_connected.return_value = True
        prop = _make_wired_property(mock_client)

        assert prop.publish_value() is True
        mock_client.publish.assert_called_once()

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

    def test_publish_none_after_published_clears_retained(self):
        # Regression for SDK-ef1 / GH #2: once a value has been published,
        # setting it to None must retract the retained message (empty payload,
        # retain=True) rather than silently leaving the stale value behind.
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(mock_client)
        prop.publish_value()  # mark as published
        mock_client.publish.reset_mock()

        prop._value = None
        result = prop.publish_value()

        assert result is True
        assert prop.was_ever_published() is False
        call_args = mock_client.publish.call_args
        assert call_args[0][1] == ""  # empty payload clears the retained topic
        assert call_args[1]["retain"] is True

    def test_set_value_none_after_published_clears_retained(self):
        # The publisher-facing path: set_value(None) must clear, mirroring what
        # the adapter/bridge pattern relies on when an app property goes None.
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(mock_client)
        prop.set_value(99.0)
        mock_client.publish.reset_mock()

        result = prop.set_value(None)

        assert result is True
        call_args = mock_client.publish.call_args
        assert call_args[0][1] == ""
        assert call_args[1]["retain"] is True

    def test_publish_none_never_published_does_not_clear(self):
        # The never-published None case must remain a silent no-op (no phantom
        # retained-empty topic).
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(mock_client, value=None)

        result = prop.publish_value()

        assert result is True
        mock_client.publish.assert_not_called()
        assert prop.was_ever_published() is False

    def test_publish_boolean_coerced(self):
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(mock_client, value=True, datatype=PropertyDatatype.BOOLEAN, id="active")

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
        prop = _make_wired_property(
            mock_client, id="mode", settable=True, set_callback=cb, datatype=PropertyDatatype.STRING
        )

        topic = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/dev1/node1/mode/set"
        prop._settable_callback(topic, b"manual")
        cb.assert_called_once_with("manual")

    def test_settable_callback_json_datatype(self):
        cb = MagicMock()
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(
            mock_client, id="config", settable=True, set_callback=cb, datatype=PropertyDatatype.JSON
        )

        topic = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/dev1/node1/config/set"
        payload = json.dumps({"key": "value"}).encode()
        prop._settable_callback(topic, payload)
        cb.assert_called_once_with({"key": "value"})

    def test_settable_callback_not_settable_noop(self):
        cb = MagicMock()
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(
            mock_client, id="temp", settable=False, set_callback=cb, datatype=PropertyDatatype.FLOAT
        )

        topic = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/dev1/node1/temp/set"
        prop._settable_callback(topic, b"99")
        cb.assert_not_called()

    def test_settable_callback_invalid_topic_noop(self):
        cb = MagicMock()
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(
            mock_client, id="mode", settable=True, set_callback=cb, datatype=PropertyDatatype.STRING
        )

        # Wrong domain
        topic = "wrong/5/dev1/node1/mode/set"
        prop._settable_callback(topic, b"manual")
        cb.assert_not_called()


class TestEmptyStringEncoding:
    # Homie 5: an empty-string *value* is carried as a single 0x00 byte, to
    # distinguish it from a zero-length payload (which clears the retained topic).

    def test_encode_empty_string(self):
        assert encode_empty_string("") == HOMIE_EMPTY_STRING_PAYLOAD
        assert encode_empty_string("") == "\x00"

    def test_encode_non_empty_passthrough(self):
        assert encode_empty_string("hello") == "hello"
        assert encode_empty_string("0") == "0"

    def test_decode_empty_string(self):
        assert decode_empty_string(HOMIE_EMPTY_STRING_PAYLOAD) == ""
        assert decode_empty_string("\x00") == ""

    def test_decode_non_empty_passthrough(self):
        assert decode_empty_string("hello") == "hello"

    def test_encode_decode_roundtrip(self):
        for v in ["", "hello", "0", "false", "multi word"]:
            assert decode_empty_string(encode_empty_string(v)) == v

    def test_publish_empty_string_value_encodes_null_byte(self):
        # A string property whose value is "" must publish 0x00, not a
        # zero-length payload (which the broker would treat as a clear).
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(mock_client, id="label", value="", datatype=PropertyDatatype.STRING)

        result = prop.publish_value()

        assert result is True
        call_args = mock_client.publish.call_args
        assert call_args[0][1] == "\x00"
        assert call_args[1]["retain"] is True  # default retained

    def test_settable_callback_decodes_null_byte_to_empty_string(self):
        cb = MagicMock()
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(
            mock_client, id="mode", settable=True, set_callback=cb, datatype=PropertyDatatype.STRING
        )

        topic = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/dev1/node1/mode/set"
        prop._settable_callback(topic, b"\x00")
        cb.assert_called_once_with("")


class TestHomiePropertySetSubscribe:
    def test_set_subscribe_settable(self):
        mock_client = _mock_mqtt_client()
        prop = _make_wired_property(mock_client, id="mode", settable=True, datatype=PropertyDatatype.STRING)

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

        prop = node.add_property_from_dict(
            {
                "id": "humidity",
                "value": 50.0,
                "datatype": PropertyDatatype.FLOAT,
            }
        )
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

    def test_delete_property_republishes_description(self, mock_paho):
        """The mirror of add_property() must also re-announce the property set.

        Without the republish the broker keeps a device in `ready` whose
        $description still names a property that no longer exists, with nothing
        to correct it later.
        """
        device, mock_client = _make_device(mock_paho, device_id="dev-1")
        with device.state_transition():
            node = device.add_node_from_dict({"id": "core", "type": "sensor"})
            node.add_property_from_dict({"id": "temp", "datatype": PropertyDatatype.FLOAT})
            node.add_property_from_dict({"id": "humidity", "datatype": PropertyDatatype.FLOAT})
        mock_client.publish.reset_mock()

        assert node.delete_property("temp") is True

        descriptions = [c[0][1] for c in mock_client.publish.call_args_list if c[0][0].endswith("/dev-1/$description")]
        assert descriptions, f"delete_property published no $description: {mock_client.publish.call_args_list}"
        published = json.loads(descriptions[-1])
        props = published["nodes"]["core"]["properties"]
        assert "temp" not in props, f"deleted property still in published $description: {props}"
        assert "humidity" in props, f"surviving property missing from $description: {props}"

    def test_delete_property_batches_inside_state_transition(self, mock_paho):
        """N deletions in one transition collapse to one $description publish.

        Same guarantee add_node/add_property already give, so the two halves of
        the API stay symmetric under batching.
        """
        device, mock_client = _make_device(mock_paho, device_id="dev-1")
        with device.state_transition():
            node = device.add_node_from_dict({"id": "core", "type": "sensor"})
            for pid in ("a", "b", "c"):
                node.add_property_from_dict({"id": pid, "datatype": PropertyDatatype.FLOAT})
        mock_client.publish.reset_mock()

        with device.state_transition():
            for pid in ("a", "b", "c"):
                node.delete_property(pid)

        descriptions = [c for c in mock_client.publish.call_args_list if c[0][0].endswith("/dev-1/$description")]
        assert len(descriptions) == 1, f"expected 1 consolidated $description, got {len(descriptions)}"
        assert json.loads(descriptions[0][0][1])["nodes"]["core"]["properties"] == {}


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
        p = Property(id="temp", name="Temperature", value=72, datatype=PropertyDatatype.FLOAT)
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

    def test_will_exposes_lost_descriptor(self, mock_paho):
        """will() exposes the root's $state=lost LWT for a bring-your-own-transport caller (#13)."""
        device, _ = _make_device(mock_paho, device_id="panel-1")
        will = device.will()
        assert will["payload"] == DeviceState.LOST.value
        assert will["topic"].endswith("/panel-1/$state")

    def test_will_matches_the_owned_client_lwt(self, mock_paho):
        """The LWT the SDK installs on a client it builds is exactly will(), so the two cannot drift (#13)."""
        with patch("ebus_sdk.homie.MqttClient.from_config") as mock_from_config:
            mock_from_config.return_value = _mock_mqtt_client()
            device = Device(id="panel-1", mqtt_cfg={"host": "localhost", "port": 1883})
            assert mock_from_config.call_args[1]["lwt"] == device.will()

    def test_will_describes_root_from_a_child(self, mock_paho):
        """Children share the root's connection; will() names the root's $state from any handle (#13)."""
        with patch("ebus_sdk.homie.MqttClient.from_config") as mock_from_config:
            mock_from_config.return_value = _mock_mqtt_client()
            root = Device(id="panel-1", mqtt_cfg={"host": "localhost", "port": 1883})
            child = Device(id="bess-1", parent=root)
            assert child.will()["topic"].endswith("/panel-1/$state")

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


def _state_payloads_for(mock_client, device_id):
    """Return the sequence of $state payloads published for the given device id."""
    topic_prefix = f"/{device_id}/$state"
    return [c[0][1] for c in mock_client.publish.call_args_list if topic_prefix in c[0][0]]


def _description_publishes_for(mock_client, device_id):
    """Return count of $description publishes for the given device id."""
    topic_prefix = f"/{device_id}/$description"
    return sum(1 for c in mock_client.publish.call_args_list if topic_prefix in c[0][0])


class TestChildLifecycleProtocol:
    """SDK-4cq: Homie add-child / remove-child 6-step protocol."""

    def test_single_add_runs_full_protocol(self, mock_paho):
        """Adding a child to a READY parent: child INIT→READY, then parent INIT→READY."""
        panel, mock_client = _make_device(mock_paho, device_id="panel-1")
        assert panel.state() == DeviceState.READY
        mock_client.publish.reset_mock()

        Device(id="circuit-1", parent=panel)

        # Child completed its own INIT→READY
        circuit_states = _state_payloads_for(mock_client, "circuit-1")
        assert DeviceState.INIT in circuit_states
        assert DeviceState.READY in circuit_states
        # Parent flapped INIT→READY around the change (steps 4-6)
        panel_states = _state_payloads_for(mock_client, "panel-1")
        assert panel_states[-2:] == [DeviceState.INIT, DeviceState.READY]
        # And parent's description was republished including the new child
        assert _description_publishes_for(mock_client, "panel-1") >= 1
        # Order: child READY must come BEFORE parent's final INIT→READY (steps 1-3 < 4-6)
        last_circuit_ready = max(
            i
            for i, c in enumerate(mock_client.publish.call_args_list)
            if "/circuit-1/$state" in c[0][0] and c[0][1] == DeviceState.READY
        )
        first_panel_init_after = next(
            i
            for i, c in enumerate(mock_client.publish.call_args_list)
            if "/panel-1/$state" in c[0][0] and c[0][1] == DeviceState.INIT and i > last_circuit_ready
        )
        assert last_circuit_ready < first_panel_init_after

    def test_batched_adds_produce_one_parent_flap(self, mock_paho):
        """S1: 32 children added inside one state_transition → exactly one parent INIT→READY."""
        panel, mock_client = _make_device(mock_paho, device_id="panel-1")
        mock_client.publish.reset_mock()

        with panel.state_transition():
            for i in range(32):
                Device(id=f"circuit-{i}", parent=panel)

        panel_states = _state_payloads_for(mock_client, "panel-1")
        # One INIT at transition entry, one READY at exit — nothing else.
        assert panel_states == [DeviceState.INIT, DeviceState.READY]
        # All 32 children registered
        assert len(panel.children_ids()) == 32

    def test_add_grandchild_flaps_only_immediate_parent(self, mock_paho):
        """A grandchild add changes the immediate parent's children list, not the root's."""
        panel, mock_client = _make_device(mock_paho, device_id="panel-1")
        bess = Device(id="bess-1", parent=panel)
        mock_client.publish.reset_mock()

        Device(id="mid-1", parent=bess)

        # bess flaps INIT→READY (its description gained mid-1)
        bess_states = _state_payloads_for(mock_client, "bess-1")
        assert bess_states[-2:] == [DeviceState.INIT, DeviceState.READY]
        # panel does NOT flap — its own children list is unchanged
        panel_states = _state_payloads_for(mock_client, "panel-1")
        assert DeviceState.INIT not in panel_states
        assert DeviceState.READY not in panel_states

    def test_delete_child_runs_remove_protocol(self, mock_paho):
        """Deleting a child clears its retained data and flaps the parent."""
        panel, mock_client = _make_device(mock_paho, device_id="panel-1")
        circuit = Device(id="circuit-1", parent=panel)
        mock_client.publish.reset_mock()

        circuit.delete()

        # Child's $state, $description, etc. cleared (empty-string retained publishes)
        cleared = [c for c in mock_client.publish.call_args_list if "/circuit-1/" in c[0][0] and c[0][1] == ""]
        assert cleared, "expected retained-clear publishes for the deleted child"
        # Detached from parent
        assert circuit.parent() is None
        assert "circuit-1" not in panel.children_ids()
        # Parent flapped INIT→READY with new description
        panel_states = _state_payloads_for(mock_client, "panel-1")
        assert panel_states[-2:] == [DeviceState.INIT, DeviceState.READY]

    def test_batched_deletes_produce_one_parent_flap(self, mock_paho):
        """S3: removes inside parent.state_transition() collapse to one parent INIT→READY."""
        panel, mock_client = _make_device(mock_paho, device_id="panel-1")
        circuits = [Device(id=f"c-{i}", parent=panel) for i in range(5)]
        mock_client.publish.reset_mock()

        with panel.state_transition():
            for c in circuits:
                c.delete()

        panel_states = _state_payloads_for(mock_client, "panel-1")
        assert panel_states == [DeviceState.INIT, DeviceState.READY]
        assert panel.children_ids() == []

    def test_delete_root_cascades_to_children(self, mock_paho):
        """delete() on a root recursively deletes children first (leaves-first)."""
        panel, mock_client = _make_device(mock_paho, device_id="panel-1")
        bess = Device(id="bess-1", parent=panel)
        Device(id="mid-1", parent=bess)
        mock_client.publish.reset_mock()

        panel.delete()

        # Every device in the tree had its $state cleared
        for device_id in ("mid-1", "bess-1", "panel-1"):
            cleared_state = [
                c for c in mock_client.publish.call_args_list if f"/{device_id}/$state" in c[0][0] and c[0][1] == ""
            ]
            assert cleared_state, f"expected $state retained-clear for {device_id}"

    def test_delete_clears_state_before_description(self, mock_paho):
        """SDK-905: Homie 5 removal order clears $state before other retained topics."""
        panel, mock_client = _make_device(mock_paho, device_id="panel-1")
        panel.add_node(panel.new_node("core"))
        mock_client.publish.reset_mock()

        panel.delete()

        def first_clear_index(suffix):
            for i, c in enumerate(mock_client.publish.call_args_list):
                if c[0][0].endswith(f"/panel-1/{suffix}") and c[0][1] == "":
                    return i
            return None

        state_i = first_clear_index("$state")
        desc_i = first_clear_index("$description")
        assert state_i is not None and desc_i is not None
        assert state_i < desc_i, "$state must be cleared before $description (spec removal order)"


class TestCrossDeviceTransitionCoordination:
    """SDK-yb4: cross-device state_transition() coordination."""

    def test_recursive_delete_suppresses_intermediate_flaps(self, mock_paho):
        """A dying device shouldn't publish INIT/READY flaps for its dying descendants."""
        panel, mock_client = _make_device(mock_paho, device_id="panel-1")
        bess = Device(id="bess-1", parent=panel)
        Device(id="mid-1", parent=bess)
        mock_client.publish.reset_mock()

        panel.delete()

        # No non-empty INIT/READY state publishes on bess-1 during cascade — only the
        # retained-clear (empty payload) we expect at the end.
        bess_state_calls = [c for c in mock_client.publish.call_args_list if "/bess-1/$state" in c[0][0]]
        non_clear_payloads = [c[0][1] for c in bess_state_calls if c[0][1] != ""]
        assert non_clear_payloads == [], (
            f"bess-1 should not flap INIT/READY while being deleted, got {non_clear_payloads}"
        )

    def test_mixed_add_and_delete_in_one_transition(self, mock_paho):
        """A state_transition() can contain a mix of child adds and deletes; parent flaps once."""
        panel, mock_client = _make_device(mock_paho, device_id="panel-1")
        to_remove = Device(id="old-1", parent=panel)
        Device(id="old-2", parent=panel)
        mock_client.publish.reset_mock()

        with panel.state_transition():
            to_remove.delete()
            Device(id="new-1", parent=panel)
            Device(id="new-2", parent=panel)

        panel_states = _state_payloads_for(mock_client, "panel-1")
        # Exactly one INIT→READY for the whole transaction
        assert panel_states == [DeviceState.INIT, DeviceState.READY]
        assert set(panel.children_ids()) == {"old-2", "new-1", "new-2"}

    def test_sibling_subtree_transitions_are_independent(self, mock_paho):
        """A transition on one subtree must not flap an unrelated sibling subtree."""
        panel, mock_client = _make_device(mock_paho, device_id="panel-1")
        bess = Device(id="bess-1", parent=panel)
        evse = Device(id="evse-1", parent=panel)
        mock_client.publish.reset_mock()

        # Adding a grandchild under bess should flap bess (its description changed)
        # but NOT evse and NOT panel (their children lists are unchanged).
        with bess.state_transition():
            Device(id="mid-1", parent=bess)

        bess_states = _state_payloads_for(mock_client, "bess-1")
        assert bess_states == [DeviceState.INIT, DeviceState.READY]
        assert _state_payloads_for(mock_client, "evse-1") == []
        assert _state_payloads_for(mock_client, "panel-1") == []
        assert evse  # silence unused-var lint

    def test_exception_in_batched_transition_still_finalizes_parent(self, mock_paho):
        """If a child-add raises inside a parent transition, the parent still reaches READY."""
        panel, mock_client = _make_device(mock_paho, device_id="panel-1")
        mock_client.publish.reset_mock()

        with pytest.raises(RuntimeError):
            with panel.state_transition():
                Device(id="child-a", parent=panel)
                raise RuntimeError("oops mid-batch")

        # Parent still ends READY even though the batch raised
        assert panel.state() == DeviceState.READY
        panel_states = _state_payloads_for(mock_client, "panel-1")
        assert panel_states[-1] == DeviceState.READY
        # The successfully-added child stays in the tree (caller's responsibility to clean up)
        assert "child-a" in panel.children_ids()

    def test_nested_state_transition_on_same_device_is_idempotent(self, mock_paho):
        """SDK-v3p: nested state_transition() emits exactly one INIT and one READY.

        Init→ready forces every controller in the wild to resync, so emitting
        only the minimum is a correctness concern, not a polish concern.
        """
        panel, mock_client = _make_device(mock_paho, device_id="panel-1")
        mock_client.publish.reset_mock()

        with panel.state_transition():
            with panel.state_transition():
                Device(id="circuit-a", parent=panel)

        panel_states = _state_payloads_for(mock_client, "panel-1")
        assert panel_states == [DeviceState.INIT, DeviceState.READY]
        # And the child was added correctly
        assert "circuit-a" in panel.children_ids()

    def test_triply_nested_state_transition_is_idempotent(self, mock_paho):
        """SDK-v3p: depth-3 nesting still emits exactly one INIT and one READY."""
        panel, mock_client = _make_device(mock_paho, device_id="panel-1")
        mock_client.publish.reset_mock()

        with panel.state_transition():
            with panel.state_transition():
                with panel.state_transition():
                    Device(id="circuit-a", parent=panel)
                    Device(id="circuit-b", parent=panel)

        panel_states = _state_payloads_for(mock_client, "panel-1")
        assert panel_states == [DeviceState.INIT, DeviceState.READY]
        assert set(panel.children_ids()) == {"circuit-a", "circuit-b"}

    def test_single_state_transition_emits_exactly_one_init_ready(self, mock_paho):
        """Baseline guard: even with no nesting, exactly one INIT and one READY."""
        panel, mock_client = _make_device(mock_paho, device_id="panel-1")
        mock_client.publish.reset_mock()

        with panel.state_transition():
            Device(id="circuit-a", parent=panel)

        panel_states = _state_payloads_for(mock_client, "panel-1")
        assert panel_states == [DeviceState.INIT, DeviceState.READY]


class TestDescriptionPublishSuppression:
    """SDK-9ps (defer interim publishes in a transition) + SDK-n83 (no-op when
    the description content, ignoring the version timestamp, is unchanged)."""

    def test_adds_inside_transition_publish_one_description(self, mock_paho):
        # SDK-9ps: N add_node calls inside one transition -> exactly 1 $description
        # on the wire (the consolidated publish at exit), not N+1.
        device, mock_client = _make_device(mock_paho, device_id="dev-1")
        mock_client.publish.reset_mock()

        with device.state_transition():
            for i in range(5):
                device.add_node(device.new_node(f"node-{i}"))

        assert _description_publishes_for(mock_client, "dev-1") == 1
        # ...and that single publish reflects all 5 nodes.
        desc_payloads = [c[0][1] for c in mock_client.publish.call_args_list if "/dev-1/$description" in c[0][0]]
        final = json.loads(desc_payloads[-1])
        assert set(final["nodes"].keys()) == {f"node-{i}" for i in range(5)}

    def test_noop_transitions_suppress_description_republish(self, mock_paho):
        # SDK-n83: transitions that change nothing structural don't republish the
        # (potentially large) $description.
        device, mock_client = _make_device(mock_paho, device_id="dev-1")
        device.add_node(device.new_node("core"))
        mock_client.publish.reset_mock()

        for _ in range(10):
            with device.state_transition():
                pass

        assert _description_publishes_for(mock_client, "dev-1") == 0

    def test_changed_publishes_then_unchanged_suppressed(self, mock_paho):
        # A real structural change publishes once; an immediately-following no-op
        # transition publishes zero.
        device, mock_client = _make_device(mock_paho, device_id="dev-1")
        mock_client.publish.reset_mock()

        with device.state_transition():
            device.add_node(device.new_node("core"))
        assert _description_publishes_for(mock_client, "dev-1") == 1

        mock_client.publish.reset_mock()
        with device.state_transition():
            pass
        assert _description_publishes_for(mock_client, "dev-1") == 0

    def test_distinct_changes_each_publish(self, mock_paho):
        # Two transitions that each make a *different* structural change each
        # publish — the no-op suppression must not swallow a genuine change.
        device, mock_client = _make_device(mock_paho, device_id="dev-1")
        mock_client.publish.reset_mock()

        with device.state_transition():
            device.add_node(device.new_node("a"))
        with device.state_transition():
            device.add_node(device.new_node("b"))

        assert _description_publishes_for(mock_client, "dev-1") == 2

    def test_reconnect_republishes_even_when_unchanged(self, mock_paho):
        # republish=True (reconnect cascade) must restore the retained $description
        # regardless of the content-hash no-op — the broker may have lost it.
        device, mock_client = _make_device(mock_paho, device_id="dev-1")
        device.add_node(device.new_node("core"))
        device.initial_broker_connection = False
        mock_client.publish.reset_mock()

        device.on_connect()

        assert _description_publishes_for(mock_client, "dev-1") >= 1

    def test_empty_transition_still_flaps_state(self, mock_paho):
        # Documents intentional scope: the $state INIT->READY flap is NOT
        # suppressed (that stays the adapter's job per SDK-n83 "Layer 1"); only
        # the redundant $description payload is.
        device, mock_client = _make_device(mock_paho, device_id="dev-1")
        device.add_node(device.new_node("core"))
        mock_client.publish.reset_mock()

        with device.state_transition():
            pass

        assert _state_payloads_for(mock_client, "dev-1") == [DeviceState.INIT, DeviceState.READY]
        assert _description_publishes_for(mock_client, "dev-1") == 0


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


class TestDeviceTree:
    def test_root_alone(self, mock_paho):
        device, _ = _make_device(mock_paho)
        assert device.parent() is None
        assert device.parent_id() is None
        assert device.root_id() is None
        assert device.root() is device
        assert device.children() == []
        assert device.children_ids() == []

    def test_child_registered_with_parent(self, mock_paho):
        root, _ = _make_device(mock_paho, device_id="root-1")
        child = Device(id="child-1", parent=root)
        assert child.parent() is root
        assert child.parent_id() == "root-1"
        assert child.root() is root
        assert child.root_id() == "root-1"
        assert root.children() == [child]
        assert root.children_ids() == ["child-1"]

    def test_grandchild_walks_to_root(self, mock_paho):
        """S2: panel root -> child -> grandchild; root() ascends two levels."""
        root, _ = _make_device(mock_paho, device_id="panel-1")
        child = Device(id="bess-1", parent=root)
        grandchild = Device(id="mid-1", parent=child)
        assert grandchild.parent() is child
        assert grandchild.parent_id() == "bess-1"
        assert grandchild.root() is root
        assert grandchild.root_id() == "panel-1"

    def test_child_shares_root_mqtt_client(self, mock_paho):
        root, mock_client = _make_device(mock_paho, device_id="root-1")
        child = Device(id="child-1", parent=root)
        assert child.mqttc is None
        assert child.get_mqtt_client() is mock_client
        assert child.get_mqtt_client() is root.get_mqtt_client()

    def test_child_with_mqtt_cfg_raises(self, mock_paho):
        root, _ = _make_device(mock_paho, device_id="root-1")
        with pytest.raises(ValueError, match="cannot pass both parent= and mqtt_cfg="):
            Device(id="child-1", parent=root, mqtt_cfg={"host": "x", "port": 1})

    def test_child_with_unconnected_parent_raises(self, mock_paho):
        """A child cannot attach to a tree whose root has no MqttClient yet."""
        root, _ = _make_device(mock_paho, device_id="root-1")
        root.mqttc = None  # simulate failed connect_broker()
        with pytest.raises(RuntimeError, match="has no MQTT client"):
            Device(id="child-1", parent=root)

    def test_grandchild_via_unconnected_root_raises(self, mock_paho):
        """The reachability check must walk to the root, not stop at the immediate parent."""
        root, _ = _make_device(mock_paho, device_id="root-1")
        child = Device(id="child-1", parent=root)
        root.mqttc = None  # break the root after child was attached
        with pytest.raises(RuntimeError, match="has no MQTT client"):
            Device(id="grandchild-1", parent=child)


class TestDeviceDescription:
    def test_description_structure(self, mock_paho):
        device, _ = _make_device(mock_paho, device_id="panel-1", name="Panel", type="electrical-panel")
        desc = device.description()
        assert desc["name"] == "Panel"
        assert desc["type"] == "electrical-panel"
        assert "homie" in desc
        assert "version" in desc
        assert "nodes" in desc
        assert desc["children"] == []
        assert desc["extensions"] == []

    def test_description_with_root_and_parent(self, mock_paho):
        root, _ = _make_device(mock_paho, device_id="root-1")
        child = Device(id="child-1", parent=root)
        desc = child.description()
        assert desc["root"] == "root-1"
        assert desc["parent"] == "root-1"

    def test_description_grandchild_root_vs_parent(self, mock_paho):
        """S2: grandchild's $description.root walks to the top, parent is direct."""
        root, _ = _make_device(mock_paho, device_id="panel-1")
        child = Device(id="bess-1", parent=root)
        grandchild = Device(id="mid-1", parent=child)
        desc = grandchild.description()
        assert desc["root"] == "panel-1"
        assert desc["parent"] == "bess-1"

    def test_description_parent_lists_child(self, mock_paho):
        root, _ = _make_device(mock_paho, device_id="root-1")
        Device(id="child-a", parent=root)
        Device(id="child-b", parent=root)
        desc = root.description()
        assert desc["children"] == ["child-a", "child-b"]

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

    def test_publish_nodes_snapshots_against_concurrent_add(self, mock_paho):
        """SDK-e3k: publish_nodes() must snapshot self._nodes so the main
        thread adding a node mid-iteration doesn't raise
        'dictionary changed size during iteration' on the MQTT loop thread."""
        device, _ = _make_device(mock_paho)

        # Simulate the race: while iterating, one node's publish() mutates
        # the underlying dict (as the main thread's add_node would).
        racing_node = MagicMock()

        def mutate_during_publish():
            device._nodes["late-arrival"] = MagicMock()

        racing_node.publish.side_effect = mutate_during_publish
        device._nodes = {"core": racing_node}

        # Without the list() snapshot fix, this raises RuntimeError.
        device.publish_nodes()


class TestDeviceOnConnect:
    def test_initial_connection_publishes_full_tree(self, mock_paho):
        """Initial connect must publish the COMPLETE device — $description and
        $state, not just node values. With an asynchronous (connect_async)
        broker connection a device can be constructed while the broker is down,
        so the construction-time $state/$description publishes never landed;
        publishing only node values here would leave a half-published device."""
        device, mock_client = _make_device(mock_paho, device_id="dev-1")
        node = device.new_node("core", "Core", "sensor")
        device.add_node(node)
        device.initial_broker_connection = True
        mock_client.publish.reset_mock()

        device.on_connect()

        assert device.initial_broker_connection is False
        topics = [c[0][0] for c in mock_client.publish.call_args_list]
        assert any("/dev-1/$description" in t for t in topics), topics
        assert any("/dev-1/$state" in t for t in topics), topics

    def test_initial_connection_cascades_to_children(self, mock_paho):
        """A child constructed before the first connect (possible when the
        broker was down at construction time) must also be fully published when
        that first connect finally arrives — the initial path cascades the whole
        tree exactly like a reconnect."""
        root, mock_client = _make_device(mock_paho, device_id="panel-1")
        Device(id="circuit-a", parent=root)
        root.initial_broker_connection = True
        mock_client.publish.reset_mock()

        root.on_connect()

        topics = [c[0][0] for c in mock_client.publish.call_args_list]
        assert any("/panel-1/$description" in t for t in topics), topics
        assert any("/circuit-a/$description" in t for t in topics), topics
        assert any("/circuit-a/$state" in t for t in topics), topics

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
        mock_client.publish.assert_called_once_with("ebus/5/panel-1/core/temp", "", retain=True, qos=device.qos)

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

    def test_connect_broker_reraises_genuine_fault(self, mock_paho):
        """A genuine construction fault (malformed config, unreadable TLS cert)
        must surface out of Device() rather than leaving a silent mqttc=None
        zombie. The down-broker case no longer reaches this except clause — the
        transport connects asynchronously and never raises on a down broker — so
        an exception here is always a real fault worth failing fast on."""
        with patch("ebus_sdk.homie.MqttClient.from_config", side_effect=ValueError("bad cfg")):
            with pytest.raises(ValueError, match="bad cfg"):
                Device(id="dev-x", mqtt_cfg={"host": "localhost", "port": 1883})


class TestDeviceIsConnected:
    def test_is_connected_true(self, mock_paho):
        device, mock_client = _make_device(mock_paho)
        mock_client.is_connected.return_value = True
        assert device.is_connected() is True

    def test_is_connected_false_when_link_down(self, mock_paho):
        # Between construction and the first async connect, the link is down.
        device, mock_client = _make_device(mock_paho)
        mock_client.is_connected.return_value = False
        assert device.is_connected() is False

    def test_is_connected_false_when_no_client(self, mock_paho):
        device, _ = _make_device(mock_paho)
        device.mqttc = None
        assert device.is_connected() is False

    def test_is_connected_child_reflects_root(self, mock_paho):
        root, mock_client = _make_device(mock_paho, device_id="panel-1")
        child = Device(id="circuit-a", parent=root)
        mock_client.is_connected.return_value = True
        assert child.is_connected() is True
        mock_client.is_connected.return_value = False
        assert child.is_connected() is False


class TestDeviceRefreshTree:
    def test_refresh_tree_single_device(self, mock_paho):
        device, mock_client = _make_device(mock_paho)
        mock_client.publish.reset_mock()

        device.refresh_tree()

        topics = [c[0][0] for c in mock_client.publish.call_args_list]
        assert any("$description" in t for t in topics)
        assert any("$state" in t for t in topics)

    def test_refresh_tree_cascades_to_children(self, mock_paho):
        """S6: reconnect republish must touch every device in the tree."""
        root, mock_client = _make_device(mock_paho, device_id="panel-1")
        Device(id="circuit-a", parent=root)
        Device(id="circuit-b", parent=root)
        mock_client.publish.reset_mock()

        root.refresh_tree()

        topics = [c[0][0] for c in mock_client.publish.call_args_list]
        # Each device publishes its own $description and $state through the shared client
        for device_id in ("panel-1", "circuit-a", "circuit-b"):
            assert any(f"/{device_id}/$description" in t for t in topics), (
                f"missing $description for {device_id} in {topics}"
            )
            assert any(f"/{device_id}/$state" in t for t in topics), f"missing $state for {device_id} in {topics}"

    def test_refresh_tree_publishes_own_state_after_its_children(self, mock_paho):
        """A device must not announce ``ready`` before the children it names.

        ``$description`` lists this device's children, so publishing
        ``$state=ready`` before those children have published anything
        advertises a tree that is not yet on the broker. Homie 5 invites a
        controller to gate on the root's state, and on reconnect
        (``on_connect`` -> ``refresh_tree``) such a controller would proceed
        against children whose own ``$description`` had not arrived.

        Order only: the set of messages is the same either way, which is what
        ``test_refresh_tree_cascades_to_children`` already pins.
        """
        root, mock_client = _make_device(mock_paho, device_id="panel-1")
        Device(id="circuit-a", parent=root)
        Device(id="circuit-b", parent=root)
        mock_client.publish.reset_mock()

        root.refresh_tree()

        topics = [c[0][0] for c in mock_client.publish.call_args_list]
        root_state = [i for i, t in enumerate(topics) if t.endswith("/panel-1/$state")]
        child_states = [
            i for i, t in enumerate(topics) if t.endswith("/circuit-a/$state") or t.endswith("/circuit-b/$state")
        ]

        assert root_state, f"root published no $state in {topics}"
        assert len(child_states) == 2, f"expected both children to publish $state in {topics}"
        assert min(root_state) > max(child_states), (
            "root announced $state before its children; a controller gating on the root "
            f"would see a tree whose children have not published. topics={topics}"
        )

        # The root's own description still leads, so the tree's shape is on the
        # broker before anything claims to be ready.
        root_description = [i for i, t in enumerate(topics) if t.endswith("/panel-1/$description")]
        assert root_description and min(root_description) < min(child_states)

    def test_refresh_tree_snapshots_against_concurrent_child_add(self, mock_paho):
        """SDK-e3k: refresh_tree() must snapshot self._children so a child
        appended by the main thread mid-cascade isn't pulled into the current
        republish on the MQTT loop thread. (Lists don't raise on
        mutation-during-iteration the way dicts do, but processing a
        half-constructed child is its own correctness hazard.)"""
        root, _ = _make_device(mock_paho, device_id="panel-1")
        existing_child = Device(id="circuit-a", parent=root)
        late_arrival = MagicMock(spec=Device)

        original_refresh = existing_child.refresh_tree

        def mutate_during_refresh():
            # Simulate the main thread appending a new child while the MQTT
            # thread is mid-cascade.
            root._children.append(late_arrival)
            original_refresh()

        existing_child.refresh_tree = mutate_during_refresh

        root.refresh_tree()

        # Snapshot semantics: late_arrival was appended after iteration began,
        # so it must NOT be touched by this refresh cycle. Without the
        # list() snapshot, CPython's list iterator picks it up.
        late_arrival.refresh_tree.assert_not_called()

    def test_refresh_tree_three_levels(self, mock_paho):
        """S2 + S6: grandchildren also republish on reconnect."""
        root, mock_client = _make_device(mock_paho, device_id="panel-1")
        bess = Device(id="bess-1", parent=root)
        Device(id="mid-1", parent=bess)
        mock_client.publish.reset_mock()

        root.refresh_tree()

        topics = [c[0][0] for c in mock_client.publish.call_args_list]
        assert any("/mid-1/$description" in t for t in topics)
        assert any("/mid-1/$state" in t for t in topics)

        # State-after-children is recursive, not a root special case: the
        # intermediate device must also announce after its own child. Without
        # this, a fix that reordered only the root passes the whole suite.
        bess_state = [i for i, t in enumerate(topics) if t.endswith("/bess-1/$state")]
        mid_state = [i for i, t in enumerate(topics) if t.endswith("/mid-1/$state")]
        assert bess_state and mid_state
        assert min(bess_state) > max(mid_state), f"intermediate device announced $state before its child: {topics}"

    def test_on_connect_reconnect_cascades(self, mock_paho):
        """On reconnect, root.on_connect() walks the whole tree."""
        root, mock_client = _make_device(mock_paho, device_id="panel-1")
        Device(id="circuit-a", parent=root)
        # Flip from initial to reconnect path
        root.initial_broker_connection = False
        mock_client.publish.reset_mock()

        root.on_connect()

        topics = [c[0][0] for c in mock_client.publish.call_args_list]
        assert any("/panel-1/$description" in t for t in topics)
        assert any("/circuit-a/$description" in t for t in topics)
        assert any("/circuit-a/$state" in t for t in topics)


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


class TestDeviceDescriptionExtras:
    """extensions list + custom $description fields (SDK-dn4 imported-from support)."""

    def test_extensions_default_empty_and_isolated(self, mock_paho):
        # No shared mutable default: mutating one device's list never leaks.
        d1, _ = _make_device(mock_paho, device_id="d1")
        d2, _ = _make_device(mock_paho, device_id="d2")
        assert d1.description()["extensions"] == []
        d1._extensions.append("x")
        assert d2.description()["extensions"] == []

    def test_extensions_listed_in_description(self, mock_paho):
        d, _ = _make_device(mock_paho, extensions=["energy.ebus.imported:1.0.0:[5.x]"])
        assert d.description()["extensions"] == ["energy.ebus.imported:1.0.0:[5.x]"]

    def test_description_extras_merged(self, mock_paho):
        d, _ = _make_device(mock_paho, description_extras={"imported-from": "ha"})
        assert d.description()["imported-from"] == "ha"

    def test_description_extras_never_clobber_core_fields(self, mock_paho):
        d, _ = _make_device(mock_paho, description_extras={"name": "HACKED", "nodes": {"x": {}}})
        desc = d.description()
        assert desc["name"] != "HACKED"  # core name wins over an extra of the same key
        assert desc["nodes"] == {}  # core nodes win


class TestDeviceStop:
    """Device.stop(): bounded graceful teardown (SDK-y68)."""

    def test_publishes_disconnected_then_stops_when_connected(self, mock_paho):
        device, mock_client = _make_device(mock_paho, device_id="dev-stop")
        mock_client.is_connected.return_value = True

        device.stop()

        # Graceful $state=disconnected flushed before teardown.
        mock_client.publish_and_flush.assert_called_once()
        args, kwargs = mock_client.publish_and_flush.call_args
        assert args[0] == f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/dev-stop/$state"
        assert args[1] == DeviceState.DISCONNECTED.value == "disconnected"
        assert kwargs["retain"] is True
        assert device._state == DeviceState.DISCONNECTED
        # Bounded client stop, then the reference is cleared.
        mock_client.stop.assert_called_once()
        assert mock_client.stop.call_args.kwargs["timeout"] == 2.0
        assert device.mqttc is None

    def test_skips_publish_when_broker_unreachable(self, mock_paho):
        device, mock_client = _make_device(mock_paho, device_id="dev-stop2")
        mock_client.is_connected.return_value = False

        device.stop()

        # No graceful publish attempted on a dead broker; still tears down promptly.
        mock_client.publish_and_flush.assert_not_called()
        mock_client.stop.assert_called_once()
        assert device.mqttc is None

    def test_no_mqtt_client_is_noop(self, mock_paho):
        device, mock_client = _make_device(mock_paho)
        device.mqttc = None
        device.stop()  # must not raise
        mock_client.stop.assert_not_called()


class TestDeviceBYOTransport:
    """Bring-your-own-transport: inject a client into a root Device (#14).

    The SDK uses an injected client as-is and never starts or stops it; its
    lifecycle stays the caller's (e.g. a Home Assistant host on its own loop).
    """

    def test_injected_client_used_without_from_config(self, mock_paho):
        client = _mock_mqtt_client()
        with patch("ebus_sdk.homie.MqttClient.from_config") as mock_from_config:
            device = Device(id="panel-1", mqttc=client)
            mock_from_config.assert_not_called()  # the SDK does not build a client
        assert device.mqttc is client
        assert device.get_mqtt_client() is client
        assert device._owns_client is False

    def test_sdk_never_starts_an_injected_client(self, mock_paho):
        client = _mock_mqtt_client()
        client.is_running = False  # even if not running, the SDK must not start it
        device = Device(id="panel-1", mqttc=client)
        device.start_mqtt_client()
        client.start.assert_not_called()

    def test_stop_injected_publishes_disconnected_without_flush_or_close(self, mock_paho):
        client = _mock_mqtt_client()
        client.is_connected.return_value = True
        device = Device(id="panel-1", mqttc=client)

        client.reset_mock()  # ignore construction-time publishes
        device.stop()

        client.publish.assert_called_once()
        topic, payload = client.publish.call_args.args
        assert topic == f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state"
        assert payload == DeviceState.DISCONNECTED.value
        assert client.publish.call_args.kwargs["retain"] is True
        client.publish_and_flush.assert_not_called()  # owned-only, off the injected surface
        client.stop.assert_not_called()  # the caller closes its own client
        assert device.mqttc is None

    def test_stop_injected_broker_down_is_silent(self, mock_paho):
        client = _mock_mqtt_client()
        client.is_connected.return_value = False
        device = Device(id="panel-1", mqttc=client)

        client.reset_mock()
        device.stop()

        client.publish.assert_not_called()
        client.publish_and_flush.assert_not_called()
        client.stop.assert_not_called()
        assert device.mqttc is None

    def test_mqttc_and_mqtt_cfg_are_mutually_exclusive(self, mock_paho):
        with pytest.raises(ValueError, match="mqtt_cfg= and mqttc="):
            Device(id="panel-1", mqtt_cfg={"host": "x"}, mqttc=_mock_mqtt_client())

    def test_mqttc_and_parent_are_mutually_exclusive(self, mock_paho):
        root = Device(id="panel-1", mqttc=_mock_mqtt_client())
        with pytest.raises(ValueError, match="parent= and mqttc="):
            Device(id="circuit-1", parent=root, mqttc=_mock_mqtt_client())

    def test_injected_root_takes_children_sharing_the_client(self, mock_paho):
        client = _mock_mqtt_client()
        root = Device(id="panel-1", mqttc=client)
        child = Device(id="circuit-1", parent=root)
        assert child.get_mqtt_client() is client  # child borrows the root's injected client
        assert root._owns_client is False

    def test_property_start_never_starts_an_injected_client(self, mock_paho):
        client = _mock_mqtt_client()
        client.is_running = False
        node = Node(id="core", name="Core", type="sensor")
        Device(id="panel-1", mqttc=client, nodes=[node])  # wires node -> device (root, not owned)
        prop = Property(id="power", value=1.0, datatype=PropertyDatatype.FLOAT)
        node.add_property(prop)

        client.reset_mock()
        prop.start_mqtt_client()  # the other start site besides Device.start_mqtt_client
        client.start.assert_not_called()

    def test_on_disconnect_is_accepted_but_inert_for_an_injected_client(self, mock_paho):
        client = _mock_mqtt_client()
        cb = MagicMock()
        with patch("ebus_sdk.homie.logger") as mock_logger:
            device = Device(id="panel-1", mqttc=client, on_disconnect=cb)
        assert device._on_disconnect is cb  # accepted (mirrors Controller), not rejected
        assert any("OnDisconnectInert" in str(c) for c in mock_logger.warning.call_args_list)

    def test_parent_and_empty_mqtt_cfg_are_mutually_exclusive(self, mock_paho):
        root = Device(id="panel-1")  # transport-free root
        with pytest.raises(ValueError, match="parent= and mqtt_cfg="):
            Device(id="circuit-1", parent=root, mqtt_cfg={})


class TestMqttDeviceTransportProtocol:
    """The Device injection point is typed by what the SDK calls on an injected client (#14).

    MqttDeviceTransport = MqttTransport (publish/subscribe) + is_connected() + is_running.
    It has a data member (is_running), so use isinstance, not issubclass.
    """

    def test_protocol_is_exported_from_the_package_root(self):
        import ebus_sdk

        assert "MqttDeviceTransport" in ebus_sdk.__all__
        assert ebus_sdk.MqttDeviceTransport is not None

    def test_a_minimal_client_is_a_valid_device_transport(self):
        """publish / subscribe / is_connected / is_running is the whole injected-Device
        contract. Deliberately no start / stop / publish_and_flush: the SDK never calls
        those on an injected client, so a minimal client that lacks them still works."""
        from ebus_sdk import MqttDeviceTransport

        class Minimal:
            is_running = True

            def publish(self, topic, data, qos=1, retain=False):
                return None

            def subscribe(self, sub, param, qos=1):
                return None

            def is_connected(self):
                return True

        client = Minimal()
        assert isinstance(client, MqttDeviceTransport)

        device = Device(id="panel-1", type="dev.test", mqttc=client)
        device.stop()  # must not reach start()/stop() on a client that has neither

    def test_owned_client_handle_is_none_when_injected(self):
        client = _mock_mqtt_client()
        device = Device(id="panel-1", mqttc=client)
        assert device._owned_client is None
        device.start_mqtt_client()  # no-op for an injected client
        client.start.assert_not_called()

    def test_owned_client_handle_is_set_and_cleared_for_an_sdk_built_client(self):
        with patch("ebus_sdk.homie.MqttClient.from_config") as mock_from_config:
            client = _mock_mqtt_client()
            client.is_connected.return_value = True
            mock_from_config.return_value = client
            device = Device(id="panel-1", mqtt_cfg={"host": "x"})
            assert device._owned_client is client
            device.stop()
            client.stop.assert_called_once()  # stops via the owned handle
            assert device._owned_client is None


class TestInboundAsyncLoop:
    """Inbound /set async dispatch: async_loop promoted to the root, thread-safe (#15)."""

    def test_async_loop_default_is_none(self):
        # Was `Optional[...] = False` (a bool against the loop annotation); now None.
        assert Property(id="mode").async_loop is None

    def test_device_async_loop_propagates_via_add_node(self):
        loop = MagicMock()
        prop = Property(id="setpoint", datatype=PropertyDatatype.FLOAT)
        node = Node(id="core", name="Core", type="sensor", properties={"setpoint": prop})
        Device(id="dev", async_loop=loop, nodes=[node])  # add_node propagates to the node's props
        assert prop.async_loop is loop

    def test_device_async_loop_propagates_via_node_add_property(self):
        loop = MagicMock()
        device = Device(id="dev", async_loop=loop)  # transport-free root
        node = Node(id="core", name="Core", type="sensor")
        device.add_node(node)
        prop = Property(id="setpoint", value=1.0, datatype=PropertyDatatype.FLOAT)
        node.add_property(prop)  # node is attached -> propagates the device's loop
        assert prop.async_loop is loop

    def test_child_device_inherits_root_async_loop(self):
        loop = MagicMock()
        root = Device(id="root", async_loop=loop)  # transport-free root
        child = Device(id="child", parent=root)
        assert child._async_loop is loop  # one loop per tree, inherited by children
        node = Node(id="core", name="Core", type="sensor")
        child.add_node(node)
        prop = Property(id="setpoint", value=1.0, datatype=PropertyDatatype.FLOAT)
        node.add_property(prop)
        assert prop.async_loop is loop

    def test_async_dispatch_uses_run_coroutine_threadsafe(self):
        """An async /set callback is scheduled onto the consumer's loop thread-safely;
        /set arrives on the transport's network thread, so ensure_future is unsafe."""

        async def cb(payload):
            return None

        loop = MagicMock()
        prop = Property(id="mode", settable=True, set_callback=cb)
        prop.async_loop = loop
        with patch("asyncio.run_coroutine_threadsafe") as rct, patch("asyncio.ensure_future") as ef:
            prop._settable_callback("ebus/5/dev/node/mode/set", b"LOAD_UP")
        rct.assert_called_once()
        assert rct.call_args.args[1] is loop  # scheduled onto the given loop
        ef.assert_not_called()  # never the thread-unsafe ensure_future path
        rct.call_args.args[0].close()  # close the un-awaited coroutine (rct is mocked)

    def test_sync_dispatch_when_no_loop(self):
        seen = []
        prop = Property(id="mode", settable=True, set_callback=lambda v: seen.append(v))
        assert prop.async_loop is None
        with patch("asyncio.run_coroutine_threadsafe") as rct:
            prop._settable_callback("ebus/5/dev/node/mode/set", b"LOAD_UP")
        assert seen == ["LOAD_UP"]  # invoked synchronously
        rct.assert_not_called()

    def test_sync_callback_stays_inline_under_device_loop(self):
        """A device-level loop must not force a sync callback onto the async path: the
        dispatch branches on the callback's return, not on the loop's presence (#15)."""
        loop = MagicMock()
        seen = []
        prop = Property(id="mode", settable=True, set_callback=lambda v: seen.append(v))
        prop.async_loop = loop  # a device-level loop is propagated to every property
        with patch("asyncio.run_coroutine_threadsafe") as rct:
            prop._settable_callback("ebus/5/dev/node/mode/set", b"ON")
        assert seen == ["ON"]  # ran inline, once
        rct.assert_not_called()  # not scheduled: a sync callback returns no coroutine

    def test_per_property_loop_survives_when_device_has_no_loop(self):
        """Backward compat: a per-Property async_loop is kept when the device sets none."""
        loop_a = MagicMock()
        prop = Property(id="setpoint", datatype=PropertyDatatype.FLOAT, async_loop=loop_a)
        node = Node(id="core", name="Core", type="sensor", properties={"setpoint": prop})
        Device(id="dev", nodes=[node])  # no async_loop -> propagation is skipped
        assert prop.async_loop is loop_a

    def test_device_loop_overrides_per_property_loop(self):
        """One loop per tree: a device-level loop wins over a pre-set per-Property loop."""
        loop_a, loop_b = MagicMock(), MagicMock()
        prop = Property(id="setpoint", datatype=PropertyDatatype.FLOAT, async_loop=loop_a)
        node = Node(id="core", name="Core", type="sensor", properties={"setpoint": prop})
        Device(id="dev", async_loop=loop_b, nodes=[node])
        assert prop.async_loop is loop_b

    def test_async_set_callback_exception_is_surfaced(self):
        """A raising async /set handler is logged, not swallowed by the discarded Future."""
        prop = Property(id="mode")
        future = MagicMock()
        future.cancelled.return_value = False
        future.exception.return_value = ValueError("bad setpoint")
        with patch("ebus_sdk.homie.logger") as mock_logger:
            prop._log_async_set_result(future, "mode")
        assert any("propertySetAsyncCallbackException" in str(c) for c in mock_logger.error.call_args_list)


class TestDeviceWithoutTransport:
    """`mqtt_cfg=None` — the declared default — builds a device tree with no transport."""

    def test_declared_default_constructs_without_raising(self):
        """Regression: constructing with the documented default raised AttributeError.

        `Device.__init__` called `connect_broker()` unconditionally for roots, so the
        annotated `mqtt_cfg: Optional[dict] = None` reached
        `MqttClient.from_config(None)` and died on `None.get("host", ...)`.
        """
        device = Device(id="no-transport", name="Passive", type="dev.test")

        assert device.mqttc is None
        assert device.id() == "no-transport"

    def test_empty_cfg_still_connects(self):
        """`mqtt_cfg={}` keeps its meaning: connect using ebus-mqtt-client's defaults.

        Only `None` changes behaviour, and `None` previously raised — so no caller that
        works today is affected.
        """
        with patch("ebus_sdk.homie.MqttClient.from_config") as mock_from_config:
            mock_from_config.return_value = _mock_mqtt_client()
            device = Device(id="default-cfg", type="dev.test", mqtt_cfg={})

        mock_from_config.assert_called_once()
        assert device.mqttc is not None

    def test_children_attach_to_a_transport_free_root(self):
        """A root with no transport is transport-free by design, and so are its children."""
        root = Device(id="root", type="dev.root")
        child = Device(id="child", type="dev.child", parent=root)
        grandchild = Device(id="grandchild", type="dev.child", parent=child)

        assert [c.id() for c in root._children] == ["child"]
        assert grandchild.root() is root
        assert child.mqttc is None

    def test_transport_free_tree_still_composes_descriptions(self):
        """The model is fully usable — ids, nodes, $description, children — just silent."""
        root = Device(id="root", name="Root", type="dev.root")
        child = Device(id="child", name="Child", type="dev.child", parent=root)
        node = child.add_node_from_dict({"id": "meter", "name": "meter", "type": "cap.meter"})
        node.add_property_from_dict(
            {"id": "active-power", "name": "P", "datatype": PropertyDatatype.FLOAT, "unit": Unit("W")}
        )

        root_desc = root.description()
        child_desc = child.description()

        assert root_desc["children"] == ["child"]
        assert child_desc["parent"] == "root"
        assert child_desc["root"] == "root"
        assert child_desc["nodes"]["meter"]["properties"]["active-power"]["unit"] == "W"

    def test_publishing_on_a_transport_free_tree_is_a_noop(self, caplog):
        """No client means no writes — and no exceptions."""
        root = Device(id="root", type="dev.root")
        node = root.add_node_from_dict({"id": "meter", "name": "meter", "type": "cap.meter"})
        prop = node.add_property_from_dict({"id": "active-power", "name": "P", "datatype": PropertyDatatype.FLOAT})

        prop.set_value(42.0)  # must not raise
        root.stop()  # must not raise

    def test_guard_still_fires_when_a_configured_root_has_no_client(self):
        """The original guard's real target: a root that was given a config and has none.

        That is the "never started, or already stopped" mistake, and it must still raise —
        only the transport-free-by-design case is now allowed through.
        """
        with patch("ebus_sdk.homie.MqttClient.from_config") as mock_from_config:
            mock_from_config.return_value = _mock_mqtt_client()
            root = Device(id="configured", type="dev.root", mqtt_cfg={"host": "localhost"})

        root.mqttc = None  # e.g. after stop()

        with pytest.raises(RuntimeError, match="has no MQTT client"):
            Device(id="child", type="dev.child", parent=root)


class TestTransportFreeLogSeverity:
    """A tree built without transport reports "no client" at DEBUG, not WARNING (#11).

    The message is right either way; only the cause differs. Transport-free means the caller
    asked for no client, so every traversal announcing one is noise — a 31-device tree emitted
    1,593 WARNING lines saying only that it got what it requested. A root that was given a
    config, or handed a client, is the case where a missing client is a real fault, and that
    one stays exactly as loud as it was.
    """

    @staticmethod
    def _tree(**root_kwargs):
        root = Device(id="root", name="Root", type="dev.root", **root_kwargs)
        node = root.add_node_from_dict({"id": "meter", "name": "meter", "type": "cap.meter"})
        prop = node.add_property_from_dict({"id": "power", "name": "power", "datatype": "float", "settable": True})
        return root, node, prop

    @staticmethod
    def _no_client_records(caplog, level):
        return [r for r in caplog.records if r.levelno == level and "NoMqttClient" in r.getMessage()]

    def test_transport_free_tree_reports_missing_client_at_debug(self, caplog):
        """The whole point: no WARNING anywhere in a tree that asked for no transport."""
        root, _node, prop = self._tree()

        with caplog.at_level(logging.DEBUG, logger="homie"):
            prop.get_mqtt_client()  # property -> node -> device, three sites in one call
            prop.publish_value()
            prop.set_subscribe()
            prop.start_mqtt_client()
            root.start_mqtt_client()
            root.stop()

        assert self._no_client_records(caplog, logging.WARNING) == []
        assert self._no_client_records(caplog, logging.DEBUG)

    def test_a_root_given_a_config_still_warns(self, caplog):
        """The "you forgot to start the root" case, which the issue is careful to preserve.

        `_mqtt_cfg` is what separates it: a root that was told how to build a client and has
        none is broken, where a root told nothing is simply passive.
        """
        with patch("ebus_sdk.homie.MqttClient.from_config") as mock_from_config:
            mock_from_config.return_value = _mock_mqtt_client()
            root, _node, prop = self._tree(mqtt_cfg={"host": "broker.invalid"})
        root.mqttc = None  # client expected, absent — the genuine fault

        with caplog.at_level(logging.DEBUG, logger="homie"):
            prop.get_mqtt_client()

        assert root._transport_free() is False
        assert self._no_client_records(caplog, logging.WARNING)

    def test_an_injected_client_is_not_transport_free(self):
        """Bring-your-own-transport is the opposite of transport-free, even though both
        leave `_mqtt_cfg` unset — the client is present, so its absence would be an anomaly."""
        root = Device(id="root", type="dev.root", mqttc=_mock_mqtt_client())

        assert root._transport_free() is False

    def test_the_predicate_resolves_from_every_level(self):
        """Property and Node answer for their tree, not for themselves."""
        root, node, prop = self._tree()
        child = Device(id="child", type="dev.child", parent=root)

        assert root._transport_free() is True
        assert child._transport_free() is True
        assert node._transport_free() is True
        assert prop._transport_free() is True

    def test_a_detached_entity_stays_loud(self, caplog):
        """An incomplete chain cannot prove the tree is transport-free, so it does not go
        quiet: a property with no node is a bug, and silencing it would hide one."""
        orphan = Property(id="power", name="power", datatype=PropertyDatatype.FLOAT)

        assert orphan._transport_free() is False

    def test_device_publish_keeps_its_own_severity_when_a_client_was_expected(self, caplog):
        """`devicePublishNoMqttClient` was already INFO on main. Transport-free drops it to
        DEBUG; the expected-a-client case keeps INFO rather than being escalated."""
        with patch("ebus_sdk.homie.MqttClient.from_config") as mock_from_config:
            mock_from_config.return_value = _mock_mqtt_client()
            root, _node, _prop = self._tree(mqtt_cfg={"host": "broker.invalid"})
        root.mqttc = None

        with caplog.at_level(logging.DEBUG, logger="homie"):
            root.publish(attribute="$state", value="ready")

        assert [
            r for r in caplog.records if r.levelno == logging.INFO and "devicePublishNoMqttClient" in r.getMessage()
        ]
