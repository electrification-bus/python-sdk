"""Tests for the json datatype codec + $format JSONSchema support (SDK-5f6)."""

from __future__ import annotations

import json
import logging
import subprocess
import sys
from unittest.mock import MagicMock, patch

import ebus_sdk.homie as homie
from ebus_sdk import (
    Controller,
    Device,
    DiscoveredDevice,
    GroupedPropertyDict,
    PropertyDatatype,
    PropertySpec,
    build_from_declarations,
    validate_json_format,
)

# A flex/request-style control-surface schema (the SDK-5f6 priority driver).
FLEX_SCHEMA = {
    "type": "object",
    "properties": {
        "mode": {"enum": ["SHED", "LOAD_UP", "NORMAL"]},
        "level": {"type": "integer", "minimum": 0, "maximum": 100},
    },
    "required": ["mode"],
}
FLEX_SCHEMA_STR = json.dumps(FLEX_SCHEMA)

DESC = {
    "nodes": {
        "flex": {
            "type": "energy.ebus.capability.flex",
            "properties": {
                "request": {"datatype": "json", "settable": True, "format": FLEX_SCHEMA_STR},
                "active-request": {"datatype": "json"},
                "response": {"datatype": "enum", "format": "NONE,CURTAILED,BOOSTED"},
            },
        }
    }
}


# --- validate_json_format ----------------------------------------------------


def test_validate_valid_returns_none():
    assert validate_json_format({"mode": "SHED", "level": 50}, FLEX_SCHEMA_STR) is None


def test_validate_invalid_enum_returns_error():
    err = validate_json_format({"mode": "BOGUS"}, FLEX_SCHEMA_STR)
    assert isinstance(err, str) and err


def test_validate_out_of_range_returns_error():
    assert validate_json_format({"mode": "SHED", "level": 250}, FLEX_SCHEMA_STR) is not None


def test_validate_missing_required_returns_error():
    assert validate_json_format({"level": 10}, FLEX_SCHEMA_STR) is not None


def test_validate_accepts_already_parsed_dict_schema():
    assert validate_json_format({"mode": "NORMAL"}, FLEX_SCHEMA) is None


def test_validate_no_schema_skips():
    assert validate_json_format({"anything": 1}, None) is None
    assert validate_json_format({"anything": 1}, "") is None


def test_validate_unparseable_schema_skips():
    assert validate_json_format({"x": 1}, "{ not json") is None


def test_validate_gracefully_skips_when_jsonschema_absent(monkeypatch):
    # Simulate the base install (no jsonschema): validation is skipped, not fatal.
    monkeypatch.setattr(homie, "_jsonschema", None)
    monkeypatch.setattr(homie, "_jsonschema_warned", False)
    assert homie.validate_json_format({"mode": "BOGUS"}, FLEX_SCHEMA_STR) is None


def test_validate_skips_and_warns_once_when_jsonschema_import_fails(monkeypatch, caplog):
    # The real lazy-import path: `import jsonschema` raises ImportError.
    monkeypatch.setitem(sys.modules, "jsonschema", None)
    monkeypatch.setattr(homie, "_jsonschema", homie._JSONSCHEMA_UNLOADED)
    monkeypatch.setattr(homie, "_jsonschema_warned", False)
    with caplog.at_level(logging.WARNING, logger="homie"):
        assert homie.validate_json_format({"mode": "BOGUS"}, FLEX_SCHEMA_STR) is None
        assert homie.validate_json_format({"mode": "BOGUS"}, FLEX_SCHEMA_STR) is None
    assert [r.message for r in caplog.records].count(
        "reason=jsonSchemaValidationUnavailable,"
        "hint=install ebus-sdk[validation] (jsonschema) to enable $format JSONSchema validation"
    ) == 1


def test_import_does_not_load_jsonschema():
    # GH #113: jsonschema is imported on first $format validation, not at import.
    # A fresh interpreter, since this test session has already loaded it.
    code = (
        "import sys, ebus_sdk, ebus_sdk.homie\n"
        "assert 'jsonschema' not in sys.modules\n"
        "ebus_sdk.homie.validate_json_format({}, {'type': 'object'})\n"
        "assert 'jsonschema' in sys.modules\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


# --- device-side /set: decode + validate before entity_setter ----------------


def _settable_json_device(received):
    with patch("ebus_sdk.homie.MqttClient.from_config") as mock_from_config:
        mock_client = MagicMock()
        mock_client.sub_callbacks = {}
        mock_client.is_connected.return_value = True
        mock_from_config.return_value = mock_client
        device = Device("dev-flex", mqtt_cfg={"host": "localhost", "port": 1883})
    device.start_mqtt_client()
    model = GroupedPropertyDict()
    spec = PropertySpec(
        "flex",
        "request",
        PropertyDatatype.JSON,
        settable=True,
        format=FLEX_SCHEMA_STR,
        entity_setter=lambda v: received.append(v),
    )
    homie_prop = build_from_declarations(device, model, [spec])[("flex", "request")]
    return device, homie_prop


def _set(homie_prop, device, obj):
    homie_prop._settable_callback(f"ebus/5/{device.id()}/flex/request/set", json.dumps(obj).encode())


def test_valid_json_set_is_decoded_and_reaches_entity_setter():
    received = []
    device, hp = _settable_json_device(received)
    _set(hp, device, {"mode": "SHED", "level": 50})
    # entity_setter receives a parsed dict, not a raw string.
    assert received == [{"mode": "SHED", "level": 50}]


def test_schema_invalid_json_set_is_rejected():
    received = []
    device, hp = _settable_json_device(received)
    _set(hp, device, {"mode": "SHED", "level": 50})  # valid
    _set(hp, device, {"mode": "BOGUS"})  # schema-invalid enum
    _set(hp, device, {"level": 10})  # missing required 'mode'
    # Only the valid command was delivered; the two invalid ones were rejected.
    assert received == [{"mode": "SHED", "level": 50}]


# --- controller-side: get_property_json + set_property_json -------------------


def _controller():
    with patch("ebus_sdk.homie.MqttClient.from_config") as mock_from_config:
        mock_client = MagicMock()
        mock_client.sub_callbacks = {}
        mock_from_config.return_value = mock_client
        ctrl = Controller(mqtt_cfg={"host": "localhost", "port": 1883})
    dev = DiscoveredDevice("dev-flex")
    dev.update_description(json.dumps(DESC))
    ctrl.devices["dev-flex"] = dev
    return ctrl, mock_client, dev


def test_get_property_json_decodes_json_property():
    _, _, dev = _controller()
    dev.update_property("flex", "active-request", json.dumps({"mode": "SHED", "ends-at": "2026-07-11T18:00:00Z"}))
    assert dev.get_property_json("flex", "active-request") == {"mode": "SHED", "ends-at": "2026-07-11T18:00:00Z"}


def test_get_property_json_passes_through_non_json():
    _, _, dev = _controller()
    dev.update_property("flex", "response", "CURTAILED")
    assert dev.get_property_json("flex", "response") == "CURTAILED"  # enum, returned raw


def test_set_property_json_valid_publishes_to_set():
    ctrl, mock_client, _ = _controller()
    assert ctrl.set_property_json("dev-flex", "flex", "request", {"mode": "SHED", "level": 40}) is True
    mock_client.publish.assert_called_once()
    topic, payload = mock_client.publish.call_args.args[0], mock_client.publish.call_args.args[1]
    assert topic.endswith("/flex/request/set")
    assert json.loads(payload) == {"mode": "SHED", "level": 40}


def test_set_property_json_invalid_is_not_published():
    ctrl, mock_client, _ = _controller()
    assert ctrl.set_property_json("dev-flex", "flex", "request", {"mode": "BOGUS"}) is False
    mock_client.publish.assert_not_called()


def test_set_property_json_skips_validation_when_disabled():
    ctrl, mock_client, _ = _controller()
    # validate=False sends even a schema-invalid command (caller opts out).
    assert ctrl.set_property_json("dev-flex", "flex", "request", {"mode": "BOGUS"}, validate=False) is True
    mock_client.publish.assert_called_once()


# --- Phase 3: schema-derived control-surface introspection -------------------

from ebus_sdk import JsonFieldConstraint, json_format_field, json_format_fields  # noqa: E402

# The three canonical flex `level` cases from the spec.
SCHEMA_ENUM_LEVEL = {
    "type": "object",
    "properties": {"mode": {"enum": ["SHED", "NORMAL"]}, "level": {"enum": [40, 60, 100]}},
    "required": ["mode"],
}
SCHEMA_RANGE_LEVEL = {
    "type": "object",
    "properties": {
        "mode": {"enum": ["SHED", "NORMAL"]},
        "level": {"type": "integer", "minimum": 0, "maximum": 100, "multipleOf": 10},
    },
    "required": ["mode"],
}
SCHEMA_NO_LEVEL = {"type": "object", "properties": {"mode": {"enum": ["SHED", "NORMAL"]}}, "required": ["mode"]}


def test_field_enum_level():
    c = json_format_field(SCHEMA_ENUM_LEVEL, "level")
    assert c.present and c.kind == "enum"
    assert c.enum == [40, 60, 100]
    assert c.required is False


def test_field_range_level():
    c = json_format_field(SCHEMA_RANGE_LEVEL, "level")
    assert c.kind == "range"
    assert (c.minimum, c.maximum, c.multiple_of) == (0.0, 100.0, 10.0)
    assert c.type == "integer"


def test_field_absent_level():
    c = json_format_field(SCHEMA_NO_LEVEL, "level")
    assert c.present is False and c.kind == "absent"


def test_field_required_enum_mode():
    c = json_format_field(SCHEMA_ENUM_LEVEL, "mode")
    assert c.kind == "enum" and c.required is True and c.enum == ["SHED", "NORMAL"]


def test_field_accepts_json_string_schema():
    c = json_format_field(json.dumps(SCHEMA_RANGE_LEVEL), "level")
    assert c.kind == "range" and c.maximum == 100.0


def test_field_no_schema_is_absent():
    assert json_format_field(None, "level").kind == "absent"
    assert json_format_field("{ not json", "level").kind == "absent"


def test_fields_maps_all_properties():
    fields = json_format_fields(SCHEMA_RANGE_LEVEL)
    assert set(fields) == {"mode", "level"}
    assert all(isinstance(v, JsonFieldConstraint) for v in fields.values())
    assert fields["mode"].kind == "enum"
    assert fields["level"].kind == "range"


def test_fields_empty_when_no_properties():
    assert json_format_fields(None) == {}
    assert json_format_fields({"type": "string"}) == {}


def test_discovered_device_introspects_format(monkeypatch=None):
    dev = DiscoveredDevice("d")
    desc = {
        "nodes": {
            "flex": {
                "properties": {
                    "request": {"datatype": "json", "settable": True, "format": json.dumps(SCHEMA_ENUM_LEVEL)}
                }
            }
        }
    }
    dev.update_description(json.dumps(desc))
    fields = dev.get_property_format_fields("flex", "request")
    assert fields["level"].enum == [40, 60, 100]
    assert fields["mode"].required is True
