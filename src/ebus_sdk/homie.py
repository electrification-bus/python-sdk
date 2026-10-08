# This may be removed in Python 3.10+.
from __future__ import annotations

"""
Classes and Enums to support Homie (version 5)

   https://github.com/homieiot/convention
   https://homieiot.github.io
   https://homieiot.github.io/specification/

This initial version is focused on providing a Homie representation for some entity(s)
Support for Homie "clients" is TBD/future-work, e.g. discovery, etc.

This is the initial version, there are things to add in the future (as needed):
* Make getting and setting a property's value thread-safe, and add thread-safety throughout
* Support for child devices
    Likely there will be a need to share the MQTT connection between parent and child devices, TBD how
* Support for the target attribute for Properties
* Graceful removal of a Device, including its Nodes and their Properties
    Devices can remove old properties and nodes by deleting the respective MQTT topics
    by publishing an empty message to those topics
    (an actual empty string on MQTT level, so NOT the escaped 0x00 byte, see also empty string values)
    https://github.com/eclipse-paho/paho.mqtt.python/blob/master/examples/client_mqtt_clear_retain.py#L43
* Empty string values (IMPLEMENTED — see encode_empty_string / decode_empty_string):
    MQTT will treat an empty string payload as a “delete” instruction for the topic,
    therefore an empty string value is represented by a 1-character string containing a single byte value 0 (Hex: 0x00, Dec: 0).
    The empty string (passed as an MQTT payload) can only occur in 3 places;
        homie / 5 / [device ID] / [node ID] / [property ID]; reported property values (for string types)
        homie / 5 / [device ID] / [node ID] / [property ID] / set; the topic to set properties (of string types)
        homie / 5 / [device ID] / [node ID] / [property ID] / $target; the target property value (for string types)
    The SDK encodes "" as 0x00 on publish of a string value (Property.publish_value, Controller.set_property) and decodes
    0x00 back to "" on receive (Controller._on_property_message / _on_target_message, Property._settable_callback).
    This convention specifies no way to represent an actual value of a 1-character string with a single byte 0.
    If a device needs this, then it should provide an escape mechanism on the application level.
* Given that Nodes and Properties belong to, and contain pointers to, the owning Device (and Node, for Properties),
    seems likely that we can leverage that to obtain the MQTT client (mqttc) of the owning Device, instead of
    having all downstream entities maintain a local pointer to that
"""

import asyncio
import contextlib
import datetime
import decimal
import hashlib
import inspect
import json
import logging
import math
import numbers
import os
import re
import socket
import sys
import threading
import time
import uuid
from enum import Enum

try:
    from enum import StrEnum
except ImportError:
    # Python < 3.11 compatibility
    class StrEnum(str, Enum):
        pass


from dataclasses import dataclass
from fractions import Fraction
from functools import partial
from threading import RLock

# from deprecated import deprecated
from typing import Any, Callable, List, Optional, Type, Union
from ebus_mqtt_client import MqttClient

from ebus_sdk.transport import MqttControllerTransport, MqttDeviceTransport

# Optional: JSONSchema validation of a `json` property's `$format`. Kept optional
# (see `ebus-sdk[validation]`) so a constrained build can omit it; absent it,
# validation is gracefully skipped (see `validate_json_format`).
try:
    import jsonschema as _jsonschema
except ImportError:  # pragma: no cover - exercised via the graceful-skip path
    _jsonschema = None

logger = logging.getLogger("homie")


def _log_missing_client(message: str, *, by_design: bool, level: int = logging.WARNING) -> None:
    """Log a missing MQTT client at the severity its cause deserves.

    A tree built without transport has no client because that is what was asked for, so
    every entity in it reports one on each traversal — thousands of lines saying only that
    the caller got what they requested. That case is DEBUG.

    Everywhere else a client was expected: the root was given a config, or handed one, and
    its absence means something went wrong. That case keeps the severity it had, so the
    "you forgot to start the root" warning stays as loud as it was.
    """
    logger.log(logging.DEBUG if by_design else level, message)


def _warn_invalid_once(owner: Any, problem: str, message: str) -> None:
    """Warn about invalid producer input once per object and problem.

    The policy for invalid producer input (#91): warn in this release, still
    publish, and raise in the next minor release. Once per object keeps a tree
    rebuilt on every reconnect from repeating itself.
    """
    warned = owner.__dict__.setdefault("_invalid_input_warned", set())
    if problem in warned:
        return
    warned.add(problem)
    logger.warning(f"{message},hint=invalid Homie 5 input; this will raise in the next minor release")


# One-time warning when a `$format` JSONSchema is present but jsonschema is not.
_jsonschema_warned = False

# eBus MQTT topic constants
EBUS_HOMIE_DOMAIN = "ebus"
EBUS_HOMIE_VERSION_MAJOR = 5
EBUS_HOMIE_VERSION_MINOR = 0
EBUS_HOMIE_VERSION_PATCH = 0
EBUS_HOMIE_MQTT_QOS_DEFAULT = "2"

EBUS_HOMIE_MQTT_QOS = int(os.environ.get("EBUS_HOMIE_MQTT_QOS_SITE", EBUS_HOMIE_MQTT_QOS_DEFAULT))


def validate_json_format(value: Any, format_schema: Union[str, dict, None]) -> Optional[str]:
    """Validate a decoded ``json``-property value against its ``$format`` JSONSchema.

    A Homie 5 ``json`` property MAY carry a ``$format`` that is a JSON Schema (the
    device's self-description of the value it accepts, e.g. the ``flex/request``
    control surface). ``format_schema`` is that schema, as a JSON string (the
    Homie wire form) or an already-parsed dict.

    Returns ``None`` when the value is valid OR validation is SKIPPED, and a
    human-readable error string when the value is INVALID against a usable
    schema. Never raises. Validation is skipped (returns ``None``) when there is
    no schema, when the schema cannot be parsed, or when the optional
    ``jsonschema`` package is not installed (a one-time warning is logged then;
    install ``ebus-sdk[validation]`` to enable it).
    """
    global _jsonschema_warned
    if not format_schema:
        return None
    if _jsonschema is None:
        if not _jsonschema_warned:
            _jsonschema_warned = True
            logger.warning(
                "reason=jsonSchemaValidationUnavailable,"
                "hint=install ebus-sdk[validation] (jsonschema) to enable $format JSONSchema validation"
            )
        return None
    schema = format_schema
    if isinstance(schema, str):
        try:
            schema = json.loads(schema)
        except (ValueError, TypeError):
            logger.warning("reason=jsonSchemaFormatNotParseable")
            return None
    if not isinstance(schema, dict):
        return None
    try:
        _jsonschema.validate(instance=value, schema=schema)
        return None
    except _jsonschema.ValidationError as e:
        return e.message
    except _jsonschema.SchemaError as e:
        logger.warning(f"reason=jsonSchemaInvalidSchema,error={e}")
        return None


if EBUS_HOMIE_MQTT_QOS < 1:
    logger.warning(
        f"reason=homieQosLessThanOne,specifiedQos={EBUS_HOMIE_MQTT_QOS},defaultQos={EBUS_HOMIE_MQTT_QOS_DEFAULT}"
    )

# Homie 5 empty-string value encoding.
#
# A zero-length MQTT payload means "clear the retained topic" (see
# Property.clear_value / set_value(None)). So the convention encodes an actual
# empty-string *value* as a 1-character payload containing a single null byte
# (0x00). This lets "" be distinguished on the wire from "delete this topic".
# Applies to the three places an empty string can occur: a reported property
# value, a .../set payload, and a $target value (all for string types).
#
# The convention provides no way to represent a genuine 1-character string whose
# sole character is 0x00; a device needing that must escape it at the
# application level (see module header).
HOMIE_EMPTY_STRING_PAYLOAD = "\x00"

# The color formats a `color` property's $format may list (convention.md:393).
_COLOR_FORMATS = frozenset({"rgb", "hsv", "xyz"})

# The Homie 5 core $description fields (convention.md:212-222). description_extras
# may add extension fields, never one of these.
_DESCRIPTION_CORE_FIELDS = frozenset(
    {"homie", "version", "nodes", "name", "type", "children", "root", "parent", "extensions"}
)


@dataclass(frozen=True)
class JsonFieldConstraint:
    """The control-surface constraint on one field of a ``json`` ``$format`` schema.

    Derived from a Homie 5 ``json`` property's ``$format`` JSONSchema so a
    consumer/UI can honor the device's advertised surface without re-parsing the
    schema. For the canonical ``flex/request`` ``level`` cases: ``enum`` names the
    exact supported values (render buttons), a numeric range gives
    ``minimum``/``maximum``/``multiple_of`` (render a slider, stepped if
    ``multiple_of``), and an absent field means the device does not accept it.
    Complex schema constructs (``anyOf``/``oneOf``/nested objects) are not
    decomposed; such a field reports ``kind == "free"`` with whatever scalar
    facets are present.
    """

    name: str
    present: bool  # is the field declared in the schema's `properties`?
    required: bool  # is it listed in the schema's `required`?
    type: Optional[str] = None  # JSON type: "integer" / "number" / "string" / ...
    enum: Optional[list] = None  # exact allowed values, if an enum
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    multiple_of: Optional[float] = None

    @property
    def kind(self) -> str:
        """`"absent"` | `"enum"` | `"range"` | `"free"` — a UI-rendering hint."""
        if not self.present:
            return "absent"
        if self.enum is not None:
            return "enum"
        if self.minimum is not None or self.maximum is not None:
            return "range"
        return "free"


def _as_schema_dict(format_schema: Union[str, dict, None]) -> Optional[dict]:
    """Parse a `$format` (JSON string or dict) into a schema dict, or None."""
    if not format_schema:
        return None
    schema = format_schema
    if isinstance(schema, str):
        try:
            schema = json.loads(schema)
        except (ValueError, TypeError):
            return None
    return schema if isinstance(schema, dict) else None


def json_format_field(format_schema: Union[str, dict, None], field: str) -> JsonFieldConstraint:
    """Introspect one field of a ``json`` property's ``$format`` JSONSchema.

    ``format_schema`` is the ``$format`` as a JSON string (the Homie wire form) or
    a parsed dict. Returns a `JsonFieldConstraint`; a missing schema or field
    yields ``present=False`` (``kind == "absent"``).
    """
    schema = _as_schema_dict(format_schema)
    props = schema.get("properties") if isinstance(schema, dict) else None
    required = set(schema.get("required", [])) if isinstance(schema, dict) else set()
    if not isinstance(props, dict) or field not in props or not isinstance(props[field], dict):
        return JsonFieldConstraint(name=field, present=False, required=field in required)
    f = props[field]
    minimum = f.get("minimum")
    maximum = f.get("maximum")
    return JsonFieldConstraint(
        name=field,
        present=True,
        required=field in required,
        type=f.get("type"),
        enum=list(f["enum"]) if isinstance(f.get("enum"), list) else None,
        minimum=float(minimum) if isinstance(minimum, (int, float)) and not isinstance(minimum, bool) else None,
        maximum=float(maximum) if isinstance(maximum, (int, float)) and not isinstance(maximum, bool) else None,
        multiple_of=(
            float(f["multipleOf"])
            if isinstance(f.get("multipleOf"), (int, float)) and not isinstance(f.get("multipleOf"), bool)
            else None
        ),
    )


def json_format_fields(format_schema: Union[str, dict, None]) -> dict:
    """Introspect every top-level field of a ``json`` ``$format`` JSONSchema.

    Returns ``{field_name: JsonFieldConstraint}`` for each property the schema
    declares (empty dict if the schema is missing/unusable or declares no
    ``properties``). See `json_format_field`.
    """
    schema = _as_schema_dict(format_schema)
    props = schema.get("properties") if isinstance(schema, dict) else None
    if not isinstance(props, dict):
        return {}
    return {name: json_format_field(schema, name) for name in props}


def encode_empty_string(value: str) -> str:
    """
    Encode a property value for the wire per the Homie 5 empty-string convention:
    an empty string becomes a single 0x00 byte; every other value passes through
    unchanged. Call this only for genuine values — a cleared/absent value (None)
    goes through clear_value(), not here.
    """
    return HOMIE_EMPTY_STRING_PAYLOAD if value == "" else value


def decode_empty_string(payload: str) -> str:
    """
    Decode an inbound MQTT payload per the Homie 5 empty-string convention: a
    single 0x00 byte becomes an empty string; every other payload passes through
    unchanged. (A truly zero-length payload is a topic clear and is handled by
    the caller before reaching here.)
    """
    return "" if payload == HOMIE_EMPTY_STRING_PAYLOAD else payload


# Helper character constants for units
UNICODE_DEGREE = "\u00b0"
UNICODE_EXPONENT_3 = "\u00b3"
UNICODE_EXPONENT_MINUS = "\u207b"
UNICODE_EXPONENT_1 = "\u00b9"


class Unit(StrEnum):
    DEGREE_CELSIUS = UNICODE_DEGREE + "C"
    DEGREE_FAHRENHEIT = UNICODE_DEGREE + "F"
    DEGREE = UNICODE_DEGREE
    LITER = "L"
    GALLON = "gal"
    VOLTS = "V"
    WATT = "W"
    KILOWATT = "kW"
    KILOWATT_HOUR = "kWh"
    AMPERE = "A"
    HERTZ = "Hz"
    REVOLUTIONS_PER_MINUTE = "rpm"
    PERCENT = "%"
    METER = "m"
    CUBIC_METER = "m" + UNICODE_EXPONENT_3
    FEET = "ft"
    METERS_PER_SECOND = "m/s"
    KNOTS = "kn"
    PASCAL = "Pa"
    POUNDS_PER_SQUARE_INCH = "psi"
    PARTS_PER_MILLION = "ppm"
    SECONDS = "s"
    MINUTES = "min"
    HOURS = "h"
    LUX = "lx"
    KELVIN = "K"
    MIRED = "MK" + UNICODE_EXPONENT_MINUS + UNICODE_EXPONENT_1
    COUNT_OR_AMOUNT = "#"
    WATT_HOUR = "Wh"
    # Apparent- and reactive-power/energy units. Not in the Homie convention's
    # recommended list (which stops at the common SI symbols), but the eBus
    # `meter` capability defines them. Casing follows IEC 80000-6, the same
    # SI-symbol style the Homie convention uses (W, Hz, Pa, ...): apparent
    # power/energy are the uppercase VA / VAh, and reactive power/energy are the
    # lowercase var / varh (var is the IEC standardized symbol, not VAR). See
    # https://github.com/homieiot/convention/issues/318 and eBus meter.md.
    VOLT_AMPERE = "VA"
    VOLT_AMPERE_HOUR = "VAh"
    VOLT_AMPERE_REACTIVE = "var"
    VOLT_AMPERE_REACTIVE_HOUR = "varh"


class PropertyDatatype(StrEnum):
    """
    https://homieiot.github.io/specification/
    PropertyDatatype.STRING.value -> 'string'
    PropertyDatatype[foo].value -> 'string' for foo == 'STRING'
    PropertyDatatype('string').name -> 'STRING'
    """

    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    STRING = "string"
    ENUM = "enum"
    COLOR = "color"
    DATETIME = "datetime"
    DURATION = "duration"
    JSON = "json"


# ── Payload encoding and validation (convention.md:74-150) ──────────────────
#
# Each encoder takes a Python value and returns ``(payload, None)`` for a valid
# wire payload, or ``(None, problem)`` when the datatype cannot represent the
# value. The device refuses to publish a refused value; the /set path uses the
# same grammar to reject a command.

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
# re.ASCII: a payload's digits are 0-9 only (convention.md:89, :97), where a bare
# \d also matches every other Unicode decimal digit.
_INTEGER_PAYLOAD = re.compile(r"-?\d+", re.ASCII)
_FLOAT_PAYLOAD = re.compile(r"-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE]-?\d+)?", re.ASCII)
_DURATION_PAYLOAD = re.compile(r"PT(?:\d+H)?(?:\d+M)?(?:\d+(?:\.\d+)?S)?", re.ASCII)
# ISO 8601 (convention.md:132) in its calendar, ordinal and week date forms,
# extended or basic, with an optional time of day and zone designator.
_DATETIME_PAYLOAD = re.compile(
    r"""
    (?P<year>\d{4})
    (?:
        -(?P<month>\d{2})(?:-(?P<day>\d{2}))?
      | (?P<bmonth>\d{2})(?P<bday>\d{2})
      | -?(?P<ordinal>\d{3})
      | -?W(?P<week>\d{2})(?:-?(?P<weekday>\d))?
    )?
    (?:
        T(?P<hour>\d{2})(?::?(?P<minute>\d{2})(?::?(?P<second>\d{2}))?)?(?:[.,]\d+)?
        (?:Z|[+-](?P<zhour>\d{2})(?::?(?P<zminute>\d{2}))?)?
    )?
    """,
    re.ASCII | re.VERBOSE,
)
_FLOAT_MAX_DECIMAL = decimal.Decimal(sys.float_info.max)
_COLOR_RANGES = {"rgb": (255.0, 255.0, 255.0), "hsv": (360.0, 100.0, 100.0), "xyz": (1.0, 1.0)}


def _reject_json_constant(name: str) -> Any:
    raise ValueError(f"{name} is not JSON")


def _format_float(number: float) -> Optional[str]:
    """A finite float in the convention's grammar: no `+` in the exponent (:97)."""
    if not math.isfinite(number):
        return None
    return repr(float(number)).replace("e+", "e")


def _encode_integer(value: Any) -> tuple:
    if isinstance(value, bool):
        return None, "booleanOnNumeric"
    if isinstance(value, str):
        if not _INTEGER_PAYLOAD.fullmatch(value):
            return None, "integerGrammar"
        number = int(value)
    elif isinstance(value, numbers.Integral):
        number = int(value)
    elif isinstance(value, numbers.Real):
        if not math.isfinite(value) or not float(value).is_integer():
            return None, "integerNotWhole"
        number = int(value)
    else:
        return None, "integerType"
    if not _INT64_MIN <= number <= _INT64_MAX:
        return None, "integerOutOf64Bit"
    return (value if isinstance(value, str) else str(number)), None


def _encode_float(value: Any) -> tuple:
    if isinstance(value, bool):
        return None, "booleanOnNumeric"
    if isinstance(value, str):
        if not _FLOAT_PAYLOAD.fullmatch(value) or not math.isfinite(float(value)):
            return None, "floatGrammar"
        return value, None
    if isinstance(value, decimal.Decimal):
        if not value.is_finite():
            return None, "floatNotFinite"
        if abs(value) > _FLOAT_MAX_DECIMAL:
            return None, "floatOutOfRange"
        return str(value).replace("E+", "E"), None
    if isinstance(value, numbers.Integral):
        # Float range (convention.md:95-96).
        if abs(int(value)) > sys.float_info.max:
            return None, "floatOutOfRange"
        return str(int(value)), None
    if isinstance(value, numbers.Real):
        # A non-float Real (numpy's float32, say) keeps its own shortest text when
        # that is already a payload: repr(float()) would expand float32 0.1 to
        # 0.10000000149011612.
        if not isinstance(value, float):
            text = str(value)
            if _FLOAT_PAYLOAD.fullmatch(text) and math.isfinite(float(text)):
                return text, None
        try:
            payload = _format_float(float(value))
        except OverflowError:
            return None, "floatOutOfRange"
        return (payload, None) if payload is not None else (None, "floatNotFinite")
    return None, "floatType"


def _encode_boolean(value: Any) -> tuple:
    if not isinstance(value, bool):
        return None, "booleanType"
    return ("true" if value else "false"), None


def _enum_values(format_: Any) -> Optional[list]:
    return format_.split(",") if isinstance(format_, str) and format_ else None


def _encode_enum(value: Any, format_: Any) -> tuple:
    payload = str(value.value) if isinstance(value, Enum) else str(value)
    if payload == "":
        return None, "enumEmpty"
    allowed = _enum_values(format_)
    if allowed is not None and payload not in allowed:
        return None, "enumNotInFormat"
    return payload, None


def _check_color_payload(payload: str, format_: Any) -> Optional[str]:
    parts = payload.split(",")
    kind = parts[0]
    ranges = _COLOR_RANGES.get(kind)
    if ranges is None:
        return "colorType"
    allowed = _enum_values(format_)
    if allowed is not None and kind not in allowed:
        return "colorTypeNotInFormat"
    if len(parts) != len(ranges) + 1:
        return "colorComponentCount"
    for text, upper in zip(parts[1:], ranges):
        if not _FLOAT_PAYLOAD.fullmatch(text):
            return "colorComponentGrammar"
        if not 0.0 <= float(text) <= upper:
            return "colorComponentRange"
    return None


def _encode_color(value: Any, format_: Any) -> tuple:
    if isinstance(value, str):
        problem = _check_color_payload(value, format_)
        return (None, problem) if problem else (value, None)
    if isinstance(value, (tuple, list)):
        # Components alone: encode them in the property's preferred (first) color
        # format (convention.md:393).
        allowed = _enum_values(format_)
        if not allowed:
            return None, "colorFormatMissing"
        components = []
        for component in value:
            text, problem = _encode_float(component)
            if problem:
                return None, "colorComponentGrammar"
            components.append(text)
        payload = ",".join([allowed[0], *components])
        problem = _check_color_payload(payload, format_)
        return (None, problem) if problem else (payload, None)
    return None, "colorType"


def _check_datetime_payload(text: str) -> Optional[str]:
    """None if ``text`` is an ISO 8601 date or date-time (convention.md:132), else a problem."""
    match = _DATETIME_PAYLOAD.fullmatch(text)
    if not match:
        return "datetimeGrammar"
    fields = {k: int(v) for k, v in match.groupdict().items() if v is not None}
    if "hour" in fields and not fields.keys() & {"day", "bday", "ordinal", "weekday"}:
        return "datetimeGrammar"  # a time of day needs a complete date
    try:
        if "month" in fields or "bmonth" in fields:
            datetime.date(
                fields["year"], fields.get("month", fields.get("bmonth")), fields.get("day", fields.get("bday", 1))
            )
    except ValueError:
        return "datetimeRange"
    limits = {"ordinal": (1, 366), "week": (1, 53), "weekday": (1, 7), "hour": (0, 24), "minute": (0, 59)}
    limits.update({"second": (0, 60), "zhour": (0, 23), "zminute": (0, 59)})
    for name, (low, high) in limits.items():
        if name in fields and not low <= fields[name] <= high:
            return "datetimeRange"
    return None


def _encode_datetime(value: Any) -> tuple:
    if isinstance(value, (datetime.datetime, datetime.date)):
        # ISO 8601 extended form, `T` separator (convention.md:132).
        return value.isoformat(), None
    if isinstance(value, str):
        problem = _check_datetime_payload(value)
        return (None, problem) if problem else (value, None)
    return None, "datetimeType"


def _encode_duration(value: Any) -> tuple:
    if isinstance(value, datetime.timedelta):
        if value < datetime.timedelta(0):
            return None, "durationNegative"
        # PTxHxMxS (convention.md:137-144): days fold into hours, which the
        # format has no larger unit than.
        hours, rest = divmod(value.days * 86400 + value.seconds, 3600)
        minutes, seconds = divmod(rest, 60)
        payload = "PT"
        if hours:
            payload += f"{hours}H"
        if minutes:
            payload += f"{minutes}M"
        if value.microseconds:
            payload += f"{seconds}.{value.microseconds:06d}".rstrip("0") + "S"
        elif seconds or payload == "PT":
            payload += f"{seconds}S"
        return payload, None
    if isinstance(value, str):
        if value == "PT" or not _DURATION_PAYLOAD.fullmatch(value):
            return None, "durationGrammar"
        return value, None
    return None, "durationType"


def _decode_json_container(text: str) -> tuple:
    """Parse a json payload: an array or object, never NaN/Infinity (convention.md:150)."""
    try:
        decoded = json.loads(text, parse_constant=_reject_json_constant)
    except (ValueError, TypeError):
        return None, "jsonInvalid"
    if not isinstance(decoded, (list, dict)):
        return None, "jsonNotArrayOrObject"
    return decoded, None


def _encode_json(value: Any) -> tuple:
    if isinstance(value, str):
        _, problem = _decode_json_container(value)
        # A valid JSON string passes through unchanged rather than re-encoded.
        return (None, problem) if problem else (value, None)
    if not isinstance(value, (list, tuple, dict)):
        return None, "jsonNotArrayOrObject"
    try:
        return json.dumps(value, allow_nan=False), None
    except (TypeError, ValueError):
        return None, "jsonInvalid"


def _encode_property_value(value: Any, datatype: Any, format_: Any = None) -> tuple:
    """Encode ``value`` as a Homie 5 payload for ``datatype``.

    Returns ``(payload, None)``, or ``(None, problem)`` when ``datatype`` cannot
    represent ``value`` (convention.md:78). ``problem`` is a short camelCase reason.
    The payload is the value's text; the ``0x00`` empty-string encoding is applied
    by the publisher, and only for ``string`` (convention.md:65-67).
    """
    if datatype == PropertyDatatype.INTEGER:
        return _encode_integer(value)
    if datatype == PropertyDatatype.FLOAT:
        return _encode_float(value)
    if datatype == PropertyDatatype.BOOLEAN:
        return _encode_boolean(value)
    if datatype == PropertyDatatype.ENUM:
        return _encode_enum(value, format_)
    if datatype == PropertyDatatype.COLOR:
        return _encode_color(value, format_)
    if datatype == PropertyDatatype.DATETIME:
        return _encode_datetime(value)
    if datatype == PropertyDatatype.DURATION:
        return _encode_duration(value)
    if datatype == PropertyDatatype.JSON:
        return _encode_json(value)
    # string, and a missing or unknown datatype (already warned about).
    return (str(value.value) if isinstance(value, Enum) else str(value)), None


def _numeric_format(format_: Any) -> tuple:
    """``(min, max, step)`` as Fractions (each may be None) from ``[min]:[max][:step]``.

    A format that does not parse imposes no constraint. A step that is not > 0 is
    ignored (convention.md:390-391).
    """
    if not isinstance(format_, str) or not format_:
        return None, None, None
    parts = format_.split(":")
    if len(parts) not in (2, 3):
        return None, None, None

    def bound(text: str) -> Optional[Fraction]:
        if not _FLOAT_PAYLOAD.fullmatch(text):
            return None
        return Fraction(text)

    low, high = bound(parts[0]), bound(parts[1])
    step = bound(parts[2]) if len(parts) == 3 else None
    if step is not None and step <= 0:
        step = None
    return low, high, step


def _round_and_range_check(number: Fraction, format_: Any, current: Any) -> tuple:
    """Round to the format's step, then check min/max (convention.md:397-413).

    The step base is ``min``, else ``max``, else the property's current value;
    with none of them there is nothing to round against and the value stands.
    Rounding is ``floor(x + 0.5)`` so it always goes up, as :408-413 recommend.
    Returns ``(rounded, problem)``.
    """
    low, high, step = _numeric_format(format_)
    if step is not None:
        base = low if low is not None else high
        if base is None and isinstance(current, numbers.Real) and not isinstance(current, bool):
            if math.isfinite(current):
                base = Fraction(current)
        if base is not None:
            number = math.floor((number - base) / step + Fraction(1, 2)) * step + base
    if low is not None and number < low:
        return None, "belowMin"
    if high is not None and number > high:
        return None, "aboveMax"
    return number, None


def _parse_set_payload(text: str, datatype: Any, format_: Any, current: Any) -> tuple:
    """Validate an inbound ``/set`` payload for ``datatype``.

    Returns ``(value, None)`` with the value to hand the set callback, or
    ``(None, problem)``. The value is the payload text for every datatype but
    ``json`` (the decoded array or object) and ``string`` (with ``0x00`` decoded
    to ``""``). A number that step rounding changed is handed over in its
    rounded payload form.
    """
    if datatype == PropertyDatatype.JSON:
        return _decode_json_container(text)
    if datatype == PropertyDatatype.INTEGER:
        if not _INTEGER_PAYLOAD.fullmatch(text) or not _INT64_MIN <= int(text) <= _INT64_MAX:
            return None, "integerGrammar"
        rounded, problem = _round_and_range_check(Fraction(int(text)), format_, current)
        if problem:
            return None, problem
        if rounded.denominator != 1:
            return None, "integerStepNotWhole"
        return (text if rounded == int(text) else str(int(rounded))), None
    if datatype == PropertyDatatype.FLOAT:
        if not _FLOAT_PAYLOAD.fullmatch(text) or not math.isfinite(float(text)):
            return None, "floatGrammar"
        number = Fraction(text)
        rounded, problem = _round_and_range_check(number, format_, current)
        if problem:
            return None, problem
        return (text if rounded == number else _format_float(float(rounded))), None
    if datatype == PropertyDatatype.BOOLEAN:
        return (text, None) if text in ("true", "false") else (None, "booleanGrammar")
    if datatype == PropertyDatatype.ENUM:
        allowed = _enum_values(format_)
        if text == "" or (allowed is not None and text not in allowed):
            return None, "enumNotInFormat"
        return text, None
    if datatype == PropertyDatatype.COLOR:
        problem = _check_color_payload(text, format_)
        return (None, problem) if problem else (text, None)
    if datatype == PropertyDatatype.DURATION:
        return _encode_duration(text)
    if datatype == PropertyDatatype.DATETIME:
        return _encode_datetime(text)
    # string, and a missing or unknown datatype (already warned about).
    return decode_empty_string(text), None


def _subscribe_supports_with_retain(mqttc: Any) -> bool:
    """Whether ``mqttc.subscribe`` takes ``with_retain`` (ebus-mqtt-client with #24).

    Only a parameter of that name counts: a transport taking ``**kwargs`` may not
    deliver the flag.
    """
    try:
        return "with_retain" in inspect.signature(mqttc.subscribe).parameters
    except (TypeError, ValueError):
        return False


def sanitize_homie_id(value: Optional[str]) -> str:
    """Coerce an arbitrary string to a Homie-legal id segment (a-z, 0-9, -).

    The Homie 5 spec allows a topic-level id to contain ONLY lowercase letters
    ``a``-``z``, digits ``0``-``9``, and the hyphen (``-``). There is no
    leading-letter requirement, and leading/trailing hyphens are not prohibited
    (see the convention's topic-ids rule). This helper coerces vendor-supplied
    strings (serial numbers, model names, etc.) to that character set by:

      1. lowercasing
      2. replacing underscores, whitespace, and dots with hyphens
      3. dropping any other character outside ``[a-z0-9-]``
      4. collapsing runs of hyphens
      5. stripping leading/trailing hyphens

    Steps 4 and 5 are deliberate extra normalization, not spec requirements: a
    pure-digit id (e.g. a bare meter serial ``000010024176``) is legal as-is.
    The result is always spec-legal, just slightly tidier than the strict
    minimum the convention demands.

    Empty input (``None`` or empty string) returns an empty string. The
    caller is responsible for handling the empty-string case — e.g. by
    falling back to a synthesized id when the sanitized form is empty,
    or by raising if the value was required to be non-empty.

    Composing device-ids from multiple vendor-supplied segments (e.g.
    ``f"{panel_serial}-{bess_serial}"``) MUST apply this helper to each
    segment independently, so that publisher and consumer always agree
    on the resulting Homie-legal id. Composing first and sanitizing the
    composite is not equivalent — a hyphen-joiner can be collapsed if an
    adjacent segment ends/begins with characters that drop out.
    """
    if not value:
        return ""
    result = value.lower()
    result = re.sub(r"[_\s.]+", "-", result)
    result = re.sub(r"[^a-z0-9-]", "", result)
    result = re.sub(r"-+", "-", result)
    return result.strip("-")


def datatype_from_type(type: Type) -> Optional[PropertyDatatype]:
    """
    Returns Homie PropertyDatatype from Python type
    PropertyDatatypes with no native Python type are specified as strings
    """
    if type == int:
        return PropertyDatatype.INTEGER
    elif type == float:
        return PropertyDatatype.FLOAT
    elif type == bool:
        return PropertyDatatype.BOOLEAN
    elif type == str:
        return PropertyDatatype.STRING
    elif type == StrEnum:
        return PropertyDatatype.ENUM
    elif type == "color":
        return PropertyDatatype.COLOR
    elif type == "datetime":
        return PropertyDatatype.DATETIME
    elif type == "duration":
        return PropertyDatatype.DURATION
    elif type == "json":
        return PropertyDatatype.JSON
    else:
        logger.warning(f"reason=datatypeFromTypeUnknownType,type={type}")
        return None


class DeviceState(StrEnum):
    """
    https://homieiot.github.io/specification/
    DeviceState.READY.value -> 'ready'
    DeviceState[foo].value -> 'ready' for foo == 'READY'
    DeviceState('ready').name -> 'READY'
    """

    INIT = "init"
    READY = "ready"
    DISCONNECTED = "disconnected"
    SLEEPING = "sleeping"
    LOST = "lost"


# Homie 5 effective-state precedence table (SDK-zt2).
#
# A non-root device's effective state is determined by its root's reported state:
# whenever the root is in a non-ready state, that state propagates down the tree
# (the root is the gateway to the children, so if it's lost/disconnected/sleeping/
# init the children are effectively the same). Only when the root is `ready` do
# the children's own reported states stand.
#
# Mapping: root_state -> override_for_children. A None value means "no override,
# use the child's own state".
HOMIE_EFFECTIVE_STATE_TABLE: dict = {
    DeviceState.INIT: DeviceState.INIT,
    DeviceState.DISCONNECTED: DeviceState.DISCONNECTED,
    DeviceState.SLEEPING: DeviceState.SLEEPING,
    DeviceState.LOST: DeviceState.LOST,
    DeviceState.READY: None,
}


class Property:
    """
    Object representing a Homie MQTT Property
    https://homieiot.github.io/specification/
    Note that device and node are NOT overwritten if they exist
    Question: Should we subclass per datatype???
    TODO: Should device_id come from Node -> Device -> id?
    TODO: Fail loudly if "id" not provided
    """

    def __init__(
        self,
        id: Optional[str] = None,
        value: Optional[Any] = None,
        name: Optional[str] = None,
        datatype: PropertyDatatype = None,
        format: Optional[str] = None,
        settable: Optional[bool] = False,
        set_callback: Optional[Callable] = None,
        retained: Optional[bool] = True,
        unit: Optional[str] = None,
        round_to: Optional[int] = None,
        supports_target: Optional[bool] = False,
        node: Optional[Node] = None,
        device: Optional[Device] = None,
        async_loop: Optional[asyncio.AbstractEventLoop] = None,
        from_dict: Optional[dict] = None,
    ):
        if from_dict:
            # from_dict not tiven
            id = from_dict.get("id", None)
            value = from_dict.get("value", None)
            name = from_dict.get("name", None)
            datatype = from_dict.get("datatype", None)
            format = from_dict.get("format", None)
            settable = from_dict.get("settable", False)
            retained = from_dict.get("retained", True)
            unit = from_dict.get("unit", None)
            round_to = from_dict.get("round_to", None)
            supports_target = from_dict.get("supports_target", False)
            node = from_dict.get("node", None)
            device = from_dict.get("device", None)
            set_callback = from_dict.get("set_callback", None)
            async_loop = from_dict.get("async_loop", None)
        # Regardless of how we got this info, construct it
        # AKA, the "business logic" of the constructor
        self._id = id
        self._round = round_to
        self._value = value
        if name:
            self._name = name
        else:
            self._name = id
        self._datatype = datatype
        self._format = format
        self._settable = bool(settable)
        # Don't assign set_callback unless this property is settable
        if settable:
            self._set_callback = set_callback
        else:
            self._set_callback = None
        if retained is None:
            # `retained` is a non-nullable boolean in $description (convention.md:343).
            # The publish path always treated None as non-retained, so keep that.
            _warn_invalid_once(self, "retainedNull", f"reason=propertyRetainedNull,propertyID={id},treatedAs=false")
        self._retained = bool(retained)
        self._unit = unit
        self._supports_target = supports_target
        if supports_target:
            # $target is not implemented (publish_target_value() is a stub), so a
            # property must never use it (convention.md:333).
            logger.warning(f"reason=propertySupportsTargetInert,propertyID={id},hint=$target is not implemented")
        self._node = node
        self._device = device
        self.async_loop = async_loop
        # QoS for MQTT operations (may be overridden by Device when adopted)
        self._qos = EBUS_HOMIE_MQTT_QOS
        # Track whether this property has ever been published (FIX for MQTT topic persistence)
        self._ever_published = False
        # The last (topic, payload) pair this property actually put on the wire, or
        # None. publish_value() skips a retained republish whose final payload is
        # byte-identical on the same topic (GH #50). The TOPIC is half the key
        # because it is derived at publish time from the node/device ids, and
        # set_node()/set_device() are public: a reparented property must not have
        # its first publish on the new topic suppressed by the old topic's memo.
        # Held as ONE tuple so the pair is written in a single atomic assignment.
        self._last_published: Optional[tuple] = None
        # Serializes "compute the payload, publish it, record what was published" so
        # that triple is atomic per property. Two threads reach it: the application
        # thread via set_value(), and the MQTT loop thread via on_connect ->
        # refresh_tree() -> Node.publish() -> publish_value(force=True). Without this
        # they can interleave so the memo records an OLDER payload than the one that
        # actually went out last, and the GH #50 gate then suppresses the publish that
        # would correct the broker, stranding the wrong retained value.
        #
        # REENTRANT, and not optionally so: set_value() takes this lock and then calls
        # publish_value(), which takes it again, so a plain Lock self-deadlocks on the
        # single most common call in the SDK. publish_value() -> clear_value() (the
        # retraction path) nests the same way. Same reason GroupedPropertyDict uses an
        # RLock. Verified by mutation: swapping RLock for Lock hangs set_value().
        #
        # Per-property, so it never serializes a tree walk, and no code path holds two
        # property locks at once, so there is no lock-ordering hazard here.
        #
        # It IS held across the transport's publish(), which is deliberate: releasing
        # it earlier reopens the very window this closes. That is safe for the paho
        # transport the SDK ships, and the reason is worth recording because it is not
        # obvious. The dangerous shape would be an A-B/B-A cycle in which the network
        # thread holds a transport lock while invoking on_connect (-> refresh_tree ->
        # publish_value, which wants this lock) that publish() also needs. In paho
        # 2.x it does not arise: _handle_connack invokes on_connect holding only
        # _in_callback_mutex and acquires _out_message_mutex only AFTER the callback
        # returns (the two blocks are sequential, not nested), and the one place the
        # publish path touches _in_callback_mutex (_packet_queue) uses a NON-blocking
        # acquire(False) that threaded mode skips entirely.
        #
        # A bring-your-own transport could still construct that cycle by holding its
        # own lock across the on-connect handler it wires to refresh_tree() while
        # requiring the same lock in publish(). Such a transport must not do that.
        self._publish_lock = RLock()
        self._initial_value_was_none = value is None
        # Check for skip_initial_publish flag from dict
        self._skip_initial_publish = from_dict.get("skip_initial_publish", False) if from_dict else False
        self._check_description_fields()

    def _check_description_fields(self) -> None:
        """Warn (once per problem) about $description fields Homie 5 rejects.

        ``datatype`` is required (convention.md:340); ``enum`` and ``color`` require
        a ``format``; an enum format lists at least one value, none empty and none
        duplicated (:392); a color format lists only ``rgb``, ``hsv`` and ``xyz``
        (:393).
        """
        pid = self._id
        datatype = self._datatype
        if datatype is None:
            _warn_invalid_once(self, "datatypeMissing", f"reason=propertyDatatypeMissing,propertyID={pid}")
            return
        if datatype not in PropertyDatatype._value2member_map_:
            _warn_invalid_once(
                self, "datatypeUnknown", f"reason=propertyDatatypeUnknown,propertyID={pid},datatype={datatype}"
            )
            return
        fmt = self._format
        if datatype in (PropertyDatatype.ENUM, PropertyDatatype.COLOR) and not fmt:
            _warn_invalid_once(
                self, "formatMissing", f"reason=propertyFormatMissing,propertyID={pid},datatype={datatype}"
            )
            return
        if datatype == PropertyDatatype.ENUM and isinstance(fmt, str):
            values = fmt.split(",")
            if "" in values:
                _warn_invalid_once(self, "enumEmptyValue", f"reason=propertyEnumFormatEmptyValue,propertyID={pid}")
            if len(set(values)) != len(values):
                _warn_invalid_once(self, "enumDuplicate", f"reason=propertyEnumFormatDuplicate,propertyID={pid}")
        if datatype == PropertyDatatype.COLOR and isinstance(fmt, str):
            if not set(fmt.split(",")) <= _COLOR_FORMATS:
                _warn_invalid_once(
                    self, "colorFormatInvalid", f"reason=propertyColorFormatInvalid,propertyID={pid},format={fmt}"
                )

    def as_dict(self) -> dict:
        return {
            "id": self.id(),
            "name": self.name(),
            "value": self.value(),
            "datatype": self.datatype(),
            "format": self.format(),
            "settable": self.settable(),
        }

    def set_node(self, node: Node) -> None:
        self._node = node

    def node(self) -> Node:
        """
        Returns Node containing Property
        """
        return self._node

    def _transport_free(self) -> bool:
        """See ``Device._transport_free``. Resolved node -> device, mirroring the walk in
        ``start_mqtt_client``; an incomplete chain falls through as *not* transport-free so
        a half-built tree stays loud rather than going quiet."""
        node = self.node()
        device = node.device() if node is not None else None
        return device._transport_free() if device is not None else False

    def _homie_domain(self) -> str:
        """The domain of the tree this property belongs to.

        Same node -> device walk as ``_transport_free``. A property not yet
        attached to a tree falls back to the eBus domain, which is what every
        topic here was hardcoded to before the domain was configurable.
        """
        node = self.node()
        device = node.device() if node is not None else self._device
        return device.homie_domain() if device is not None else EBUS_HOMIE_DOMAIN

    def get_node_id(self) -> str:
        """
        Why is this needed?
        do my_property.node().id()
        TODO: Find callers and change them!
        """
        node = self.node()
        if not node:
            logger.warning(f"reason=propertyGetNodeNoNode,propertyID={self._id}")
            return None
        return self.node().id()

    def get_device_id(self) -> str:
        """
        Why is this needed?
        do my_property.device().id()
        TODO: Find callers and change them!
        """
        node = self.node()
        if not node:
            logger.warning(f"reason=propertyGetDeviceIdNoNode,propertyID={self._id}")
            return None
        # return node.get_device_id() # TODO how about node.device().id()
        return node.device().id()

    def set_device(self, device: Device) -> None:
        self._device = device
        return None

    def set_value(self, value: Any) -> bool:
        """
        Set the property's value to ``value`` and publish it to MQTT.

        Publishing is gated on change (GH #50): a RETAINED property whose new value
        produces a wire payload byte-identical to the one it last published on this
        topic is not republished, because the broker's retained store already holds
        exactly that payload and every subscriber already has it. The comparison is
        on the FINAL payload (after rounding, datatype coercion and empty-string
        encoding), so two distinct Python values that serialize identically collapse
        to a single publish -- which a caller holding a raw reading cannot determine
        for itself, since ``round_to`` and the coercion live in here.

        Three carve-outs: a non-retained (event) property is never gated, retraction
        (``set_value(None)``) is never gated, and ``publish_value(force=True)``
        bypasses the gate for whole-tree republishes.

        Returns False on failure, else True. A suppressed republish returns True:
        nothing failed, and the broker holds the value. Callers cannot distinguish
        suppressed from published from the return; use ``get_last_published_value()``
        if you need the memo itself.

        Thread-safe: the value write and its publish are one atomic unit per property,
        so a concurrent forced republish (the MQTT loop thread's reconnect refresh)
        cannot land between them.
        """
        with self._publish_lock:
            self._value = value
            return self.publish_value()

    def round(self) -> Optional[int]:
        """
        Returns the property's round attribute
        """
        return self._round

    def value(self) -> Any:
        """
        Returns the property's value, potentially rounded
        """
        # TODO: Decide if we really want this to round()
        round_to = self.round()
        # round(None, X) raises TypeError. Properties can legitimately hold None
        # (e.g., before initial backing-store sync), and as_dict() / description()
        # call value() unconditionally for diagnostic logging — so we must return
        # None unchanged rather than crash.
        if round_to and self._value is not None:
            rounded_value = round(self._value, round_to)
            logger.debug(f"reason=propertyGetRounding,id={self._id},rounded={rounded_value},value={self._value}")
            return rounded_value
        else:
            return self._value

    def format(self) -> str:
        """
        Returns format of Property
        """
        return self._format

    def set_format(self, new_format: Optional[str]) -> None:
        """Set this property's ``$format`` (e.g. a dynamic enum/range that changes
        at runtime, such as an EVSE's advertised current range).

        ``$format`` lives inside the device ``$description``, so the change reaches
        the wire on the next ``$description`` republish. Call this inside a
        ``device.state_transition()`` so the batched INIT->READY republish carries
        it; the SDK does not republish a single property attribute on its own. This
        is the public replacement for adapters that previously assigned the private
        ``_format`` because no setter existed (SDK-6do.2).
        """
        self._format = new_format
        self._check_description_fields()

    def coerced_value(self) -> Optional[str]:
        """
        Returns the property's value (potentially rounded) as its Homie 5 payload
        text, or None if the value is None or the datatype cannot represent it
        (convention.md:78-150). A refusal is logged at warning.

        datetime/date values encode as ISO 8601, timedelta as ``PTxHxMxS``, a
        color tuple as the first ``$format`` color type plus its components, and a
        json value must be an array or object (a JSON string of one passes through
        unchanged). The ``0x00`` empty-string encoding is not applied here.
        """
        payload, problem = self._encode()
        if problem is not None:
            logger.warning(
                f"reason=propertyValueRefused,propertyId={self._id},datatype={self._datatype},"
                f"problem={problem},value={self._value!r}"
            )
        return payload

    def _encode(self) -> tuple:
        """``(payload, problem)`` for the current value; both None for a None value."""
        property_value = self.value()
        if property_value is None:
            return None, None
        return _encode_property_value(property_value, self._datatype, self._format)

    def id(self) -> str:
        """
        Returns the property's id
        """
        return self._id

    def name(self) -> str:
        """
        Returns the property's name
        """
        return self._name

    def datatype(self) -> str:
        """
        Returns the property's datatype.value
        """
        datatype = self._datatype
        logger.debug(f"reason=getDatatype,datatype={datatype}")
        return datatype

    def get_mqtt_client(self) -> Optional[MqttDeviceTransport]:
        """
        Who calls this function, and why?
        """
        node = self.node()
        if not node:
            logger.warning(f"reason=propertyGetMqttClientNoNode,propertyID={self._id}")
            return None
        mqttc = node.get_mqtt_client()
        if not mqttc:
            _log_missing_client(
                f"reason=propertyGetMqttClientNoMqttClient,propertyID={self._id}", by_design=self._transport_free()
            )
        return mqttc

    def start_mqtt_client(self) -> None:
        """
        Who calls this function, and why?
        """
        mqttc = self.get_mqtt_client()
        if not mqttc:
            _log_missing_client(
                f"reason=propertyStartMqttClientNoMqttClient,propertyID={self._id}", by_design=self._transport_free()
            )
            return
        # Never start a caller-owned client (bring-your-own-transport): mirror the
        # ownership guard on Device.start_mqtt_client(), and start via the concrete
        # owned handle (start() is owned-only, off the MqttDeviceTransport surface).
        # Resolve the root via node -> device -> root; an incomplete chain is a no-op.
        node = self.node()
        device = node.device() if node else None
        root = device.root() if device else None
        if root is None or not root._owns_client or root._owned_client is None:
            return
        try:
            if not root._owned_client.is_running:
                root._owned_client.start()
        except Exception as e:
            logger.warning(f"reason=propertyStartMqttClientException,e={e}")

    def settable(self) -> bool:
        return self._settable

    def set_settable(self, value: bool) -> None:
        """
        Update the settable attribute of this property.
        If setting to True, also subscribes to the /set topic.
        Note: Caller should republish the device description after calling this.
        """
        if self._settable == value:
            return  # No change
        self._settable = value
        if value:
            # Subscribe to the /set topic now that the property is settable
            self.set_subscribe()
        logger.info(f"reason=propertySetSettable,id={self._id},settable={self._settable}")

    def retained(self) -> bool:
        return self._retained

    def is_json_datatype(self) -> bool:
        return self._datatype == PropertyDatatype.JSON

    def get_set_callback(self) -> Callable:
        return self._set_callback

    def set_set_callback(self, callback: Callable) -> None:
        """Set the callback function for handling /set topic messages."""
        self._set_callback = callback

    def supports_target(self) -> bool:
        """
        Returns supports_target
        """
        return self._supports_target

    def publish_target_value(self, payload) -> None:
        """
        The $target attribute must either be used for every value update (including the initial one), or it must never be used.
        TODO: Currently unimplemented, TBD how $target gets set on initial property value set...
        Nothing in the SDK calls this; ``supports_target=True`` is inert.
        """
        logger.info(f"reason=propertyPublishTargetValue,propertyID={self._id},value={payload}")
        logger.warning(f"reason=propertyPublishTargetValueNotImplemented,propertyID={self._id},value={payload}")

    def publish_value(self, *, force: bool = False) -> bool:
        """
        Publishes the property's value to Homie/eBus broker.

        ``force=True`` bypasses the unchanged-payload skip described in
        ``set_value`` (GH #50). Every whole-tree republish forces --
        ``refresh_tree()`` -> ``publish_nodes()`` -> ``Node.publish()`` -> here, plus
        the structural republishes in ``Node.add_property()`` and
        ``Device.add_node()``. That is what repopulates a broker whose retained store
        is empty (one restarted without persistence, or a fresh one): a gated
        reconnect would find every payload equal to what it "last published", send
        nothing, and leave those topics empty until each value happened to change.
        """
        # Serialize compute-publish-memoize so the memo can never record an older
        # payload than the one that actually went out last (see _publish_lock).
        with self._publish_lock:
            mqttc = self.get_mqtt_client()
            # Gate on connectivity, not just the SDK-owned run flag. A bring-your-own-
            # transport client (mqttc=) is driven on the caller's loop and never has
            # is_running set by the SDK's start(), yet can still publish once connected.
            # is_running covers the owned path (True after start()); for an owned client
            # connected implies running, so this does not change owned behavior.
            if not mqttc or not (mqttc.is_running or mqttc.is_connected()):
                _log_missing_client(
                    f"reason=propertyPublishValueNoMqttClient,id={self._id}", by_design=self._transport_free()
                )
                return False
            node_id = self.get_node_id()
            device_id = self.get_device_id()
            if not (device_id and node_id):
                logger.warning(
                    f"propertyPublishValueInsufficientIDs,deviceID={device_id},nodeID={node_id},propertyID={self._id}"
                )
                return False
            # FIX: Don't publish if value is None and we've never published before or skip flag is set
            if self._value is None and (not self._ever_published or self._skip_initial_publish):
                logger.debug(f"reason=propertySkipPublishNoneValue,propertyID={self._id}")
                return True
            topic = f"{self._homie_domain()}/{EBUS_HOMIE_VERSION_MAJOR}/{device_id}/{node_id}/{self._id}"
            if self._value is None:
                # Value was cleared after having been published. Emit the empty
                # retained message so the prior retained value is retracted from the
                # broker rather than silently left behind (a reconnecting subscriber
                # would otherwise read the stale value). Reaching here implies
                # _ever_published is True and _skip_initial_publish is False — the
                # earlier guard returns for the never-published / skip-initial case.
                #
                # NOTE: this clears the topic (empty MQTT payload). It does NOT
                # represent an actual empty-string *value*, which the Homie 5
                # convention encodes as a 1-character 0x00 payload — see the module
                # header "empty string values" note. That encoding IS implemented,
                # below, via encode_empty_string(); the two payloads are distinct and
                # only the zero-length one retracts a retained topic.
                logger.debug(
                    f"reason=propertyPublishValueIsNoneClearing,deviceID={device_id},nodeID={node_id},propertyID={self._id}"
                )
                return self.clear_value()
            try:
                value = self.coerced_value()
                if value is None:
                    # Refused (logged by coerced_value): publish nothing rather than a
                    # payload the datatype does not allow (convention.md:78).
                    return False
                # Encode an empty-string value as a single 0x00 byte so the broker
                # does not mistake it for a zero-length "clear retained" payload. Only
                # for string (convention.md:65-67); another datatype never encodes to
                # "" because its encoder refuses an empty value. A missing datatype was
                # warned about at construction and keeps the string behavior.
                payload = encode_empty_string(value)
                # GH #50: skip a republish whose final wire payload is byte-identical to
                # the one already sitting on this topic. Compared AFTER coercion and
                # empty-string encoding, so the rounding/enum/JSON collapse is inside the
                # comparison and an empty-string value ("\x00") can never alias the
                # zero-length "clear retained" payload.
                #
                # RETAINED only. The broker stores nothing for an event property, so an
                # identical consecutive payload there is a second real event and dropping
                # it would lose information rather than save a redundant write.
                if not force and self.retained() and self._ever_published and self._last_published == (topic, payload):
                    logger.debug(f"reason=propertyPublishValueUnchanged,propertyID={self._id},topic={topic}")
                    return True
                logger.debug(f"reason=propertyPublishValue,value={value},topic={topic},retained={self.retained()}")
                # A non-retained (event) property publishes non-retained at QoS 0
                # (convention.md:50, :695): an event arrives now or not at all.
                qos = self._qos if self.retained() else 0
                mqttc.publish(topic, payload, retain=self.retained(), qos=qos)
                self._ever_published = True  # FIX: Mark as published
                # Memoize only after publish() returns, inside the try: a transport that
                # raises must not leave a memo claiming the broker holds a payload it
                # never received, which would suppress that value forever.
                self._last_published = (topic, payload)
                self._skip_initial_publish = False  # FIX: Clear skip flag after first publish
                return True
            except Exception as e:
                logger.warning(f"reason=propertyPublishValuePublishException,e={e}")
                return False

    def clear_value(self) -> bool:
        """
        Clear (retract) the property's retained value on the broker.

        Publishes a zero-length payload with retain=True, which MQTT treats as
        a "delete" instruction for the retained message on the topic — so a
        subscriber that connects afterwards receives no retained value rather
        than a stale one. This is the empty-retained convention referenced in
        the module header; it is also how ``set_value(None)`` clears a
        previously-published property (see ``publish_value``).

        This clears the topic; it does NOT publish an actual empty-string
        *value*, which the Homie 5 convention encodes as a 1-character 0x00
        payload and ``publish_value()`` emits via ``encode_empty_string()``.

        No-ops (returns True) if the property was never published, to avoid
        creating a phantom retained-empty topic. Returns True on success, else
        False.

        Also forgets the publish-on-change memo (GH #50), since the broker no longer
        holds what that memo claims: re-setting the pre-retraction value afterwards
        republishes rather than being skipped.
        """
        # Same lock as publish_value (reentrant: publish_value delegates here on
        # the retraction path), so the retract and the memo reset are atomic.
        with self._publish_lock:
            # FIX: Don't clear if we never published a value
            # This prevents creating phantom topics during cleanup
            if not self._ever_published:
                logger.info(f"reason=propertySkipClearNeverPublished,propertyID={self._id}")
                return True
            if not self.retained():
                # The broker stores nothing for an event property, so there is nothing
                # to retract; a retained empty message on an event topic would deliver
                # a zero-length "event" to every live subscriber instead.
                self._ever_published = False
                self._last_published = None
                return True

            mqttc = self.get_mqtt_client()
            # See publish_value: gate on connectivity so an injected (caller-driven)
            # client can retract a retained value even without the SDK's is_running.
            if not mqttc or not (mqttc.is_running or mqttc.is_connected()):
                _log_missing_client(
                    f"reason=propertyClearValueNoMqttClient,propertyID={self._id}", by_design=self._transport_free()
                )
                return False
            node_id = self.get_node_id()
            device_id = self.get_device_id()
            if not (device_id and node_id):
                logger.warning(
                    f"reason=propertyClearValueInsufficientIDs,deviceID={device_id},nodeID={node_id},propertyID={self._id}"
                )
                return False
            topic = f"{self._homie_domain()}/{EBUS_HOMIE_VERSION_MAJOR}/{device_id}/{node_id}/{self._id}"
            try:
                # Publishing empty string clears retained message
                mqttc.publish(topic, "", retain=True, qos=self._qos)
                logger.info(f"reason=propertyClearedValue,propertyID={self._id},topic={topic}")
                self._ever_published = False  # FIX: Reset the flag
                # The memo means "the broker holds this payload on this topic"; the
                # retraction above has just made that false (GH #50).
                self._last_published = None
                return True
            except Exception as e:
                logger.warning(f"reason=propertyClearValueException,propertyID={self._id},topic={topic},exception={e}")
                return False

    def was_ever_published(self) -> bool:
        """Return whether this property has ever been published to MQTT (FIX for MQTT topic persistence)"""
        return self._ever_published

    def invalidate_publish_cache(self) -> None:
        """Forget what this property last published (GH #50).

        The publish-on-change skip assumes the broker still holds the payload this
        property last sent. Anything that deletes that retained topic behind the
        property's back -- ``Device.delete_all_from_mqtt()``,
        ``Device.clear_retained_topic()`` aimed at a property topic, an operator
        wiping the broker -- must call this, or the next ``set_value()`` of that same
        value is skipped and the topic stays empty.

        Does NOT touch ``_ever_published``: this says "I no longer know what the
        broker holds", not "I have never published".
        """
        with self._publish_lock:
            self._last_published = None

    def get_last_published_value(self) -> Optional[str]:
        """Return the wire PAYLOAD this property last published, or None if it has
        published nothing since construction or since its last retraction.

        This is the post-coercion, post-encoding string that went to the broker (an
        empty-string value reads back as ``"\\x00"``), not the Python value -- use
        ``value()`` for that. Before 0.20.0 this returned the current value, which
        was a documented placeholder rather than a real record (GH #50).
        """
        # Read the tuple ONCE into a local. Testing the attribute and then subscripting
        # it would load it twice, and a concurrent clear_value() / invalidate_publish_
        # cache() nulling it in between would raise TypeError. The GIL makes that window
        # practically unreachable today; a free-threaded build removes that accident.
        memo = self._last_published
        return memo[1] if memo else None

    def description(self) -> dict:
        """
        Returns a dict containing the Homie 5 $description of the Property
        """
        logger.debug(f"reason=propertyDescriptionEntered,id={self._id}")
        property = dict()
        property["name"] = self._name
        if self._datatype is not None:
            # Required (convention.md:340); a missing datatype was warned about at
            # construction, and null is not a valid value for it.
            property["datatype"] = self.datatype()
        if self._format:
            fmt = self.format()
            # A json format is a JSONschema carried as a string, NOT a nested
            # object (convention.md:395).
            property["format"] = json.dumps(fmt) if isinstance(fmt, dict) else fmt
        if self._settable:
            property["settable"] = True
        if not self._retained:
            property["retained"] = False
        if self._unit:
            property["unit"] = self._unit
        return property

    def _settable_callback(self, topic: str, payload: Union[bytes, bytearray], retained: Optional[bool] = None) -> None:
        """
        For each settable property, there is a property/set topic that can be published to
        This is the callback for the subscription to each such property/set topic
        Examples:
        [homieDomain]/[homieVerson]/[deviceID]/[nodeID]/mode/set
        [homieDomain]/[homieVerion]/[deviceID]/[nodeID]/setpoint/set

        ``retained`` is the MQTT retain flag, delivered when the transport supports
        ``subscribe(..., with_retain=True)``. A retained ``/set`` is a stale command
        the broker replays at subscribe time (controllers publish ``/set``
        non-retained only, convention.md:48, :483), so it is ignored.

        The payload is validated for the datatype before the callback runs, and an
        invalid one is dropped with a ``reason=propertySetRejected`` warning: json
        must be an array or object (and match a ``$format`` JSONschema when one can
        be checked), integer and float must follow the payload grammar, a number is
        rounded to the format's step and then checked against min/max
        (convention.md:399), an enum must be in ``$format``, a boolean must be
        ``true`` or ``false``. The ``0x00`` empty-string decoding applies to string
        properties only.
        """
        logger.debug(f"reason=propertySetCallback,topic={topic}")
        try:
            topic_segments = topic.split("/")
            homie_domain = topic_segments[0]
            homie_version = topic_segments[1]
            _device_id = topic_segments[2]  # noqa: F841
            _node_id = topic_segments[3]  # noqa: F841
            property_id = topic_segments[4]
            property_id_set = topic_segments[5]
        except Exception as e:
            logger.warning(f"reason=nodeSetCallbackTopicParseException,e={e}")
            return
        if not (
            (homie_domain == self._homie_domain())
            and (homie_version == str(EBUS_HOMIE_VERSION_MAJOR))
            and (property_id_set == "set")
        ):
            logger.debug(f"reason=nodeSetCallbackInvalidTopic,topic={topic}")
            return
        if retained:
            logger.warning(f"reason=propertySetRetainedIgnored,propertyID={property_id},topic={topic}")
            return
        # It is possible that we have a valid property/set
        set_callback = self.get_set_callback()
        if not self.settable():
            logger.info(f"reason=propertySetCallbackPropertyNotSettable,propertyID={property_id}")
            return
        if not set_callback:
            logger.info(f"reason=propertySetCallbackPropertyNoSetCallback,propertyID={property_id}")
            return
        try:
            decoded_payload = payload.decode("utf-8")
            value, problem = _parse_set_payload(decoded_payload, self._datatype, self._format, self._value)
            if problem is None and self.is_json_datatype():
                # Validate the decoded command against the property's $format
                # JSONSchema (its advertised control surface). Graceful: skipped if
                # there is no $format or the jsonschema package is not installed.
                error = validate_json_format(value, self._format)
                if error is not None:
                    problem = f"schemaInvalid:{error}"
            if problem is not None:
                logger.warning(
                    f"reason=propertySetRejected,propertyID={property_id},datatype={self._datatype},"
                    f"problem={problem},payload={decoded_payload!r}"
                )
                return
            payload = value
            # We have the payload
            logger.debug(
                f"reason=propertySetCallbackValue,propertyID={property_id},payload={payload},callback={set_callback}"
            )
            # Call the property's set_callback function
            # Run the callback: a sync callback runs inline here (on the transport's
            # network thread, as before); an async (coroutine) callback is scheduled onto
            # the consumer's event loop thread-safely via run_coroutine_threadsafe
            # (ensure_future is NOT safe to call from a thread other than the loop's own).
            # Decide on the callback's actual return, not just async_loop's presence, so a
            # sync callback stays inline even when a device-level loop is set for the tree.
            result = set_callback(payload)
            if self.async_loop is not None and asyncio.iscoroutine(result):
                future = asyncio.run_coroutine_threadsafe(result, self.async_loop)
                # The Future is otherwise discarded, so an exception raised inside the
                # coroutine would vanish silently (the inline path is caught below by the
                # surrounding try/except). Surface it, matching the sync path's logging.
                future.add_done_callback(partial(self._log_async_set_result, property_id=property_id))
        except Exception as e:
            logger.exception(f"reason=propertySetCallbackException,e={e}")

    def _log_async_set_result(self, future, property_id) -> None:
        """Done-callback for an async /set handler scheduled via run_coroutine_threadsafe.

        The scheduling Future is otherwise discarded, and a discarded concurrent.futures
        Future swallows a stored exception silently (unlike an asyncio.Task). Surface it,
        matching the synchronous path's ``propertySetCallbackException`` logging.
        """
        if future.cancelled():
            return
        exc = future.exception()
        if exc is not None:
            logger.error(f"reason=propertySetAsyncCallbackException,propertyID={property_id},exc={exc!r}")

    def set_subscribe(self) -> None:
        """
        Subscribe to property/set topic on Homie broker
        TODO: Not sure why this is a public method...
        """
        logger.debug(f"reason=propertySetSubscribe,id={self._id}")
        mqttc = self.get_mqtt_client()
        if not mqttc:
            _log_missing_client("reason=propertySetSubscribeNoMqttClient", by_design=self._transport_free())
            return
        if not self.settable():
            logger.debug(f"reason=propertySetSubscribePropertyNotSettable,id={self._id}")
            return
        # Property is settable
        node_id = self.get_node_id()
        device_id = self.get_device_id()
        if not (device_id and node_id):
            logger.warning(
                f"propertySetSubscribeInsufficientIDs,deviceID={device_id},nodeID={node_id},propertyID={self._id}"
            )
            return
        topic = f"{self._homie_domain()}/{EBUS_HOMIE_VERSION_MAJOR}/{device_id}/{node_id}/{self._id}/set"
        try:
            if _subscribe_supports_with_retain(mqttc):
                # Deliver the retain flag so a stale retained /set is ignored.
                mqttc.subscribe(topic, param=partial(self._settable_callback), qos=self._qos, with_retain=True)
            else:
                logger.debug(f"reason=propertySetSubscribeNoRetainFlag,id={self._id},hint=retained /set not detected")
                mqttc.subscribe(topic, param=partial(self._settable_callback), qos=self._qos)
        except Exception as e:
            logger.warning(f"reason=propertySetSubscribeSubscribeException,e={e}")
        # Start the MQTT client loop() thread
        # TODO: Is this the best, or even a good, place to do this???
        # self.start_mqtt_client()


class Node:
    """
    Object representing a Homie MQTT Node
    https://homieiot.github.io/specification/
    """

    def __init__(
        self,
        id: Optional[str] = None,
        name: Optional[str] = None,
        type: Optional[str] = None,
        properties: Optional[dict] = None,
        device: Optional[Device] = None,
        # mqttc: Optiona[MqttClient] = None, # DCJ pretty sure we can remove this
        from_dict: Optional[dict] = None,
    ):
        """
        There are two ways to specify the arguments of a new Node:
          1. Explict named parameters
          2. Provide a dict whose keys are the parameter names
        These are mutually exclusive choices, if you specify from_dict, the parameters with are used
        """
        if from_dict:
            # Instantiating Node from dict
            self._id = from_dict.get("id", None)
            self._name = from_dict.get("name", self._id)
            self._type = from_dict.get("type", None)
            self._properties = from_dict.get("properties", {})
            self._device = from_dict.get("device", None)
        else:
            self._id = id
            if name:
                self._name = name
            else:
                self._name = id
            self._type = type
            self._properties = properties if properties is not None else {}
            self._device = device

    def as_dict(self) -> dict:
        returned_dict = {"id": self.id(), "name": self.name(), "type": self.type()}
        properties_dict = {}
        for id, property in self.properties().items():
            properties_dict.update({id: property.as_dict()})
        returned_dict.update({"properties": properties_dict})
        return returned_dict

    def id(self) -> str:
        """
        Returns id of Node
        """
        return self._id

    def name(self) -> str:
        """
        Returns name of Node
        """
        return self._name

    def type(self) -> str:
        """
        Returns type of Node
        """
        return self._type

    def get_device_id(self) -> str:
        """
        Why is this a thing?
        """
        return self._device.id()

    def device(self) -> Device:
        return self._device

    def _transport_free(self) -> bool:
        """See ``Device._transport_free``. An incomplete chain falls through as *not*
        transport-free so a half-built tree stays loud rather than going quiet."""
        device = self.device()
        return device._transport_free() if device is not None else False

    def set_device(self, device: Device) -> None:
        self._device = device

    def get_mqtt_client(self) -> Optional[MqttDeviceTransport]:
        device = self.device()
        if not device:
            logger.warning(f"reason=nodeGetMqttClientNoDevice,nodeID={self._id}")
            return None
        mqttc = device.get_mqtt_client()
        if not mqttc:
            _log_missing_client(
                f"reason=nodeGetMqttClientNoMqttClient,nodeID={self._id}", by_design=self._transport_free()
            )
        return mqttc

    def add_property(self, property: Property) -> Property:
        """
        Adds the property to properties, and returns property
        """
        if not property.node():
            property.set_node(self)
        # Propagate QoS (and the async /set dispatch loop, if set) from the device.
        if self._device and hasattr(self._device, "_qos"):
            property._qos = self._device._qos
        if self._device and getattr(self._device, "_async_loop", None) is not None:
            property.async_loop = self._device._async_loop
        # Note set_subscribe() checks if property is settable...
        property.set_subscribe()
        # Outside a transition a ready device goes to init first, so the value lands
        # before the $description that names it and before ready (convention.md:279).
        # A node not yet attached to its device, or a re-declaration with identical
        # content, leaves the $description unchanged and publishes only the value.
        self._properties.update({property.id(): property})
        with self.device()._reconfiguration():
            # force: announcing a property is a structural republish, so its value must
            # land regardless of the GH #50 skip. A fresh property would pass the gate
            # anyway (it has never published), and so would one re-added after
            # delete_property() (clear_value() resets both gate conjuncts). What force
            # actually covers is the property whose retained topic was deleted behind its
            # back -- clear_retained_topic(), or an operator wiping the broker -- where
            # the memo still claims the broker holds this payload and it does not.
            # An event property is not replayed by a structural change (:695).
            if property.retained():
                property.publish_value(force=True)
        return property

    def add_property_from_dict(self, property_dict: dict) -> Property:
        """
        Adds the property to properties, and returns property
        """
        return self.add_property(Property(from_dict=property_dict))

    def properties(self) -> dict:
        """
        Returns dict of Node's properties keyed by propertyID
        """
        return self._properties

    def get_properties(self) -> dict:
        """
        Returns dict of Node's properties keyed by propertyID
        """
        return self.properties()

    def get_property(self, property_id: str) -> Optional[Property]:
        """Safe getter for a property"""
        return self._properties.get(property_id, None)

    def delete_property(self, property_id: str) -> bool:
        """
        Remove property, clear its MQTT topic, and republish $description.

        The mirror of add_property(): both mutate the node's property set, so
        both must re-announce it. Without the republish the broker kept a device
        in `ready` whose $description still named a property that no longer
        existed, and nothing ever corrected it.

        Batching several deletions inside `device.state_transition()` collapses
        the republishes to one, exactly as it does for additions.

        Returns True if removed, False if not found.
        """
        if property_id not in self._properties:
            logger.warning(f"reason=nodeDeletePropertyNotFound,nodeId={self._id},propertyId={property_id}")
            return False
        property = self._properties[property_id]
        device = self.device()
        if device:
            # The description that drops the property is published when the
            # reconfiguration scope closes, after the removal.
            del self._properties[property_id]
            with device._reconfiguration():
                property.clear_value()
        else:
            property.clear_value()
            del self._properties[property_id]
        logger.info(f"reason=nodeDeletedProperty,nodeId={self._id},propertyId={property_id}")
        return True

    def clear_all_properties(self) -> None:
        """Remove all properties (for node deletion)"""
        # FIX: Track which properties were cleared vs skipped
        cleared_count = 0
        skipped_count = 0

        for property_id, property in list(self._properties.items()):
            # FIX: Only clear properties that were actually published
            if hasattr(property, "was_ever_published") and property.was_ever_published():
                property.clear_value()
                cleared_count += 1
            elif hasattr(property, "_ever_published") and property._ever_published:
                property.clear_value()
                cleared_count += 1
            else:
                skipped_count += 1

        self._properties.clear()
        # FIX: Enhanced logging with counts
        logger.info(
            f"reason=nodeClearedAllProperties,nodeId={self._id},cleared={cleared_count},skipped={skipped_count}"
        )

    def description(self) -> dict:
        """
        Returns dict representing the Node's $description attribute
        """
        logger.debug(f"reason=nodeDescriptionEntered,id={self._id}")
        description = dict()
        description["name"] = self._name
        if self._type is not None:
            # Optional and not nullable (convention.md:305): omitted when unset.
            description["type"] = self._type
        properties = dict()
        properties_snapshot = dict(self._properties)
        for property_id, attributes in properties_snapshot.items():
            properties[property_id] = attributes.description()
        description["properties"] = properties
        return description

    def publish(self, *, force: bool = True) -> None:
        """
        Publishes Node, specifically its Properties to MQTT.

        ``force`` defaults to True because this is a republish walk: it exists to put
        the node's whole property set on the broker, so the GH #50 unchanged-payload
        skip must not suppress it. Pass ``force=False`` for the gated behavior of the
        ordinary value path.
        """
        node_id = self.id()
        property_count = len(self._properties)
        logger.debug(f"reason=nodePublish,nodeId={node_id},propertyCount={property_count}")
        # Use list() to create a shallow copy, preventing crash if dict changes during iteration
        for property_id, property in list(self._properties.items()):
            if not property.retained():
                # A republish walk restores the broker's retained store. An event
                # property has nothing there, and replaying its last event would
                # report it again (convention.md:695).
                continue
            logger.debug(f"reason=nodePublishProperty,nodeId={node_id},propertyId={property_id}")
            # Best-effort per property. Property.publish_value() reaches the MQTT
            # client directly and does not wrap it, so an injected transport that
            # raises on one property would otherwise abort this node's remaining
            # properties, its device's $state, and (via refresh_tree) the entire
            # rest of the tree's reconnect. See Device.refresh_tree().
            try:
                property.publish_value(force=force)
            except Exception as e:
                logger.exception(f"reason=nodePublishPropertyFailed,nodeId={node_id},propertyId={property_id},e={e}")


class StateTransitionContext:
    """
    Context manager for Homie device state transitions.

    Ensures the device state is set to INIT at the start and READY at the end,
    even if an exception occurs during the transition.

    Usage:
        with device.state_transition():
            # Add/remove nodes, modify schema
            device.add_node(...)
            device.delete_node(...)
        # State is automatically set to READY here
    """

    def __init__(self, device: "Device"):
        self.device = device

    def __enter__(self):
        self.device._begin_state_transition()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Always end the state transition, even if an exception occurred
        try:
            self.device._end_state_transition()
        except Exception as e:
            logger.warning(f"reason=stateTransitionContextExitException,deviceId={self.device._id},e={e}")
        # Return False to let any exception from the with block propagate to the caller
        # (returning True would suppress it)
        return False


def _dispatch_disconnect(callback: Optional[Callable[[bool], None]], rc, source: str) -> None:
    """Best-effort invoke a consumer ``on_disconnect(clean: bool)`` callback.

    The transport (paho) reason code ``rc`` is normalized HERE to an SDK-owned
    boolean and never surfaced, so a paho type or its value semantics do not
    leak into an eBus consumer (paho is a hidden transitive dependency behind
    ebus-mqtt-client). ``rc == 0`` (paho ``MQTT_ERR_SUCCESS``) is a clean,
    orderly disconnect; anything else is an unexpected drop. ``getattr(rc,
    "value", rc)`` also accepts a paho v5 ``ReasonCode`` (via its ``.value``)
    should the transport ever forward one, without changing this contract. A
    consumer exception is logged and swallowed so it can never disrupt the MQTT
    network loop (the client also guards on its side).
    """
    if callback is None:
        return
    clean = getattr(rc, "value", rc) == 0
    try:
        callback(clean)
    except Exception:
        logger.exception(f"reason=onDisconnectCallbackException,source={source}")


def _drop_connection_without_disconnect(client: Any, *, timeout: float) -> None:
    """End an owned client's connection WITHOUT an MQTT DISCONNECT packet.

    The broker then treats the connection as badly disconnected and publishes the
    Last Will. ``Device.stop(announce=False)`` uses this so a teardown that announced
    nothing is a bad disconnect (convention.md:281, :284) rather than a clean one.

    ebus-mqtt-client has no unclean-stop API, so this stops the paho network loop
    (which sends nothing) and shuts the socket down underneath it. The client's own
    ``stop()`` runs afterwards to release its resources; its DISCONNECT then has no
    live connection to reach. Bounded by ``timeout`` like ``MqttClient.stop()``.
    """
    paho = getattr(client, "mqttc", None)
    if paho is None or not callable(getattr(paho, "socket", None)):
        logger.warning("reason=deviceDropConnectionUnsupported,hint=the will may not fire")
        return

    def _drop() -> None:
        with contextlib.suppress(Exception):
            paho.loop_stop()
        sock = paho.socket()
        if sock is None:
            return
        with contextlib.suppress(Exception):
            sock.shutdown(socket.SHUT_RDWR)
        with contextlib.suppress(Exception):
            sock.close()

    worker = threading.Thread(target=_drop, name="ebus-sdk-drop-connection", daemon=True)
    worker.start()
    worker.join(timeout)
    if worker.is_alive():
        logger.warning(f"reason=deviceDropConnectionTimeout,timeout={timeout}")


class Device:
    """
    Object representing a Homie MQTT Device
    https://homieiot.github.io/specification/

    A Device is either a *root* (owns the MQTT connection and LWT) or a *child*
    (borrows its root's connection). The distinction is set at construction:

        # Root device
        panel = Device(id="panel-1", type="...", mqtt_cfg={...})

        # Child device (any depth — children may themselves have children)
        circuit = Device(id="circuit-1", type="...", parent=panel)

    Children share the root's single MQTT connection. Only the root registers
    a Last Will so the entire tree is marked lost on adapter death.

    mqtt_cfg is a dict, two examples:

        {"host": "127.0.0.1",
         "port": 1885,
         "homie_domains": ["ebus"]}

        {"host": "mqtt.example.com",
         "port": 1883,
         "homie_domains": ["ebus"],
         "authentication": {"type": "USER_PASS",
                            "username": "MyUserName",
                            "password": "SECRET"}}

    The ``homie_domains`` key in the broker config is not read by this class. To
    publish a tree under a domain other than ``ebus``, pass ``homie_domain=`` to
    the ROOT Device; see ``homie_domain()``.

    mqtt_cfg={} connects using ebus-mqtt-client's defaults. mqtt_cfg=None opens no socket:
    the tree still composes $description and resolves ids and topics, it just never
    publishes — for tests, schema derivation, and hosts that publish through their own
    client.

    mqttc= injects a client you already own (bring-your-own-transport): the SDK
    uses it as-is and never starts or stops it, so a host that owns its MQTT
    connection (e.g. a Home Assistant integration driving MQTT on its own event
    loop) can publish an eBus tree through it. Root-only, and mutually exclusive
    with mqtt_cfg=. An injected client bypasses the SDK's connect path, so the
    caller wires the two pieces that ride it: set will() on the client before
    connecting, and call refresh_tree() from the client's on-connect handler.
    stop() publishes a final $state=disconnected through the client but does not
    flush or close it. on_disconnect= is inert for an injected client (its handler
    is wired only on an SDK-owned client): register disconnect handling on your own
    client.

        panel = Device(id="panel-1", type="...")
        circuit = Device(id="circuit-1", type="...", parent=panel)
    """

    def __init__(
        self,
        id: str,
        name: Optional[str] = None,
        type: Optional[str] = None,
        parent: Optional["Device"] = None,
        nodes: Optional[List] = None,
        extensions: Optional[List] = None,
        description_extras: Optional[dict] = None,
        mqtt_cfg: Optional[dict] = None,
        mqttc: Optional[MqttDeviceTransport] = None,
        homie_domain: Optional[str] = None,
        qos: int = EBUS_HOMIE_MQTT_QOS,
        async_loop: Optional[asyncio.AbstractEventLoop] = None,
        on_disconnect: Optional[Callable[[bool], None]] = None,
    ):
        # Root vs. child invariants — mutually exclusive. Test presence by identity
        # (is not None) throughout: mqtt_cfg={} is a real "connect on defaults" value
        # a child must still be refused, not silently dropped by a truthiness check.
        if parent is not None and mqtt_cfg is not None:
            raise ValueError(
                f"Device id={id}: cannot pass both parent= and mqtt_cfg=; children share the root's MQTT connection"
            )
        if parent is not None and mqttc is not None:
            raise ValueError(
                f"Device id={id}: cannot pass both parent= and mqttc=; children share the root's MQTT connection"
            )
        # A device id that collides with an ancestor's is always a mistake, and a
        # silent one: Device.__init__ appends to parent._children with no check, so
        # a child carrying the root's id makes the root name itself as its own
        # child in $description, and both publish to the same topics. Refuse it at
        # construction, where the caller can still see which id was wrong.
        if parent is not None:
            ancestor: Optional[Device] = parent
            while ancestor is not None:
                if id == ancestor.id():
                    raise ValueError(
                        f"Device id={id}: a child cannot carry the same id as its "
                        f"{'parent' if ancestor is parent else 'ancestor'}; both would publish to the "
                        "same topics and the ancestor would name itself in its own children"
                    )
                ancestor = ancestor.parent()
            # Same defect one step sideways: two children of one parent sharing an
            # id derive the same base topic, so their $description publishes
            # overwrite each other on the broker and the parent names the child
            # twice in its own `children`. The parent already tracks its children,
            # so this costs no new state.
            if any(child.id() == id for child in parent.children()):
                raise ValueError(
                    f"Device id={id}: parent id={parent.id()} already has a child with this id; "
                    "both would publish to the same topics and the parent would name it twice"
                )

        # The domain is a per-TREE property, like the connection and the QoS: one
        # tree publishes under one prefix, and a child under a different domain
        # would sit outside its own root's subtree. Refuse it on a child rather
        # than silently ignoring it, matching the mqtt_cfg/mqttc rule above.
        if parent is not None and homie_domain is not None:
            raise ValueError(
                f"Device id={id}: cannot pass both parent= and homie_domain=; a tree shares one domain, "
                "set it on the root"
            )
        if mqtt_cfg is not None and mqttc is not None:
            raise ValueError(
                f"Device id={id}: cannot pass both mqtt_cfg= and mqttc=; pass mqtt_cfg to have the SDK "
                "build and own a client, or mqttc to inject one whose lifecycle you own"
            )
        # The `_mqtt_cfg` term separates "root never started" (an error) from "root built
        # transport-free" (mqtt_cfg=None), which must still take children. Drop it and the
        # transport-free tree can only ever be a single node.
        if parent is not None and parent.root().mqttc is None and parent.root()._mqtt_cfg is not None:
            raise RuntimeError(
                f"Device id={id}: parent's tree (root id={parent.root().id()}) has no MQTT client; "
                "construct and start the root before attaching children"
            )

        # Basic initialization. An injected client (mqttc=) is used as-is and its
        # lifecycle stays the caller's; _owns_client=False then gates start()/stop()
        # so the SDK never starts or closes a client it was handed (mirrors
        # Controller's bring-your-own-transport seam). mqttc=None is the owned path
        # (the SDK builds the client from mqtt_cfg) or transport-free (mqtt_cfg=None,
        # no socket). Only the root holds a client; children read root._owns_client.
        self.mqttc: Optional[MqttDeviceTransport] = mqttc
        self._owns_client = mqttc is None
        # The SDK-constructed client, kept as its concrete type so start() / stop() /
        # publish_and_flush() -- which exist only on a client we own -- stay callable.
        # Stays None for an injected client, which makes "never started, never stopped"
        # a property of the types rather than a promise in a comment (mirrors Controller).
        self._owned_client: Optional[MqttClient] = None
        self._state = None
        self._qos = qos
        # The consumer's asyncio event loop for dispatching inbound /set callbacks on
        # settable properties. Set once per tree on the root and propagated to every
        # property via add_node() / Node.add_property() (like _qos), rather than
        # per-property. Children inherit the root's loop unless given their own. None
        # means /set callbacks run synchronously on the transport's network thread.
        self._async_loop = (
            async_loop if async_loop is not None else (parent._async_loop if parent is not None else None)
        )
        # Optional consumer hook: called on the ROOT's MQTT (dis)connect. Only a
        # root owns a client (children share it), so it fires on the root only.
        # Contract is transport-neutral: on_disconnect(clean: bool), never a paho
        # reason code (SDK-al5). Must be set before connect_broker() below.
        self._on_disconnect = on_disconnect
        if mqttc is not None and on_disconnect is not None:
            # An injected client bypasses connect_broker(), the only place the SDK
            # registers its disconnect handler, so on_disconnect never fires for a
            # bring-your-own-transport client (the same limitation Controller
            # documents). Accept it rather than raise, but warn: the caller must
            # register disconnect handling on their own client.
            logger.warning(f"reason=deviceInjectedClientOnDisconnectInert,id={id}")
        self._id = id
        # Only a root carries the domain; descendants read the root's via
        # homie_domain(). Defaults to the eBus domain, so a publisher that never
        # mentions it is unaffected.
        self._homie_domain = (homie_domain or EBUS_HOMIE_DOMAIN) if parent is None else None
        self._name = name if name else id
        self._type = type
        self._parent: Optional[Device] = parent
        self._children: List[Device] = []
        self._mqtt_cfg = mqtt_cfg if parent is None else None
        self._nodes = {}
        # Copy into fresh lists so callers never share a mutable default across
        # Device instances (and a later append can't leak between devices).
        self._extensions = list(extensions) if extensions else []
        # Extra top-level fields merged into the $description JSON document.
        # Used to carry extension-defined device attributes (e.g. the
        # `imported-from` attribute of the `energy.ebus.imported` extension).
        # Homie 5 forward-compat requires controllers to ignore unknown
        # description fields (convention §Forward compatibility), so these are
        # safe; core fields always take precedence over an extra of the same key.
        self._description_extras = dict(description_extras) if description_extras else {}
        for key in sorted(self._description_extras.keys() & _DESCRIPTION_CORE_FIELDS):
            # A core field is the SDK's to compute: an extra could otherwise add
            # `root`/`parent` to a root device (convention.md:220) or a `type` the
            # SDK omits. Dropped, with a warning per the invalid-input policy.
            del self._description_extras[key]
            _warn_invalid_once(
                self, f"extrasCoreField:{key}", f"reason=deviceDescriptionExtrasCoreFieldDropped,id={id},field={key}"
            )
        # Counter of how many state_transition() / delete() scopes are currently active
        # on this device. >0 means "a transition is in progress" — suppresses child-induced
        # parent flaps and makes nested state_transition()s reentrant (only the outermost
        # entry/exit publishes INIT/READY). Init→ready transitions force every controller
        # in the wild to resync, so emitting only the minimum is a correctness concern.
        self._transition_depth = 0
        # Set by delete(): the device no longer exists on the broker, so nothing may
        # republish its $state (stop(), a reconnect's refresh_tree(), the will).
        self._deleted = False
        # Alerts this device has raised and not cleared: alert id -> message.
        # Tracked so a reconnect republishes them and delete() clears them.
        self._alerts: dict = {}
        # SDK-n83: hash of the last $description we actually published, with
        # `version` removed. publish_description() uses it
        # to skip a republish whose content has not changed (saves the ~KB
        # payload and the gratuitous INIT→READY flap). Maintained in publish().
        self._last_description_content_hash = None
        # Distinguish between initial and subsequent connections to broker
        self.initial_broker_connection = True
        if parent is None:
            # `is not None`, not truthiness: `{}` still connects on defaults, `None` means
            # no transport. `if mqtt_cfg:` would fold the two together.
            if mqtt_cfg is not None:
                self.connect_broker()
        else:
            parent._children.append(self)
        # Child's own INIT → publish description+nodes → READY (Homie add-child steps 1-3).
        with self.state_transition():
            for node in nodes or []:
                self.add_node(node)
        # Homie add-child steps 4-6: parent INIT → publish description (now includes self in
        # `children`) → READY. Skipped if parent is mid-transition — in that case the parent's
        # own state_transition will publish the updated description on exit (S1: 32 adds → 1 flap).
        if parent is not None:
            parent._notify_structural_change()

    def as_dict(self) -> dict:
        nodes = {}
        for node_id, node in self.nodes().items():
            nodes[node_id] = node.as_dict()
        return {
            "id": self.id(),
            "name": self.name(),
            "type": self.type(),
            "children_ids": self.children_ids(),
            "parent_id": self.parent_id(),
            "root_id": self.root_id(),
            "extensions": self.extensions(),
            "nodes": nodes,
        }

    def root(self) -> "Device":
        """
        Return the root Device of this tree. For a root device, returns self.
        For a child or grandchild, walks self._parent up to the top.
        """
        return self if self._parent is None else self._parent.root()

    def _transport_free(self) -> bool:
        """True when this tree was deliberately built without transport.

        The root holds no client and was given no config to build one from, so "no client"
        is the requested state rather than a fault: the tree is serving as a naming and
        structure model for topic derivation, ``$description``, or tests. A root that was
        handed a client or given a config is the opposite case — there a missing client is
        an anomaly and stays a warning.
        """
        root = self.root()
        return root.mqttc is None and root._mqtt_cfg is None

    @staticmethod
    def now_ems() -> int:
        """
        Returns current time as Epoch milliseconds
        """
        return round(time.time() * 1000)

    def id(self) -> str:
        """
        Returns id of Device
        """
        return self._id

    def name(self) -> str:
        """
        Returns name of Device
        """
        return self._name

    def type(self) -> str:
        """
        Returns type of Device
        """
        return self._type

    def state(self) -> DeviceState:
        """
        Returns state of Device, a DeviceState
        """
        return self._state

    def root_id(self) -> Optional[str]:
        """
        Returns root_id of Device, or None if self is the root.
        """
        return None if self._parent is None else self.root().id()

    def parent_id(self) -> Optional[str]:
        """
        Returns parent_id of Device, or None if self is the root.
        """
        return None if self._parent is None else self._parent.id()

    def parent(self) -> Optional["Device"]:
        """
        Returns the parent Device, or None if self is the root.
        """
        return self._parent

    def children(self) -> List["Device"]:
        """
        Returns list of child Device references (live objects, not IDs).
        """
        return list(self._children)

    def children_ids(self) -> List[str]:
        """
        Returns list of Device's children's IDs (computed from live refs).
        """
        return [child.id() for child in self._children]

    def extensions(self) -> List:
        """
        Returns list of Device's extensions
        """
        return self._extensions

    def homie_domain(self) -> str:
        """The Homie domain (topic prefix) this device's TREE publishes under.

        Defaults to ``ebus``, which the eBus specification mandates for energy
        devices. A publisher that also speaks for non-energy home-automation
        devices can put a tree under the standard ``homie`` domain, or any
        other, by passing ``homie_domain=`` to the ROOT device; every topic the
        tree derives follows, including the Last Will.

        Per-tree, never per-device: a child inherits its root's domain and is
        refused its own, the same way it is refused its own connection.
        """
        return self.root()._homie_domain or EBUS_HOMIE_DOMAIN

    @property
    def qos(self) -> int:
        """Returns the MQTT QoS level for this device"""
        return self._qos

    def nodes(self) -> dict:
        """
        Returns a dict Device's Nodes, keyed by Node-ID
        """
        return self._nodes

    def get_mqtt_client(self) -> Optional[MqttDeviceTransport]:
        """
        Return the MQTT client for this device's tree.
        For root devices, returns self.mqttc. For children, ascends to root.
        """
        mqttc = self.root().mqttc
        if not mqttc:
            _log_missing_client(
                f"reason=deviceGetMqttClientNoMqttClient,id={self._id}", by_design=self._transport_free()
            )
        return mqttc

    def start_mqtt_client(self) -> None:
        """
        Start the root device's MQTT client loop. Children share the root's
        connection — calling start_mqtt_client() on a child starts the root's.
        """
        root = self.root()
        if root.mqttc is None:
            _log_missing_client(
                f"reason=deviceStartMqttClientNoMqttClient,id={self._id}", by_design=self._transport_free()
            )
            return
        if not root._owns_client:
            # Bring-your-own-transport: the caller owns the client's lifecycle, so
            # the SDK never starts it (it is expected to be connected already).
            return
        # Owned path: start via the concrete handle (start() is owned-only, off the
        # MqttDeviceTransport surface). _owned_client is set whenever _owns_client is True.
        if root._owned_client is not None and not root._owned_client.is_running:
            root._owned_client.start()

    def is_connected(self) -> bool:
        """
        True when this device tree's MQTT link is up.

        Because ebus-mqtt-client connects asynchronously (connect_async on the
        network loop started by start_mqtt_client()), the link is not up the
        instant a Device is constructed: is_connected() returns False between
        construction and the first successful connect (and while disconnected
        between reconnect attempts), then True once connected. Children share the
        root's connection, so for any device in the tree this reflects the root's
        link. A consumer that must not publish before the link is up can gate on
        this; publishing anyway is safe — values are retained and on_connect()
        republishes the whole tree once the link comes up.
        """
        mqttc = self.root().mqttc
        return bool(mqttc and mqttc.is_connected())

    def stop(self, *, announce: bool = True, flush_timeout: float = 1.0, stop_timeout: float = 2.0) -> None:
        """Gracefully and promptly tear down this device tree's MQTT connection.

        Publishes a final ``$state=disconnected`` for EVERY device in the tree
        (best-effort), descendants first and the root last, then stops the root's
        MQTT client with a clean disconnect. Homie 5 requires a device to send
        ``disconnected`` before it cleanly disconnects (convention.md:280-281), and
        children share the root's connection, so each of them disconnects too; the
        root-to-child cascade (convention.md:262-267) covers only ``lost``. This is
        a TREE-level teardown: a call on any device stops the whole tree.

        BOUNDED end to end (at most ~``flush_timeout`` + ``stop_timeout``): if the
        broker is unreachable the disconnected publishes are skipped and ``stop()``
        still returns promptly, so a shutting-down process never stalls on a dead
        broker. The root's publish is the one flushed; it is sent last, so the
        children's publishes precede it on the wire. Because ``MqttClient.stop()``
        performs a clean disconnect (which suppresses the LWT), publishing
        ``disconnected`` first is what lets consumers see a clean shutdown rather
        than a stale ``ready``. After ``stop()`` this device tree should not be
        reused.

        For a bring-your-own-transport root (``mqttc=``) the SDK does not own the
        client: ``stop()`` publishes the same ``$state=disconnected`` messages as
        plain retained messages and returns immediately, without flushing and
        without closing the client, so it never blocks the caller's loop. The
        caller stops and disconnects its own client. ``flush_timeout`` and
        ``stop_timeout`` apply to the owned path only.

        ``announce=False`` publishes nothing and ends the connection WITHOUT a
        clean MQTT DISCONNECT, so the broker delivers the Last Will (the root's
        ``$state=lost``). Homie 5 forbids a clean disconnect without a preceding
        ``disconnected`` (convention.md:281) and defines ``lost`` as a bad
        disconnect (convention.md:284), so a silent teardown is a bad disconnect.
        Pair it with ``declare_lost()`` for a producer that is dying rather than
        shutting down: that publishes ``lost`` while the link is up, and the will
        then re-asserts the same retained payload. On a bring-your-own transport
        the SDK closes nothing; the caller must drop its connection without a clean
        DISCONNECT for the same guarantee.

        After ``delete()`` on the root, ``stop()`` announces nothing in either mode
        and always disconnects cleanly: the device no longer exists
        (convention.md:288), and a ``disconnected`` or a will-driven ``lost`` would
        re-create it as a bare ``$state`` with no ``$description``.
        """
        root = self.root()
        mqttc = root.mqttc
        if mqttc is None:
            _log_missing_client(f"reason=deviceStopNoMqttClient,id={self._id}", by_design=self._transport_free())
            return
        owned = root._owns_client and root._owned_client is not None
        if root._deleted:
            logger.info(f"reason=deviceStopDeletedTreeSilent,id={root._id}")
            if owned and mqttc.is_connected():
                # Nothing to announce, but delete()'s retained clears may still be in
                # flight. Re-clearing the root's $state (a no-op deletion) is flushed,
                # and with it everything queued before it, ahead of the disconnect.
                state_topic = f"{root.homie_domain()}/{EBUS_HOMIE_VERSION_MAJOR}/{root._id}/$state"
                root._owned_client.publish_and_flush(state_topic, "", qos=root._qos, retain=True, timeout=flush_timeout)
        elif not announce:
            logger.info(f"reason=deviceStopSilent,id={root._id}")
            if owned:
                # No disconnected was sent, so the disconnect must not be clean: drop
                # the connection and let the broker publish the will.
                _drop_connection_without_disconnect(root._owned_client, timeout=stop_timeout)
        elif mqttc.is_connected():
            # Best-effort graceful $state=disconnected for the whole tree. The state
            # moves live inside this branch, so announce=False cannot overwrite a
            # $state a caller just declared.
            for device in root._tree_post_order():
                device._state = DeviceState.DISCONNECTED
                state_topic = f"{root.homie_domain()}/{EBUS_HOMIE_VERSION_MAJOR}/{device._id}/$state"
                if device is root and owned:
                    # publish_and_flush is bounded and returns False (never blocks or
                    # raises) when the broker is unreachable, so this cannot stall
                    # shutdown.
                    flushed = root._owned_client.publish_and_flush(
                        state_topic, DeviceState.DISCONNECTED.value, qos=root._qos, retain=True, timeout=flush_timeout
                    )
                    logger.info(f"reason=deviceStopDisconnectedPublished,id={root._id},flushed={flushed}")
                    continue
                # A descendant, or any device on an injected transport: a plain
                # retained publish. publish_and_flush is owned-only and off the
                # injected transport surface, so this never blocks the caller's thread.
                try:
                    mqttc.publish(state_topic, DeviceState.DISCONNECTED.value, retain=True, qos=root._qos)
                except Exception as e:
                    logger.warning(f"reason=deviceStopDisconnectedPublishFailed,id={device._id},e={e}")
                    continue
                if device is root:
                    logger.info(f"reason=deviceStopDisconnectedPublishedInjected,id={root._id}")
        else:
            logger.info(f"reason=deviceStopBrokerUnreachable,id={root._id}")
        if owned:
            root._owned_client.stop(timeout=stop_timeout)
        root.mqttc = None
        root._owned_client = None

    def _tree_post_order(self) -> List["Device"]:
        """This device and every descendant, each device after all of its descendants."""
        ordered: List[Device] = []
        for child in list(self._children):
            ordered.extend(child._tree_post_order())
        ordered.append(self)
        return ordered

    def _tree_pre_order(self) -> List["Device"]:
        """This device and every descendant, each device before any of its descendants."""
        ordered: List[Device] = [self]
        for child in list(self._children):
            ordered.extend(child._tree_pre_order())
        return ordered

    def declare_lost(self, *, flush_timeout: float = 1.0) -> bool:
        """Declare this device tree dead: move the ROOT to ``$state=lost`` and publish it.

        The third teardown, alongside graceful shutdown (``stop()``, which announces
        ``disconnected``) and ungraceful death (the Last Will, which fires only on an
        UNCLEAN disconnect). This is for a producer that knows it is dying: a fatal
        error handler, a supervisor about to kill it, hardware that has gone away, or
        a simulator acting the part. Such a producer previously had to announce
        ``disconnected``, which is a lie, or reach around the SDK to its client.

        TREE-level, like ``will()`` and ``stop()``. It publishes the ROOT's ``$state``,
        which per the Homie 5 effective-state rule makes every descendant lost too, and
        it publishes exactly the topic and payload ``will()`` describes so the declared
        and will-driven paths cannot drift. To mark ONE device lost (a proxy whose
        single upstream vanished), call ``set_state(DeviceState.LOST)`` on that device
        instead: this method would blank the whole tree's liveness.

        The state move and the publish happen together, and the move is unconditional.
        Publishing a state the Device does not hold is how a later ``refresh_tree()``
        silently republishes ``ready`` over it. For the same reason a reconnect after
        this re-asserts ``lost`` until something sets the device back.

        Owned client: the publish is flushed, bounded by ``flush_timeout``. Injected
        client (bring-your-own-transport): the message is handed to the caller's loop
        with no flush, since ``publish_and_flush`` is owned-only and off the
        ``MqttDeviceTransport`` surface, so draining it before closing the client is
        the caller's obligation. Publishing is skipped when the broker is unreachable;
        the state still moves, and the next connect republishes it.

        Returns True if ``$state`` actually moved to ``lost``, False if the root was
        already lost, the same convention as ``set_state``. On an injected transport
        True means "queued, now drain" and False means "nothing to wait for". It is
        NOT a delivery signal and cannot be one there.

        Does NOT stop the client. Follow with ``stop(announce=False)`` to tear down
        without overwriting the ``lost`` just published.
        """
        root = self.root()
        if root._transition_depth > 0:
            # _end_state_transition() publishes READY on exit, which would land on top
            # of this. Warn rather than refuse: the caller may be dying mid-transition.
            logger.warning(f"reason=deviceDeclareLostInsideStateTransition,id={root._id}")
        changed = root._state != DeviceState.LOST
        root._state = DeviceState.LOST
        mqttc = root.mqttc
        if mqttc is None:
            _log_missing_client(f"reason=deviceDeclareLostNoMqttClient,id={root._id}", by_design=root._transport_free())
            return changed
        if not mqttc.is_connected():
            # An injected transport that queues while offline could deliver a stale
            # `lost` long after recovery, which is worse than not sending it.
            logger.info(f"reason=deviceDeclareLostBrokerUnreachable,id={root._id}")
            return changed
        state_topic = f"{root.homie_domain()}/{EBUS_HOMIE_VERSION_MAJOR}/{root._id}/$state"
        # Ownership decides, never isinstance: a caller may legitimately inject a real
        # MqttClient (driven by asyncio_driver), and publish_and_flush/stop must not be
        # called on a client the SDK does not own.
        if root._owns_client and root._owned_client is not None:
            flushed = root._owned_client.publish_and_flush(
                state_topic, DeviceState.LOST.value, qos=root._qos, retain=True, timeout=flush_timeout
            )
            logger.info(f"reason=deviceDeclareLostPublished,id={root._id},flushed={flushed}")
        else:
            mqttc.publish(state_topic, DeviceState.LOST.value, retain=True, qos=root._qos)
            logger.info(f"reason=deviceDeclareLostPublishedInjected,id={root._id}")
        return changed

    def description(self) -> dict:
        """
        Returns a dict of the $description attribute of the Device
        """
        logger.debug(f"reason=deviceDescriptionEntered,id={self._id}")
        description = dict()
        description["homie"] = f"{EBUS_HOMIE_VERSION_MAJOR}.{EBUS_HOMIE_VERSION_MINOR}"
        # Placeholder keeps `version` second in the document; filled in below from
        # the content, which the hash computes without it.
        description["version"] = 0
        if self._type is not None:
            # Optional and not nullable (convention.md:218): omitted when unset.
            description["type"] = self._type
        description["name"] = self._name
        nodes_descriptions = dict()
        nodes_snapshot = dict(self._nodes)
        for node_id, node in nodes_snapshot.items():
            nodes_descriptions[node_id] = node.description()
        description["nodes"] = nodes_descriptions
        description["children"] = self.children_ids()
        if self._parent is not None:
            # Required if the device is NOT the root device, MUST be omitted otherwise.
            description["root"] = self.root().id()
            # Required if the parent is NOT the root device. Defaults to the value of the root property.
            description["parent"] = self._parent.id()
        description["extensions"] = self._extensions
        # Merge extension-defined device attributes (core fields were dropped from
        # the extras at construction).
        for key, value in self._description_extras.items():
            description.setdefault(key, value)
        # A new version whenever the document changes (convention.md:215), and the
        # same version while it does not, so a reconnect republishes an identical
        # document: the top 52 bits of the content hash, an integer below 2^53 that
        # every JSON consumer reads exactly.
        description["version"] = int(self._description_content_hash(description)[:13], 16)
        return description

    def set_state(self, state: DeviceState) -> bool:
        """
        Sets state, representing the $state attribute
        If the new state equals the existing state, noop, and returns False
        Returns True if state was set, and publishes $description to broker
        """
        if state != self._state:
            self._state = state
            self.publish_state()
            return True
        else:
            return False

    def new_node(self, id: str, name: str = None, type: str = None) -> Node:
        """
        Returns a new Node, with device and device_id set
        """
        return Node(id=id, name=name, type=type, device=self)

    def add_node(self, node: Node) -> Node:
        """
        Add node to nodes
        """
        if not node.device():
            node.set_device(self)
        # Propagate device QoS (and the async /set dispatch loop, if set) to every
        # property in this node.
        for prop in node.properties().values():
            prop._qos = self._qos
            if self._async_loop is not None:
                prop.async_loop = self._async_loop
        node_id = node.id()
        # Outside a transition a ready device goes to init first, so the node's
        # values land before the $description that names them and before ready.
        self._nodes.update({node_id: node})
        with self._reconfiguration():
            # Explicit force (also Node.publish's default): adopting a node is a
            # structural republish, so every property lands on the broker under this
            # device regardless of the GH #50 unchanged-payload skip.
            node.publish(force=True)
        return node

    def add_node_from_dict(self, node_dict: dict) -> Node:
        """
        Create and add Node (as specified by node_dict), returns new Node
        """
        return self.add_node(Node(from_dict=node_dict))

    def get_node(self, node_id: str) -> Optional[Node]:
        """Safe getter that returns None if node doesn't exist"""
        return self._nodes.get(node_id, None)

    def remove_node(self, node_id: str) -> bool:
        """
        Removes node with node_id from nodes and republishes $description.

        This drops the node from the SCHEMA ONLY. Its properties' retained value
        topics are left on the broker, so a subscriber that already holds them
        keeps seeing values for a node the description no longer names, and a
        wildcard subscriber still receives them on connect. Use this when the
        topics are wanted (a node being re-parented, or a cleanup the caller is
        doing itself); use ``delete_node()`` to also clear them.

        Batch several removals inside ``device.state_transition()`` to collapse
        the republishes into one.

        Returns True if removed, else False.
        """
        if node_id in self._nodes:
            self._nodes.pop(node_id, None)
            with self._reconfiguration():
                pass
            return True
        else:
            return False

    def delete_node(self, node_id: str) -> bool:
        """
        Remove node from device, clear all its retained property topics, and
        republish $description.

        The broker-cleaning counterpart to ``remove_node()``, which drops the
        node from the schema but leaves its retained values behind. This is
        almost always the one you want: without it the removed node's values sit
        on the broker indefinitely with nothing left to describe them. Only
        properties that were actually published are cleared, so no phantom
        empty-retained topics are created.

        Batch several deletions inside ``device.state_transition()`` to collapse
        the republishes into one.

        Returns True if removed, False if not found.
        """
        if node_id not in self._nodes:
            logger.warning(f"reason=deviceDeleteNodeNotFound,deviceId={self._id},nodeId={node_id}")
            return False
        node = self._nodes[node_id]
        # The description that drops the node is published when the reconfiguration
        # scope closes, after its property topics are cleared.
        del self._nodes[node_id]
        with self._reconfiguration():
            # Clear each published property's retained message so no orphaned
            # topics are left in the broker.
            node.clear_all_properties()
        logger.info(f"reason=deviceDeletedNode,deviceId={self._id},nodeId={node_id}")
        return True

    def delete_all_from_mqtt(self) -> None:
        """
        Clear this device's retained property values, alerts and $description from the broker.

        A low-level data-cleanup helper: it clears every published property value and
        the $description topic, but deliberately does NOT touch $state, so on its own it
        is NOT a device-removal or shutdown method:

          * to permanently REMOVE a device, use delete(), which additionally clears the
            retained $state (the Homie removal signal) and detaches from the parent tree;
          * for SHUTDOWN, $state is managed by the teardown itself: stop() publishes
            DISCONNECTED, declare_lost() publishes LOST for a producer that knows it is
            dying, and the LWT publishes LOST on an unclean disconnect. All three
            typically leave retained values in place so consumers recover state across
            a restart.

        Does NOT republish anything and does NOT publish node descriptions.
        """
        logger.info(f"reason=deviceDeleteAllFromMqtt,deviceId={self._id}")

        mqttc = self.get_mqtt_client()
        if not mqttc:
            _log_missing_client(
                f"reason=deviceDeleteAllFromMqttNoMqttClient,deviceId={self._id}", by_design=self._transport_free()
            )
            return

        base_topic = f"{self.homie_domain()}/{EBUS_HOMIE_VERSION_MAJOR}/{self._id}"

        # Step 1: Clear all property values that were actually published
        for node_id, node in list(self._nodes.items()):
            if hasattr(node, "_properties"):
                for prop_id, prop in list(node._properties.items()):
                    # Only clear if property was ever published
                    was_published = False
                    if hasattr(prop, "was_ever_published") and prop.was_ever_published():
                        was_published = True
                    elif hasattr(prop, "_ever_published") and prop._ever_published:
                        was_published = True

                    if was_published and not prop.retained():
                        # Nothing retained to clear on an event topic (see clear_value).
                        prop.invalidate_publish_cache()
                    elif was_published:
                        prop_topic = f"{base_topic}/{node_id}/{prop_id}"
                        try:
                            mqttc.publish(prop_topic, "", retain=True, qos=self._qos)
                            # The retained topic is gone, so the property's publish-on-
                            # change memo now describes a broker state that no longer
                            # exists; without this, re-setting that same value would be
                            # skipped and the topic would stay empty (GH #50).
                            # hasattr-guarded to match the duck-typed reads above.
                            if hasattr(prop, "invalidate_publish_cache"):
                                prop.invalidate_publish_cache()
                            logger.debug("reason=deviceClearedProperty...")
                        except Exception:
                            logger.warning("reason=deviceClearPropertyFailed...")

        # Step 2: Clear the main device $description (this removes all nodes from schema)
        description_topic = f"{base_topic}/$description"
        try:
            mqttc.publish(description_topic, "", retain=True, qos=self._qos)
            self.invalidate_description_cache()
            logger.info(f"reason=deviceClearedDescription,deviceId={self._id},topic={description_topic}")
        except Exception as e:
            logger.warning(f"reason=deviceClearDescriptionFailed,deviceId={self._id},error={e}")

        # Step 3: Clear raised alerts; a removed device leaves no topics behind
        # (convention.md:289).
        for alert_id in list(self._alerts):
            self.clear_alert(alert_id)

        # Step 4: Clear internal tracking (no publishing happens here)
        self._nodes.clear()

        logger.info(f"reason=deviceDeleteAllFromMqttComplete,deviceId={self._id}")

    def delete(self) -> None:
        """
        Remove this device, and every descendant, from the broker (Homie removal).

        On a child, in the order convention.md:635-641 gives: the parent is updated
        first (``init``, ``$description`` without this child in ``children``,
        ``ready``), then this device's topics are cleared starting with its
        ``$state``. The parent's update is deferred, as for any structural change,
        when the parent is already inside a ``state_transition()``.

        The removed subtree is cleared leaves first, and each device's ``$state``
        is cleared before its other topics: values, alerts and ``$description``
        (convention.md:287-289). No device in the subtree publishes an
        ``init``/``ready`` flap on its way out.

        On a root: clears all retained data for the tree. It does not stop the MQTT
        client; ``stop()`` afterwards disconnects cleanly and announces nothing,
        and a reconnect republishes nothing, since an absent retained ``$state`` is
        the removal signal and any later ``$state`` would re-create the device with
        no ``$description``. The will is armed on the connection, so it still fires
        if the process dies uncleanly before ``stop()``. Do not follow this with
        ``declare_lost()``; to retire a tree as dead but still present, use
        ``declare_lost()`` plus ``stop(announce=False)`` instead.

        After delete(), this Device object should not be used further.
        """
        logger.info(f"reason=deviceDelete,deviceId={self._id},isRoot={self._parent is None}")
        subtree = self._tree_pre_order()
        for device in subtree:
            device._deleted = True
        parent = self._parent
        if parent is not None:
            # Step 1: the parent drops this child from `children` and re-announces.
            # The child keeps its _parent link until its topics are cleared: the
            # topic domain and the MQTT client are both resolved through the root.
            parent._children.remove(self)
            parent._notify_structural_change()
        # Step 2: clear the subtree leaves first, so no device is seen whose parent
        # is already gone. Each device's $state goes before its other topics, so it
        # ceases to exist first (convention.md:287-289).
        for device in reversed(subtree):
            base_topic = f"{device.homie_domain()}/{EBUS_HOMIE_VERSION_MAJOR}/{device._id}"
            device.clear_retained_topic(f"{base_topic}/$state")
            device.delete_all_from_mqtt()
        for device in subtree:
            device._children = []
        self._parent = None

    def invalidate_description_cache(self) -> None:
        """Forget which ``$description`` this device last published.

        ``publish_description()`` skips a document identical to the last one it
        published, on the assumption that the broker still holds it. Anything that
        deletes the retained ``$description`` behind the device's back must call
        this, or the next ``publish_description()`` is skipped and the device stays
        without a description. ``clear_retained_topic()`` aimed at this device's
        ``$description`` and ``delete_all_from_mqtt()`` call it themselves.
        """
        self._last_description_content_hash = None

    def clear_retained_topic(self, topic_path: str) -> bool:
        """
        Publish empty string to clear retained message on topic
        Returns True on success, False on failure

        This is a raw topic operation: a property's value topic can be cleared this
        way, but the owning ``Property`` does not know it happened. Call
        ``Property.invalidate_publish_cache()`` on it too, or the GH #50
        unchanged-payload skip will suppress the next ``set_value()`` of that same
        value and the topic will stay deleted.
        """
        mqttc = self.get_mqtt_client()
        if not mqttc:
            _log_missing_client(
                f"reason=deviceClearTopicNoMqttClient,topic={topic_path}", by_design=self._transport_free()
            )
            return False
        try:
            mqttc.publish(topic_path, "", retain=True, qos=self._qos)
            logger.info(f"reason=deviceClearedTopic,topic={topic_path}")
            if topic_path == f"{self.homie_domain()}/{EBUS_HOMIE_VERSION_MAJOR}/{self._id}/$description":
                # The broker no longer holds the document the hash describes.
                self.invalidate_description_cache()
            return True
        except Exception as e:
            logger.warning(f"reason=deviceClearTopicException,topic={topic_path},e={e}")
            return False

    def _begin_state_transition(self) -> None:
        """
        Enter a state-transition scope.

        Re-entrant: only the outermost entry publishes INIT (set_state's
        same-state no-op would suppress redundant INIT publishes anyway,
        but skipping the set_state() call avoids the log noise too).
        """
        self._transition_depth += 1
        logger.info(f"reason=deviceBeginStateTransition,deviceId={self._id},depth={self._transition_depth}")
        if self._transition_depth == 1:
            self.set_state(DeviceState.INIT)

    def _end_state_transition(self) -> None:
        """
        Exit a state-transition scope.

        Re-entrant: only the outermost exit publishes the final $description
        and flips state to READY. Inner exits decrement the depth and return
        — letting the outermost scope batch everything into one INIT→READY
        cycle so controllers only resync once per logical change-set.
        """
        logger.info(f"reason=deviceEndStateTransition,deviceId={self._id},depth={self._transition_depth}")
        if self._transition_depth == 1:
            # Leave the transition scope BEFORE the consolidated publish so that
            # final, intended $description is not suppressed by the in-transition
            # defer guard in publish_description() (SDK-9ps).
            self._transition_depth = 0
            self.publish_description()
            self.set_state(DeviceState.READY)
        else:
            self._transition_depth -= 1

    def _notify_structural_change(self) -> None:
        """
        Republish this device's $description after a structural change
        (a child was added or removed). Performs INIT → publish description
        → READY (or back to sleeping) unless this device is already inside a state_transition() —
        in which case the in-progress transition will publish on exit and
        we suppress the per-change flap (S1: many children, one parent cycle).

        Safe to call before the device has been transitioned to READY at all
        (state is None) — in that case we just publish the description with
        no state flap.
        """
        if self._transition_depth > 0:
            logger.debug(
                f"reason=deviceStructuralChangeSuppressed,deviceId={self._id},transitionDepth={self._transition_depth}"
            )
            return
        # publish_description() wraps the change in INIT when the device is ready
        # or sleeping, and publishes it directly otherwise.
        self.publish_description()

    def _description_changed(self) -> bool:
        """Whether ``$description`` now differs from the one last published."""
        try:
            return self._description_content_hash(self.description()) != self._last_description_content_hash
        except Exception:
            # publish_description() logs the compose failure.
            return True

    @contextlib.contextmanager
    def _reconfiguration(self):
        """Publish a structural change made outside an explicit ``state_transition()``.

        The caller applies the change to the in-memory tree first, without
        publishing, then publishes inside this scope what the change requires (new
        values included).

        Homie 5 reconfiguration (convention.md:207, :279, :611): a ``ready`` or
        ``sleeping`` device whose ``$description`` the change alters goes to
        ``init``, runs the body, publishes the new ``$description``, and only then
        returns to the state it was in, so ``ready`` is never announced ahead of the
        values it vouches for. A change that leaves the ``$description`` as last
        published (a node not attached to the device, an identical re-declaration)
        runs the body with no ``init``/``ready`` flap (convention.md:616). Inside an
        open transition the change joins it. In any other state (not yet announced,
        ``init``, ``disconnected``, ``lost``) the body and the new ``$description``
        are published directly.
        """
        if self._transition_depth > 0:
            yield
            return
        prior = self._state
        if prior not in (DeviceState.READY, DeviceState.SLEEPING) or not self._description_changed():
            yield
            self.publish_description()
            return
        self._transition_depth += 1
        self.set_state(DeviceState.INIT)
        try:
            yield
        finally:
            self._transition_depth -= 1
            self.publish_description()
            self.set_state(prior)

    def state_transition(self) -> StateTransitionContext:
        """
        Return a context manager for state transitions.

        Usage:
            with device.state_transition():
                # Add/remove nodes, modify schema
                device.add_node(...)
        # State is automatically set to READY here, even if an exception occurred
        """
        return StateTransitionContext(self)

    def refresh_tree(self, *, force: bool = True) -> None:
        """
        Republish this device and every descendant (description, nodes,
        property values, state). Used on broker reconnect (S6) so the entire
        tree's retained-state is re-established under the root's connection.

        ``force`` defaults to True, so every property value is republished even where
        its payload is unchanged (GH #50). That is the whole point on reconnect: the
        broker's retained store may be empty (restarted without persistence, or a new
        broker), in which case a gated refresh would find every payload equal to what
        it last published, send nothing, and leave the tree's values missing. It is
        keyword-only with a default because callers wire this in bare as an
        on-connect callback.

        For a client the SDK owns, ``on_connect`` calls this automatically on
        every (re)connect. A bring-your-own-transport caller must call it from
        their own on-connect handler, so the retained tree re-announces after a
        broker reconnect the SDK's ``on_connect`` never sees.

        BEST-EFFORT: a descendant whose republish raises is logged and skipped,
        and the cascade continues. Reconnect is exactly when one sick device
        must not be able to keep its siblings and every ancestor off the broker,
        so a partial refresh beats an aborted one. The exception is not
        re-raised; callers wanting to detect it should watch for
        ``reason=deviceRefreshTreeChildFailed`` in the log.
        """
        if self._deleted:
            # delete() removed this device; republishing would re-create it.
            logger.info(f"reason=deviceRefreshTreeDeletedSkipped,deviceId={self._id}")
            return
        logger.info(
            f"reason=deviceRefreshTree,deviceId={self._id},"
            f"nodeCount={len(self._nodes)},childCount={len(self._children)}"
        )
        # Description and nodes first, then descendants, then this device's own
        # state. A device's $description names its children, so publishing
        # ``ready`` ahead of the recursion advertises a tree that is not on the
        # broker yet; this order also matches the add-child path, where a child
        # publishes itself fully before its parent re-announces (__init__).
        # It matters most after an ungraceful drop: the LWT leaves the root
        # retained as ``lost``, Homie 5 makes every child of a ``lost`` root
        # ``lost`` too, so holding this device's state until the recursion
        # finishes makes the refresh one atomic commit, flipped by one publish.
        # This narrows a producer-side window; it is NOT a guarantee a consumer
        # may build on (see doc/consuming-a-homie-tree.md). Message set unchanged.
        # A document that differs from the last one published (its memo was
        # invalidated, or never stored) is a change, so a ready or sleeping device
        # announces init first (convention.md:207); the state publish below ends it.
        if self._state in (DeviceState.READY, DeviceState.SLEEPING) and self._description_changed():
            self.publish_state(DeviceState.INIT)
        self.publish_description(republish=True)
        self.publish_nodes(force=force)
        for alert_id, message in list(self._alerts.items()):
            self._publish_alert_topic(alert_id, message)
        # Snapshot — main thread may construct child devices (which append
        # to self._children) while this runs on the MQTT loop thread.
        for child in list(self._children):
            # Best-effort per child: Device.publish() swallows its own
            # exceptions, but Node.publish() and Property.publish_value() do
            # not, so an injected transport that raises would otherwise abort
            # the whole cascade from wherever it failed. That would take out
            # every later sibling AND this device's own state publish below,
            # turning one sick device into a tree-wide reconnect failure.
            try:
                child.refresh_tree(force=force)
            except Exception as e:
                logger.exception(f"reason=deviceRefreshTreeChildFailed,deviceId={self._id},childId={child._id},e={e}")
        self.publish_state()

    def publish(self, attribute: str = "", value: Optional[Any] = None) -> None:
        """
        Publishes the value argument to the device's attribute MQTT topic,
        or if the value is not provided, publishes the current (self) attribute value.

        For child devices, the publish is routed through the root's MQTT client
        but uses self._id (this device's ID) in the topic — so each device in
        the tree publishes its own ebus/5/<id>/... topics.
        """
        mqttc = self.get_mqtt_client()
        if not mqttc:
            _log_missing_client(
                f"reason=devicePublishNoMqttClient,attribute={attribute}",
                by_design=self._transport_free(),
                level=logging.INFO,
            )
            return
        if not self._id:
            logger.info("reason=devicePublishNoDeviceID")
            return
        try:
            base_topic = f"{self.homie_domain()}/{EBUS_HOMIE_VERSION_MAJOR}/{self._id}/"
            if attribute == "$state":
                topic = base_topic + "$state"
                if value:
                    payload = value
                else:
                    payload = self._state
            elif attribute == "$description":
                topic = base_topic + "$description"
                description = value if value else self.description()
                payload = json.dumps(description) if description else None
            elif attribute == "$alert":
                # An alert lives at $alert/[alert ID] (convention.md:517); a bare
                # $alert topic is not one. Use publish_alert().
                logger.warning(
                    f"reason=devicePublishAlertWithoutId,id={self._id},hint=use publish_alert(alert_id, message)"
                )
                return
            if payload:
                mqttc.publish(topic, payload, retain=True, qos=self._qos)
                if attribute == "$description":
                    # SDK-n83: remember what we just put on the wire (sans
                    # version) so a later unchanged republish no-ops.
                    # Updated here — the single $description chokepoint — so every
                    # caller (publish_description, _notify_structural_change,
                    # reconnect) keeps the hash current.
                    self._last_description_content_hash = self._description_content_hash(description)
        except Exception as e:
            logger.exception(f"reason=devicePublishException,id={self._id},attribute={attribute},value={value},e={e}")

    def _alert_topic(self, alert_id: str) -> str:
        return f"{self.homie_domain()}/{EBUS_HOMIE_VERSION_MAJOR}/{self._id}/$alert/{alert_id}"

    def _publish_alert_topic(self, alert_id: str, message: str) -> bool:
        mqttc = self.get_mqtt_client()
        if not mqttc:
            _log_missing_client(
                f"reason=devicePublishAlertNoMqttClient,id={self._id}", by_design=self._transport_free()
            )
            return False
        try:
            mqttc.publish(self._alert_topic(alert_id), message, retain=True, qos=self._qos)
            return True
        except Exception as e:
            logger.warning(f"reason=devicePublishAlertException,id={self._id},alertId={alert_id},e={e}")
            return False

    def publish_alert(self, alert_id: str, message: str) -> bool:
        """Raise a user-facing alert at ``$alert/[alert_id]`` (convention.md:512-527).

        Retained, at the tree QoS. Raising the same id again replaces its message.
        The alert stays until ``clear_alert(alert_id)``; ``delete()`` clears every
        alert the device raised, and a reconnect republishes them.

        ``alert_id`` must be a single topic level; one containing ``/``, ``+`` or
        ``#``, or an empty one, is refused. An id outside the Homie ID format
        (``a-z``, ``0-9``, ``-``) is warned about and still published. ``message``
        must be a non-empty string: an empty payload deletes the topic, so clear an
        alert with ``clear_alert()``. Returns True if published.
        """
        if not alert_id or not isinstance(alert_id, str) or any(c in alert_id for c in "/+#"):
            logger.warning(f"reason=devicePublishAlertInvalidId,id={self._id},alertId={alert_id!r}")
            return False
        if not re.fullmatch(r"[a-z0-9-]+", alert_id):
            _warn_invalid_once(
                self, f"alertId:{alert_id}", f"reason=devicePublishAlertIdFormat,id={self._id},alertId={alert_id}"
            )
        if not isinstance(message, str) or message == "":
            logger.warning(f"reason=devicePublishAlertEmptyMessage,id={self._id},alertId={alert_id}")
            return False
        self._alerts[alert_id] = message
        return self._publish_alert_topic(alert_id, message)

    def clear_alert(self, alert_id: str) -> bool:
        """Remove the alert ``alert_id`` by deleting its retained topic (convention.md:519).

        Returns True if the deletion was published. Clearing an alert this device
        did not raise still publishes the deletion, so a stale alert left by an
        earlier run can be removed.
        """
        if not alert_id or not isinstance(alert_id, str) or any(c in alert_id for c in "/+#"):
            logger.warning(f"reason=deviceClearAlertInvalidId,id={self._id},alertId={alert_id!r}")
            return False
        self._alerts.pop(alert_id, None)
        mqttc = self.get_mqtt_client()
        if not mqttc:
            _log_missing_client(f"reason=deviceClearAlertNoMqttClient,id={self._id}", by_design=self._transport_free())
            return False
        try:
            mqttc.publish(self._alert_topic(alert_id), "", retain=True, qos=self._qos)
            return True
        except Exception as e:
            logger.warning(f"reason=deviceClearAlertException,id={self._id},alertId={alert_id},e={e}")
            return False

    def alerts(self) -> dict:
        """The alerts this device has raised and not cleared, ``{alert_id: message}``."""
        return dict(self._alerts)

    def publish_state(self, state: Optional[DeviceState] = None) -> None:
        """
        Publishes the value of the state argument to the device's $state topic,
        or if state argument not provided, publishes value of self._state
        """
        if state:
            self.publish("$state", value=state)
        else:
            self.publish("$state", value=self._state)

    @staticmethod
    def _description_content_hash(description: dict) -> str:
        """
        SHA-256 of a $description dict with `version` removed. ``description()``
        derives ``version`` from this hash, and the publish gate compares it to
        decide whether the document changed.
        """
        content = {k: v for k, v in description.items() if k != "version"}
        return hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()

    def publish_description(self, republish: bool = False) -> None:
        """Publish this device's ``$description`` if its content changed.

        Homie 5 lets ``$description`` change only while ``$state`` is ``init``,
        ``disconnected`` or ``lost`` (convention.md:207). A change made while the
        device is ``ready`` or ``sleeping`` is therefore wrapped: ``init``, the new
        document, then the state the device was in.

        Unchanged content is skipped. ``version`` is derived from the content
        (see ``description()``), so ``republish=True`` (the reconnect path) puts
        the same document back on the broker, which is not a change and needs no
        ``init``. ``republish=True`` publishes the document without the wrap even
        when it changed: ``refresh_tree()`` publishes ``init`` ahead of it and the
        device's state after everything the document names. ``republish=True`` is
        also exempt from the in-transition defer.
        """
        # SDK-9ps: while a state_transition() is open, defer interim $description
        # publishes to the single consolidated publish at _end_state_transition().
        # Adding N nodes inside one transition then puts 1 description on the wire,
        # not N+1. (_end_state_transition leaves the transition scope before its own
        # call so that consolidated publish isn't deferred.)
        if self._transition_depth > 0 and not republish:
            logger.debug(
                f"reason=publishDescriptionDeferredInTransition,deviceId={self._id},depth={self._transition_depth}"
            )
            return

        try:
            description = self.description()
            changed = self._description_content_hash(description) != self._last_description_content_hash
        except Exception as e:
            # Best-effort, like every Device.publish(): one unserializable entry must
            # not abort a tree-wide refresh.
            logger.exception(f"reason=publishDescriptionComposeFailed,deviceId={self._id},e={e}")
            return
        if not changed and not republish:
            # SDK-n83: nothing to announce; skip the ~KB republish and the INIT flap.
            logger.debug(f"reason=publishDescriptionUnchanged,deviceId={self._id}")
            return
        prior = self._state
        if changed and not republish and prior in (DeviceState.READY, DeviceState.SLEEPING):
            self.publish_state(DeviceState.INIT)
            self.publish("$description", description)
            self.publish_state(prior)
        else:
            self.publish("$description", description)

    def publish_nodes(self, *, force: bool = True) -> None:
        # Snapshot — invoked from on_connect() on the MQTT loop thread while
        # the main thread may be inside state_transition() calling add_node().
        # Without the snapshot, dict-size-changed-during-iteration crashes the
        # MQTT thread on initial connect.
        # force defaults to True for the same reason as Node.publish: this is a
        # republish walk, not the gated value path (GH #50).
        for node in list(self._nodes.values()):
            node.publish(force=force)

    def on_connect(self) -> None:
        """
        Called when the root device's MQTT connection (re-)opens. Only roots
        register a connection — children share, so this only fires on the root.

        Republishes the ENTIRE tree — every device's $description, nodes,
        property values, and $state — on BOTH the initial connect and every
        reconnect, via refresh_tree() (which is idempotent).

        The initial connect must be as complete as a reconnect because the
        broker connection is asynchronous (ebus-mqtt-client connect_async): a
        root Device can be constructed while the broker is briefly unavailable,
        in which case the construction-time state_transition publishes
        ($state=init -> $description -> $state=ready) never reached the broker.
        Publishing only node values here (the earlier behavior) would then leave
        a HALF-PUBLISHED device — nodes present but $state/$description missing —
        which a Homie consumer sees as broken. refresh_tree() also recurses to
        any children constructed before this first connect. On a reconnect it
        re-establishes retained state the broker may have dropped (S6). The
        redundant republish on a broker that WAS up at construction time is
        harmless: the publishes are retained and idempotent.
        """
        logger.info(
            f"reason=deviceOnConnectInvocation,initialBrokerConnection={self.initial_broker_connection},"
            f"rootId={self._id},nodeCount={len(self._nodes)},childCount={len(self._children)}"
        )
        # The initial-vs-reconnect flag is now observability only — both paths do
        # the same complete republish. Clearing it keeps the log honest.
        self.initial_broker_connection = False
        # force: this is the site the GH #50 force path exists for. A broker restarted
        # with an empty retained store must be fully repopulated, and every payload
        # here matches what the property "last published".
        self.refresh_tree(force=True)

    def _handle_disconnect(self, rc=None) -> None:
        """Transport disconnect handler for the root's MQTT client.

        Invoked by ebus-mqtt-client with paho's integer reason code. The code is
        absorbed here (logged for diagnostics, never surfaced) and the consumer's
        transport-neutral ``on_disconnect(clean: bool)`` hook is notified so a
        paho type/value never leaks upward (SDK-al5).
        """
        logger.info(f"reason=deviceDisconnect,rootId={self._id},transportRc={rc}")
        _dispatch_disconnect(self._on_disconnect, rc, f"device:{self._id}")

    def will(self) -> dict:
        """The Last Will and Testament for this device tree: the root's ``$state=lost``.

        The SDK installs this on any client it constructs (see ``connect_broker``).
        It is exposed because a bring-your-own-transport caller (a root ``Device``
        handed a live client) must set the will on that client BEFORE connecting:
        the will rides the MQTT CONNECT packet, so the SDK cannot add it to a
        client it is merely given after that client has connected. Children share
        the root's connection, so this always describes the root regardless of
        which device in the tree it is called on.

        ``declare_lost()`` publishes this same topic and payload explicitly, for a
        producer that knows it is dying: the will fires only on an unclean
        disconnect, and the clean disconnect ``stop()`` performs suppresses it.

        ``qos`` and ``retain`` are the tree's: every other ``$state`` publish is
        retained at the tree QoS (convention.md:45-47), and a will on the same topic
        at a lower QoS would break the ordering a single QoS level guarantees
        (convention.md:689-691).
        """
        root = self.root()
        return {
            "topic": f"{root.homie_domain()}/{EBUS_HOMIE_VERSION_MAJOR}/{root._id}/$state",
            "payload": DeviceState.LOST.value,
            "qos": root._qos,
            "retain": True,
        }

    def connect_broker(self) -> None:
        """
        Connect to MQTT broker using configuration from mqtt_cfg.
        Only called on root devices — children share the root's connection
        and skip this entirely (no own MqttClient, no per-child LWT).

        Construction is resilient to a briefly-unavailable broker: ebus-mqtt-client
        (>=0.1.8) registers the target with connect_async and establishes the link
        on its own network loop (started by start_mqtt_client()), retrying with
        backoff until the broker appears. A broker that is down or unreachable at
        construction time therefore does NOT raise here and does NOT leave a
        silent, never-connecting client; observe when the link is up via
        is_connected().

        Because the down-broker case no longer reaches this except clause, any
        exception that from_config still raises now signals a GENUINE
        construction fault (e.g. a malformed mqtt_cfg or an unreadable TLS
        certificate). We RE-RAISE it rather than swallow: a silent mqttc=None
        would hide a real failure from every caller and yield a
        dead-but-"running" publisher.
        """
        if self._parent is not None:
            # Children share the root's MQTT connection.
            return
        if self.mqttc:
            # If we already have a mqtt client, don't reconnect...
            return
        try:
            # Bind to a local of the concrete type so start() / stop() resolve later:
            # self.mqttc is MqttDeviceTransport, which deliberately has neither. Both
            # references are set before any start(), so behavior is unchanged.
            client = MqttClient.from_config(
                mqtt_cfg=self._mqtt_cfg,
                client_id=self._id,
                lwt=self.will(),
                on_connect_callback=partial(self.on_connect),
                on_disconnect_callback=self._handle_disconnect,
            )
            self._owned_client = client
            self.mqttc = client
        except Exception:
            logger.exception(f"reason=deviceConnectBrokerFailed,id={self._id}")
            raise


def ebus_cfg_add_auth(cfg, username, password):
    """
    Add authentication to the config dictionary
    """
    from ebus_mqtt_client import AUTH_TYPE_USER_PASS

    cfg["authentication"] = {
        "type": AUTH_TYPE_USER_PASS,
        "username": username,
        "password": password,
    }
    return cfg


class DiscoveredDevice:
    """
    Represents a device discovered by a Controller.
    Stores device metadata, description, and current property values.
    """

    def __init__(self, device_id: str, homie_domain: str = EBUS_HOMIE_DOMAIN):
        self.device_id = device_id
        self.homie_domain = homie_domain
        self.state = None
        self.description = None  # Parsed JSON from $description topic
        self.properties = {}  # {node_id: {property_id: value}}
        self.property_targets = {}  # {node_id: {property_id: target_value}}
        self.last_seen = None

    def update_state(self, state: str) -> None:
        """Update device state"""
        self.state = state
        self.last_seen = time.time()

    def update_description(self, description_json: str) -> None:
        """Parse and store device description"""
        try:
            self.description = json.loads(description_json)
            self.last_seen = time.time()
        except json.JSONDecodeError as e:
            logger.error(f"reason=descriptionParseError,deviceID={self.device_id},error={e}")

    def update_property(self, node_id: str, property_id: str, value: str) -> None:
        """Update a property value"""
        if node_id not in self.properties:
            self.properties[node_id] = {}
        self.properties[node_id][property_id] = value
        self.last_seen = time.time()

    def update_property_target(self, node_id: str, property_id: str, target: str) -> None:
        """Update a property target value"""
        if node_id not in self.property_targets:
            self.property_targets[node_id] = {}
        self.property_targets[node_id][property_id] = target
        self.last_seen = time.time()

    def get_property(self, node_id: str, property_id: str) -> Optional[str]:
        """Get current value of a property (raw string as received)"""
        return self.properties.get(node_id, {}).get(property_id)

    def get_property_json(self, node_id: str, property_id: str) -> Any:
        """Get a property value decoded from JSON, for a ``json``-datatype property.

        Looks up the property's `datatype` in this device's `$description`: if it
        is `json`, the stored raw value is `json.loads`ed and the parsed
        dict/list is returned; a non-`json` property returns its raw value
        unchanged. Returns None if the value is absent or cannot be parsed. This
        is the consumer-side counterpart to a publisher's `json` property, so a
        controller reads a parsed object (e.g. `flex/active-request`) rather than
        a raw JSON string.
        """
        raw = self.get_property(node_id, property_id)
        if raw is None:
            return None
        props = self.get_node_properties(node_id)
        datatype = props.get(property_id, {}).get("datatype") if isinstance(props, dict) else None
        if datatype != PropertyDatatype.JSON:
            return raw
        try:
            return json.loads(raw)
        except (ValueError, TypeError):
            logger.warning(f"reason=getPropertyJsonParseError,node={node_id},property={property_id}")
            return None

    def get_property_format_fields(self, node_id: str, property_id: str) -> dict:
        """Introspect a `json` property's `$format` control surface, from the description.

        Returns `{field_name: JsonFieldConstraint}` derived from the property's
        `$format` JSONSchema (empty dict if there is no schema). Lets a controller
        honor a settable json control surface it discovered, e.g. rendering
        `flex/request`'s `level` as buttons (enum) or a slider (range). See
        `json_format_fields`.
        """
        props = self.get_node_properties(node_id)
        format_schema = props.get(property_id, {}).get("format") if isinstance(props, dict) else None
        return json_format_fields(format_schema)

    def get_property_target(self, node_id: str, property_id: str) -> Optional[str]:
        """Get target value of a property"""
        return self.property_targets.get(node_id, {}).get(property_id)

    @property
    def root_id(self) -> str:
        """
        ID of the root device of this device's tree.

        Per Homie 5: the `root` field is omitted on root devices and required
        on non-roots. When absent, this device IS the root, so root_id = device_id.
        """
        if not self.description:
            return self.device_id
        return self.description.get("root", self.device_id)

    @property
    def parent_id(self) -> Optional[str]:
        """
        ID of the immediate parent, or None if this device is a root.
        Reads description.parent — present only on non-root devices.
        """
        if not self.description:
            return None
        return self.description.get("parent")

    @property
    def children_ids(self) -> List[str]:
        """
        IDs of this device's immediate children from description.children.
        Empty list if no description or no children. These IDs may or may not
        themselves be discovered yet — use Controller.get_children() to get
        the DiscoveredDevice objects that ARE discovered.
        """
        if not self.description:
            return []
        return list(self.description.get("children", []))

    @property
    def is_root(self) -> bool:
        """True iff this device is the root of its tree (no parent)."""
        return self.parent_id is None

    def get_nodes(self) -> List[str]:
        """Get list of node IDs from description"""
        if not self.description or "nodes" not in self.description:
            return []
        return list(self.description["nodes"].keys())

    def get_node_properties(self, node_id: str) -> dict:
        """Get properties dict for a node from description"""
        if not self.description or "nodes" not in self.description:
            return {}
        nodes = self.description["nodes"]
        if node_id in nodes:
            return nodes[node_id].get("properties", {})
        return {}


# Controller subscription pacing and stuck-device healing (GH #97). Subscribing to many
# devices at once makes the broker queue every matching retained message for this one
# client; past its per-client limits (mosquitto: max_queued_messages 1000) it drops the
# excess, and retained messages are sent only at subscribe time, so a dropped $state or
# $description never arrives.
CONTROLLER_SUBSCRIPTION_BATCH_SIZE = 8
CONTROLLER_STUCK_DEVICE_TIMEOUT = 10.0
CONTROLLER_MAX_RESUBSCRIBE_ATTEMPTS = 3
# Upper bound on how often message handling runs the opportunistic stuck check.
_STUCK_CHECK_INTERVAL = 1.0
# Per-device topic filter suffixes. Wildcard discovery hears $state through its
# broker-wide {domain}/5/+/$state filter, so a device found that way needs only the rest.
_DEVICE_FILTERS = ("$state", "$description", "+/+", "+/+/$target")
_WILDCARD_DEVICE_FILTERS = ("$description", "+/+", "+/+/$target")


@dataclass
class _SubscriptionWatch:
    """A device whose filters are subscribed but whose $state or $description has not arrived."""

    filters: tuple
    subscribed_at: float
    holds_slot: bool
    attempts: int = 0


class Controller:
    """
    Homie MQTT Controller - discovers and interacts with Homie devices

    A controller can:
    - Auto-discover devices on the MQTT broker
    - Read device descriptions and understand their structure
    - Monitor property values
    - Send commands to settable properties
    - Broadcast messages to all devices

    Usage example:
        controller = Controller(mqtt_cfg={'host': 'localhost', 'port': 1883})
        controller.set_on_device_discovered_callback(lambda dev: print(f"Found: {dev.device_id}"))
        controller.set_on_property_changed_callback(
            lambda dev_id, node, prop, val: print(f"{dev_id}/{node}/{prop} = {val}"))
        controller.start_discovery()

        # Send a command to a device
        controller.set_property('my-device-id', 'lights', 'power', 'true')
    """

    def __init__(
        self,
        mqtt_cfg: Optional[dict] = {},
        homie_domain: str = EBUS_HOMIE_DOMAIN,
        auto_start: bool = False,
        device_id: Optional[str] = None,
        root_device_id: Optional[str] = None,
        qos: int = EBUS_HOMIE_MQTT_QOS,
        mqttc: Optional[MqttControllerTransport] = None,
        subscription_batch_size: Optional[int] = CONTROLLER_SUBSCRIPTION_BATCH_SIZE,
        stuck_device_timeout: Optional[float] = CONTROLLER_STUCK_DEVICE_TIMEOUT,
        max_resubscribe_attempts: int = CONTROLLER_MAX_RESUBSCRIBE_ATTEMPTS,
    ):
        """
        Initialize a Homie Controller

        Three discovery modes are mutually exclusive:
        - Wildcard (default): device_id=None, root_device_id=None — sees every
          device on the broker by subscribing to {domain}/5/+/$state.
        - Single-device: device_id=<id> — subscribes to exactly that device's
          four topic patterns; no children, no wildcard in the device-id slot.
        - Tree-rooted: root_device_id=<id> — starts at the named root, then
          walks $description.children and subscribes to each descendant as it
          announces. Subscription changes are gated on the parent's init→ready
          state edge (per Homie 5: $state=ready is the trust signal).

        Args:
            mqtt_cfg: MQTT broker configuration (same format as Device class)
            homie_domain: Homie domain to monitor (default: 'ebus')
            auto_start: If True, automatically start discovery on init
            device_id: If set, subscribe only to this specific device (no wildcards)
            root_device_id: If set, subscribe to this root and auto-subscribe to
                its descendants as the tree is announced (SDK-o1h)
            qos: MQTT QoS level for all subscribe/publish operations (default: EBUS_HOMIE_MQTT_QOS)
            mqttc: Optional pre-built MQTT client to use instead of constructing
                one from mqtt_cfg (bring-your-own-transport, SDK-61t.6). When
                given, the SDK uses it as-is and does NOT start() or stop() it:
                the caller owns its lifecycle and event loop (e.g. a Home
                Assistant consumer driving MQTT on its own loop). Drive discovery
                with start_discovery() once the client is connected. Default
                None: the SDK constructs, starts, and owns a client from mqtt_cfg.
            subscription_batch_size: Most devices whose per-device subscriptions
                may await their retained $state and $description at once (GH #97).
                Further devices (tree descendants, or devices found by wildcard
                discovery) wait in order and are subscribed as earlier ones finish
                or are declared stuck, so the broker's per-client queue does not
                overflow and drop retained messages. None or 0 subscribes every
                device immediately.
            stuck_device_timeout: Seconds a subscribed device may lack $state or
                $description before it is declared stuck: it gives up its batch
                slot and its filters are unsubscribed and resubscribed so the
                broker resends its retained messages. Message handling runs this
                check at most once a second; see check_stuck_children(). None
                disables the automatic check.
            max_resubscribe_attempts: Resubscribes per stuck device before it is
                logged as unrecoverable and no longer checked. 0 only logs.
        """
        if device_id is not None and root_device_id is not None:
            raise ValueError(
                "device_id and root_device_id are mutually exclusive; "
                "pick single-device mode (device_id) or tree-rooted mode (root_device_id)"
            )

        self.homie_domain = homie_domain
        self.device_id = device_id
        self.root_device_id = root_device_id
        self._qos = qos
        self._mqtt_cfg = mqtt_cfg
        # Bring-your-own-transport (SDK-61t.6): an injected client is used as-is
        # and its lifecycle stays the caller's; a None here means the SDK
        # constructs, starts, and owns the client (the default, unchanged path).
        self.mqttc: Optional[MqttControllerTransport] = mqttc
        self._owns_client = mqttc is None
        # The SDK-constructed client, kept as its concrete type so start()/stop() —
        # which exist only on a client we own — remain callable. Stays None for an
        # injected client, which is what makes "never started, never stopped" a
        # property of the types rather than a promise in a comment.
        self._owned_client: Optional[MqttClient] = None
        self.devices = {}  # {device_id: DiscoveredDevice}
        # Tree-rooted mode: {parent_device_id: set_of_subscribed_child_ids}.
        # Authoritative record of what we've subscribed for under each parent,
        # independent of any child's own description (which may not have
        # arrived yet). Reconcile diffs against this rather than walking
        # parent_id linkages so a pre-created-but-not-yet-described child
        # doesn't look "missing" and get re-subscribed every reconcile.
        self._subscribed_children: dict = {}

        # Subscription pacing and stuck-device healing (GH #97). Event-driven: state
        # changes only on message arrival, device removal, resync, or a call to
        # check_stuck_children(). The lock guards the bookkeeping only; transport
        # calls are made after releasing it.
        if subscription_batch_size is not None and subscription_batch_size < 0:
            raise ValueError("subscription_batch_size must be None or >= 0")
        if max_resubscribe_attempts < 0:
            raise ValueError("max_resubscribe_attempts must be >= 0")
        self._subscription_batch_size = subscription_batch_size or None
        self._stuck_device_timeout = stuck_device_timeout
        self._max_resubscribe_attempts = max_resubscribe_attempts
        self._pacing_lock = threading.RLock()
        # {device_id: _SubscriptionWatch}: subscribed, awaiting $state and $description.
        self._watches: dict = {}
        # {device_id: filters}, insertion-ordered: requested but not yet subscribed.
        self._pending_subscriptions: dict = {}
        self._last_stuck_check = time.monotonic()

        # Callbacks
        self._on_device_discovered = None
        self._on_device_state_changed = None
        self._on_device_removed = None
        self._on_property_changed = None
        self._on_description_received = None
        self._on_tree_ready = None
        # Per-root last-known result of is_tree_complete(), so on_tree_ready can
        # be edge-triggered AND re-arm when a tree grows a new child.
        self._tree_complete: dict = {}
        # Consumer disconnect hook (SDK-al5), set via set_on_disconnect_callback.
        # Only effective when the controller OWNS its client (constructed from
        # mqtt_cfg); a bring-your-own-client caller registers disconnect handling
        # on its own client. Contract is transport-neutral: on_disconnect(clean).
        self._on_disconnect = None

        # Connect to broker
        self._connect_broker()

        if auto_start:
            self.start_discovery()

    @property
    def is_tree_rooted(self) -> bool:
        """True if this controller was created in tree-rooted mode (SDK-o1h)."""
        return self.root_device_id is not None

    def _connect_broker(self) -> None:
        """Connect to MQTT broker"""
        if self.mqttc:
            return

        client_id = f"homie-controller-{uuid.uuid4()}"
        try:
            # Bound to a local of the concrete type so start() resolves — self.mqttc is
            # MqttControllerTransport, which deliberately has no start(). Assignment order is
            # unchanged from before: both references are set before start(), so a
            # start() that raises leaves self.mqttc set exactly as it did previously.
            client = MqttClient.from_config(
                mqtt_cfg=self._mqtt_cfg,
                client_id=client_id,
                on_connect_callback=partial(self._on_connect),
                on_disconnect_callback=self._handle_disconnect,
            )
            self._owned_client = client
            self.mqttc = client
            client.start(blocking=False)
            logger.info(f"reason=controllerConnected,clientID={client_id}")
        except Exception as e:
            logger.exception(f"reason=controllerConnectException,error={e}")

    def _handle_disconnect(self, rc=None) -> None:
        """Transport disconnect handler for the controller's owned MQTT client.

        Invoked by ebus-mqtt-client with paho's integer reason code, which is
        absorbed here (logged, never surfaced); the consumer's transport-neutral
        ``on_disconnect(clean: bool)`` hook is notified (SDK-al5).
        """
        logger.info(f"reason=controllerDisconnect,transportRc={rc}")
        _dispatch_disconnect(self._on_disconnect, rc, "controller")

    @property
    def qos(self) -> int:
        """Returns the MQTT QoS level for this controller"""
        return self._qos

    def _on_connect(self) -> None:
        """Called when the controller's owned MQTT client (re-)connects.

        MqttClient re-subscribes its own sub_callbacks dict on reconnect, so
        topic-level recovery is already handled; the discovery-state reset for
        tree-rooted mode lives in the public ``resync()``, which a
        bring-your-own-transport caller wires onto its own client.
        """
        logger.info("reason=controllerOnConnect")
        self.resync()

    def resync(self) -> None:
        """Reset discovery bookkeeping so retained state re-walks the tree from scratch.

        In tree-rooted mode this wipes the in-memory device registry and the
        subscribed-children map and re-seeds the root, so the retained
        ``$state``/``$description`` the broker replays after a (re)connect
        drives a clean re-walk from the root (the state edge that gates
        descendant discovery is seen rather than short-circuited by stale
        state). A no-op in wildcard and single-device modes, where MqttClient's
        own sub_callbacks recovery is sufficient.

        The SDK calls this on every (re)connect for a client it owns. A
        bring-your-own-transport caller (``Controller(mqttc=...)``) in
        tree-rooted mode must call it from their own on-connect handler, since
        the SDK's ``on_connect`` is registered only on a client it constructs
        (via ``MqttClient.from_config``), which an injected client bypasses.
        """
        if self.is_tree_rooted:
            # The re-walk re-requests every descendant, paced, so pending work from the
            # previous connection is dropped. The root's filters were resubscribed by the
            # transport; watch it so a root whose retained messages are lost also heals.
            with self._pacing_lock:
                self._pending_subscriptions.clear()
                self._watches = {
                    self.root_device_id: _SubscriptionWatch(_DEVICE_FILTERS, time.monotonic(), holds_slot=True)
                }
            # Cold restart of tree-rooted bookkeeping. paho-mqtt's MqttClient
            # has already re-subscribed our root's four filters; retained
            # state/description will arrive momentarily and the state-edge
            # handler will reconcile descendants from scratch. Wipe the
            # in-memory device registry so the init→ready edge sees the
            # transition (the previous state would otherwise short-circuit it).
            self.devices = {}
            self._subscribed_children = {}
            device = DiscoveredDevice(self.root_device_id, self.homie_domain)
            self.devices[self.root_device_id] = device

    def start_discovery(self, homie_domain: Optional[str] = None) -> None:
        """
        Start auto-discovery of Homie devices

        Behavior depends on the constructor-selected mode:
        - root_device_id: tree-rooted — subscribe to root, then auto-subscribe
          to descendants on the root's init→ready edge (SDK-o1h).
        - device_id: single-device — subscribe to exact topics for that device.
        - neither: wildcard — subscribe to {domain}/5/+/$state.

        Args:
            homie_domain: Optional specific domain to monitor (default: uses instance domain)
        """
        if not self.mqttc:
            logger.error("reason=discoveryFailedNoConnection")
            return

        domain = homie_domain or self.homie_domain

        if self.is_tree_rooted:
            logger.info(f"reason=startDiscoveryTreeRooted,rootDeviceID={self.root_device_id}")
            # Pre-create the root entry; descendants are added as they're
            # discovered via the parent's $description.children
            device = DiscoveredDevice(self.root_device_id, domain)
            self.devices[self.root_device_id] = device
            self._request_subscription(self.root_device_id, _DEVICE_FILTERS)
        elif self.device_id:
            # Single-device mode: subscribe to exact topics, no wildcard
            # in the device-id position
            logger.info(f"reason=startDiscoverySingleDevice,deviceID={self.device_id}")
            # Pre-create the DiscoveredDevice entry
            device = DiscoveredDevice(self.device_id, domain)
            self.devices[self.device_id] = device
            self._request_subscription(self.device_id, _DEVICE_FILTERS)
        else:
            # Wildcard discovery mode (original behavior)
            discovery_topic = f"{domain}/{EBUS_HOMIE_VERSION_MAJOR}/+/$state"
            logger.info(f"reason=startDiscovery,topic={discovery_topic}")
            self.mqttc.subscribe(discovery_topic, param=self._on_state_message, qos=self._qos)

    def _subscribe_filters(self, device_id: str, filters: tuple) -> None:
        """Subscribe the given per-device filter suffixes for device_id, in order.

        Single-device and tree-rooted modes subscribe all four (_DEVICE_FILTERS).
        Wildcard discovery already hears $state through {domain}/5/+/$state and
        subscribes the other three (_WILDCARD_DEVICE_FILTERS).
        """
        if not self.mqttc:
            return
        base = f"{self.homie_domain}/{EBUS_HOMIE_VERSION_MAJOR}/{device_id}"
        callbacks = {
            "$state": self._on_state_message,
            "$description": partial(self._on_description_message, device_id),
            "+/+": partial(self._on_property_message, device_id),
            "+/+/$target": partial(self._on_target_message, device_id),
        }
        for suffix in filters:
            self.mqttc.subscribe(f"{base}/{suffix}", param=callbacks[suffix], qos=self._qos)

    def _unsubscribe_filters(self, device_id: str, filters: tuple) -> None:
        if not self.mqttc:
            return
        base = f"{self.homie_domain}/{EBUS_HOMIE_VERSION_MAJOR}/{device_id}"
        for suffix in filters:
            self.mqttc.unsubscribe(f"{base}/{suffix}")

    # ── Subscription pacing and stuck-device healing (GH #97) ──────────────

    def _slot_free_locked(self) -> bool:
        if self._subscription_batch_size is None:
            return True
        in_use = sum(1 for w in self._watches.values() if w.holds_slot)
        return in_use < self._subscription_batch_size

    def _take_pending_locked(self) -> list:
        """Move pending devices into free batch slots; the caller subscribes them unlocked."""
        started = []
        while self._pending_subscriptions and self._slot_free_locked():
            device_id = next(iter(self._pending_subscriptions))
            filters = self._pending_subscriptions.pop(device_id)
            self._watches[device_id] = _SubscriptionWatch(filters, time.monotonic(), holds_slot=True)
            started.append((device_id, filters))
        return started

    def _start_subscriptions(self, started: list) -> None:
        for device_id, filters in started:
            logger.info(f"reason=pacedSubscribe,deviceID={device_id},pending={len(self._pending_subscriptions)}")
            self._subscribe_filters(device_id, filters)

    def _request_subscription(self, device_id: str, filters: tuple) -> None:
        """Subscribe a device's filters now if a batch slot is free, else queue it in order."""
        with self._pacing_lock:
            if device_id in self._watches or device_id in self._pending_subscriptions:
                return
            self._pending_subscriptions[device_id] = filters
            started = self._take_pending_locked()
            if device_id in self._pending_subscriptions:
                logger.debug(
                    f"reason=subscriptionDeferred,deviceID={device_id},pending={len(self._pending_subscriptions)}"
                )
        self._start_subscriptions(started)

    def _note_device_progress(self, device_id: str) -> None:
        """Stop watching a device once it has both $state and $description; free its slot."""
        device = self.devices.get(device_id)
        with self._pacing_lock:
            if device_id not in self._watches:
                return
            if device is not None and (device.state is None or device.description is None):
                return
            del self._watches[device_id]
            started = self._take_pending_locked()
        self._start_subscriptions(started)

    def _forget_subscription(self, device_id: str) -> bool:
        """Drop a device from pacing; True when it was still pending (never subscribed)."""
        with self._pacing_lock:
            was_pending = self._pending_subscriptions.pop(device_id, None) is not None
            self._watches.pop(device_id, None)
            started = self._take_pending_locked()
        self._start_subscriptions(started)
        return was_pending

    def check_stuck_children(self, timeout: Optional[float] = None) -> List[str]:
        """Heal subscribed devices whose retained $state or $description never arrived (GH #97).

        A device counts as stuck when its filters have been subscribed for at
        least ``timeout`` seconds (default: the constructor's
        ``stuck_device_timeout``) and it still lacks ``$state`` or
        ``$description``. A stuck device gives up its batch slot, so the next
        pending device is subscribed, and its filters are unsubscribed and
        resubscribed so the broker resends its retained messages. After
        ``max_resubscribe_attempts`` resubscribes it is logged as unrecoverable
        and no longer checked; its subscriptions stay in place, so a device
        that appears later is still discovered.

        Message handling calls this at most once a second when
        ``stuck_device_timeout`` is set, so a controller receiving traffic heals
        without help. On a quiet connection nothing arrives to trigger it: call
        it from your own timer or event loop. It uses no threads and is safe to
        call from any thread.

        Returns the ids of the devices resubscribed by this call.
        """
        if timeout is None:
            timeout = self._stuck_device_timeout
        if timeout is None:
            return []
        now = time.monotonic()
        resubscribe = []
        with self._pacing_lock:
            self._last_stuck_check = now
            for device_id, watch in list(self._watches.items()):
                if now - watch.subscribed_at < timeout:
                    continue
                device = self.devices.get(device_id)
                if device is None:
                    del self._watches[device_id]
                    continue
                missing = [
                    name for name, v in (("$state", device.state), ("$description", device.description)) if v is None
                ]
                if not missing:
                    del self._watches[device_id]
                    continue
                watch.holds_slot = False
                if watch.attempts >= self._max_resubscribe_attempts:
                    del self._watches[device_id]
                    logger.error(
                        f"reason=stuckDeviceGaveUp,deviceID={device_id},missing={'+'.join(missing)},"
                        f"attempts={watch.attempts}"
                    )
                    continue
                watch.attempts += 1
                watch.subscribed_at = now
                resubscribe.append((device_id, watch.filters, watch.attempts, missing))
            started = self._take_pending_locked()

        for device_id, filters, attempt, missing in resubscribe:
            logger.warning(
                f"reason=stuckDeviceResubscribe,deviceID={device_id},missing={'+'.join(missing)},"
                f"attempt={attempt}/{self._max_resubscribe_attempts}"
            )
            self._unsubscribe_filters(device_id, filters)
            self._subscribe_filters(device_id, filters)
        self._start_subscriptions(started)
        return [device_id for device_id, *_ in resubscribe]

    def _maybe_check_stuck(self) -> None:
        """Opportunistic, rate-limited stuck check run from message handling."""
        timeout = self._stuck_device_timeout
        if timeout is None or not self._watches:
            return
        if time.monotonic() - self._last_stuck_check < min(_STUCK_CHECK_INTERVAL, timeout):
            return
        self.check_stuck_children()

    def _on_state_message(self, topic: str, payload: bytes) -> None:
        """
        Handle device $state messages

        Topic format: {domain}/5/{device_id}/$state
        Payload: init, ready, disconnected, sleeping, lost, or empty (device removal)
        """
        self._maybe_check_stuck()
        parts = topic.split("/")
        if len(parts) != 4 or parts[3] != "$state":
            logger.warning(f"reason=invalidStateTopic,topic={topic}")
            return

        homie_domain = parts[0]
        device_id = parts[2]

        # Decode payload
        payload_str = payload.decode("utf-8") if isinstance(payload, bytes) else payload

        # Empty payload indicates device removal
        if not payload_str or len(payload_str) == 0:
            logger.info(f"reason=deviceRemoved,deviceID={device_id}")
            self._forget_subscription(device_id)
            if device_id in self.devices:
                removed_device = self.devices[device_id]
                del self.devices[device_id]
                if self._on_device_removed:
                    self._on_device_removed(removed_device)
            return

        # New or existing device
        if device_id not in self.devices:
            # New device discovered (wildcard mode only; single-device mode
            # and tree-rooted mode pre-create their entries in start_discovery)
            logger.info(
                f"reason=deviceDiscovered,deviceID={device_id},state={payload_str},knownDevices={list(self.devices.keys())}"
            )
            device = DiscoveredDevice(device_id, homie_domain)
            old_state = None
            device.update_state(payload_str)
            self.devices[device_id] = device

            # Subscribe to device's $description and all properties, paced
            self._request_subscription(device_id, _WILDCARD_DEVICE_FILTERS)

            if self._on_device_discovered:
                self._on_device_discovered(device)
        elif self.devices[device_id].state is None:
            # Pre-created entry (single-device or tree-rooted mode):
            # first $state message
            device = self.devices[device_id]
            old_state = None
            device.update_state(payload_str)
            mode = "treeRooted" if self.is_tree_rooted else "singleDevice"
            logger.info(f"reason=deviceDiscovered,deviceID={device_id},state={payload_str},mode={mode}")
            if self._on_device_discovered:
                self._on_device_discovered(device)
        else:
            # Existing device state changed
            device = self.devices[device_id]
            old_state = device.state

            # Only trigger callback if state actually changed
            if old_state != payload_str:
                device.update_state(payload_str)
                logger.info(
                    f"reason=deviceStateChanged,deviceID={device_id},oldState={old_state},newState={payload_str}"
                )
                if self._on_device_state_changed:
                    self._on_device_state_changed(device, old_state, payload_str)
            else:
                # Still update last_seen even if state didn't change
                device.update_state(payload_str)
                logger.debug(f"reason=deviceStateRefreshed,deviceID={device_id},state={payload_str}")

        # Tree-rooted mode: any device transitioning INTO ready is the trust
        # signal to act on its $description.children. Per Homie 5, only the
        # init→ready edge guarantees a consistent description; mid-flight
        # description updates while state=init are stashed but not acted on.
        if self.is_tree_rooted and payload_str == DeviceState.READY.value and old_state != DeviceState.READY.value:
            self._reconcile_descendants(device_id)

        self._note_device_progress(device_id)

    def _reconcile_descendants(self, device_id: str) -> None:
        """Diff a device's announced children against what's subscribed (SDK-o1h).

        Fired on the init→ready edge in tree-rooted mode. Added children get
        full topic subscriptions (their own retained $state/$description then
        cascade through this same handler, surfacing grandchildren). Removed
        children are unsubscribed and dropped from the registry recursively.

        The state-edge gate (in _on_state_message) guarantees we only act when
        $state=ready confirms the description is current — never on a partial
        view stashed during $state=init.
        """
        if not self.mqttc:
            return
        device = self.devices.get(device_id)
        if device is None:
            return

        declared = set(device.children_ids)
        current = set(self._subscribed_children.get(device_id, set()))

        added = declared - current
        removed = current - declared

        # Declared order, so paced subscriptions follow the description's order.
        for child_id in [c for c in dict.fromkeys(device.children_ids) if c in added]:
            logger.info(f"reason=treeRootedAddDescendant,parentID={device_id},childID={child_id}")
            # Pre-create child entry; its own retained $state/$description
            # arrive via the new subscriptions and drive update + cascade.
            if child_id not in self.devices:
                self.devices[child_id] = DiscoveredDevice(child_id, self.homie_domain)
            self._subscribed_children.setdefault(device_id, set()).add(child_id)
            self._request_subscription(child_id, _DEVICE_FILTERS)

        for child_id in removed:
            logger.info(f"reason=treeRootedRemoveDescendant,parentID={device_id},childID={child_id}")
            self._unsubscribe_and_drop(child_id)

    def _unsubscribe_and_drop(self, device_id: str) -> None:
        """Recursively drop a descendant and all of its own descendants (SDK-o1h).

        Unsubscribes the four topic filters, removes the entry from the
        registry, and fires on_device_removed (leaves-first so callbacks see a
        consistent view: when fired for a parent, its children are already
        gone). No-op if the device isn't tracked.
        """
        if device_id not in self.devices:
            return
        # Snapshot before mutating: collect this device's transitive
        # descendants from our subscription registry (the authoritative record
        # of what we subscribed for; doesn't depend on the child's own
        # description having arrived). Recurse leaves-first.
        children = list(self._subscribed_children.get(device_id, set()))
        for child_id in children:
            self._unsubscribe_and_drop(child_id)

        # This device is no longer a parent in our tree
        self._subscribed_children.pop(device_id, None)
        # Remove this device from any parent's subscribed-children set
        for siblings in self._subscribed_children.values():
            siblings.discard(device_id)

        device = self.devices.pop(device_id, None)
        if device is None:
            return

        # A device still waiting for a batch slot was never subscribed.
        if not self._forget_subscription(device_id):
            self._unsubscribe_filters(device_id, _DEVICE_FILTERS)

        if self._on_device_removed:
            try:
                self._on_device_removed(device)
            except Exception:
                logger.exception(f"reason=onDeviceRemovedCallbackException,deviceID={device_id}")

        # Dropping a descendant shrinks the declared tree, which can complete a
        # root that was waiting on the device just removed.
        self._tree_complete.pop(device_id, None)
        self._reevaluate_tree_completeness()

    def _on_description_message(self, device_id: str, topic: str, payload: bytes) -> None:
        """Handle device $description messages"""
        self._maybe_check_stuck()
        if device_id not in self.devices:
            return

        payload_str = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        device = self.devices[device_id]
        device.update_description(payload_str)

        logger.info(f"reason=descriptionReceived,deviceID={device_id}")
        if self._on_description_received:
            self._on_description_received(device)

        # SDK-gsn: on initial connect with retained state+description, $state
        # often arrives before $description (we subscribe to $state first, and
        # paho delivers in subscription order). The state-edge reconcile in
        # _on_state_message then sees an empty children list and subscribes to
        # nothing. Catch the late-arriving description here: when the device
        # is already ready, run reconcile against the now-current description.
        # Idempotent — a no-op when children are already subscribed, so safe
        # in the design-intended order (description-then-state) too.
        if self.is_tree_rooted and device.state == DeviceState.READY.value:
            self._reconcile_descendants(device_id)

        # A description is the only thing that can complete a tree (it is what
        # "described" means) and also the only thing that can un-complete one
        # (by declaring a new child). Re-evaluate after reconcile, so any child
        # this description just introduced is already registered.
        self._reevaluate_tree_completeness()

        self._note_device_progress(device_id)

    def _on_property_message(self, device_id: str, topic: str, payload: bytes) -> None:
        """
        Handle property value messages

        Topic format: {domain}/5/{device_id}/{node_id}/{property_id}
        Skip $target topics (handled separately)
        """
        self._maybe_check_stuck()
        # Skip $target topics
        if topic.endswith("/$target"):
            return

        parts = topic.split("/")
        if len(parts) != 5:
            return

        node_id = parts[3]
        property_id = parts[4]

        # Skip attribute topics (starting with $)
        if property_id.startswith("$"):
            return

        if device_id not in self.devices:
            return

        payload_str = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        # Homie 5: a single 0x00 byte denotes an empty-string value (a truly
        # zero-length payload would instead be a retained-topic clear).
        payload_str = decode_empty_string(payload_str)
        device = self.devices[device_id]
        old_value = device.get_property(node_id, property_id)
        device.update_property(node_id, property_id, payload_str)

        logger.debug(
            f"reason=propertyChanged,deviceID={device_id},node={node_id},property={property_id},value={payload_str}"
        )
        if self._on_property_changed:
            self._on_property_changed(device_id, node_id, property_id, payload_str, old_value)

    def _on_target_message(self, device_id: str, topic: str, payload: bytes) -> None:
        """
        Handle property $target messages

        Topic format: {domain}/5/{device_id}/{node_id}/{property_id}/$target
        """
        self._maybe_check_stuck()
        parts = topic.split("/")
        if len(parts) != 6 or parts[5] != "$target":
            return

        node_id = parts[3]
        property_id = parts[4]

        if device_id not in self.devices:
            return

        payload_str = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        payload_str = decode_empty_string(payload_str)
        device = self.devices[device_id]
        device.update_property_target(node_id, property_id, payload_str)

        logger.debug(
            f"reason=targetChanged,deviceID={device_id},node={node_id},property={property_id},target={payload_str}"
        )

    def set_property(
        self,
        device_id: str,
        node_id: str,
        property_id: str,
        value: str,
        qos: Optional[int] = None,
    ) -> bool:
        """
        Send a command to set a device property

        Publishes to: {domain}/5/{device_id}/{node_id}/{property_id}/set
        Uses non-retained messages as per Homie convention

        Args:
            device_id: Target device ID
            node_id: Target node ID
            property_id: Target property ID
            value: Value to set (as string)
            qos: MQTT QoS level (default: controller's QoS)

        Returns:
            True if message was sent successfully, False otherwise
        """
        if not self.mqttc:
            logger.error("reason=setPropertyFailedNoConnection")
            return False

        effective_qos = qos if qos is not None else self._qos
        set_topic = f"{self.homie_domain}/{EBUS_HOMIE_VERSION_MAJOR}/{device_id}/{node_id}/{property_id}/set"

        logger.info(f"reason=settingProperty,topic={set_topic},value={value}")
        try:
            # Non-retained message as per convention. Encode an empty-string
            # value as a single 0x00 byte (Homie 5) so the device's /set handler
            # receives "" rather than a zero-length payload.
            payload = encode_empty_string(value)
            self.mqttc.publish(set_topic, payload, qos=effective_qos, retain=False)
            return True
        except Exception as e:
            logger.error(f"reason=setPropertyException,error={e}")
            return False

    def set_property_json(
        self,
        device_id: str,
        node_id: str,
        property_id: str,
        obj: Any,
        *,
        validate: bool = True,
        qos: Optional[int] = None,
    ) -> bool:
        """Publish a JSON command to a settable ``json`` property's ``/set`` topic.

        Serializes `obj` (a dict/list) to JSON and sends it via `set_property`.
        When `validate` is True and the target property advertises a `$format`
        JSONSchema in the discovered `$description`, the command is validated
        against that schema first; an invalid command is NOT sent (returns
        False). Validation is graceful: with no schema (or without the optional
        `jsonschema` package) it is skipped. Returns True if the command was
        sent. Use this for settable json control surfaces such as `flex/request`.
        """
        if validate:
            device = self.devices.get(device_id)
            format_schema = None
            if device is not None:
                props = device.get_node_properties(node_id)
                if isinstance(props, dict):
                    format_schema = props.get(property_id, {}).get("format")
            error = validate_json_format(obj, format_schema)
            if error is not None:
                logger.warning(
                    f"reason=setPropertyJsonRejectedSchemaInvalid,deviceID={device_id},property={property_id},error={error}"
                )
                return False
        try:
            payload = json.dumps(obj)
        except (TypeError, ValueError) as e:
            logger.error(f"reason=setPropertyJsonEncodeError,deviceID={device_id},property={property_id},error={e}")
            return False
        return self.set_property(device_id, node_id, property_id, payload, qos=qos)

    def broadcast(self, subtopic: str, message: str, qos: Optional[int] = None) -> bool:
        """
        Broadcast a message to all Homie devices

        Publishes to: {domain}/5/$broadcast/{subtopic}

        Args:
            subtopic: Broadcast subtopic (can be multi-level)
            message: Message payload
            qos: MQTT QoS level (default: controller's QoS)

        Returns:
            True if message was sent successfully, False otherwise
        """
        if not self.mqttc:
            logger.error("reason=broadcastFailedNoConnection")
            return False

        effective_qos = qos if qos is not None else self._qos
        broadcast_topic = f"{self.homie_domain}/{EBUS_HOMIE_VERSION_MAJOR}/$broadcast/{subtopic}"

        logger.info(f"reason=broadcasting,topic={broadcast_topic}")
        try:
            self.mqttc.publish(broadcast_topic, message, qos=effective_qos, retain=False)
            return True
        except Exception as e:
            logger.error(f"reason=broadcastException,error={e}")
            return False

    def get_device(self, device_id: str) -> Optional[DiscoveredDevice]:
        """Get a discovered device by ID"""
        return self.devices.get(device_id)

    def get_all_devices(self) -> dict:
        """Get all discovered devices"""
        return self.devices.copy()

    def get_root_devices(self) -> List[DiscoveredDevice]:
        """
        Return all discovered devices that are tree roots (parent_id is None).

        Useful for traversal: walk each root, then descend via get_children().
        """
        return [d for d in self.devices.values() if d.is_root]

    def get_root(self, device_id: str) -> Optional[DiscoveredDevice]:
        """
        Return the root DiscoveredDevice for the tree containing device_id.

        Reads the device's description.root field (which on a non-root device
        always points at the top of the tree per the Homie 5 spec). Returns
        None if the device isn't discovered, or if the named root device
        isn't (yet) in the controller's registry.
        """
        device = self.devices.get(device_id)
        if device is None:
            return None
        return self.devices.get(device.root_id)

    def get_children(self, device_id: str) -> List[DiscoveredDevice]:
        """
        Return discovered children of device_id (immediate, not descendants).

        Children IDs come from the parent's description.children. A listed ID
        that hasn't yet published its own $state is omitted — callers can
        register a discovery callback to react when it arrives.
        """
        device = self.devices.get(device_id)
        if device is None:
            return []
        return [self.devices[cid] for cid in device.children_ids if cid in self.devices]

    def get_descendants(self, device_id: str) -> List[DiscoveredDevice]:
        """
        Return all discovered descendants of device_id in breadth-first order.
        Does not include device_id itself.
        """
        out: List[DiscoveredDevice] = []
        queue: List[str] = [device_id]
        seen = {device_id}
        while queue:
            current = queue.pop(0)
            for child in self.get_children(current):
                if child.device_id in seen:
                    continue
                seen.add(child.device_id)
                out.append(child)
                queue.append(child.device_id)
        return out

    def is_tree_complete(self, root_id: str) -> bool:
        """
        True when every device transitively declared under ``root_id`` has
        published its own ``$description``.

        This is a RECONCILING PREDICATE, not a barrier. It reads current state
        and is safe to call at any time, as often as you like; it will flip back
        to False when a device declares a new child, because a Homie tree can
        grow at any moment (children are commissioned out of band). Consumers
        that need a "the tree I can see is coherent" gate should evaluate this on
        every update, not await it once. See doc/consuming-a-homie-tree.md.

        Specifically NOT the same as ``root $state == ready``. That is a
        per-device signal meaning "my own $description is current"; it never
        promised anything about descendants and cannot be made to. This walks
        the declared tree and checks.

        A device counts as described once its ``$description`` has been parsed,
        regardless of its ``$state``: a declared child that is `lost` has still
        told you what it is. Use ``get_effective_state()`` for liveness.

        Returns False when ``root_id`` is unknown or has published no
        ``$description`` of its own (nothing has declared a tree yet).
        """
        root = self.devices.get(root_id)
        if root is None or root.description is None:
            return False

        # Breadth-first over DECLARED children, with a visited set: a malformed
        # or mid-reconfiguration tree can name a cycle, and this must terminate
        # rather than trusting the wire.
        seen = {root_id}
        queue = list(root.children_ids)
        while queue:
            child_id = queue.pop(0)
            if child_id in seen:
                continue
            seen.add(child_id)
            child = self.devices.get(child_id)
            if child is None or child.description is None:
                return False
            queue.extend(child.children_ids)
        return True

    def _reevaluate_tree_completeness(self) -> None:
        """Fire on_tree_ready for any root that just became complete.

        Edge-triggered on the incomplete -> complete transition, and re-arming:
        a root that grows a new child goes back to incomplete and will fire
        again once that child describes itself. That re-arming is the point.
        A one-shot barrier is the exact consumer bug this API exists to prevent,
        so this must not be one either.

        Called after every $description update and after a descendant is
        dropped, which are the only things that can change the predicate.
        """
        if not self._on_tree_ready:
            return
        for root in self.get_root_devices():
            root_id = root.device_id
            now_complete = self.is_tree_complete(root_id)
            was_complete = self._tree_complete.get(root_id, False)
            self._tree_complete[root_id] = now_complete
            if now_complete and not was_complete:
                logger.info(
                    f"reason=treeComplete,rootID={root_id},deviceCount={len(self.get_descendants(root_id)) + 1}"
                )
                try:
                    self._on_tree_ready(root)
                except Exception:
                    logger.exception(f"reason=onTreeReadyCallbackException,rootID={root_id}")

    def get_effective_state(self, device_id: str) -> Optional[str]:
        """
        Return the device's effective state per the Homie 5 spec (SDK-zt2).

        For a root device: returns its own reported state.

        For a child: applies HOMIE_EFFECTIVE_STATE_TABLE — when the root is in a
        non-ready state (init/disconnected/sleeping/lost), that state propagates
        down the tree. Only when the root is ready do children's own states stand.

        Returns None when the device isn't discovered. Returns the device's own
        state (best-effort) when the device's named root isn't yet discovered —
        Homie 5's "root may be missing" case is rare in practice but worth
        handling cleanly.
        """
        device = self.devices.get(device_id)
        if device is None:
            return None
        if device.is_root:
            return device.state
        root = self.get_root(device_id)
        if root is None or root.state is None:
            return device.state
        override = HOMIE_EFFECTIVE_STATE_TABLE.get(root.state)
        return override if override is not None else device.state

    def stop(self) -> None:
        """Stop the controller, release resources, and disconnect from broker.

        A SDK-owned client is stopped here as before. An injected
        (bring-your-own-transport) client is NOT stopped: its lifecycle belongs
        to the caller; the controller only drops its reference.
        """
        if self.mqttc:
            logger.info(f"reason=stoppingController,deviceCount={len(self.devices)}")
            # Stops via the owned handle, never via self.mqttc: an injected client has no
            # stop() in its contract, and _owned_client is None precisely when one was
            # injected. Same condition as before — _owns_client still decides.
            if self._owns_client and self._owned_client is not None:
                self._owned_client.stop()
            self.mqttc = None
            self._owned_client = None
        # Release DiscoveredDevice objects and their property dicts
        self.devices.clear()
        self._subscribed_children.clear()
        self._tree_complete.clear()
        with self._pacing_lock:
            self._watches.clear()
            self._pending_subscriptions.clear()
        # Clear callback references to break reference cycles
        self._on_device_discovered = None
        self._on_device_state_changed = None
        self._on_device_removed = None
        self._on_property_changed = None
        self._on_description_received = None
        self._on_tree_ready = None

    # Callback setters
    def set_on_device_discovered_callback(self, callback: Callable[[DiscoveredDevice], None]) -> None:
        """Set callback for when a new device is discovered"""
        self._on_device_discovered = callback

    def set_on_device_state_changed_callback(self, callback: Callable[[DiscoveredDevice, str, str], None]) -> None:
        """Set callback for when a device state changes (device, old_state, new_state)"""
        self._on_device_state_changed = callback

    def set_on_device_removed_callback(self, callback: Callable[[DiscoveredDevice], None]) -> None:
        """Set callback for when a device is removed"""
        self._on_device_removed = callback

    def set_on_property_changed_callback(self, callback: Callable[[str, str, str, str, Optional[str]], None]) -> None:
        """Set callback for property changes (device_id, node_id, property_id, new_value, old_value)"""
        self._on_property_changed = callback

    def set_on_tree_ready_callback(self, callback: Callable[[DiscoveredDevice], None]) -> None:
        """Fire when a root's declared tree becomes fully described.

        Called with the ROOT DiscoveredDevice on the incomplete -> complete
        edge of ``is_tree_complete(root_id)``. It RE-ARMS: a root that grows a
        new child goes back to incomplete and fires again once that child
        describes itself, so a tree commissioned in stages produces one call per
        settled shape rather than one call ever.

        That re-arming is deliberate. Treating the first call as a barrier and
        unsubscribing afterwards reintroduces the bug this exists to avoid: a
        device commissioned later is simply missed. Handle every call.
        """
        self._on_tree_ready = callback

    def set_on_description_received_callback(self, callback: Callable[[DiscoveredDevice], None]) -> None:
        """Set callback for when a device description is received"""
        self._on_description_received = callback

    def set_on_disconnect_callback(self, callback: Callable[[bool], None]) -> None:
        """Set callback for when the controller's MQTT connection drops (SDK-al5).

        The callback receives a single ``clean: bool`` argument: True for an
        orderly/expected disconnect (e.g. stop()), False for an unexpected drop.
        The transport (paho) reason code is normalized to this boolean at the SDK
        boundary and never surfaced, so no paho type/value leaks into consumers.
        Best-effort (a callback exception is logged, not propagated). Effective
        only when the controller owns its client (constructed from mqtt_cfg); a
        bring-your-own-client caller registers disconnect handling on its client.
        """
        self._on_disconnect = callback
