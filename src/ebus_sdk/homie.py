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
* Support/handle empty string values:
    MQTT will treat an empty string payload as a “delete” instruction for the topic,
    therefore an empty string value is represented by a 1-character string containing a single byte value 0 (Hex: 0x00, Dec: 0).
    The empty string (passed as an MQTT payload) can only occur in 3 places;
        homie / 5 / [device ID] / [node ID] / [property ID]; reported property values (for string types)
        homie / 5 / [device ID] / [node ID] / [property ID] / set; the topic to set properties (of string types)
        homie / 5 / [device ID] / [node ID] / [property ID] / $target; the target property value (for string types)
    This convention specifies no way to represent an actual value of a 1-character string with a single byte 0.
    If a device needs this, then it should provide an escape mechanism on the application level.
* Given that Nodes and Properties belong to, and contain pointers to, the owning Device (and Node, for Properties),
    seems likely that we can leverage that to obtain the MQTT client (mqttc) of the owning Device, instead of
    having all downstream entities maintain a local pointer to that
"""

import asyncio
import json
import logging
import os
import time
import uuid
from enum import Enum

try:
    from enum import StrEnum
except ImportError:
    # Python < 3.11 compatibility
    class StrEnum(str, Enum):
        pass


from functools import partial

# from deprecated import deprecated
from typing import Any, Callable, List, Optional, Type, Union
from .mqtt import MqttClient

# FIXME debug only?
from pprint import pformat

logger = logging.getLogger("homie")
logger.setLevel(logging.INFO)

# eBus MQTT topic constants
EBUS_HOMIE_DOMAIN = "ebus"
EBUS_HOMIE_VERSION_MAJOR = 5
EBUS_HOMIE_VERSION_MINOR = 0
EBUS_HOMIE_VERSION_PATCH = 0
EBUS_HOMIE_MQTT_QOS_DEFAULT = "2"

EBUS_HOMIE_MQTT_QOS = int(os.environ.get("EBUS_HOMIE_MQTT_QOS_SITE", EBUS_HOMIE_MQTT_QOS_DEFAULT))

if EBUS_HOMIE_MQTT_QOS < 1:
    logger.warning(
        f"reason=homieQosLessThanOne,specifiedQos={EBUS_HOMIE_MQTT_QOS},defaultQos={EBUS_HOMIE_MQTT_QOS_DEFAULT}"
    )

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
    # VOLT_AMPERE_REACTIVE not in Homie specification, but we need it
    # https://github.com/homieiot/convention/issues/318
    VOLT_AMPERE_REACTIVE = "var"
    WATT_HOUR = "Wh"


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
        async_loop: Optional[asyncio.SelectorEventLoop] = False,
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
        self._settable = settable
        # Don't assign set_callback unless this property is settable
        if settable:
            self._set_callback = set_callback
        else:
            self._set_callback = None
        self._retained = retained
        self._unit = unit
        self._supports_target = supports_target
        self._node = node
        self._device = device
        self.async_loop = async_loop
        # QoS for MQTT operations (may be overridden by Device when adopted)
        self._qos = EBUS_HOMIE_MQTT_QOS
        # Track whether this property has ever been published (FIX for MQTT topic persistence)
        self._ever_published = False
        self._initial_value_was_none = value is None
        # Check for skip_initial_publish flag from dict
        self._skip_initial_publish = from_dict.get("skip_initial_publish", False) if from_dict else False

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
        Set the property's value to value, and publishes the new value to MQTT
        Returns False on failure, else True
        """
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
        if round_to:
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

    def coerced_value(self) -> Optional[str]:
        """
        Returns the property's value (potentially rounded), as a string.
        Returns None if the value is invalid or cannot be coerced.
        """
        property_value = self.value()
        if property_value is None:
            return None

        property_type = self.datatype()
        if property_type == PropertyDatatype.BOOLEAN:
            if not isinstance(property_value, bool):
                logger.warning(f"reason=coercedValueInvalidBoolean,propertyId={self._id},value={property_value}")
                return None
            return str(property_value).lower()

        # For enum values, use .value to get the underlying value
        if isinstance(property_value, Enum):
            return str(property_value.value)

        return str(property_value)

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

    def get_mqtt_client(self) -> MqttClient:
        """
        Who calls this function, and why?
        """
        node = self.node()
        if not node:
            logger.warning(f"reason=propertyGetMqttClientNoNode,propertyID={self._id}")
            return None
        mqttc = node.get_mqtt_client()
        if not mqttc:
            logger.warning(f"reason=propertyGetMqttClientNoMqttClient,propertyID={self._id}")
        return mqttc

    def start_mqtt_client(self) -> None:
        """
        Who calls this function, and why?
        """
        mqttc = self.get_mqtt_client()
        if not mqttc:
            logger.warning(f"reason=propertyStartMqttClientNoMqttClient,propertyID={self._id}")
            return
        try:
            if not mqttc.is_running:
                mqttc.start()
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
        """
        logger.info(f"reason=propertyPublishTargetValue,propertyID={self._id},value={payload}")
        logger.warning(f"reason=propertyPublishTargetValueNotImplemented,propertyID={self._id},value={payload}")

    def publish_value(self) -> bool:
        """
        Publishes the property's value to Homie/eBus broker
        """
        mqttc = self.get_mqtt_client()
        if not mqttc or not mqttc.is_running:
            logger.warning(f"reason=propertyPublishValueNoMqttClient,id={self._id}")
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
        topic = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/{device_id}/{node_id}/{self._id}"
        if self._value is None:
            logger.debug(
                f"reason=propertyPublishValueIsNone,deviceID={device_id},nodeID={node_id},propertyID={self._id}"
            )
            return False
        try:
            value = self.coerced_value()
            if value is None:
                logger.warning(
                    f"reason=propertyPublishValueCoercionFailed,propertyID={self._id},rawValue={self._value}"
                )
                return False
            logger.debug(f"reason=propertyPublishValue,value={value},topic={topic},retained={self.retained()}")
            mqttc.publish(topic, value, retain=self.retained(), qos=self._qos)
            self._ever_published = True  # FIX: Mark as published
            self._skip_initial_publish = False  # FIX: Clear skip flag after first publish
            return True
        except Exception as e:
            logger.warning(f"reason=propertyPublishValuePublishException,e={e}")
            return False

    def clear_value(self) -> bool:
        """
        Clear the property's value by publishing null/empty to its topic
        Returns True on success, else False
        """
        # FIX: Don't clear if we never published a value
        # This prevents creating phantom topics during cleanup
        if not self._ever_published:
            logger.info(f"reason=propertySkipClearNeverPublished,propertyID={self._id}")
            return True

        mqttc = self.get_mqtt_client()
        if not mqttc or not mqttc.is_running:
            logger.warning(f"reason=propertyClearValueNoMqttClient,propertyID={self._id}")
            return False
        node_id = self.get_node_id()
        device_id = self.get_device_id()
        if not (device_id and node_id):
            logger.warning(
                f"reason=propertyClearValueInsufficientIDs,deviceID={device_id},nodeID={node_id},propertyID={self._id}"
            )
            return False
        topic = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/{device_id}/{node_id}/{self._id}"
        try:
            # Publishing empty string clears retained message
            mqttc.publish(topic, "", retain=True, qos=self._qos)
            logger.info(f"reason=propertyClearedValue,propertyID={self._id},topic={topic}")
            self._ever_published = False  # FIX: Reset the flag
            return True
        except Exception as e:
            logger.warning(f"reason=propertyClearValueException,propertyID={self._id},topic={topic},exception={e}")
            return False

    def was_ever_published(self) -> bool:
        """Return whether this property has ever been published to MQTT (FIX for MQTT topic persistence)"""
        return self._ever_published

    def get_last_published_value(self) -> Any:
        """Return the last published value (currently just returns current value)"""
        return self.value()

    def description(self) -> dict:
        """
        Returns a dict containing the Homie 5 $description of the Property
        """
        logger.info(f"reason=propertyDescriptionEntered,id={self._id}")
        property = dict()
        property["name"] = self._name
        property["datatype"] = self.datatype()
        if self._format:
            property["format"] = self.format()
        if self._settable:
            property["settable"] = self._settable
        if not self._retained:
            property["retained"] = self._retained
        if self._unit:
            property["unit"] = self._unit
        return property

    def _settable_callback(self, topic: str, payload: Union[bytes, bytearray]) -> None:
        """
        For each settable property, there is a property/set topic that can be published to
        This is the callback for the subscription to each such property/set topic
        Examples:
        [homieDomain]/[homieVerson]/[deviceID]/[nodeID]/mode/set
        [homieDomain]/[homieVerion]/[deviceID]/[nodeID]/setpoint/set
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
            (homie_domain == EBUS_HOMIE_DOMAIN)
            and (homie_version == str(EBUS_HOMIE_VERSION_MAJOR))
            and (property_id_set == "set")
        ):
            logger.debug(f"reason=nodeSetCallbackInvalidTopic,topic={topic}")
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
            decoded_payload = payload.decode("utf-8")  # do we need to str() this?
            if self.is_json_datatype():
                payload = json.loads(decoded_payload)
            else:
                payload = decoded_payload
            # We have the payload
            logger.debug(
                f"reason=propertySetCallbackValue,propertyID={property_id},payload={payload},callback={set_callback}"
            )
            if self.supports_target():
                # Property supports_target, publish that!
                self.publish_target_value(payload)
            # Call the property's set_callback function
            if self.async_loop:
                asyncio.ensure_future(set_callback(payload), loop=self.async_loop)
            else:
                set_callback(payload)
        except Exception as e:
            logger.exception(f"reason=propertySetCallbackException,e={e}")

    def set_subscribe(self) -> None:
        """
        Subscribe to property/set topic on Homie broker
        TODO: Not sure why this is a public method...
        """
        logger.debug(f"reason=propertySetSubscribe,id={self._id}")
        mqttc = self.get_mqtt_client()
        if not mqttc:
            logger.warning("reason=propertySetSubscribeNoMqttClient")
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
        topic = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/{device_id}/{node_id}/{self._id}/set"
        try:
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
        properties: dict = {},
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
            self._properties = properties
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

    def set_device(self, device: Device) -> None:
        self._device = device

    def get_mqtt_client(self) -> MqttClient:
        device = self.device()
        if not device:
            logger.warning(f"reason=nodeGetMqttClientNoDevice,nodeID={self._id}")
            return None
        mqttc = device.get_mqtt_client()
        if not mqttc:
            logger.warning(f"reason=nodeGetMqttClientNoMqttClient,nodeID={self._id}")
        return mqttc

    def add_property(self, property: Property) -> Property:
        """
        Adds the property to properties, and returns property
        """
        if not property.node():
            property.set_node(self)
        # Propagate QoS from device if available
        if self._device and hasattr(self._device, "_qos"):
            property._qos = self._device._qos
        # Note set_subscribe() checks if property is settable...
        property.set_subscribe()
        # Add property to dictionary BEFORE publishing description
        self._properties.update({property.id(): property})
        self.device().publish_description()
        # TODO FIXME DCJ is property.publish_value() the right thing to do here?
        property.publish_value()
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
        Remove property and clear its MQTT topic
        Returns True if removed, False if not found
        """
        if property_id not in self._properties:
            logger.warning(f"reason=nodeDeletePropertyNotFound,nodeId={self._id},propertyId={property_id}")
            return False
        property = self._properties[property_id]
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
        logger.info(f"reason=nodeDescriptionEntered,id={self._id}")
        logger.info(f"reason=nodeDescriptionNode,node={pformat(self.as_dict())}")
        description = dict()
        description["name"] = self._name
        description["type"] = self._type
        properties = dict()
        properties_snapshot = dict(self._properties)
        for property_id, attributes in properties_snapshot.items():
            properties[property_id] = attributes.description()
        description["properties"] = properties
        return description

    def publish(self) -> None:
        """
        Publishes Node, specifically its Properties to MQTT
        """
        node_id = self.id()
        property_count = len(self._properties)
        logger.debug(f"reason=nodePublish,nodeId={node_id},propertyCount={property_count}")
        # Use list() to create a shallow copy, preventing crash if dict changes during iteration
        for property_id, property in list(self._properties.items()):
            logger.debug(f"reason=nodePublishProperty,nodeId={node_id},propertyId={property_id}")
            property.publish_value()


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


class Device:
    """
    Object representing a Homie MQTT Device
    https://homieiot.github.io/specification/
    TODO: Child devices might (or must?) use the root's MQTT client

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

    homie_domains config for future use, not currently supported by this code
    """

    def __init__(
        self,
        id: str,
        name: Optional[str] = None,
        type: Optional[str] = None,
        children_ids: Optional[List] = [],
        root_id: Optional[str] = None,
        parent_id: Optional[str] = None,
        nodes: Optional[List] = [],
        extensions: Optional[List] = [],
        mqtt_cfg: Optional[dict] = {},
        qos: int = EBUS_HOMIE_MQTT_QOS,
    ):
        # Basic initialization
        self.mqttc = None
        self._state = None
        self._qos = qos
        # Store the arguments
        self._id = id
        if name:
            self._name = name
        else:
            self._name = id
        self._type = type
        self._children_ids = children_ids
        self._mqtt_cfg = mqtt_cfg
        # If the device is NOT the root device, both root_id and parent_id are required
        if (root_id and not parent_id) or (not root_id and parent_id):
            logger.exception(f"reason=deviceInitRootParentException,id={id},rootID={root_id},parentID={parent_id}")
        self._root_id = root_id
        self._parent_id = parent_id
        # Initialize nodes here, but note that we'll add any provided nodes below
        self._nodes = {}
        self._extensions = extensions
        # Set the interesting/dynamic stuff
        # Distinguish between initial and subsequent connections to broker
        self.initial_broker_connection = True
        self.connect_broker()
        with self.state_transition():
            for node in nodes:
                self.add_node(node)

    def as_dict(self) -> dict:
        nodes = {}
        for node_id, node in self.nodes().items():
            nodes.update({node_id, node.as_dict()})
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

    def root_id(self) -> str:
        """
        Returns root_id of Device
        """
        return self._root_id

    def parent_id(self) -> str:
        """
        Returns parent_id of Device
        """
        return self._parent_id

    def children_ids(self) -> List:
        """
        Returns list of Device's children_ids
        """
        return self._children_ids

    def extensions(self) -> List:
        """
        Returns list of Device's extensions
        """
        return self._extensions

    @property
    def qos(self) -> int:
        """Returns the MQTT QoS level for this device"""
        return self._qos

    def nodes(self) -> dict:
        """
        Returns a dict Device's Nodes, keyed by Node-ID
        """
        return self._nodes

    def get_mqtt_client(self) -> MqttClient:
        mqttc = self.mqttc
        if not mqttc:
            logger.warning(f"reason=deviceGetMqttClientNoMqttClient,id={self._id}")
        return mqttc

    def start_mqtt_client(self) -> None:
        if not self.mqttc.is_running:
            self.mqttc.start()

    def description(self) -> dict:
        """
        Returns a dict of the $description attribute of the Device
        """
        logger.info(f"reason=deviceDescriptionEntered,id={self._id}")
        description = dict()
        description["homie"] = f"{EBUS_HOMIE_VERSION_MAJOR}.{EBUS_HOMIE_VERSION_MINOR}"
        # Version should be changed any time the description document is changed
        description["version"] = Device.now_ems()
        description["type"] = self._type
        description["name"] = self._name
        nodes_descriptions = dict()
        nodes_snapshot = dict(self._nodes)
        for node_id, node in nodes_snapshot.items():
            nodes_descriptions[node_id] = node.description()
        description["nodes"] = nodes_descriptions
        description["children"] = self._children_ids
        if self._root_id:
            # ID of the root parent device.
            # Required if the device is NOT the root device, MUST be omitted otherwise.
            description["root"] = self._root_id
        if self._parent_id:
            # ID of the parent device.
            # Required if the parent is NOT the root device. Defaults to the value of the root property.
            description["parent"] = self._parent_id
        description["extensions"] = self._extensions
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

    def add_child(self, child_id: str) -> bool:
        """
        Append child_id to children_ids
        Returns True if added, else False
        """
        if child_id in self._children_ids:
            return False
        else:
            self._children_ids.append(child_id)
            return True

    def remove_child(self, child_id: str) -> bool:
        """
        Remove child_id from children_ids, if it included
        Returns True if removed, else False
        """
        if child_id in self._children_ids:
            self._children_ids.remove(child_id)
            return True
        else:
            return False

    def set_parent(self, parent_id: str) -> None:
        """
        Sets parent_id
        """
        self._parent_id = parent_id

    def unset_parent(self) -> bool:
        """
        Unset parent_id if set
        Returns True if unset, else False
        """
        if self._parent_id:
            self._parent_id = None
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
        # Propagate device QoS to all properties in this node
        for prop in node.properties().values():
            prop._qos = self._qos
        node_id = node.id()
        self._nodes.update({node_id: node})
        node.publish()
        self.publish_description()
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
        Removes node with node_id from nodes, if it exists
        Returns True if removed, else False
        """
        if node_id in self._nodes:
            self._nodes.pop(node_id, None)
            self.publish_description()
            return True
        else:
            return False

    def delete_node(self, node_id: str) -> bool:
        """
        Remove node from device, clear all node topics, and update description
        Returns True if removed, False if not found
        """
        if node_id not in self._nodes:
            logger.warning(f"reason=deviceDeleteNodeNotFound,deviceId={self._id},nodeId={node_id}")
            return False
        node = self._nodes[node_id]
        # Clear all property topics first
        # Note: This explicitly clears each property's retained message from MQTT
        # to avoid leaving orphaned topics in the broker
        node.clear_all_properties()
        # Remove node from device's internal structure
        del self._nodes[node_id]
        # Update device description (which removes the node from the schema)
        self.publish_description()
        logger.info(f"reason=deviceDeletedNode,deviceId={self._id},nodeId={node_id}")
        return True

    def delete_all_from_mqtt(self) -> None:
        """
        Delete entire device from MQTT broker: all nodes, properties, and description.
        Used for clean shutdown. Does NOT republish anything, does NOT publish node descriptions.
        """
        logger.info(f"reason=deviceDeleteAllFromMqtt,deviceId={self._id}")

        if not self.mqttc:
            logger.warning(f"reason=deviceDeleteAllFromMqttNoMqttClient,deviceId={self._id}")
            return

        base_topic = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/{self._id}"

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

                    if was_published:
                        prop_topic = f"{base_topic}/{node_id}/{prop_id}"
                        try:
                            self.mqttc.publish(prop_topic, "", retain=True, qos=self._qos)
                            logger.debug("reason=deviceClearedProperty...")
                        except Exception:
                            logger.warning("reason=deviceClearPropertyFailed...")

        # Step 2: Clear the main device $description (this removes all nodes from schema)
        description_topic = f"{base_topic}/$description"
        try:
            self.mqttc.publish(description_topic, "", retain=True, qos=self._qos)
            logger.info(f"reason=deviceClearedDescription,deviceId={self._id},topic={description_topic}")
        except Exception as e:
            logger.warning(f"reason=deviceClearDescriptionFailed,deviceId={self._id},error={e}")

        # Step 3: Clear internal tracking (no publishing happens here)
        self._nodes.clear()

        logger.info(f"reason=deviceDeleteAllFromMqttComplete,deviceId={self._id}")

    def clear_retained_topic(self, topic_path: str) -> bool:
        """
        Publish empty string to clear retained message on topic
        Returns True on success, False on failure
        """
        if not self.mqttc:
            logger.warning(f"reason=deviceClearTopicNoMqttClient,topic={topic_path}")
            return False
        try:
            self.mqttc.publish(topic_path, "", retain=True, qos=self._qos)
            logger.info(f"reason=deviceClearedTopic,topic={topic_path}")
            return True
        except Exception as e:
            logger.warning(f"reason=deviceClearTopicException,topic={topic_path},e={e}")
            return False

    def _begin_state_transition(self) -> None:
        """Set device state to INIT to begin a state transition"""
        logger.info(f"reason=deviceBeginStateTransition,deviceId={self._id}")
        self.set_state(DeviceState.INIT)

    def _end_state_transition(self) -> None:
        """Set device state to READY and publish updated description"""
        logger.info(f"reason=deviceEndStateTransition,deviceId={self._id}")
        self.publish_description()
        self.set_state(DeviceState.READY)

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

    def refresh_all_nodes(self) -> None:
        """Republish entire device state (for reconnection)"""
        logger.info(f"reason=deviceRefreshAllNodes,deviceId={self._id},nodeCount={len(self._nodes)}")
        self.publish_description()
        self.publish_nodes()
        self.publish_state()

    def publish(self, attribute: str = "", value: Optional[Any] = None) -> None:
        """
        Publishes the value argument to the device's attribute MQTT topic,
        or if the value is not provided, publishes the current (self) attribute value
        """
        if not self.mqttc:
            logger.info(f"reason=devicePublishNoMqttClient,attribute={attribute}")
            return
        if not self._id:
            logger.info("reason=devicePublishNoDeviceID")
            return
        try:
            base_topic = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/{self._id}/"
            if attribute == "$state":
                topic = base_topic + "$state"
                if value:
                    payload = value
                else:
                    payload = self._state
            elif attribute == "$description":
                topic = base_topic + "$description"
                if value:
                    payload = json.dumps(value)
                else:
                    description = self.description()
                    if description:
                        payload = json.dumps(description)
                    else:
                        payload = None
            elif attribute == "$alert":
                topic = base_topic + "$alert"
                if value:
                    payload = value
                else:
                    logger.info(f"reason=devicePublishAlertNoValue,id={self._id}")
                    return
            if payload:
                self.mqttc.publish(topic, payload, retain=True, qos=self._qos)
        except Exception as e:
            logger.exception(f"reason=devicePublishException,id={self._id},attribute={attribute},value={value},e={e}")

    def publish_state(self, state: Optional[DeviceState] = None) -> None:
        """
        Publishes the value of the state argument to the device's $state topic,
        or if state argument not provided, publishes value of self._state
        """
        if state:
            self.publish("$state", value=state)
        else:
            self.publish("$state", value=self._state)

    def publish_description(self, republish: bool = False) -> None:
        if republish:
            self.publish("$description")
        else:
            if self._state == DeviceState.READY:
                # Need to transition first to INIT
                self.publish_state(DeviceState.INIT)
                self.publish("$description")
                # Now that we've republished, restore $state to ready
                self.publish_state(DeviceState.READY)
            else:
                # TODO: should we be able to publish if DISCONNECTED, SLEEPING, or LOST?
                # If not in READY state, then we don't need to transition to INIT...
                logger.info(f"reason=publishDescriptionNotRepublishNotReady,state={self._state.name}")
                # Just publish description
                self.publish("$description")

    def publish_nodes(self) -> None:
        for node in self._nodes.values():
            node.publish()

    def on_connect(self) -> None:
        """
        This method will be called when the Homie/eBus client connects to the broker
        ATM the callback function signature has no arguments so use functools.partial to wrap this method
        Current intended use is to re-publish the Device's $state on connection (especially re-connection)
        """
        logger.info(f"reason=deviceOnConnectInvocation,initialBrokerConnection={self.initial_broker_connection}")
        if self.initial_broker_connection:
            self.initial_broker_connection = False
            # Also publish nodes on initial connection, FIX for G3P-19041
            self.publish_nodes()
        else:
            logger.info(f"reason=deviceRepublishingAfterReconnect,nodeCount={len(self._nodes)}")
            for node_id in self._nodes.keys():
                logger.debug(f"reason=deviceRepublishingNode,nodeId={node_id}")
            self.publish_description(republish=True)
            self.publish_nodes()
            self.publish_state()

    def connect_broker(self) -> None:
        """
        Connect to MQTT broker using configuration from mqtt_cfg.
        TODO: If device is a child, likely this needs to happen on the device's root!
        """
        if self.mqttc:
            # If we already have a mqtt client, don't reconnect...
            return
        lwt = {
            "topic": f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/{self._id}/$state",
            "payload": DeviceState.LOST.value,
        }
        try:
            self.mqttc = MqttClient.from_config(
                mqtt_cfg=self._mqtt_cfg,
                client_id=self._id,
                lwt=lwt,
                on_connect_callback=partial(self.on_connect),
            )
        except Exception as e:
            logger.warning(f"reason=deviceConnectBrokerException,e={e}")


def ebus_cfg_add_auth(cfg, username, password):
    """
    Add authentication to the config dictionary
    """
    from .mqtt import AUTH_TYPE_USER_PASS

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
        """Get current value of a property"""
        return self.properties.get(node_id, {}).get(property_id)

    def get_property_target(self, node_id: str, property_id: str) -> Optional[str]:
        """Get target value of a property"""
        return self.property_targets.get(node_id, {}).get(property_id)

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
        qos: int = EBUS_HOMIE_MQTT_QOS,
    ):
        """
        Initialize a Homie Controller

        Args:
            mqtt_cfg: MQTT broker configuration (same format as Device class)
            homie_domain: Homie domain to monitor (default: 'ebus')
            auto_start: If True, automatically start discovery on init
            device_id: If set, subscribe only to this specific device (no wildcards)
            qos: MQTT QoS level for all subscribe/publish operations (default: EBUS_HOMIE_MQTT_QOS)
        """
        self.homie_domain = homie_domain
        self.device_id = device_id
        self._qos = qos
        self._mqtt_cfg = mqtt_cfg
        self.mqttc = None
        self.devices = {}  # {device_id: DiscoveredDevice}

        # Callbacks
        self._on_device_discovered = None
        self._on_device_state_changed = None
        self._on_device_removed = None
        self._on_property_changed = None
        self._on_description_received = None

        # Connect to broker
        self._connect_broker()

        if auto_start:
            self.start_discovery()

    def _connect_broker(self) -> None:
        """Connect to MQTT broker"""
        if self.mqttc:
            return

        client_id = f"homie-controller-{uuid.uuid4()}"
        try:
            self.mqttc = MqttClient.from_config(
                mqtt_cfg=self._mqtt_cfg,
                client_id=client_id,
                on_connect_callback=partial(self._on_connect),
            )
            self.mqttc.start(blocking=False)
            logger.info(f"reason=controllerConnected,clientID={client_id}")
        except Exception as e:
            logger.exception(f"reason=controllerConnectException,error={e}")

    @property
    def qos(self) -> int:
        """Returns the MQTT QoS level for this controller"""
        return self._qos

    def _on_connect(self) -> None:
        """Called when controller connects to MQTT broker"""
        logger.info("reason=controllerOnConnect")
        # Re-subscribe to all topics on reconnect
        if self.devices:
            for device_id in self.devices.keys():
                self._subscribe_to_device(device_id)

    def start_discovery(self, homie_domain: Optional[str] = None) -> None:
        """
        Start auto-discovery of Homie devices

        When device_id is set, subscribes to exact topics for that single device
        (no wildcard in the device-id position). Otherwise, subscribes to the
        wildcard discovery topic pattern: {domain}/5/+/$state

        Args:
            homie_domain: Optional specific domain to monitor (default: uses instance domain)
        """
        if not self.mqttc:
            logger.error("reason=discoveryFailedNoConnection")
            return

        domain = homie_domain or self.homie_domain

        if self.device_id:
            # Single-device mode: subscribe to exact topics, no wildcard
            # in the device-id position
            base = f"{domain}/{EBUS_HOMIE_VERSION_MAJOR}/{self.device_id}"
            logger.info(f"reason=startDiscoverySingleDevice,deviceID={self.device_id}")

            # Pre-create the DiscoveredDevice entry
            device = DiscoveredDevice(self.device_id, domain)
            self.devices[self.device_id] = device

            # Subscribe to $state
            self.mqttc.subscribe(
                f"{base}/$state",
                param=self._on_state_message,
                qos=self._qos,
            )
            # Subscribe to $description
            self.mqttc.subscribe(
                f"{base}/$description",
                param=partial(self._on_description_message, self.device_id),
                qos=self._qos,
            )
            # Subscribe to all properties: {base}/{node_id}/{property_id}
            self.mqttc.subscribe(
                f"{base}/+/+",
                param=partial(self._on_property_message, self.device_id),
                qos=self._qos,
            )
            # Subscribe to all property targets
            self.mqttc.subscribe(
                f"{base}/+/+/$target",
                param=partial(self._on_target_message, self.device_id),
                qos=self._qos,
            )
        else:
            # Wildcard discovery mode (original behavior)
            discovery_topic = f"{domain}/{EBUS_HOMIE_VERSION_MAJOR}/+/$state"
            logger.info(f"reason=startDiscovery,topic={discovery_topic}")
            self.mqttc.subscribe(discovery_topic, param=self._on_state_message, qos=self._qos)

    def _on_state_message(self, topic: str, payload: bytes) -> None:
        """
        Handle device $state messages

        Topic format: {domain}/5/{device_id}/$state
        Payload: init, ready, disconnected, sleeping, lost, or empty (device removal)
        """
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
            if device_id in self.devices:
                removed_device = self.devices[device_id]
                del self.devices[device_id]
                if self._on_device_removed:
                    self._on_device_removed(removed_device)
            return

        # New or existing device
        if device_id not in self.devices:
            # New device discovered (wildcard mode only; single-device mode
            # pre-creates the entry in start_discovery)
            logger.info(
                f"reason=deviceDiscovered,deviceID={device_id},state={payload_str},knownDevices={list(self.devices.keys())}"
            )
            device = DiscoveredDevice(device_id, homie_domain)
            device.update_state(payload_str)
            self.devices[device_id] = device

            # Subscribe to device's $description and all properties
            self._subscribe_to_device(device_id)

            if self._on_device_discovered:
                self._on_device_discovered(device)
        elif self.devices[device_id].state is None:
            # Pre-created entry (single-device mode): first $state message
            device = self.devices[device_id]
            device.update_state(payload_str)
            logger.info(f"reason=deviceDiscovered,deviceID={device_id},state={payload_str},mode=singleDevice")
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

    def _subscribe_to_device(self, device_id: str) -> None:
        """Subscribe to all topics for a discovered device"""
        if not self.mqttc:
            return

        base_topic = f"{self.homie_domain}/{EBUS_HOMIE_VERSION_MAJOR}/{device_id}"

        # Subscribe to $description
        description_topic = f"{base_topic}/$description"
        self.mqttc.subscribe(
            description_topic,
            param=partial(self._on_description_message, device_id),
            qos=self._qos,
        )

        # Subscribe to all properties and targets
        property_topic = f"{base_topic}/+/+"
        self.mqttc.subscribe(
            property_topic,
            param=partial(self._on_property_message, device_id),
            qos=self._qos,
        )

        # Subscribe to all property targets
        target_topic = f"{base_topic}/+/+/$target"
        self.mqttc.subscribe(
            target_topic,
            param=partial(self._on_target_message, device_id),
            qos=self._qos,
        )

    def _on_description_message(self, device_id: str, topic: str, payload: bytes) -> None:
        """Handle device $description messages"""
        if device_id not in self.devices:
            return

        payload_str = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        device = self.devices[device_id]
        device.update_description(payload_str)

        logger.info(f"reason=descriptionReceived,deviceID={device_id}")
        if self._on_description_received:
            self._on_description_received(device)

    def _on_property_message(self, device_id: str, topic: str, payload: bytes) -> None:
        """
        Handle property value messages

        Topic format: {domain}/5/{device_id}/{node_id}/{property_id}
        Skip $target topics (handled separately)
        """
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
        parts = topic.split("/")
        if len(parts) != 6 or parts[5] != "$target":
            return

        node_id = parts[3]
        property_id = parts[4]

        if device_id not in self.devices:
            return

        payload_str = payload.decode("utf-8") if isinstance(payload, bytes) else payload
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
            # Non-retained message as per convention
            self.mqttc.publish(set_topic, value, qos=effective_qos, retain=False)
            return True
        except Exception as e:
            logger.error(f"reason=setPropertyException,error={e}")
            return False

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

    def stop(self) -> None:
        """Stop the controller, release resources, and disconnect from broker"""
        if self.mqttc:
            logger.info(f"reason=stoppingController,deviceCount={len(self.devices)}")
            self.mqttc.stop()
            self.mqttc = None
        # Release DiscoveredDevice objects and their property dicts
        self.devices.clear()
        # Clear callback references to break reference cycles
        self._on_device_discovered = None
        self._on_device_state_changed = None
        self._on_device_removed = None
        self._on_property_changed = None
        self._on_description_received = None

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

    def set_on_description_received_callback(self, callback: Callable[[DiscoveredDevice], None]) -> None:
        """Set callback for when a device description is received"""
        self._on_description_received = callback
