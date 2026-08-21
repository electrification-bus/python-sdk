"""
ebus-sdk: Python SDK for Homie MQTT Convention (eBus)

This SDK provides Device and Controller roles for the Homie MQTT convention.
"""

# Core Homie classes
from .homie import (
    Device,
    Node,
    Property,
    Controller,
    DiscoveredDevice,
    StateTransitionContext,
)

# Enums
from .homie import (
    DeviceState,
    PropertyDatatype,
    Unit,
)

# Constants
from .homie import (
    EBUS_HOMIE_MQTT_QOS,
    HOMIE_EFFECTIVE_STATE_TABLE,
    HOMIE_EMPTY_STRING_PAYLOAD,
)

# Utility functions
from .homie import (
    datatype_from_type,
    ebus_cfg_add_auth,
    sanitize_homie_id,
    encode_empty_string,
    decode_empty_string,
    validate_json_format,
    JsonFieldConstraint,
    json_format_field,
    json_format_fields,
)

# Property abstractions
from .property import (
    Property as ObservableProperty,
    GroupedPropertyDict,
    PropertyDict,
    ChangeEvent,
    BulkUpdateContext,
)

# Proxy / adapter helpers (see doc/building-a-proxy.md)
from .adapter import (
    bind_property_to_homie,
    set_homie_property_from_python_property,
)

# Declarative property specs + builder + resolver (see doc/building-a-proxy.md)
from .declaration import (
    DeviceSpec,
    DeviceTreeBuilder,
    PropertySpec,
    ResolvedProperty,
    build_from_declarations,
    python_type_for,
    resolve,
    specs_and_values,
)

# Consumer-side site-topology assembler (see doc/building-a-proxy.md / connection capability)
from .topology import (
    CONNECTION_NODE_TYPE,
    ConnectionRecord,
    SiteTopology,
    TopologyEdge,
    TopologyNode,
)

# MQTT client
from ebus_mqtt_client import MqttClient

# Structural types for a caller-supplied MQTT client
from ebus_sdk.transport import MqttControllerTransport, MqttDeviceTransport, MqttTransport

__version__ = "0.22.0"

__all__ = [
    # Homie classes
    "Device",
    "Node",
    "Property",
    "Controller",
    "DiscoveredDevice",
    "StateTransitionContext",
    # Enums
    "DeviceState",
    "PropertyDatatype",
    "Unit",
    # Constants
    "EBUS_HOMIE_MQTT_QOS",
    "HOMIE_EFFECTIVE_STATE_TABLE",
    "HOMIE_EMPTY_STRING_PAYLOAD",
    # Utilities
    "datatype_from_type",
    "ebus_cfg_add_auth",
    "sanitize_homie_id",
    "encode_empty_string",
    "decode_empty_string",
    "validate_json_format",
    "JsonFieldConstraint",
    "json_format_field",
    "json_format_fields",
    # Property abstractions
    "ObservableProperty",
    "GroupedPropertyDict",
    "PropertyDict",
    "ChangeEvent",
    "BulkUpdateContext",
    # Proxy / adapter helpers
    "set_homie_property_from_python_property",
    "bind_property_to_homie",
    # Declarative specs + builder + resolver
    "PropertySpec",
    "ResolvedProperty",
    "DeviceSpec",
    "DeviceTreeBuilder",
    "build_from_declarations",
    "python_type_for",
    "resolve",
    "specs_and_values",
    # Site-topology assembler
    "SiteTopology",
    "ConnectionRecord",
    "TopologyNode",
    "TopologyEdge",
    "CONNECTION_NODE_TYPE",
    # MQTT
    "MqttClient",
    "MqttTransport",
    "MqttControllerTransport",
    "MqttDeviceTransport",
]
