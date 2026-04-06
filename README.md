# ebus-sdk

[![PyPI](https://img.shields.io/pypi/v/ebus-sdk)](https://pypi.org/project/ebus-sdk/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

Python SDK for the [Electrification Bus (eBus)](https://ebus.energy) integration framework, which adopts and supports the [Homie Convention](https://homieiot.github.io).

## Installation

```bash
pip install ebus-sdk
```

## Quick Start

### Device Role

Create a Homie device that publishes sensor data:

```python
from ebus_sdk import Device, Node, PropertyDatatype, Unit

# Create device
device = Device('my-device-id', name='My Sensor', mqtt_cfg={
    'host': 'mqtt.example.com',
    'port': 1883
})

# Add a node with properties
node = device.add_node_from_dict({
    'id': 'sensors',
    'name': 'Sensors',
    'type': 'sensor'
})

# Add a temperature property
temp = node.add_property_from_dict({
    'id': 'temperature',
    'name': 'Temperature',
    'datatype': PropertyDatatype.FLOAT,
    'unit': Unit.DEGREE_CELSIUS
})

# Start and publish
device.start_mqtt_client()
temp.set_value(23.5)
```

### Controller Role

Discover and monitor Homie devices:

```python
from ebus_sdk import Controller, DiscoveredDevice

def on_device_discovered(device: DiscoveredDevice):
    print(f'Found: {device.device_id}')

def on_property_changed(device_id, node_id, prop_id, new_val, old_val):
    print(f'{device_id}/{node_id}/{prop_id} = {new_val}')

controller = Controller(mqtt_cfg={'host': 'mqtt.example.com', 'port': 1883})
controller.set_on_device_discovered_callback(on_device_discovered)
controller.set_on_property_changed_callback(on_property_changed)
controller.start_discovery()
```

## Module Structure

```
src/ebus_sdk/
├── __init__.py     # Package exports
├── homie.py        # Homie convention implementation
├── mqtt.py         # MQTT client wrapper
└── property.py     # Property abstractions
```

### homie.py

Core Homie convention implementation:

- **Device** - Represents a Homie device with nodes and properties
- **Node** - Groups related properties within a device
- **Property** - Individual data points (sensors, controls)
- **Controller** - Discovers and monitors Homie devices on a broker
- **DiscoveredDevice** - Represents a device found by the controller
- **DeviceState** - Enum: `init`, `ready`, `disconnected`, `sleeping`, `lost`
- **PropertyDatatype** - Enum: `STRING`, `INTEGER`, `FLOAT`, `BOOLEAN`, `ENUM`, `COLOR`, `DATETIME`, `DURATION`, `JSON`
- **Unit** - Common units: `DEGREE_CELSIUS`, `PERCENT`, `WATT`, `KILOWATT_HOUR`, etc.

### mqtt.py

- **MqttClient** - Wrapper around paho-mqtt with automatic reconnection, TLS support, and subscription management

### property.py

Application-level property abstractions for bridging application state to Homie:

- **Property** - Thread-safe observable property with change callbacks
- **GroupedPropertyDict** - Two-level dictionary organizing properties by group
- **PropertyDict** - Simple property dictionary
- **ChangeEvent** - Enum for property change event types

## Examples

See [`examples/README.md`](examples/README.md) for example scripts demonstrating device and controller usage.

## Requirements

- Python 3.10+
- paho-mqtt >= 1.6.1

## License

[MIT License](LICENSE) — Copyright (c) 2026 Clark Communications Corporation
