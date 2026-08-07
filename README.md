# ebus-sdk

[![PyPI](https://img.shields.io/pypi/v/ebus-sdk)](https://pypi.org/project/ebus-sdk/)
[![CI](https://github.com/electrification-bus/python-sdk/actions/workflows/lint.yml/badge.svg)](https://github.com/electrification-bus/python-sdk/actions/workflows/lint.yml)
[![Python versions](https://img.shields.io/pypi/pyversions/ebus-sdk.svg)](https://pypi.org/project/ebus-sdk/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

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

#### Resilient connect

Constructing a `Device` never blocks or fails just because the broker is momentarily unreachable (startup, restart, a network blip). The connection is opened asynchronously on the network loop that `start_mqtt_client()` starts, and retried with backoff until the broker appears; when it connects, the SDK publishes the device's complete state (`$state`, `$description`, nodes, and property values), so a device built against a down broker comes up fully published rather than half-published. You can publish before the link is up (values are retained and re-published on connect), or gate on `device.is_connected()` if you must not publish until connected. A genuinely bad configuration (a malformed `mqtt_cfg` or an unreadable TLS certificate) still fails fast: it raises out of the `Device(...)` constructor rather than leaving a silent, dead publisher.

#### Transport-free construction

A `Device` tree can also be built with no transport at all: pass `mqtt_cfg=None` (the default) and the tree composes `$description`, resolves ids and topics, and holds property values without opening a socket: useful for tests and for deriving the wire schema offline. (A host that wants to *publish* an eBus tree through its own connected client uses bring-your-own-transport, below, not this.) `mqtt_cfg={}` still connects on the transport's defaults; only `None` skips the connection. Children attach to a transport-free root exactly as they do to a connected one.

#### Bring-your-own-transport

A host that already owns its MQTT connection can publish an eBus device tree through it instead of letting the SDK open its own: pass a pre-built client as `Device(..., mqttc=client)` (root-only, mutually exclusive with `mqtt_cfg=`). The SDK uses the client as-is and never `start()`s or `stop()`s it: the caller owns its lifecycle and event loop. This is the producer-side mirror of `Controller(mqttc=...)`, and the case it exists for is a host like Home Assistant, whose MQTT integration is `single_config_entry` (a second SDK-owned connection is not an option) and forbids background threads.

Inject the client before it connects, then wire the two Homie-correctness pieces the SDK can only set through its own connect path: the Last Will, and the whole-tree republish on (re)connect.

```python
client = my_host_mqtt_client()                 # created, not yet connected
device = Device('panel-1', type='...', mqttc=client)

client.set_will(**device.will())               # LWT ($state=lost); must precede connect
client.on_connect(device.refresh_tree)         # re-announce the retained tree on every (re)connect
client.connect()                               # host connects on its own loop
```

`device.will()` returns the tree's Last Will descriptor and `device.refresh_tree()` republishes the whole tree; the `set_will` / `on_connect` / `connect` calls above are illustrative of your host's own MQTT API. Property values publish once the client is connected (the SDK gates on `is_connected()`, not on its own `start()`, which a caller-driven client never calls). `device.stop()` publishes a final retained `$state=disconnected` through the client and returns immediately, without flushing or closing it. `on_disconnect=` is inert for an injected client; register disconnect handling on your own client.

For the inbound direction, if the tree has settable properties whose callbacks are async coroutines, pass `Device(async_loop=<your event loop>)`: inbound `/set` arrives on the transport's network thread, and this schedules the callback onto your loop (set once for the whole tree, not per property). A synchronous callback runs inline and needs no loop.

**Running the transport on your own event loop.** Injecting a client answers *which* connection to publish through, but paho's network loop still has to be pumped somewhere, and by default that is a background thread. For a host that forbids one, `ebus-mqtt-client` 0.4.0 provides a loop-native alternative: `MqttClient.asyncio_driver(loop=None)` returns a driver that pumps the network loop on your asyncio loop (paho socket hooks plus a periodic `loop_misc`) rather than paho's thread, so all MQTT I/O runs on the host's loop. Use it instead of `start()`, not alongside it: the two are mutually exclusive per client.

```python
client = MqttClient.from_config(mqtt_cfg)
driver = client.asyncio_driver()               # call from within a running loop, or pass loop=
await driver.start()                           # connects and pumps on your loop
device = Device('panel-1', type='...', mqttc=client)
...
await driver.stop()
```

The driver module loads lazily and imports only the standard library plus paho, so a thread-mode consumer (or a constrained build such as a Yocto image) never loads it.

**A producer should own its MQTT connection.** The example above owns a *dedicated* client and connects it itself, so the SDK's Last Will (`$state=lost` on an ungraceful death) works normally: prefer this for any producer whose liveness matters. A *shared* connection owned by a host (Home Assistant is the archetype: one connection, up before your code loads, its single will already spent on the host's own) **cannot carry an eBus will**, because MQTT allows one will per connection. A producer publishing through such a connection therefore never signals ungraceful death: a crash leaves a stale retained `$state=ready`, and consumers render a dead device as alive. Reconnect is still handled (wire `refresh_tree()` to the host's reconnect callback and gate on `is_connected()`), but permanent death is not, and there is no portable substitute (a host that owns the connection also will not forward the MQTT 5 publish properties that would let `$state` expire). So do not publish a liveness-bearing device through a connection you do not own: if a host environment forbids a dedicated connection, run the producer as a **separate adapter** with its own connection rather than borrowing the host's. (The injected-client seam is still the right tool for the *consumer* role, `Controller(mqttc=...)`, which has no `$state` and no will to lose, and for tests.)

#### Clearing a value vs. an empty-string value

Homie 5 distinguishes two things that both look "empty" on the wire, and the SDK handles each automatically:

- **Clearing (retracting) a retained value** — set the property to `None`. Once it has been published, this emits a zero-length `retain=True` payload, which MQTT/Homie treats as "delete the retained topic", so a subscriber that connects later sees no stale value. (`clear_value()` does the same explicitly; `Node.delete_property()` clears on removal.) A `None` that was never published is a silent no-op — no phantom topic is created.
- **An actual empty-string value** — set a string property to `""`. This is published as a single null byte (`0x00`), the Homie 5 encoding that keeps `""` distinct from a topic-clear. Inbound `0x00` payloads are decoded back to `""` on the controller and on `/set`. Helpers `encode_empty_string()` / `decode_empty_string()` and the constant `HOMIE_EMPTY_STRING_PAYLOAD` are exported for consumers that need them directly.

```python
temp.set_value(None)     # retracts the retained topic (subscribers see nothing)
label.set_value("")      # publishes an empty-string VALUE (0x00 on the wire)
```

### Device Trees (parent / child)

Build a tree of devices that share a single MQTT connection. The root device owns the connection (and the Last Will), every child borrows it via the `parent=` constructor arg, and `$description` `root` / `parent` / `children` fields are kept in sync automatically. The tree can be any depth.

```python
panel = Device('panel-1', type='energy.ebus.device.electrical-panel', mqtt_cfg={...})
panel.start_mqtt_client()

# Add 32 circuit children inside one state transition — the broker sees
# exactly one INIT→READY cycle on the panel, not 32.
with panel.state_transition():
    for cid in commissioned_circuits:
        Device(id=cid, type='energy.ebus.device.circuit', parent=panel)

# Three-level tree: panel → BESS child → MID grandchild
bess = Device(id='bess-1', type='...battery-storage', parent=panel)
Device(id='mid-1', type='...metering', parent=bess)

# Remove a child at runtime (runs the Homie remove-child protocol)
panel.children()[0].delete()
```

Children may have children of their own. A single Last Will registered on the root marks the entire tree `lost` if the publisher process dies — controllers compute effective state per the Homie 5 precedence table (see [`HOMIE_EFFECTIVE_STATE_TABLE`](src/ebus_sdk/homie.py)).

`$description` republishes are minimized: structural changes made inside one `state_transition()` collapse to a single consolidated publish at exit (not one per `add_node`), and `publish_description()` is a no-op when the description content (ignoring its `version` timestamp) is unchanged — so a `state_transition()` that changes nothing structural does not re-emit the (potentially multi-KB) `$description`. A reconnect always republishes regardless, to restore retained state. Note this suppresses the redundant `$description` payload, not the `$state` `init`→`ready` edge of an empty transition.

### Building a Proxy or Adapter

To publish a device whose state changes over time (a proxy for a non-eBus device, an adapter for a local device, a gateway/bridge), use the **observable-model pattern**: keep the device's live state in a `GroupedPropertyDict` of observable `Property` objects, and mirror each change onto the Homie tree with a per-property on-change callback. Your acquisition code only updates the model; publishing to MQTT is an automatic side-effect.

```python
from ebus_sdk import (
    Device, PropertyDatatype, Unit,
    GroupedPropertyDict, ObservableProperty, bind_property_to_homie,
)

# Observable model (Homie-agnostic)
model = GroupedPropertyDict()
model.add_property('meter', ObservableProperty(id='active-power', type=float))

# Homie device + property
device = Device('my-meter', type='energy.ebus.device.submeter', mqtt_cfg={...})
device.start_mqtt_client()
with device.state_transition():
    node = device.add_node_from_dict({'id': 'meter', 'type': 'energy.ebus.capability.meter'})
    homie_prop = node.add_property_from_dict(
        {'id': 'active-power', 'datatype': PropertyDatatype.FLOAT, 'unit': Unit.WATT})

# Bind: a model change now publishes to MQTT automatically
bind_property_to_homie(model, 'meter', 'active-power', homie_prop)
model.set_value('meter', 'active-power', 1850.0)
```

**If you are building a proxy, read [`doc/building-a-proxy.md`](doc/building-a-proxy.md) first.** It is the comprehensive guide: declarative property definitions, the bridge-root plus proxied-children topology, dynamic device shapes, settable/bidirectional properties, and the anti-pattern to avoid (driving `homie.Device` directly from your data path). `examples/utility-meter` is the fullest worked example.

**If you are writing a controller or any subscriber, read [`doc/consuming-a-homie-tree.md`](doc/consuming-a-homie-tree.md) first.** It states the producer/consumer asymmetry the convention relies on (a producer SHOULD minimize `$state` and `$description` transitions; a consumer MUST react to every one of them unconditionally), what `$state = ready` does and does not promise, and the three ways consumers get this wrong. If you are about to gate on a root's `$state`, that document is the one you need.

**Home Assistant interop** is covered by two `ebus_sdk.ha` guides: [`doc/ha-mqtt-discovery.md`](doc/ha-mqtt-discovery.md) parses HA MQTT discovery INTO eBus, and [`doc/ha-discovery-bridge.md`](doc/ha-discovery-bridge.md) emits eBus OUT to HA via `HaDiscoveryBridge` (per-device mapping, an eBus-aware customizer, and HA <-> eBus loop-avoidance guards). `examples/ha-discovery-bridge` is a live-broker, no-HASS-needed demo.

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

Controllers can also navigate device hierarchies and compute effective state:

```python
# Walk the tree
roots = controller.get_root_devices()
for root in roots:
    for descendant in controller.get_descendants(root.device_id):
        # When the root is lost/disconnected/sleeping/init, every descendant
        # is effectively the same regardless of its own reported $state.
        print(f'{descendant.device_id}: {controller.get_effective_state(descendant.device_id)}')
```

Three controller discovery modes select what the controller listens for:

```python
# Wildcard (default) — every device on the broker
Controller(mqtt_cfg=cfg)

# Single-device — subscribe to exactly one device, no children, no wildcards
Controller(mqtt_cfg=cfg, device_id='panel-1')

# Tree-rooted — subscribe to a root and auto-subscribe to its descendants
# as they're announced; subscription changes are gated on the parent's
# $state init→ready edge per the Homie 5 spec.
Controller(mqtt_cfg=cfg, root_device_id='panel-1')
```

Tree-rooted mode is the right pick for consumers that want exactly one
device's tree on a multi-publisher broker — wildcard would re-introduce
multi-panel scope creep at the application layer, and single-device would
see the root and none of its children. As the publisher mutates the tree
(`Device(parent=...)` to add, `child.delete()` to remove), descendants are
subscribed or dropped on the parent's next init→ready transition.

## Module Structure

```
src/ebus_sdk/
├── __init__.py     # Package exports
├── homie.py        # Homie convention implementation (Device, Node, Property, Controller, ...)
├── property.py     # Observable application-state model (Property, GroupedPropertyDict)
├── adapter.py      # Proxy/adapter helpers that mirror the model onto Homie
├── declaration.py  # Declarative PropertySpec + build_from_declarations + resolve
├── topology.py     # Consumer-side site-topology assembler (SiteTopology)
└── ha/             # Home Assistant MQTT discovery interop (parse in, emit out)
```

MQTT transport lives in the separate [`ebus-mqtt-client`](https://github.com/electrification-bus/ebus-mqtt-client) package; this SDK depends on it.

### homie.py

Core Homie convention implementation:

- **Device** - Represents a Homie device; pass `parent=` to build a child in a tree, or `on_disconnect=` for a push disconnect hook (`clean: bool`)
- **Node** - Groups related properties within a device
- **Property** - Individual data points (sensors, controls)
- **Controller** - Discovers and monitors Homie devices on a broker; navigates trees and computes effective state; `set_on_disconnect_callback` for push disconnect notification
- **DiscoveredDevice** - Represents a device found by the controller; exposes `root_id`, `parent_id`, `children_ids`, `is_root`
- **DeviceState** - Enum: `init`, `ready`, `disconnected`, `sleeping`, `lost`
- **HOMIE_EFFECTIVE_STATE_TABLE** - Homie 5 state-precedence table used by `Controller.get_effective_state()`
- **PropertyDatatype** - Enum: `STRING`, `INTEGER`, `FLOAT`, `BOOLEAN`, `ENUM`, `COLOR`, `DATETIME`, `DURATION`, `JSON`
- **Unit** - Common units: `DEGREE_CELSIUS`, `PERCENT`, `WATT`, `KILOWATT_HOUR`, etc.

### property.py

The observable application-state model used to build proxies and adapters (see [`doc/building-a-proxy.md`](doc/building-a-proxy.md)):

- **Property** - Thread-safe observable property with change callbacks
- **GroupedPropertyDict** - Two-level dictionary organizing properties by group (one group per Homie node)
- **PropertyDict** - Simple property dictionary
- **ChangeEvent** - Enum for property change event types

### adapter.py

Helpers that mirror the observable model onto the Homie tree, so you never hand-roll the bridge:

- **set_homie_property_from_python_property** - on-change callback that copies an observable property's value to its Homie twin
- **bind_property_to_homie** - one-call convenience that registers that callback for a `(group, property_id)`

### declaration.py

The declarative "schema" layer for proxies (see [`doc/building-a-proxy.md`](doc/building-a-proxy.md)):

- **PropertySpec** - declares one eBus property (capability/node, id, datatype, unit, scale, settable)
- **build_from_declarations** - materializes a set of specs into Homie nodes/properties, the observable model, and their bindings in one call
- **resolve** / **specs_and_values** / **ResolvedProperty** - the two-tier mapping (hand-authored `mapping` first, generic `fallback` for the rest) that turns source fields into specs and scaled values

### topology.py

Consumer-side **site-topology assembler** for the `connection` capability. eBus records site wiring as distributed per-device edges (`feeds-*` / `fed-by-*`) with no central authority; `SiteTopology.assemble(devices)` / `SiteTopology.from_controller(controller)` reconstructs the graph once so every consumer gets a resolved, queryable view:

- **`root()`**, **`parents`** / **`children`** / **`what_feeds`**, **`ancestors`** / **`descendants`** (cycle-safe) - traverse the assembled graph
- **`connection_points_feeding(id)`** + **`aggregate(id, value_fn)`** - the multi-source case (e.g. a multi-unit BESS on several circuits); sum a caller-supplied metric across them
- **`backed_up_loads()`**, **`completeness()`** - which paths survive an outage; surveyed-vs-unknown coverage
- **`to_dot()`** / **`to_mermaid()`** - render the graph to Graphviz DOT / Mermaid source (pure text, no dependency; you render it, e.g. `dot -Tsvg`), with confirmed vs one-sided edges, the service-entrance root, and backed-up / undiscovered nodes visually distinguished
- Robust to partial data: dangling references become `undiscovered()` placeholders, cycles terminate, and the graph is explicitly a view (never a source of truth)

### ha/

Home Assistant MQTT discovery interop, both directions (see [`doc/ha-mqtt-discovery.md`](doc/ha-mqtt-discovery.md) and [`doc/ha-discovery-bridge.md`](doc/ha-discovery-bridge.md)):

- **Parse (HA -> eBus)** - `parse_device_config` into the neutral `HADevice` / `HAComponent` model; `derive_spec` / `unit_for` map a component's `device_class` / `unit` to an eBus `PropertySpec`
- **Emit (eBus -> HA)** - `homie_description_to_ha` / `homie_device_to_ha` / `to_config` serialize a Homie device into HA discovery config; `ebus_default_override` adds eBus-capability-aware metadata; a typed `default_entity_id` on a component preserves an entity's id (and its recorded history) when the bridge replaces an existing HA integration
- **HaDiscoveryBridge** - controller-role runtime that discovers eBus devices and publishes/clears their HA discovery topics, with per-device mapping and graceful `stop()` vs permanent `clear_all()`
- **Loop avoidance** - `is_ebus_sdk_origin` (origin self-echo) and the `energy.ebus.imported` extension + `imported-from` attribute (`is_imported` / `imported_source`) to prevent a HA <-> eBus round-trip echo

## Examples

See [`examples/README.md`](examples/README.md) for example scripts demonstrating device and controller usage.

## Requirements

- Python 3.10+
- [`ebus-mqtt-client`](https://github.com/electrification-bus/ebus-mqtt-client) >= 0.3.0 (the MQTT transport layer; it pins `paho-mqtt`, so the SDK does not depend on paho directly. 0.3.0 ships the `py.typed` marker, so a downstream type checker resolves the re-exported `MqttClient` to the concrete class rather than `Any`; 0.2.0 adds the `on_disconnect_callback` the SDK's disconnect hook adopts; and, since 0.1.8, it carries the asynchronous, down-broker-tolerant connect the resilient-connect behavior relies on)

Optional extras:

- `mdns` (`zeroconf`) — mDNS broker discovery, used by the SPAN Panel controller example
- `validation` (`jsonschema`) — `$format` JSONSchema validation for `json`-datatype properties; absent it, validation is gracefully skipped

## Releases

See [CHANGELOG.md](CHANGELOG.md). 0.2.0 introduces parent/child device trees and contains breaking changes to the `Device` constructor — see the changelog entry before upgrading from 0.1.x.

## Releasing

The version has a single source of truth: `__version__` in `src/ebus_sdk/__init__.py`. `pyproject.toml` reads it dynamically and the `setup.py` shim (Yocto/kirkstone path) parses the same literal, so they cannot drift. To cut a release:

1. Bump `__version__` in `src/ebus_sdk/__init__.py` (the only place), and finalize the `CHANGELOG.md` entry.
2. Commit it: `git commit -am "Release X.Y.Z"`.
3. Tag it to match, `v`-prefixed: `git tag vX.Y.Z`.
4. Push the tag: `git push GitHub vX.Y.Z` (a plain `git push` does not trigger a release).

Pushing a `v*` tag runs the publish workflow, which verifies the tag equals `v` + `__version__` (a mismatch fails the run before anything is published), builds the sdist and wheel, and publishes to PyPI via Trusted Publishing. See the [version single-source-of-truth convention](https://github.com/electrification-bus/specification/blob/main/conventions/version-single-source.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to file Discussions, Issues, and pull requests. Pure MQTT-transport changes (TLS, auth, paho upgrades) belong in [`ebus-mqtt-client`](https://github.com/electrification-bus/ebus-mqtt-client), not here. Normative behavior tracks the [Electrification Bus specification](https://github.com/electrification-bus/specification).

## License

[MIT License](LICENSE) — Copyright (c) 2026 Clark Communications Corporation
