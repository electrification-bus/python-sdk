# Building a proxy or adapter with ebus-sdk

This is the guide for publishing a device (or several) onto eBus / Homie 5 when its state changes over time: a proxy for a non-eBus-native device, an adapter for a local device, or a gateway/bridge. It is the pattern the SDK is built around, and it is what every mature eBus proxy uses. If you are writing a proxy and this pattern was not obvious, this document is the fix: read it first, before you reach for `homie.Device`.

## TL;DR

Keep a device's live state in an **observable model** (`GroupedPropertyDict` of observable `Property` objects), and register **per-property on-change callbacks** that mirror each change onto a Homie `Device` / `Node` / `Property` tree. Your acquisition code only ever updates the model; publishing to MQTT is an automatic side-effect. Do not drive `homie.Device` directly from your incoming data.

```python
from functools import partial
from ebus_sdk import (
    Device, PropertyDatatype, Unit,
    GroupedPropertyDict, ObservableProperty,
    set_homie_property_from_python_property, bind_property_to_homie,
)

# 1. Observable model (homie-agnostic)
model = GroupedPropertyDict()
model.add_property("meter", ObservableProperty(id="active-power", type=float))

# 2. Homie device + property
device = Device("my-meter", type="energy.ebus.device.submeter", mqtt_cfg=cfg)
device.start_mqtt_client()
with device.state_transition():
    node = device.add_node_from_dict({"id": "meter", "type": "energy.ebus.capability.meter"})
    homie_prop = node.add_property_from_dict(
        {"id": "active-power", "datatype": PropertyDatatype.FLOAT, "unit": Unit.WATT}
    )

# 3. Bind: model change -> Homie publish
bind_property_to_homie(model, "meter", "active-power", homie_prop)

# 4. Your acquisition code just updates the model; MQTT follows automatically
model.set_value("meter", "active-power", 1850.0)
```

## When to use this pattern (and when not to)

Use the observable-model pattern when your publisher has **evolving state**: values that update over time, multiple properties, multiple proxied devices, or settable/controllable properties. That is nearly every proxy and adapter.

You can skip it for the trivial case: publishing a handful of static values once. There, the plain `Device` / `Node` / `Property` API from the [README Quick Start](../README.md#quick-start) is enough. The moment you find yourself repeatedly pushing new values, reach for the model.

## The three layers

A proxy built this way has three clean layers. Keep them separate.

1. **Declarative definitions (the schema).** A list of `PropertySpec`s describing each property: its capability (Homie node), id, datatype, unit, scale, settable. This is the single source of truth for both the observable model and the Homie tree, and `build_from_declarations` materializes both from it. See [Declarative definitions](#declarative-definitions-in-practice).
2. **The observable model (`GroupedPropertyDict`).** Homie-agnostic. Holds the device's live values as observable `Property` objects grouped by capability (one group per Homie node, conventionally). Your acquisition code calls `model.set_value(group, property_id, value)` and nothing else. It knows nothing about MQTT.
3. **The adapter.** Builds the Homie `Device` / `Node` / `Property` tree from the declarations, and wires each observable property to its Homie twin with an on-change callback. This is the only layer that touches both the model and Homie.

## Data flow

```
acquisition code
      │  model.set_value(group, id, value)
      ▼
GroupedPropertyDict  ──fires──►  PROPERTY_CHANGED
      │
      ▼  on-change callback (set_homie_property_from_python_property)
homie.Property.set_value(...)  ──►  MQTT (ebus/5/<device>/<node>/<property>)
```

`GroupedPropertyDict.set_value` fires the change event (and the callback) only when the value actually changes, so re-writing the same value does not republish. That dedup is free, and it is no longer the only one: `homie.Property` independently skips a retained republish whose final wire payload is unchanged. The two sit at different granularities and compose rather than duplicate. The model gate suppresses the *callback* on an unequal-value comparison; the Homie gate suppresses the *publish* on a byte comparison made after rounding and coercion, so it also catches two distinct model values that serialize to the same payload. A tree driven directly, bypassing the model, still gets the second one.

## The exported pieces

`ebus_sdk` exports the whole layer so you never hand-roll it:

- `PropertySpec`: the declaration for one property (its `capability`/node, `prop_id`, `datatype`, `unit`, `scale`, `settable`, and an optional `entity_setter` for the inbound/control path). The schema layer, complementary to the observable `Property` (which holds the live value).
- `build_from_declarations(device, model, specs, ...)`: materializes a set of `PropertySpec`s into a live device in one call: one Homie node per capability, an observable `Property` plus a Homie property per spec, and the on-change binding between them, all inside one `state_transition()`. Returns the `{(capability, prop_id): homie.Property}` map.
- `resolve(field_names, values, mapping, *, fallback=...)`: the two-tier mapping mechanism. Turns source fields into `PropertySpec`s (and scaled values) using a hand-authored `mapping` first, then a generic `fallback` for the rest (e.g. `ebus_sdk.ha.derive_spec` over discovered components). `specs_and_values(resolved)` splits the result straight into the `specs` and `values=` that `build_from_declarations` wants.
- `set_homie_property_from_python_property(homie_property, python_property)`: the low-level on-change mirror (copies an observable property's value onto its Homie twin).
- `bind_property_to_homie(properties, group, property_id, homie_property)`: registers that mirror as a `GroupedPropertyDict` on-change callback. `build_from_declarations` calls it for you; use it directly when you build the tree yourself.

## Declarative definitions in practice

Declare the device as a list of `PropertySpec`s and let `build_from_declarations` create the model, the Homie tree, and the bindings from that one source of truth:

```python
from ebus_sdk import (
    Device, GroupedPropertyDict, PropertyDatatype, Unit,
    PropertySpec, build_from_declarations,
)

SUBMETER = [
    PropertySpec("info", "serial-number", PropertyDatatype.STRING),
    PropertySpec("meter", "active-power", PropertyDatatype.FLOAT, Unit.WATT),
    PropertySpec("meter", "imported-energy", PropertyDatatype.FLOAT, Unit.WATT_HOUR, scale=1000.0),
]

device = Device("my-meter", type="energy.ebus.device.submeter", mqtt_cfg={...})
device.start_mqtt_client()
model = GroupedPropertyDict()

# One call: nodes + observable model + Homie properties + bindings.
homie_props = build_from_declarations(device, model, SUBMETER)

# Acquisition code only updates the model; publishing follows.
model.set_value("meter", "active-power", 1850.0)
```

`build_from_declarations` groups specs by `capability` (one Homie node each), defaulting each node's type to `energy.ebus.capability.<capability>` (override with `node_type=`). Pass `values={(capability, prop_id): value}` to seed initial values through the model. Note `PropertySpec.scale` is metadata for your own value mapping (unit conversion); the builder does not apply it, so scale the value before you `set_value` it.

## Ingesting Home Assistant MQTT discovery

A common proxy source is a device that already publishes Home Assistant MQTT discovery (many gateways do). `ebus_sdk.ha` turns that into this same pattern:

- `ebus_sdk.ha.parse_device_config(payload)` parses a `homeassistant/device/<id>/config` message into a neutral `HADevice` (device metadata plus `HAComponent`s), handling abbreviated keys, the `~` base-topic macro, `value_template` field recovery, availability, and removal messages.
- `ebus_sdk.ha.derive_spec(device_class, unit_of_measurement, field_name)` turns a component's HA `device_class` + `unit_of_measurement` into a `PropertySpec` (best-effort eBus datatype / unit / scale).

Two mappers cooperate here, and the distinction matters:

- **The domain-specific mapper** is a hand-authored `{source_field: PropertySpec}` table for one device family (for example an EKM meter). It is authoritative and encodes knowledge the generic HA metadata cannot carry: the exact eBus capability and property name, the canonical unit, and any `scale`. For instance an EKM `kWh_Tot` maps to `meter` / `imported-energy` in `Wh` with `scale=1000`, and per-phase fields get the spec's `-a` / `-b` / `-c` suffixes. You write one of these per device family; it is the part that makes the output spec-correct.
- **The general HA mapper** is `ebus_sdk.ha.derive_spec`. It infers a `PropertySpec` purely from a component's HA `device_class` + `unit_of_measurement`, vendor-neutrally and best-effort (the property id is just the sanitized source field name). It knows nothing about any particular device; it only knows what Home Assistant's discovery metadata says.

`resolve` combines them with a fixed precedence: **the domain-specific mapper wins per field; the general HA mapper fills the gaps; a field with neither an explicit entry nor a usable HA hint is held (dropped, never guessed).** So the fields you know come out spec-correct, and the long tail is still covered straight from the discovery metadata. The domain-specific mapper is the `mapping` argument; the general HA mapper is the `fallback`:

```python
from ebus_sdk import resolve, specs_and_values, build_from_declarations
from ebus_sdk.ha import derive_spec

def ha_fallback(components_by_field):
    def _fb(field):
        c = components_by_field.get(field)
        return derive_spec(c.device_class, c.unit_of_measurement, c.value_field or field) if c else None
    return _fb

resolved = resolve(field_names, values, MY_EXPLICIT_MAP, fallback=ha_fallback(components_by_field))
specs, seed = specs_and_values(resolved)
build_from_declarations(device, model, specs, values=seed)
```

See [`doc/ha-mqtt-discovery.md`](ha-mqtt-discovery.md) for the discovery format. The reverse direction (emitting HA discovery from a Homie device) is planned; it reuses the same neutral `HADevice` model.

## Static vs dynamic device shape

The declarations above are static: the property set is known up front (a water heater always has the same shape). Many proxies are dynamic instead: the property set is discovered at runtime (per-meter fields from a discovery message, per-circuit properties on a panel, one child device per thing found). Both fit this pattern.

- **Static shape:** declare all properties once at construction, inside a single `state_transition()`.
- **Dynamic shape:** declare properties (and create child devices) as you discover them. Wrap structural changes in `state_transition()` so each batch is one `init` to `ready` cycle, or use `GroupedPropertyDict.bulk_update()` and observe `ChangeEvent.GROUP_CREATED` / `PROPERTY_ADDED` to mirror new structure onto Homie as it appears. The declaration table still drives what each discovered field becomes; you just apply it lazily.

## Device topology: bridge root plus proxied children

A proxy is not one flat device. Per the eBus [`proxy.md`](https://github.com/electrification-bus/specification/blob/main/devices/proxy.md) convention:

- Publish a **bridge root device** of type `energy.ebus.device.bridge`. It owns the MQTT connection (and the Last Will) and carries an `info` capability whose `vendor-name` identifies the proxy publisher. It does not publish the proxied device's measurements itself.
- Publish **one child device per proxied device**, each `Device(id=..., type=..., parent=root)`. The proxied measurements live here.
- Name each child `{proxier-id}-{proxied-id}` (the proxied id is the device's stable serial when it has one). Consumers correlate a proxy and a native publisher of the same physical device by `info/serial-number`, not by device id.

Children share the root's single MQTT connection automatically (that is what `parent=` does), and one Last Will on the root marks the whole tree `lost` if the process dies. See [Device Trees](../README.md#device-trees-parent--child) in the README.

## Lifecycle and state

- **Batch structural changes.** Adding N nodes/properties inside one `with device.state_transition():` collapses to a single `$description` publish and one `init` to `ready` edge, instead of N. Always build a device's structure inside a transition.
- **Connect before you publish.** `Device(..., mqtt_cfg=...)` connects asynchronously. If you build and publish before the broker connection is established, the first retained `$description` / `$state` the broker keeps can be a pre-connect snapshot until the SDK's on-connect refresh corrects it. Wait for `device.mqttc.is_connected()` before the initial build so the first retained state is correct.
- **Drive `$state` from availability.** When your upstream reports a device offline, set the child `DeviceState.LOST` (and `READY` when it returns). The root's Last Will covers process death.

## Settable / bidirectional properties (control back to the device)

If eBus controllers should be able to command the proxied device (a relay, a setpoint, a DR event), declare the property `settable=True` and give it an `entity_setter`: a `callable(value)` that translates an incoming command into a device action. `build_from_declarations` wires the whole inbound path from that one declaration:

```python
PropertySpec("dr", "event", PropertyDatatype.JSON, settable=True, entity_setter=self._on_dr_event_set)
```

For each settable spec that has an `entity_setter`, `build_from_declarations` registers the `entity_setter` on the observable model and sets the Homie property's `set_callback` to `partial(model.set_entity, capability, prop_id)`. The property's `/set` subscription is already established when the property is added, so no manual wiring or `set_settable` toggle is needed.

Inbound flow: MQTT `/set` -> Homie property `set_callback` -> `model.set_entity(capability, prop_id, value)` -> your `entity_setter` -> device command. Your `entity_setter` actuates the device and, once the real state changes, writes it back with `model.set_value(...)`, which mirrors onto Homie via the outbound (report) path.

If you build the tree by hand instead of via `build_from_declarations`, wire the same two seams yourself: `model.set_entity_setter(capability, prop_id, fn)` and `homie_property.set_set_callback(partial(model.set_entity, capability, prop_id))`.

### Settable `json` properties and `$format` validation

For a settable `json` property (a compound command like `flex/request`), give the `PropertySpec` a `format` that is the JSON Schema of the command surface your device accepts. An inbound `/set` payload is then `json.loads`ed to a `dict`/`list` and validated against that schema before your `entity_setter` runs, so your `entity_setter` receives a parsed, schema-valid object and a malformed or out-of-surface command is rejected for you:

```python
PropertySpec(
    "flex", "request", PropertyDatatype.JSON, settable=True,
    format=json.dumps({"type": "object",
                       "properties": {"mode": {"enum": ["SHED", "LOAD_UP", "NORMAL"]},
                                      "level": {"type": "integer", "minimum": 0, "maximum": 100}},
                       "required": ["mode"]}),
    entity_setter=self._on_flex_request,   # receives a validated dict
)
```

Validation uses the optional `jsonschema` package (`pip install ebus-sdk[validation]`). Without it the property still works and validation is skipped with a one-time warning, so a constrained build can omit the dependency. On the consumer side, a controller reads a parsed object with `discovered_device.get_property_json(node, prop)` and issues a validated command with `controller.set_property_json(device_id, node, prop, obj)`. To honor the device's advertised control surface, `discovered_device.get_property_format_fields(node, prop)` (or `json_format_fields(schema)`) introspects the `$format` into per-field `JsonFieldConstraint`s — each reporting `enum` values, a numeric `range` (`minimum`/`maximum`/`multiple_of`), or an absent field — so a UI can render `flex/request`'s `level` as buttons or a stepped slider straight from the schema.

## The anti-pattern (what not to do)

Do not drive `homie.Device` / `Node` / `Property` directly from your acquisition loop and cache raw Homie property handles:

```python
# ANTI-PATTERN: no observable model, raw Homie handles kept in a dict
self._props = {}                     # (node, prop) -> homie.Property
...
self._props[("meter", "active-power")].set_value(read_watts())   # from the read loop
```

It works, and it is tempting because it is fewer lines at first. But it reinvents, more crudely, what `GroupedPropertyDict` already gives you: it has no queryable local model of the device, no clean seam for settable properties, and it diverges from every other eBus proxy so it reads differently for the next maintainer. If you find yourself storing a `dict` of `homie.Property` handles and calling `.set_value()` on them from your data path, switch to the observable model: put the values in a `GroupedPropertyDict` and `bind_property_to_homie` them.

## Checklist

- [ ] Declarative property/node definitions are the single source of truth.
- [ ] A `GroupedPropertyDict` holds live state; acquisition code only calls `set_value`.
- [ ] The Homie tree is built from the declarations inside a `state_transition()`.
- [ ] Each property is bound with `bind_property_to_homie` (never a hand-rolled mirror).
- [ ] A bridge root (`energy.ebus.device.bridge`) plus child devices named `{proxier-id}-{proxied-id}`.
- [ ] Structural changes are batched; the device is connected before the first publish.
- [ ] Settable properties (if any) declare an `entity_setter` on their `PropertySpec` (auto-wired by `build_from_declarations`).
- [ ] No raw `homie.Property` handles cached in your data path.

## Worked examples

- [`examples/utility-meter`](../examples/utility-meter) is the fullest reference: an observable `UtilityMeter` model plus a `UtilityMeterAdapter` that mirrors it onto Homie, including a settable capability.
- [`examples/simple-device`](../examples/simple-device) is a minimal version of the same shape.

## Related

- [README Quick Start](../README.md#quick-start): the plain `Device` API for static publishing.
- [`property.py`](../src/ebus_sdk/property.py): the observable `Property` / `GroupedPropertyDict` / `ChangeEvent` classes.
- [`declaration.py`](../src/ebus_sdk/declaration.py): `PropertySpec` and `build_from_declarations`.
- [`adapter.py`](../src/ebus_sdk/adapter.py): the exported mirror helpers.
- [`ebus_sdk.ha`](../src/ebus_sdk/ha/) and [`doc/ha-mqtt-discovery.md`](ha-mqtt-discovery.md): ingesting Home Assistant MQTT discovery.
- eBus [`proxy.md`](https://github.com/electrification-bus/specification/blob/main/devices/proxy.md): the normative proxier / device-id convention.
