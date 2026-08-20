"""Declarative property specifications and a builder that materializes them.

A `PropertySpec` describes how a source field becomes an eBus property: which
capability (Homie node) it lives on, its Homie datatype and unit, an optional
unit `scale`, whether it is settable, how it is rounded and retained, whether it
is published at all, and where it lives in the observable model when that
differs from where it lives on the wire. It is the declarative "schema" layer of
the proxy pattern (see `doc/building-a-proxy.md`). It is complementary to
`property.py`: a `PropertySpec` is a static declaration, while a `property.py`
`Property` is the live observable value built from it.

`build_from_declarations` turns a set of `PropertySpec`s into a live device in
one call: one Homie node per capability, an observable `Property` plus a Homie
property per spec, and the binding between them, all inside a single state
transition. Acquisition code then only calls
`model.set_value(group_key, model_key, value)` and publishing follows.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from typing import Any, Callable, Iterable, Optional

from .adapter import bind_property_to_homie
from .homie import Device, PropertyDatatype, Unit
from .property import GroupedPropertyDict
from .property import Property as ObservableProperty

_PYTHON_TYPE = {
    PropertyDatatype.FLOAT: float,
    PropertyDatatype.INTEGER: int,
    PropertyDatatype.BOOLEAN: bool,
    PropertyDatatype.STRING: str,
    PropertyDatatype.ENUM: str,
    PropertyDatatype.DATETIME: str,
    PropertyDatatype.DURATION: str,
    PropertyDatatype.JSON: "json",
}


def python_type_for(datatype: PropertyDatatype) -> Any:
    """The observable-`Property` python type for a Homie datatype (default: `str`)."""
    return _PYTHON_TYPE.get(datatype, str)


@dataclass(frozen=True)
class PropertySpec:
    """Declaration of one eBus property: where it lives and what it is.

    `capability` is the Homie node id (an eBus capability); `prop_id` is the
    Homie property id. `scale` multiplies a source value to reach `unit` (e.g.
    kWh -> Wh is 1000); it is applied by `resolve`, NOT by
    `build_from_declarations` (see both). `python_type` overrides the
    observable-`Property` type (otherwise derived from `datatype`).

    `entity_setter` is the inbound-control translator for a settable property: a
    `callable(value)` invoked when a `/set` command arrives. When `settable=True`
    and `entity_setter` is given, `build_from_declarations` wires the whole
    inbound path automatically (see there); a settable spec without an
    `entity_setter` still gets a `/set` topic but no auto-wired handler.

    The remaining fields describe a property's wire and model behavior. Each
    defaults to what the spec did before it existed, so an existing declaration
    set is unaffected:

    * `round_to`: decimal places applied to a float on publish, by the Homie
      property itself. Since the publish-on-change gate compares the FINAL
      payload, rounding is part of what decides whether two consecutive readings
      are the same value, so a rounded property also publishes less.
    * `initial_value`: a seed value applied through the model at build time. The
      `values` argument to `build_from_declarations` overrides it.
    * `retained`: False declares an event property. The broker stores nothing
      for it, so it is exempt from the publish-on-change gate and an identical
      consecutive payload is a second real event.
    * `internal_only`: the observable model tracks the value and no Homie
      property is created, so it is never published and never appears in
      `$description`. A capability whose specs are ALL internal gets no node.
    * `conditionally_settable`: this property's settability is decided at
      runtime, per instance. The builder materializes it NOT settable, which
      keeps `$description` truthful and leaves no `/set` subscription open on a
      property that would reject the command; the caller enables it with
      `homie.Property.set_settable(True)` inside a `state_transition()`. It is
      mutually exclusive with `settable`, which means "settable now".
    * `source_id` / `model_group`: the observable-model identity, when it
      differs from the wire identity. `source_id` defaults to `prop_id` and
      `model_group` to `capability`, so they are fused unless split. Splitting
      the group is what lets two child devices in one tree both expose an
      `info` capability without colliding in a shared model.
    """

    capability: str
    prop_id: str
    datatype: PropertyDatatype
    unit: Optional[Unit] = None
    scale: float = 1.0
    settable: bool = False
    name: Optional[str] = None
    format: Optional[str] = None
    python_type: Any = None
    entity_setter: Optional[Callable] = None
    round_to: Optional[int] = None
    initial_value: Any = None
    retained: bool = True
    internal_only: bool = False
    conditionally_settable: bool = False
    source_id: Optional[str] = None
    model_group: Optional[str] = None

    def __post_init__(self) -> None:
        # Two contradictions are worth refusing at declaration time rather than
        # producing a tree that misdescribes itself.
        if self.settable and self.conditionally_settable:
            raise ValueError(
                f"{self.capability}/{self.prop_id}: settable and conditionally_settable are mutually "
                "exclusive; settable means settable now, conditionally_settable means the caller "
                "decides at runtime"
            )
        if self.internal_only and (self.settable or self.conditionally_settable):
            raise ValueError(
                f"{self.capability}/{self.prop_id}: an internal_only property is never published, so it "
                "has no /set topic and cannot be settable"
            )

    @property
    def model_key(self) -> str:
        """The observable-model property id: `source_id` if split, else `prop_id`."""
        return self.source_id or self.prop_id

    @property
    def group_key(self) -> str:
        """The observable-model group: `model_group` if split, else `capability`."""
        return self.model_group or self.capability


def _default_node_type(capability: str) -> str:
    return f"energy.ebus.capability.{capability}"


def build_from_declarations(
    device: Device,
    model: GroupedPropertyDict,
    specs: Iterable[PropertySpec],
    *,
    node_type: Callable[[str], str] = _default_node_type,
    node_name: Callable[[str], str] = lambda capability: capability,
    values: Optional[dict] = None,
) -> dict:
    """Build bound Homie nodes/properties + observable properties from `specs`.

    Groups `specs` by capability (one Homie node each) and, for every spec,
    creates an observable `Property` in `model` and a Homie property on the node,
    wired together with `bind_property_to_homie` (the outbound/report path). Runs
    inside one `device.state_transition()`. Returns
    `{(capability, prop_id): homie.Property}`, keyed by WIRE identity; a spec
    with `internal_only=True` creates no Homie property and so is absent from it.

    Values are seeded THROUGH the model after the structure is built, so they
    publish via the bindings. Two sources, in precedence order: a spec's
    `initial_value`, then the `values` argument (a `{(capability, prop_id):
    value}` map), which wins because a caller passing a runtime map is being more
    specific than the static declaration. Entries in `values` naming a property
    that was not declared are ignored.

    `PropertySpec.scale` is NOT applied here. It is applied by `resolve`, and
    `specs_and_values` hands this function values that `resolve` has ALREADY
    scaled, so scaling again would double-apply it. A caller who assembles a
    `values` map by hand therefore passes values in the property's own unit, not
    raw source units.

    A spec may split its observable-model identity from its wire identity via
    `source_id` / `model_group` (see `PropertySpec`). Everything on the model
    side of the binding uses `spec.group_key` / `spec.model_key`; everything on
    the Homie side uses `capability` / `prop_id`. Unsplit, they are the same
    strings and this reads exactly as it did before.

    For a spec with `settable=True` AND an `entity_setter`, the inbound/control
    path is wired automatically: the observable `Property`'s `entity_setter` is
    registered on `model`, and the Homie property's `set_callback` is set to
    `partial(model.set_entity, group_key, model_key)`, so an arriving `/set`
    command routes `/set` payload -> `model.set_entity` -> the `entity_setter`.
    The `/set` subscription itself is already established when the property is
    added (`Node.add_property` -> `Property.set_subscribe`), so no `set_settable`
    toggle is needed. An `internal_only` spec has no Homie property to receive a
    command, but its `entity_setter` is still registered, so `model.set_entity`
    reaches it.
    """
    grouped: dict[str, list[PropertySpec]] = {}
    for spec in specs:
        grouped.setdefault(spec.capability, []).append(spec)

    homie_props: dict = {}
    declared: dict[tuple, PropertySpec] = {}
    with device.state_transition():
        for capability, cap_specs in grouped.items():
            # A node exists to carry published properties. If every spec on this
            # capability is internal, creating one would announce an empty node.
            published = [spec for spec in cap_specs if not spec.internal_only]
            node = (
                device.add_node_from_dict(
                    {"id": capability, "name": node_name(capability), "type": node_type(capability)}
                )
                if published
                else None
            )
            for spec in cap_specs:
                group = spec.group_key
                if not model.has_group(group):
                    model.create_group(group)
                py_type = spec.python_type if spec.python_type is not None else python_type_for(spec.datatype)
                model.add_property(group, ObservableProperty(id=spec.model_key, type=py_type))
                declared[(capability, spec.prop_id)] = spec
                # An entity_setter is the translator toward the entity, so it is
                # registered whenever one is given and the model can reach it.
                if spec.entity_setter is not None and (spec.settable or spec.internal_only):
                    model.set_entity_setter(group, spec.model_key, spec.entity_setter)
                if spec.internal_only or node is None:
                    continue
                prop_dict: dict = {"id": spec.prop_id, "datatype": spec.datatype}
                if spec.name:
                    prop_dict["name"] = spec.name
                if spec.unit is not None:
                    prop_dict["unit"] = spec.unit
                if spec.settable:
                    prop_dict["settable"] = True
                if spec.format:
                    prop_dict["format"] = spec.format
                if spec.round_to is not None:
                    prop_dict["round_to"] = spec.round_to
                if not spec.retained:
                    prop_dict["retained"] = False
                homie_prop = node.add_property_from_dict(prop_dict)
                bind_property_to_homie(model, group, spec.model_key, homie_prop)
                # Inbound/control path for a settable property with a translator:
                # /set payload -> model.set_entity -> entity_setter. The /set
                # subscription is already live from add_property -> set_subscribe.
                if spec.settable and spec.entity_setter is not None:
                    homie_prop.set_set_callback(partial(model.set_entity, group, spec.model_key))
                homie_props[(capability, spec.prop_id)] = homie_prop

    # Seed declared initial values first, then let an explicit `values` entry
    # override: the runtime map is the more specific statement of the two.
    seed: dict[tuple, Any] = {
        key: spec.initial_value for key, spec in declared.items() if spec.initial_value is not None
    }
    if values:
        seed.update({key: value for key, value in values.items() if key in declared})
    for key, value in seed.items():
        spec = declared[key]
        model.set_value(spec.group_key, spec.model_key, value)
    return homie_props


@dataclass(frozen=True)
class ResolvedProperty:
    """A `PropertySpec` paired with a resolved (already-scaled) value."""

    spec: PropertySpec
    value: Any


def resolve(
    field_names: Iterable[str],
    values: dict,
    mapping: dict,
    *,
    fallback: Optional[Callable[[str], Optional[PropertySpec]]] = None,
) -> list:
    """Resolve source fields to `PropertySpec`s and values: explicit mapping, then fallback.

    For each field name: look it up in `mapping` (a `{field: PropertySpec}` dict);
    if absent and `fallback` is given, call `fallback(field)` for a spec; if still
    unresolved, the field is held (skipped). The value from `values` is multiplied
    by the spec's `scale` (numeric, non-bool values only). Duplicate field names
    resolve once. Returns a list of `ResolvedProperty`.

    This is the two-tier mapping mechanism: a hand-authored `mapping` wins, and a
    generic `fallback` (e.g. `ebus_sdk.ha.derive_spec` over discovered components)
    fills the gaps. Feed the result to `build_from_declarations` via
    `specs_and_values`.
    """
    resolved: list = []
    seen: set = set()
    for field_name in field_names:
        if field_name in seen:
            continue
        seen.add(field_name)
        spec = mapping.get(field_name)
        if spec is None and fallback is not None:
            spec = fallback(field_name)
        if spec is None:
            continue
        value = values.get(field_name)
        if value is not None and spec.scale != 1.0 and isinstance(value, (int, float)) and not isinstance(value, bool):
            value = value * spec.scale
        resolved.append(ResolvedProperty(spec, value))
    return resolved


def specs_and_values(resolved: Iterable[ResolvedProperty]) -> tuple:
    """Split a `resolve` result into `(specs, values)` for `build_from_declarations`.

    `values` is `{(capability, prop_id): value}` and omits entries whose value is
    None (declared-but-not-yet-observed properties still appear in `specs`).
    """
    resolved = list(resolved)
    specs = [r.spec for r in resolved]
    values = {(r.spec.capability, r.spec.prop_id): r.value for r in resolved if r.value is not None}
    return specs, values
