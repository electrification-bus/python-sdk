"""An eBus-aware default customizer: a smarter override hook for the emitter.

Generic unit inference (`emit`) handles any Homie device, but it cannot resolve
what a bare unit leaves ambiguous: a `%` could be battery, humidity, or power
factor; an energy register's `total_increasing` semantics depend on meaning, not
unit. `ebus_default_override` closes that gap for devices that follow the eBus
capability vocabulary (`energy.ebus.capability.<capability>` node types plus
known property ids), producing better HA metadata than inference alone.

It is a drop-in `emit.OverrideHook`: pass it as a bridge's `default_override` (or
`homie_device_to_ha(..., override=ebus_default_override)`) and it enriches
recognized properties while leaving everything else exactly as inference produced
it. It only ADDS or SHARPENS metadata; it never drops an entity, so it is safe as
a blanket default. A caller wanting per-property suppression composes their own
hook on top (call this first, then adjust).
"""

from __future__ import annotations

from typing import Optional

from .discovery import HAComponent
from .emit import PropertyContext

# (capability, property_id) -> HA metadata that domain knowledge nails better
# than unit inference. Keys naming a typed HAComponent field (see `_TYPED_KEYS`,
# e.g. `device_class` / `state_class` / `value_template`) override the inferred
# value; other keys (`entity_category` / `icon`) land in the component config
# overlay. A capability's "*" entry applies to every property on that capability.
_CAPABILITY_META = {
    # energy.ebus.capability.meter
    "meter": {
        "imported-energy": {"device_class": "energy", "state_class": "total_increasing"},
        "exported-energy": {"device_class": "energy", "state_class": "total_increasing"},
        # Reactive/apparent energy registers are cumulative like the active ones,
        # but HA has no energy device_class for varh/VAh (see `emit._UNIT_SEMANTICS`),
        # so they carry the state_class alone: without it HA keeps no long-term
        # statistics for them. A state_class without a device_class is legal.
        "imported-reactive-energy": {"state_class": "total_increasing"},
        "exported-reactive-energy": {"state_class": "total_increasing"},
        "apparent-energy-imported": {"state_class": "total_increasing"},
        "apparent-energy-exported": {"state_class": "total_increasing"},
        "active-power": {"device_class": "power", "state_class": "measurement"},
        "reactive-power": {"device_class": "reactive_power", "state_class": "measurement"},
        "apparent-power": {"device_class": "apparent_power", "state_class": "measurement"},
        "voltage": {"device_class": "voltage", "state_class": "measurement"},
        "current": {"device_class": "current", "state_class": "measurement"},
        "frequency": {"device_class": "frequency", "state_class": "measurement"},
        # Power factor is unitless, so inference has nothing to go on. The table has
        # no pattern support, so the per-phase forms are spelled out.
        "power-factor": {"device_class": "power_factor", "state_class": "measurement"},
        "power-factor-a": {"device_class": "power_factor", "state_class": "measurement"},
        "power-factor-b": {"device_class": "power_factor", "state_class": "measurement"},
        "power-factor-c": {"device_class": "power_factor", "state_class": "measurement"},
    },
    # energy.ebus.capability.soc. There is no `battery` capability: state of charge
    # lives here, for any reservoir (a BESS pack, a water heater's tank).
    "soc": {
        # A bare percent is ambiguous to inference; here it is unmistakably SoC.
        "soc": {"device_class": "battery", "state_class": "measurement"},
        # Reservoir LEVELS, not accumulating registers. Inference sees Wh/kWh and
        # says energy + total_increasing, under which HA reads every discharge as a
        # meter reset and back-fills the drop as freshly consumed energy. HA's
        # `energy_storage` is the level counterpart: same units, measurement only.
        "soe": {"device_class": "energy_storage", "state_class": "measurement"},
        "total-energy-storage": {"device_class": "energy_storage", "state_class": "measurement"},
        "loadup-headroom": {"device_class": "energy_storage", "state_class": "measurement"},
    },
    # energy.ebus.capability.info
    "info": {
        # Identity/firmware fields are diagnostics, not primary state.
        "*": {"entity_category": "diagnostic"},
        # A nameplate rating is a constant, not an accumulating register: same
        # correction as the `soc` energy levels above.
        "nameplate-capacity": {"device_class": "energy_storage", "state_class": "measurement"},
    },
}

# Metadata keys that map to typed HAComponent fields (set via attribute); every
# other key (entity_category, icon, options, ...) goes to the config overlay.
# This MUST list every typed HAComponent field a table may set: a typed field is
# pre-populated on the component and emitted from `emit._COMPONENT_FIELDS`, so
# routing it through `config.setdefault` would be silently dropped (SDK-anu).
# `value_template` in particular is pre-set to "{{ value }}", so a table
# `value_template` only takes effect when routed to the typed field.
_TYPED_KEYS = {
    "device_class",
    "state_class",
    "value_template",
    "unit_of_measurement",
    "name",
    "default_entity_id",
}


def _capability_of(ctx: PropertyContext) -> Optional[str]:
    """The eBus capability for a property: from `energy.ebus.capability.<cap>`.

    Falls back to the node id when the node `$type` is absent or not an eBus
    capability type (node ids are conventionally the capability name).
    """
    node_type = ctx.node_type or ""
    marker = ".capability."
    if marker in node_type:
        return node_type.rsplit(marker, 1)[-1]
    return ctx.node_id or None


def _meta_for(capability: Optional[str], prop_id: str) -> dict:
    """Merge a capability's `*` defaults with its per-property entry."""
    table = _CAPABILITY_META.get(capability or "", {})
    meta: dict = {}
    meta.update(table.get("*", {}))
    meta.update(table.get(prop_id, {}))
    return meta


def ebus_default_override(component: HAComponent, ctx: PropertyContext) -> HAComponent:
    """Enrich an inferred `HAComponent` using eBus capability/property knowledge.

    Recognized `(capability, property_id)` pairs get sharper `device_class` /
    `state_class` (and `entity_category` / `icon` where useful) than unit
    inference alone; unrecognized properties pass through unchanged. Always
    returns the component (never suppresses), so it is safe as a blanket default.
    """
    meta = _meta_for(_capability_of(ctx), ctx.prop_id)
    if not meta:
        return component
    for key, value in meta.items():
        if key in _TYPED_KEYS:
            setattr(component, key, value)
        else:
            component.config.setdefault(key, value)
    return component
