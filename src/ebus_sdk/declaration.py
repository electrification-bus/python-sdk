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

from contextlib import nullcontext
from dataclasses import dataclass
from functools import partial
from typing import Any, Callable, Iterable, Optional, Sequence, Union

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
      `homie.Property.set_settable(True)` inside a `state_transition()`. The
      `entity_setter` is wired at build time even though the property starts
      not-settable, because `set_settable(True)` subscribes immediately and a
      `/set` topic with no translator behind it accepts commands and discards
      them. It is mutually exclusive with `settable`, which means "settable now".
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
    node_id: Callable[[str], str] = lambda capability: capability,
    values: Optional[dict] = None,
) -> dict:
    """Build bound Homie nodes/properties + observable properties from `specs`.

    Groups `specs` by capability (one Homie node each) and, for every spec,
    creates an observable `Property` in `model` and a Homie property on the node,
    wired together with `bind_property_to_homie` (the outbound/report path). An
    observable property the model ALREADY holds is reused, never replaced, since
    replacing it would discard the live value and every callback and
    `entity_setter` attached to it. Runs inside one `device.state_transition()`. Returns
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

    `node_id` renames the Homie node a capability materializes onto, defaulting
    to the capability itself. It exists for the case a single device carries two
    instances of one capability: two meters, two lugs. `capability` stays the
    declaration's vocabulary and `node_id` is the rendering, so call this once
    per instance with a distinct `node_id` (and a distinct `PropertySpec.model_group`,
    or the instances collide in the model instead of on the wire).

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
    built = _materialize(device, model, specs, node_type=node_type, node_name=node_name, node_id=node_id)
    _seed(model, built.declared, values)
    return built.homie_props


@dataclass(frozen=True)
class _Materialized:
    """What one device's materialization produced, for the caller's bookkeeping."""

    homie_props: dict
    declared: dict
    model_keys: list  # (capability, group, model_key), so a node's share is identifiable
    created_groups: list


def _group_for(spec: PropertySpec, default_group: Optional[str]) -> str:
    """The observable-model group a spec's value lives in.

    An explicit `model_group` on the spec always wins: the caller is naming a
    group in a model they own. Otherwise `default_group` applies, which is how a
    device tree gives each device its own group; with neither, the group is the
    capability, which is what a single-device build has always done.
    """
    if spec.model_group is not None:
        return spec.model_group
    return default_group if default_group is not None else spec.capability


def _materialize(
    device: Device,
    model: GroupedPropertyDict,
    specs: Iterable[PropertySpec],
    *,
    node_type: Callable[[str], str],
    node_name: Callable[[str], str],
    node_id: Callable[[str], str] = lambda capability: capability,
    default_group: Optional[str] = None,
) -> _Materialized:
    """Build one device's nodes, properties, model entries and bindings.

    The single materialization path, shared by `build_from_declarations` (one
    device, model groups keyed by capability) and `DeviceTreeBuilder` (many
    devices, model groups keyed per device). Runs inside one
    `device.state_transition()`, so a device announces its structure once.

    An observable property already present in `model` is REUSED rather than
    replaced, and is not recorded in `model_keys`, so a later teardown removes
    only what this call created. A spec whose python type disagrees with the
    property already there raises instead of silently binding a Homie property to
    a mismatched twin.

    `node_id` maps a capability to the Homie node id it materializes onto,
    defaulting to the capability itself. Only the id is renamed: the model group
    still comes from the spec, and the returned map is still keyed by the
    declared capability.
    """
    grouped: dict[str, list[PropertySpec]] = {}
    for spec in specs:
        grouped.setdefault(spec.capability, []).append(spec)

    homie_props: dict = {}
    declared: dict[tuple, PropertySpec] = {}
    model_keys: list = []
    created_groups: list = []
    # Open a state transition only if there is something to announce. An empty
    # one still emits init -> ready, and that edge forces every controller on the
    # bus to resync, so a re-declaration that changes nothing must not cost one.
    needs_transition = _needs_materializing(device, model, grouped, node_id, default_group)
    with device.state_transition() if needs_transition else nullcontext():
        for capability, cap_specs in grouped.items():
            # A node exists to carry published properties. If every spec on this
            # capability is internal, creating one would announce an empty node.
            published = [spec for spec in cap_specs if not spec.internal_only]
            # Reuse an existing node. Device.add_node is a wholesale
            # `self._nodes.update(...)`, so re-declaring would drop the previous
            # node's properties from $description while LEAVING their retained
            # topics on the broker (only delete_node clears those), producing a
            # tree whose description and whose broker state disagree.
            node = None
            if published:
                node = device.get_node(node_id(capability)) or device.add_node_from_dict(
                    {
                        "id": node_id(capability),
                        "name": node_name(capability),
                        "type": node_type(capability),
                    }
                )
            for spec in cap_specs:
                group = _group_for(spec, default_group)
                if not model.has_group(group):
                    model.create_group(group)
                    created_groups.append(group)
                py_type = spec.python_type if spec.python_type is not None else python_type_for(spec.datatype)
                existing = model.get(group, spec.model_key)
                if existing is None:
                    model.add_property(group, ObservableProperty(id=spec.model_key, type=py_type))
                    # Only what this call created, so a later remove() deletes what
                    # it added and leaves anything the producer owned first.
                    model_keys.append((capability, group, spec.model_key))
                elif existing.type() is not py_type:
                    raise ValueError(
                        f"{capability}/{spec.prop_id}: the model already holds "
                        f"{group}/{spec.model_key} with type {existing.type()!r}, but this spec "
                        f"declares {py_type!r}. Reusing it would publish values of one type through "
                        "a property built for another; align the spec's datatype (or python_type) "
                        "with the model, or give the spec its own source_id/model_group."
                    )
                declared[(capability, spec.prop_id)] = spec
                # An entity_setter is the translator toward the entity, so it is
                # registered whenever one is given and the model can reach it.
                if spec.entity_setter is not None and (
                    spec.settable or spec.conditionally_settable or spec.internal_only
                ):
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
                # Reuse likewise: Node.add_property replaces wholesale and
                # republishes with force=True, so re-declaring an unchanged
                # property would re-announce it. A datatype that actually changed
                # is caught on the model side above, which raises.
                existing_prop = node.get_property(spec.prop_id)
                if existing_prop is not None:
                    homie_props[(capability, spec.prop_id)] = existing_prop
                    continue
                homie_prop = node.add_property_from_dict(prop_dict)
                bind_property_to_homie(model, group, spec.model_key, homie_prop)
                # The binding is on-change, and the twin starts empty, so a value
                # the model was already holding would never reach the wire: a
                # producer whose model predates the tree would publish its
                # declared default forever, and it would not self-heal, because
                # set_value fires callbacks only on an actual change.
                current = model.value(group, spec.model_key)
                if current is not None:
                    homie_prop.set_value(current)
                # Inbound/control path for a settable property with a translator:
                # /set payload -> model.set_entity -> entity_setter. The /set
                # subscription is already live from add_property -> set_subscribe.
                # conditionally_settable too, and this is the whole point of it:
                # the property is built not-settable, so no /set topic is open
                # yet, but the caller flips it with set_settable(True) later and
                # that subscribes immediately. Wiring the translator now is what
                # stops that topic from accepting commands and discarding them.
                if spec.entity_setter is not None and (spec.settable or spec.conditionally_settable):
                    homie_prop.set_set_callback(partial(model.set_entity, group, spec.model_key))
                homie_props[(capability, spec.prop_id)] = homie_prop

    return _Materialized(homie_props, declared, model_keys, created_groups)


def _needs_materializing(
    device: Device,
    model: GroupedPropertyDict,
    grouped: dict,
    node_id: Callable[[str], str],
    default_group: Optional[str],
) -> bool:
    """True when any spec still has something to create on the device or the model.

    Answered BEFORE the transition opens, because the question is whether to open
    one at all: an init -> ready edge that announces nothing is a cost paid by
    every controller on the bus.
    """
    for capability, cap_specs in grouped.items():
        for spec in cap_specs:
            if model.get(_group_for(spec, default_group), spec.model_key) is None:
                return True
            if spec.internal_only:
                continue
            node = device.get_node(node_id(capability))
            if node is None or node.get_property(spec.prop_id) is None:
                return True
    return False


def _seed(
    model: GroupedPropertyDict,
    declared: dict,
    values: Optional[dict] = None,
    *,
    default_group: Optional[str] = None,
) -> None:
    """Seed values through the model, so they publish via the bindings.

    Declared `initial_value`s first, then any explicit `values` entry overriding
    them: a caller passing a runtime map is being more specific than the static
    declaration. Entries naming an undeclared property are ignored.
    """
    # A declared initial_value SEEDS, it does not overwrite: a model that already
    # holds a value for this property holds a fresher one than the declaration.
    # An explicit `values` entry still wins below, since that caller is being
    # specific about this run rather than about the property in general.
    seed: dict[tuple, Any] = {
        key: spec.initial_value
        for key, spec in declared.items()
        if spec.initial_value is not None and model.value(_group_for(spec, default_group), spec.model_key) is None
    }
    if values:
        seed.update({key: value for key, value in values.items() if key in declared})
    for key, value in seed.items():
        spec = declared[key]
        model.set_value(_group_for(spec, default_group), spec.model_key, value)


@dataclass(frozen=True, eq=False)
class DeviceSpec:
    """Declaration of one device in a tree: what it is, where it sits, what it carries.

    The device-level counterpart to `PropertySpec`. Device class, device id and
    parent are device-level facts, so they live here rather than being repeated
    on every property of the device.

    * `device_class` is the eBus class (`circuit`, `bess`, `distribution-enclosure`).
      `device_type` defaults to `energy.ebus.device.{device_class}`, and that
      default is the main guard a consumer gets: the SDK stores `Device.type`
      verbatim and validates nothing against a registry, so a hand-written
      misspelling ships silently. Prefer the default; override only for a type
      outside the eBus namespace.
    * `device_id` is either the id or a callable returning it, returning `None`
      while it is still unknown. Child ids are often only known once an
      asynchronous identifier arrives (a DER's serial number), and a child
      published under a wrong-but-stable id leaves retained topics that outlive
      restarts and firmware updates, so waiting is worth the deferral machinery.
      Ids are used verbatim: run them through `sanitize_homie_id` yourself if
      they come from a vendor.
    * `parent` names the parent DEVICE SPEC, or `None` for a child of the
      builder's root. The builder resolves it to a live `Device`.
    * `model_group` is this device's group in the externally-owned model,
      defaulting to its resolved device id. A `PropertySpec` that names its own
      `model_group` still wins, so a consumer with an existing model keyed its
      own way keeps that keying.
    * `on_created` runs once, with the live `Device`, right after the device and
      its properties exist. For per-child side effects (ACL emission, registry
      entries) that would otherwise force the caller to post-process the tree.

    Compared by IDENTITY, not by value: a `DeviceSpec` stands for one device in
    one tree, and two devices declared with identical fields are still two
    devices. It is also what the builder keys its bookkeeping on.
    """

    device_class: str
    specs: Sequence[PropertySpec] = ()
    device_id: Union[str, Callable[[], Optional[str]]] = ""
    parent: Optional["DeviceSpec"] = None
    model_group: Union[str, Callable[[], str], None] = None
    device_type: Optional[str] = None
    name: Optional[str] = None
    on_created: Optional[Callable[[Device], None]] = None

    def resolve_device_id(self) -> Optional[str]:
        """This device's id, or None while a late-bound id is still unresolved."""
        if callable(self.device_id):
            return self.device_id()
        return self.device_id or None

    def resolve_device_type(self) -> str:
        """`device_type` if given, else the eBus type derived from `device_class`."""
        return self.device_type or f"energy.ebus.device.{self.device_class}"

    def resolve_model_group(self, device_id: str) -> str:
        """This device's model group: `model_group` if given, else its device id."""
        if callable(self.model_group):
            return self.model_group()
        return self.model_group or device_id


class DeviceTreeBuilder:
    """Materialize a set of `DeviceSpec`s into a live parent/child device tree.

    `build_from_declarations` builds exactly ONE device and creates the
    observable model itself, keyed by capability. That fits a single-device
    proxy and cannot express the shape the eBus framework actually describes: a
    root device whose circuits, lugs, MID and DERs are child devices, each with
    its own id, `$state`, `$description` and capability set.

    This builder covers that shape, and differs from the single-device one in
    four ways that all follow from there being more than one device:

    1. **The model is external, and it is yours.** It is passed in, never
       created, and each device gets its own group (its id by default). Keying
       by capability would collide the moment two children both expose `info`.
       The division: a `GroupedPropertyDict` is the model a producer is meant to
       OWN, not an adapter seam a producer maps a foreign model type onto. That
       is the observable-model pattern `doc/building-a-proxy.md` prescribes,
       where acquisition code writes values into the model and publishing is a
       reactive side effect of the bindings. The builder accepts rather than
       creates one so a single model can span a whole tree, and so a producer
       that already holds one (populated before any Homie tree exists) can hand
       it over.
    2. **Ids can be late-bound.** `add()` returns `None` for a spec whose id is
       not yet knowable and remembers it; `resolve_deferred()` retries, and a
       deferred parent unblocking its deferred children resolves in one call.
       Note the limit of that: `add()` orders late-bound IDS, and builds an
       unbuilt parent it was handed. It does not order the construction of the
       specs themselves. `DeviceSpec` is frozen and `parent` is a direct
       reference, so a child spec cannot exist before its parent spec does, and
       a caller deriving specs from a declarative source that names parents
       indirectly (by class, by type, by key) still owns that dependency
       ordering. `add()` reads as though ordering is handled generally; it is
       handled for ids.
    3. **It is incremental.** Devices come and go over a tree's life, so `add()`
       is idempotent (lifecycles re-fire) and `remove()` tears one down.
    4. **Removal is depth-first**, grandchild before parent, derived from the
       live tree rather than a caller-maintained ordering, so nothing ever
       observes an orphaned child.

    Batching: each `add()` announces its own device, and the parent republishes
    its `$description` to name the new child. To collapse a burst of adds into
    one parent announcement, wrap them in the parent's `state_transition()`.

    `node_id` is passed through to every device this builder materializes, for a
    publisher whose node ids are not simply their capability names. A tree whose
    entities are devices does not need it, since each device has its own node
    namespace; it is for the caller that also places several instances of one
    capability onto a single device.
    """

    def __init__(
        self,
        root: Device,
        model: GroupedPropertyDict,
        *,
        node_type: Callable[[str], str] = _default_node_type,
        node_name: Callable[[str], str] = lambda capability: capability,
        node_id: Callable[[str], str] = lambda capability: capability,
    ) -> None:
        self._root = root
        self._model = model
        self._node_type = node_type
        self._node_name = node_name
        self._node_id = node_id
        self._devices: dict = {}
        self._homie_props: dict = {}
        self._model_keys: dict = {}
        self._created_groups: dict = {}
        self._deferred: list = []
        # The root is not a DeviceSpec, so its bookkeeping lives beside the
        # per-spec maps rather than inside them. Nothing removes a root.
        self._root_props: dict = {}
        self._root_model_keys: list = []

    def add(self, spec: DeviceSpec) -> Optional[Device]:
        """Materialize `spec` as a device, or defer it while its id is unknown.

        Returns the live `Device`, or `None` when the spec (or an ancestor of
        it) has no id yet, in which case it is remembered for
        `resolve_deferred()`. Idempotent: re-adding a spec already built returns
        the same `Device` without touching the tree, because incremental
        lifecycles re-fire and a second add must not republish or duplicate.
        """
        # Keyed on the RESOLVED DEVICE ID, not on this spec object. A producer
        # deriving its spec set from a manifest re-derives equal-but-distinct
        # objects on every pass, and identity keying silently made each pass a
        # new device; the alternative was an unstated obligation to hold a
        # device_id -> DeviceSpec map for the process lifetime and never
        # re-derive, which defeats the point of a declarative API. A spec whose
        # id is already built returns that device unchanged: to give a built
        # device more capabilities, use extend().
        device_id = spec.resolve_device_id()
        if device_id is not None:
            existing = self._devices.get(device_id)
            if existing is not None:
                return existing

        if spec.parent is None:
            parent_device: Optional[Device] = self._root
        else:
            parent_id = spec.parent.resolve_device_id()
            parent_device = (self._devices.get(parent_id) if parent_id else None) or self.add(spec.parent)
        if parent_device is None:
            self._defer(spec)  # the parent is itself waiting on an id
            return None

        if device_id is None:
            self._defer(spec)
            return None

        device = Device(
            device_id,
            name=spec.name or device_id,
            type=spec.resolve_device_type(),
            parent=parent_device,
        )
        # Record BEFORE materializing. The device is already constructed,
        # attached and visible on the broker, so a raise below (or a re-entrant
        # add() from a producer observing its own model, since the model's events
        # dispatch synchronously) must not leave a live device the builder has no
        # record of: device_for() would return None and remove() would be a
        # silent no-op, stranding retained topics.
        self._devices[device_id] = device
        self._homie_props[device_id] = {}
        self._model_keys[device_id] = []
        self._created_groups[device_id] = []

        group = spec.resolve_model_group(device_id)
        built = _materialize(
            device,
            self._model,
            spec.specs,
            node_type=self._node_type,
            node_name=self._node_name,
            node_id=self._node_id,
            default_group=group,
        )
        _seed(self._model, built.declared, default_group=group)

        self._homie_props[device_id].update(built.homie_props)
        self._model_keys[device_id].extend(built.model_keys)
        self._created_groups[device_id].extend(built.created_groups)
        if spec in self._deferred:
            self._deferred.remove(spec)
        if spec.on_created is not None:
            spec.on_created(device)
        return device

    def resolve_deferred(self) -> list:
        """Retry every deferred spec, returning the devices that could now be built.

        Repeats while progress is being made, so a parent whose id has just
        arrived and the children waiting behind it resolve in one call rather
        than one call per generation.
        """
        built: list = []
        progress = True
        while progress:
            progress = False
            for spec in list(self._deferred):
                device = self.add(spec)
                if device is not None:
                    built.append(device)
                    progress = True
        return built

    def remove(self, spec: DeviceSpec) -> None:
        """Tear down `spec`'s device and everything under it, grandchild first.

        `Device.delete()` walks the live tree depth-first, so the ordering comes
        from the tree rather than from a list the caller has to keep correct.
        The model entries this builder added for the removed devices are deleted
        too, along with any group it created that is now empty; a group the
        caller created, or one still in use, is left alone.
        """
        # Deferred descendants go whether or not this spec was ever built: a
        # deferred child holds a frozen reference to its parent spec, so leaving
        # it in the queue lets resolve_deferred() rebuild a device that was
        # deliberately torn down.
        self._deferred = [s for s in self._deferred if not _descends_from(s, spec)]

        device_id = spec.resolve_device_id()
        device = self._devices.get(device_id) if device_id is not None else None
        if device is None:
            return  # never built (or already removed); the queue is now clean

        doomed = {id(d) for d in _descendants(device)}
        removed = [k for k, d in self._devices.items() if id(d) in doomed]
        device.delete()
        for gone in removed:
            # Bookkeeping is dropped whatever the model does, so a teardown can
            # never leave a corpse in _devices that short-circuits the next
            # add(). The model may legitimately have moved on already: a consumer
            # driving remove() from a GROUP_DELETED observer is guaranteed to
            # arrive after the group is gone, since delete_group removes it
            # before firing and dispatch is synchronous.
            try:
                for _capability, group, model_key in self._model_keys.get(gone, []):
                    if self._model.has_group(group) and self._model.get(group, model_key) is not None:
                        self._model.delete_property(group, model_key)
                for group in self._created_groups.get(gone, []):
                    if self._model.has_group(group) and not self._model.items(group):
                        self._model.delete_group(group)
            finally:
                self._model_keys.pop(gone, None)
                self._created_groups.pop(gone, None)
                self._devices.pop(gone, None)
                self._homie_props.pop(gone, None)

    def add_root_capabilities(self, specs: Iterable[PropertySpec], *, model_group: Optional[str] = None) -> dict:
        """Materialize capabilities onto the tree's ROOT device.

        `add()` only ever creates children, so a root's own capabilities (an
        enclosure's aggregate metering, its state, its control surfaces) had no
        declarative expression and had to be hand-rolled beside the builder: one
        model, two construction styles, and the root outside every guarantee the
        builder gives.

        The root already exists, so this materializes onto it rather than
        constructing anything. `model_group` defaults to the root's device id,
        matching how `add()` keys a child's group. Idempotent, so a re-fired
        lifecycle re-declares nothing.

        Returns `{(capability, prop_id): homie.Property}` for the root, and the
        map accumulates across calls, so `add_root_capabilities` twice returns
        everything the root has.
        """
        group = model_group or self._root.id()
        built = _materialize(
            self._root,
            self._model,
            specs,
            node_type=self._node_type,
            node_name=self._node_name,
            node_id=self._node_id,
            default_group=group,
        )
        _seed(self._model, built.declared, default_group=group)
        self._root_props.update(built.homie_props)
        self._root_model_keys.extend(built.model_keys)  # (capability, group, model_key)
        return dict(self._root_props)

    def root_capabilities(self) -> dict:
        """`{(capability, prop_id): homie.Property}` materialized onto the root so far."""
        return dict(self._root_props)

    def extend(self, spec: DeviceSpec, specs: Iterable[PropertySpec]) -> dict:
        """Give a device this builder already built additional capabilities.

        A device's capability set is not always known when it is first published:
        a storage system is commissioned and the enclosure gains shed and
        forecast surfaces it did not have at boot. `add()` short-circuits an
        already-built spec, so the builder modeled devices appearing and
        disappearing but not a device GROWING.

        Materializes inside one `state_transition()`, so the device announces
        once, and folds the new model keys into the same bookkeeping `remove()`
        uses. Idempotent: extending with a capability already present is a no-op
        rather than a republish, because incremental lifecycles re-fire.

        Raises `KeyError` for a spec that is not built. Use `add()` first; a
        deferred device has no tree to extend.
        """
        device_id = spec.resolve_device_id()
        device = self._devices.get(device_id) if device_id is not None else None
        if device is None:
            raise KeyError(
                f"{spec.device_class}: not built, so there is nothing to extend. "
                "add() it first (a deferred device has no tree yet)."
            )
        group = spec.resolve_model_group(device.id())
        built = _materialize(
            device,
            self._model,
            specs,
            node_type=self._node_type,
            node_name=self._node_name,
            node_id=self._node_id,
            default_group=group,
        )
        _seed(self._model, built.declared, default_group=group)
        self._homie_props[device_id].update(built.homie_props)
        self._model_keys[device_id].extend(built.model_keys)
        self._created_groups[device_id].extend(built.created_groups)
        return dict(self._homie_props[device_id])

    def remove_capabilities(self, spec: DeviceSpec, capabilities: Iterable[str]) -> None:
        """Take capabilities away from a built device: the inverse of `extend()`.

        A capability that becomes relevant at runtime can stop being relevant,
        and without this its node stayed advertised in `$description` with
        retained topics behind it. `Device.delete_node()` already clears those
        and re-announces, so the gap this closes is the bookkeeping: reaching
        around the builder to call it left `model_keys` and `created_groups`
        describing properties that no longer exist, and a later `remove()`
        working from that stale record.

        Idempotent, like `extend()`: a capability the device does not have is
        skipped rather than an error, because incremental lifecycles re-fire.
        Named for capabilities rather than nodes because that is the declarative
        vocabulary; the node id is resolved through the builder's `node_id`.

        Raises `KeyError` for a spec that is not built.
        """
        device_id = spec.resolve_device_id()
        device = self._devices.get(device_id) if device_id is not None else None
        if device is None:
            raise KeyError(f"{spec.device_class}: not built, so there is nothing to remove from. add() it first.")

        for capability in capabilities:
            if device.get_node(self._node_id(capability)) is None:
                continue  # already gone, or never had it
            device.delete_node(self._node_id(capability))

            doomed = [entry for entry in self._model_keys[device_id] if entry[0] == capability]
            for _capability, group, model_key in doomed:
                if self._model.has_group(group) and self._model.get(group, model_key) is not None:
                    self._model.delete_property(group, model_key)
            self._model_keys[device_id] = [entry for entry in self._model_keys[device_id] if entry[0] != capability]
            self._homie_props[device_id] = {
                key: prop for key, prop in self._homie_props[device_id].items() if key[0] != capability
            }
            # A group this builder created and that is now empty goes with it.
            for group in {entry[1] for entry in doomed}:
                if (
                    group in self._created_groups[device_id]
                    and self._model.has_group(group)
                    and not self._model.items(group)
                ):
                    self._model.delete_group(group)
                    self._created_groups[device_id] = [g for g in self._created_groups[device_id] if g != group]

    def device_for(self, spec: DeviceSpec) -> Optional[Device]:
        """The live `Device` for `spec`, or None if it is deferred or removed.

        Resolved by device id, so any spec naming the same device answers.
        """
        device_id = spec.resolve_device_id()
        return self._devices.get(device_id) if device_id is not None else None

    def homie_properties(self, spec: DeviceSpec) -> dict:
        """`{(capability, prop_id): homie.Property}` for `spec`, as the single-device builder returns.

        Empty for a spec that is not built. An `internal_only` property has no
        Homie twin and so is absent, exactly as in `build_from_declarations`.
        """
        device_id = spec.resolve_device_id()
        return dict(self._homie_props.get(device_id, {})) if device_id is not None else {}

    def deferred(self) -> list:
        """The specs waiting on an id, in the order they were first attempted."""
        return list(self._deferred)

    def _defer(self, spec: DeviceSpec) -> None:
        if spec not in self._deferred:
            self._deferred.append(spec)


def _descends_from(spec: DeviceSpec, ancestor: DeviceSpec) -> bool:
    """True when `spec` is `ancestor` or is declared beneath it."""
    current: Optional[DeviceSpec] = spec
    while current is not None:
        if current is ancestor:
            return True
        current = current.parent
    return False


def _descendants(device: Device) -> list:
    """`device` and every device beneath it, parents before children."""
    found = [device]
    for child in device.children():
        found.extend(_descendants(child))
    return found


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
