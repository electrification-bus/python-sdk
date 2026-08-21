"""Tests for the declarative PropertySpec + build_from_declarations builder."""

import pytest

from ebus_sdk import (
    Device,
    DeviceSpec,
    DeviceTreeBuilder,
    GroupedPropertyDict,
    ObservableProperty,
    PropertyDatatype,
    PropertySpec,
    Unit,
    build_from_declarations,
    python_type_for,
    resolve,
    specs_and_values,
)

_MAP = {
    "kWh_Tot": PropertySpec("meter", "imported-energy", PropertyDatatype.FLOAT, Unit.WATT_HOUR, scale=1000.0),
    "RMS_Watts_Tot": PropertySpec("meter", "active-power", PropertyDatatype.FLOAT, Unit.WATT),
}


def test_resolve_explicit_mapping_applies_scale():
    fields = {"kWh_Tot": 22.5, "RMS_Watts_Tot": 719}
    resolved = {r.spec.prop_id: r for r in resolve(fields.keys(), fields, _MAP)}
    assert resolved["imported-energy"].value == 22500.0  # kWh -> Wh
    assert resolved["active-power"].value == 719


def test_resolve_fallback_fills_gaps_and_holds_unmapped():
    fields = {"kWh_Tot": 1.0, "vendor_temp": 21.5, "mystery": 9}
    fb_spec = PropertySpec("meter", "vendor-temp", PropertyDatatype.FLOAT, Unit.DEGREE_CELSIUS)
    resolved = resolve(fields.keys(), fields, _MAP, fallback=lambda f: fb_spec if f == "vendor_temp" else None)
    prop_ids = {r.spec.prop_id for r in resolved}
    assert prop_ids == {"imported-energy", "vendor-temp"}  # explicit + fallback; "mystery" held
    assert len(resolved) == 2


def test_resolve_declared_without_value_yields_none():
    resolved = resolve(["kWh_Tot"], {}, _MAP)
    assert len(resolved) == 1 and resolved[0].value is None


def test_specs_and_values_split_omits_none():
    resolved = resolve(["kWh_Tot", "RMS_Watts_Tot"], {"kWh_Tot": 2.0}, _MAP)
    specs, values = specs_and_values(resolved)
    assert len(specs) == 2  # both declared
    assert values == {("meter", "imported-energy"): 2000.0}  # only the observed one, scaled


def test_python_type_for():
    assert python_type_for(PropertyDatatype.FLOAT) is float
    assert python_type_for(PropertyDatatype.INTEGER) is int
    assert python_type_for(PropertyDatatype.STRING) is str
    assert python_type_for(PropertyDatatype.BOOLEAN) is bool


def test_build_from_declarations_creates_nodes_props_and_binds(mock_paho):
    device = Device("dev-1", type="energy.ebus.device.submeter", mqtt_cfg={"host": "localhost", "port": 1883})
    device.start_mqtt_client()
    model = GroupedPropertyDict()

    specs = [
        PropertySpec("info", "serial-number", PropertyDatatype.STRING),
        PropertySpec("meter", "active-power", PropertyDatatype.FLOAT, Unit.WATT),
        PropertySpec("meter", "imported-energy", PropertyDatatype.FLOAT, Unit.WATT_HOUR, scale=1000.0),
    ]
    homie_props = build_from_declarations(
        device,
        model,
        specs,
        values={("meter", "active-power"): 1850.0, ("info", "serial-number"): "abc"},
    )

    # One Homie node per capability, default eBus capability node type.
    assert device.get_node("info") is not None
    assert device.get_node("meter") is not None
    assert device.get_node("meter").type() == "energy.ebus.capability.meter"

    # The observable model holds the properties, grouped by capability, seeded.
    assert model.value("meter", "active-power") == 1850.0
    assert model.value("info", "serial-number") == "abc"

    # Returned Homie twins are bound: a later model change mirrors onto Homie.
    assert set(homie_props) == {
        ("info", "serial-number"),
        ("meter", "active-power"),
        ("meter", "imported-energy"),
    }
    assert homie_props[("meter", "active-power")].value() == 1850.0
    model.set_value("meter", "active-power", 2000.0)
    assert homie_props[("meter", "active-power")].value() == 2000.0


def test_build_from_declarations_wires_settable_entity_setter(mock_paho):
    device = Device("dev-set", mqtt_cfg={"host": "localhost", "port": 1883})
    device.start_mqtt_client()
    model = GroupedPropertyDict()
    received = []

    specs = [
        PropertySpec("dr", "event", PropertyDatatype.JSON, settable=True, entity_setter=lambda v: received.append(v)),
    ]
    homie_props = build_from_declarations(device, model, specs)
    hp = homie_props[("dr", "event")]

    # The property is settable and its /set topic was subscribed at build.
    assert hp.settable() is True
    set_subs = [c for c in mock_paho.subscribe.call_args_list if c.args and str(c.args[0]).endswith("/dr/event/set")]
    assert set_subs, "settable property should subscribe to its /set topic at build"

    # Homie set_callback routes an inbound /set: payload -> model.set_entity -> entity_setter.
    cb = hp.get_set_callback()
    assert cb is not None
    cb('{"cmd": "shed"}')
    assert received == ['{"cmd": "shed"}']

    # The observable side is registered too: model.set_entity invokes the entity_setter.
    model.set_entity("dr", "event", "OTHER")
    assert received[-1] == "OTHER"


def test_build_from_declarations_settable_without_entity_setter_not_auto_wired(mock_paho):
    device = Device("dev-set2", mqtt_cfg={"host": "localhost", "port": 1883})
    device.start_mqtt_client()
    model = GroupedPropertyDict()
    homie_props = build_from_declarations(
        device, model, [PropertySpec("control", "enabled", PropertyDatatype.BOOLEAN, settable=True)]
    )
    hp = homie_props[("control", "enabled")]
    # Still settable (topic exists), but no auto-wired handler without an entity_setter.
    assert hp.settable() is True
    assert hp.get_set_callback() is None


def test_build_from_declarations_custom_node_type(mock_paho):
    device = Device("dev-2", mqtt_cfg={"host": "localhost", "port": 1883})
    device.start_mqtt_client()
    model = GroupedPropertyDict()
    build_from_declarations(
        device,
        model,
        [PropertySpec("sensors", "temp", PropertyDatatype.FLOAT, Unit.DEGREE_CELSIUS)],
        node_type=lambda cap: "sensor",
    )
    assert device.get_node("sensors").type() == "sensor"


# --- Property-level parity fields (GH #58) -----------------------------------


def _device(mock_paho, device_id):
    device = Device(device_id, mqtt_cfg={"host": "localhost", "port": 1883})
    device.start_mqtt_client()
    return device


def _payloads_for(mock_paho, topic_suffix):
    """Every payload published to a topic ending in `topic_suffix`."""
    return [c.args[1] for c in mock_paho.publish.call_args_list if c.args and str(c.args[0]).endswith(topic_suffix)]


def test_round_to_reaches_the_homie_property_and_the_publish_gate(mock_paho):
    device = _device(mock_paho, "dev-round")
    model = GroupedPropertyDict()
    homie_props = build_from_declarations(
        device,
        model,
        [PropertySpec("meter", "active-power", PropertyDatatype.FLOAT, Unit.WATT, round_to=1)],
    )
    hp = homie_props[("meter", "active-power")]
    assert hp.round() == 1

    model.set_value("meter", "active-power", 0.14494210481643677)
    assert hp.coerced_value() == "0.1"
    before = len(_payloads_for(mock_paho, "/meter/active-power"))
    # A genuinely different reading that rounds to the same payload is not a
    # second publish: the gate compares the final payload, after rounding.
    model.set_value("meter", "active-power", 0.14501120000000001)
    assert len(_payloads_for(mock_paho, "/meter/active-power")) == before


def test_internal_only_populates_the_model_and_publishes_nothing(mock_paho):
    device = _device(mock_paho, "dev-internal")
    model = GroupedPropertyDict()
    homie_props = build_from_declarations(
        device,
        model,
        [
            PropertySpec("meter", "active-power", PropertyDatatype.FLOAT, Unit.WATT),
            PropertySpec("meter", "raw-counter", PropertyDatatype.INTEGER, internal_only=True),
        ],
    )
    # Tracked in the model, absent from the wire and from the returned twins.
    model.set_value("meter", "raw-counter", 42)
    assert model.value("meter", "raw-counter") == 42
    assert ("meter", "raw-counter") not in homie_props
    assert device.get_node("meter").get_property("raw-counter") is None
    assert "raw-counter" not in device.get_node("meter").description()["properties"]
    assert _payloads_for(mock_paho, "/meter/raw-counter") == []


def test_capability_with_only_internal_specs_gets_no_node(mock_paho):
    device = _device(mock_paho, "dev-internal-cap")
    model = GroupedPropertyDict()
    build_from_declarations(
        device,
        model,
        [PropertySpec("scratch", "accumulator", PropertyDatatype.FLOAT, internal_only=True)],
    )
    # No node, because announcing an empty one would describe nothing.
    assert device.get_node("scratch") is None
    assert "scratch" not in device.description()["nodes"]
    assert model.value("scratch", "accumulator") is None  # the group still exists


def test_internal_only_entity_setter_is_still_reachable_through_the_model(mock_paho):
    device = _device(mock_paho, "dev-internal-set")
    model = GroupedPropertyDict()
    received = []
    build_from_declarations(
        device,
        model,
        [
            PropertySpec(
                "scratch",
                "target",
                PropertyDatatype.FLOAT,
                internal_only=True,
                entity_setter=received.append,
            )
        ],
    )
    model.set_entity("scratch", "target", 3.0)
    assert received == [3.0]


def test_retained_false_declares_an_event_property_exempt_from_the_gate(mock_paho):
    device = _device(mock_paho, "dev-event")
    model = GroupedPropertyDict()
    homie_props = build_from_declarations(
        device,
        model,
        [PropertySpec("dr", "event", PropertyDatatype.STRING, retained=False)],
    )
    hp = homie_props[("dr", "event")]
    assert hp.retained() is False
    assert hp.description()["retained"] is False

    model.set_value("dr", "event", "shed")
    before = len(_payloads_for(mock_paho, "/dr/event"))
    # The broker stores nothing for an event property, so an identical
    # consecutive payload is a second real event rather than a redundant write.
    model.set_value("dr", "event", "shed")
    assert len(_payloads_for(mock_paho, "/dr/event")) == before + 1


def test_retained_true_is_the_default_and_stays_gated(mock_paho):
    device = _device(mock_paho, "dev-retained")
    model = GroupedPropertyDict()
    homie_props = build_from_declarations(
        device, model, [PropertySpec("meter", "active-power", PropertyDatatype.FLOAT)]
    )
    hp = homie_props[("meter", "active-power")]
    assert hp.retained() is True
    assert "retained" not in hp.description()  # only emitted when False

    model.set_value("meter", "active-power", 12.0)
    before = len(_payloads_for(mock_paho, "/meter/active-power"))
    model.set_value("meter", "active-power", 12.0)
    assert len(_payloads_for(mock_paho, "/meter/active-power")) == before


def test_conditionally_settable_builds_unsettable_and_the_caller_enables_it(mock_paho):
    device = _device(mock_paho, "dev-cond")
    model = GroupedPropertyDict()
    homie_props = build_from_declarations(
        device,
        model,
        [PropertySpec("control", "limit", PropertyDatatype.FLOAT, conditionally_settable=True)],
    )
    hp = homie_props[("control", "limit")]
    # Not settable at build: $description stays truthful and no /set topic is
    # subscribed on a property that would reject the command.
    assert hp.settable() is False
    assert "settable" not in hp.description()
    assert not [
        c for c in mock_paho.subscribe.call_args_list if c.args and str(c.args[0]).endswith("/control/limit/set")
    ]

    # The caller gates it on per-instance state, inside a state transition.
    with device.state_transition():
        hp.set_settable(True)
    assert hp.settable() is True
    assert [c for c in mock_paho.subscribe.call_args_list if c.args and str(c.args[0]).endswith("/control/limit/set")]


def test_contradictory_settability_is_refused_at_declaration_time():
    with pytest.raises(ValueError, match="mutually exclusive"):
        PropertySpec("control", "limit", PropertyDatatype.FLOAT, settable=True, conditionally_settable=True)
    with pytest.raises(ValueError, match="never published"):
        PropertySpec("control", "limit", PropertyDatatype.FLOAT, settable=True, internal_only=True)
    with pytest.raises(ValueError, match="never published"):
        PropertySpec("control", "limit", PropertyDatatype.FLOAT, conditionally_settable=True, internal_only=True)


def test_identity_is_fused_by_default():
    spec = PropertySpec("meter", "active-power", PropertyDatatype.FLOAT)
    assert spec.model_key == "active-power"
    assert spec.group_key == "meter"


def test_source_id_splits_the_model_key_from_the_wire_id(mock_paho):
    device = _device(mock_paho, "dev-srcid")
    model = GroupedPropertyDict()
    homie_props = build_from_declarations(
        device,
        model,
        [PropertySpec("meter", "active-power", PropertyDatatype.FLOAT, source_id="RMS_Watts_Tot")],
    )
    # Populated under the source name, published under the eBus name.
    model.set_value("meter", "RMS_Watts_Tot", 719.0)
    assert homie_props[("meter", "active-power")].value() == 719.0
    assert device.get_node("meter").get_property("active-power") is not None
    assert device.get_node("meter").get_property("RMS_Watts_Tot") is None


def test_model_group_splits_the_model_group_from_the_capability_node(mock_paho):
    device = _device(mock_paho, "dev-grp")
    model = GroupedPropertyDict()
    homie_props = build_from_declarations(
        device,
        model,
        [PropertySpec("info", "serial-number", PropertyDatatype.STRING, model_group="bess-info")],
    )
    model.set_value("bess-info", "serial-number", "TG-1")
    assert homie_props[("info", "serial-number")].value() == "TG-1"
    assert "bess-info" in model.groups()
    assert "info" not in model.groups()
    assert device.get_node("info") is not None  # the wire is unchanged


def test_model_group_lets_two_capabilities_share_one_model_without_colliding(mock_paho):
    device = _device(mock_paho, "dev-grp2")
    model = GroupedPropertyDict()
    homie_props = build_from_declarations(
        device,
        model,
        [
            PropertySpec("info", "serial-number", PropertyDatatype.STRING, model_group="pv-info"),
            PropertySpec("meter", "serial-number", PropertyDatatype.STRING, model_group="mid-info"),
        ],
    )
    model.set_value("pv-info", "serial-number", "PV-1")
    model.set_value("mid-info", "serial-number", "MID-1")
    assert homie_props[("info", "serial-number")].value() == "PV-1"
    assert homie_props[("meter", "serial-number")].value() == "MID-1"


def test_initial_value_seeds_through_the_model(mock_paho):
    device = _device(mock_paho, "dev-seed")
    model = GroupedPropertyDict()
    homie_props = build_from_declarations(
        device,
        model,
        [
            PropertySpec("info", "vendor-name", PropertyDatatype.STRING, initial_value="Acme"),
            PropertySpec("scratch", "counter", PropertyDatatype.INTEGER, internal_only=True, initial_value=7),
        ],
    )
    assert model.value("info", "vendor-name") == "Acme"
    assert homie_props[("info", "vendor-name")].value() == "Acme"
    # An internal property seeds too, even though it has no Homie twin.
    assert model.value("scratch", "counter") == 7


def test_values_argument_overrides_initial_value(mock_paho):
    device = _device(mock_paho, "dev-seed2")
    model = GroupedPropertyDict()
    build_from_declarations(
        device,
        model,
        [PropertySpec("info", "vendor-name", PropertyDatatype.STRING, initial_value="Acme")],
        values={("info", "vendor-name"): "Runtime"},
    )
    assert model.value("info", "vendor-name") == "Runtime"


def test_build_does_not_apply_scale_to_seeded_values(mock_paho):
    """`resolve` scales; the builder does not, or the pipeline would double-apply it."""
    device = _device(mock_paho, "dev-scale")
    model = GroupedPropertyDict()
    specs, values = specs_and_values(resolve(["kWh_Tot"], {"kWh_Tot": 22.5}, _MAP))
    build_from_declarations(device, model, specs, values=values)
    # resolve() already turned 22.5 kWh into 22500.0 Wh. Scaling again here
    # would publish 22_500_000.0.
    assert model.value("meter", "imported-energy") == 22500.0

    # Same via initial_value, which is likewise taken at face value.
    model2 = GroupedPropertyDict()
    build_from_declarations(
        _device(mock_paho, "dev-scale2"),
        model2,
        [
            PropertySpec(
                "meter", "imported-energy", PropertyDatatype.FLOAT, Unit.WATT_HOUR, scale=1000.0, initial_value=22500.0
            )
        ],
    )
    assert model2.value("meter", "imported-energy") == 22500.0


# --- node_id: renaming the node a capability materializes onto ---------------


def test_node_id_defaults_to_the_capability(mock_paho):
    device = _device(mock_paho, "dev-nodeid-default")
    model = GroupedPropertyDict()
    build_from_declarations(device, model, [PropertySpec("meter", "active-power", PropertyDatatype.FLOAT)])
    assert device.get_node("meter") is not None


def test_node_id_renames_the_node_without_moving_anything_else(mock_paho):
    device = _device(mock_paho, "dev-nodeid")
    model = GroupedPropertyDict()
    homie_props = build_from_declarations(
        device,
        model,
        [PropertySpec("meter", "active-power", PropertyDatatype.FLOAT, Unit.WATT)],
        node_id=lambda capability: f"lugs-up-{capability}",
    )
    # The wire node is renamed.
    assert device.get_node("lugs-up-meter") is not None
    assert device.get_node("meter") is None
    # The declaration's vocabulary is unchanged: model group and returned key
    # both still say "meter".
    assert "meter" in model.groups()
    assert set(homie_props) == {("meter", "active-power")}
    model.set_value("meter", "active-power", 240.0)
    assert homie_props[("meter", "active-power")].value() == 240.0


def test_node_id_and_node_name_are_independent(mock_paho):
    device = _device(mock_paho, "dev-nodeid-name")
    model = GroupedPropertyDict()
    build_from_declarations(
        device,
        model,
        [PropertySpec("meter", "active-power", PropertyDatatype.FLOAT)],
        node_id=lambda capability: f"a-{capability}",
        node_name=lambda capability: "Upstream lugs",
        node_type=lambda capability: "energy.ebus.capability.meter",
    )
    node = device.get_node("a-meter")
    assert node.name() == "Upstream lugs"
    assert node.type() == "energy.ebus.capability.meter"


def test_two_instances_of_one_capability_coexist_on_one_device(mock_paho):
    """The node-on-parent shape: several instances placed on a device that already exists."""
    device = _device(mock_paho, "enclosure-1")
    model = GroupedPropertyDict()

    built = {}
    for instance in ("lugs-up", "lugs-dn"):
        built[instance] = build_from_declarations(
            device,
            model,
            [
                PropertySpec(
                    "meter",
                    "active-power",
                    PropertyDatatype.FLOAT,
                    Unit.WATT,
                    model_group=f"{instance}-meter",
                )
            ],
            node_id=lambda capability, i=instance: f"{i}-{capability}",
        )

    # Two nodes on one device, neither shadowing the other.
    assert device.get_node("lugs-up-meter") is not None
    assert device.get_node("lugs-dn-meter") is not None
    assert set(device.description()["nodes"]) == {"lugs-up-meter", "lugs-dn-meter"}

    # node_id separates them on the wire; model_group separates them in the
    # model. Without the second they would collide even with distinct nodes.
    model.set_value("lugs-up-meter", "active-power", 1000.0)
    model.set_value("lugs-dn-meter", "active-power", -250.0)
    assert built["lugs-up"][("meter", "active-power")].value() == 1000.0
    assert built["lugs-dn"][("meter", "active-power")].value() == -250.0


def test_device_tree_builder_passes_node_id_through(mock_paho):
    device = _device(mock_paho, "root-nodeid")
    model = GroupedPropertyDict()
    builder = DeviceTreeBuilder(device, model, node_id=lambda capability: f"x-{capability}")
    child = builder.add(
        DeviceSpec("bess", [PropertySpec("info", "serial-number", PropertyDatatype.STRING)], device_id="bess-1")
    )
    assert child.get_node("x-info") is not None
    assert child.get_node("info") is None


# --- Reusing a model the builder does not own (GH #66) -----------------------


def _live_model(group="dev-1", prop_id="serial-number"):
    """A model a producer already populated, before any Homie tree existed."""
    model = GroupedPropertyDict()
    model.create_group(group)
    model.add_property(group, ObservableProperty(id=prop_id, type=str))
    model.set_value(group, prop_id, "SN-LIVE")
    return model


def test_an_existing_model_property_is_reused_not_replaced(mock_paho):
    device = _device(mock_paho, "dev-reuse")
    model = _live_model(group="info")
    before = model.get("info", "serial-number")

    build_from_declarations(device, model, [PropertySpec("info", "serial-number", PropertyDatatype.STRING)])

    # The same object, so nothing attached to it was discarded.
    assert model.get("info", "serial-number") is before
    assert model.value("info", "serial-number") == "SN-LIVE"


def test_reuse_preserves_callbacks_and_the_entity_setter(mock_paho):
    device = _device(mock_paho, "dev-reuse-cb")
    model = _live_model(group="info")
    changed, commanded = [], []
    model.add_property_on_change_callback("info", "serial-number", lambda p: changed.append(p.value()))
    model.set_entity_setter("info", "serial-number", commanded.append)

    build_from_declarations(device, model, [PropertySpec("info", "serial-number", PropertyDatatype.STRING)])

    model.set_value("info", "serial-number", "SN-NEW")
    assert changed == ["SN-NEW"], "the producer's on-change callback died with the replaced property"
    model.set_entity("info", "serial-number", "CMD")
    assert commanded == ["CMD"], "the producer's actuator died while $description still advertises settable"


def test_the_tree_builder_reuses_a_producers_property_too(mock_paho):
    root = _device(mock_paho, "enclosure-1")
    model = _live_model(group="dev-1")
    builder = DeviceTreeBuilder(root, model)
    builder.add(
        DeviceSpec("circuit", [PropertySpec("info", "serial-number", PropertyDatatype.STRING)], device_id="dev-1")
    )
    assert model.value("dev-1", "serial-number") == "SN-LIVE"


def test_remove_deletes_what_the_builder_created_and_nothing_else(mock_paho):
    root = _device(mock_paho, "enclosure-2")
    model = _live_model(group="dev-1")
    builder = DeviceTreeBuilder(root, model)
    spec = DeviceSpec(
        "circuit",
        [
            PropertySpec("info", "serial-number", PropertyDatatype.STRING),  # the producer's
            PropertySpec("meter", "active-power", PropertyDatatype.FLOAT),  # the builder's
        ],
        device_id="dev-1",
    )
    builder.add(spec)
    assert model.get("dev-1", "active-power") is not None

    builder.remove(spec)
    # What it created is gone; what it found is left where it was.
    assert model.get("dev-1", "active-power") is None
    assert model.get("dev-1", "serial-number") is not None
    assert model.value("dev-1", "serial-number") == "SN-LIVE"


def test_a_type_disagreement_with_an_existing_property_reuses_it(mock_paho):
    """The observable `type` is metadata: nothing reads it, and `set_value` neither
    coerces nor validates against it, so a difference has no runtime consequence.
    Raising here misfired on the normal case of a producer using a richer python
    type than the datatype-derived default, and it raised mid-materialization."""
    device = _device(mock_paho, "dev-mismatch")
    model = _live_model(group="info")  # holds a str property
    before = model.get("info", "serial-number")

    props = build_from_declarations(device, model, [PropertySpec("info", "serial-number", PropertyDatatype.FLOAT)])

    assert model.get("info", "serial-number") is before  # reused, not replaced
    assert model.value("info", "serial-number") == "SN-LIVE"
    # And the wire still works: coercion belongs to the Homie property.
    assert props[("info", "serial-number")].value() == "SN-LIVE"


def test_reuse_still_binds_and_publishes(mock_paho):
    """Reusing the twin must not skip the wiring: a later model write still reaches Homie."""
    device = _device(mock_paho, "dev-reuse-bind")
    model = _live_model(group="info")
    homie_props = build_from_declarations(
        device, model, [PropertySpec("info", "serial-number", PropertyDatatype.STRING)]
    )
    model.set_value("info", "serial-number", "SN-NEW")
    assert homie_props[("info", "serial-number")].value() == "SN-NEW"


# --- conditionally_settable reaches the inbound path (GH #72) ----------------


def test_conditionally_settable_registers_its_entity_setter(mock_paho):
    device = _device(mock_paho, "dev-cond-setter")
    model = GroupedPropertyDict()
    received = []
    build_from_declarations(
        device,
        model,
        [
            PropertySpec(
                "control",
                "limit",
                PropertyDatatype.FLOAT,
                conditionally_settable=True,
                entity_setter=received.append,
            )
        ],
    )
    model.set_entity("control", "limit", 42.0)
    assert received == [42.0], "the translator was never registered on the model"


def test_enabling_a_conditionally_settable_property_gives_a_working_set_topic(mock_paho):
    """The whole point of the field: the /set topic opened later must have a handler."""
    device = _device(mock_paho, "dev-cond-flip")
    model = GroupedPropertyDict()
    received = []
    props = build_from_declarations(
        device,
        model,
        [
            PropertySpec(
                "control",
                "limit",
                PropertyDatatype.FLOAT,
                conditionally_settable=True,
                entity_setter=received.append,
            )
        ],
    )
    hp = props[("control", "limit")]
    assert hp.settable() is False  # still built not-settable

    with device.state_transition():
        hp.set_settable(True)

    # Subscribed AND wired: a topic that accepts commands and discards them is
    # the failure this field exists to avoid.
    assert [c for c in mock_paho.subscribe.call_args_list if c.args and str(c.args[0]).endswith("/control/limit/set")]
    assert hp.get_set_callback() is not None
    hp.get_set_callback()("42.0")
    assert received == ["42.0"]


# --- a live model value reaches the wire (GH #77) ----------------------------


def test_a_value_the_model_already_held_is_published(mock_paho):
    device = _device(mock_paho, "dev-live")
    model = _live_model(group="info")  # holds SN-LIVE before any tree exists
    props = build_from_declarations(device, model, [PropertySpec("info", "serial-number", PropertyDatatype.STRING)])
    # The binding is on-change and the twin starts empty, so without an explicit
    # push the wire would show the declared default for the process lifetime.
    assert props[("info", "serial-number")].value() == "SN-LIVE"
    assert "SN-LIVE" in [c.args[1] for c in mock_paho.publish.call_args_list if len(c.args) > 1]


def test_initial_value_seeds_but_does_not_clobber_a_live_value(mock_paho):
    device = _device(mock_paho, "dev-live-2")
    model = _live_model(group="info")
    build_from_declarations(
        device,
        model,
        [PropertySpec("info", "serial-number", PropertyDatatype.STRING, initial_value="DECLARED")],
    )
    assert model.value("info", "serial-number") == "SN-LIVE"


def test_an_explicit_values_entry_still_wins_over_a_live_value(mock_paho):
    """`values` is a statement about this run, so it is more specific than either."""
    device = _device(mock_paho, "dev-live-3")
    model = _live_model(group="info")
    build_from_declarations(
        device,
        model,
        [PropertySpec("info", "serial-number", PropertyDatatype.STRING)],
        values={("info", "serial-number"): "RUNTIME"},
    )
    assert model.value("info", "serial-number") == "RUNTIME"


def test_the_tree_builder_publishes_live_values_too(mock_paho):
    root = _device(mock_paho, "enclosure-live")
    model = _live_model(group="dev-1")
    builder = DeviceTreeBuilder(root, model)
    spec = DeviceSpec("circuit", [PropertySpec("info", "serial-number", PropertyDatatype.STRING)], device_id="dev-1")
    builder.add(spec)
    assert builder.homie_properties(spec)[("info", "serial-number")].value() == "SN-LIVE"
