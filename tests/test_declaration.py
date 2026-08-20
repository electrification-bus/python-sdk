"""Tests for the declarative PropertySpec + build_from_declarations builder."""

import pytest

from ebus_sdk import (
    Device,
    GroupedPropertyDict,
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
