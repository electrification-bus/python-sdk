"""Tests for DeviceSpec + DeviceTreeBuilder (GH #57).

One test per acceptance criterion, plus the tree shape the criteria assume.
"""

import pytest

from ebus_sdk import (
    DeviceSpec,
    DeviceTreeBuilder,
    Device,
    GroupedPropertyDict,
    PropertyDatatype,
    PropertySpec,
    Unit,
)

INFO = [PropertySpec("info", "serial-number", PropertyDatatype.STRING)]
METER = [PropertySpec("meter", "active-power", PropertyDatatype.FLOAT, Unit.WATT)]


@pytest.fixture
def root(mock_paho):
    device = Device(
        "enclosure-1",
        type="energy.ebus.device.distribution-enclosure",
        mqtt_cfg={"host": "localhost", "port": 1883},
    )
    device.start_mqtt_client()
    return device


def _topics(mock_paho):
    """Published topics, in call order."""
    return [str(c.args[0]) for c in mock_paho.publish.call_args_list if c.args]


# --- Criterion 1: the model is externally owned and keyed per device ---------


def test_two_children_sharing_a_capability_do_not_collide_in_the_model(root, mock_paho):
    model = GroupedPropertyDict()
    builder = DeviceTreeBuilder(root, model)

    bess = builder.add(DeviceSpec("bess", INFO, device_id="bess-1"))
    pv = builder.add(DeviceSpec("pv", INFO, device_id="pv-1"))

    # Same capability, same property id, two devices: distinct on the wire and
    # now distinct in the model, because the group is the device.
    model.set_value("bess-1", "serial-number", "B-1")
    model.set_value("pv-1", "serial-number", "P-1")
    assert model.value("bess-1", "serial-number") == "B-1"
    assert model.value("pv-1", "serial-number") == "P-1"
    assert bess.get_node("info").get_property("serial-number").value() == "B-1"
    assert pv.get_node("info").get_property("serial-number").value() == "P-1"


def test_the_builder_never_creates_the_model(root):
    model = GroupedPropertyDict()
    model.create_group("preexisting")
    builder = DeviceTreeBuilder(root, model)
    builder.add(DeviceSpec("bess", INFO, device_id="bess-1"))
    # It adds to the caller's model and leaves what was already there.
    assert "preexisting" in model.groups()
    assert "bess-1" in model.groups()


def test_a_property_spec_model_group_still_wins(root):
    model = GroupedPropertyDict()
    builder = DeviceTreeBuilder(root, model)
    spec = PropertySpec("info", "serial-number", PropertyDatatype.STRING, model_group="span-info")
    builder.add(DeviceSpec("bess", [spec], device_id="bess-1"))
    # The caller keyed their own model; the device default does not override it.
    assert "span-info" in model.groups()
    assert "bess-1" not in model.groups()


def test_device_model_group_can_be_named_or_computed(root):
    model = GroupedPropertyDict()
    builder = DeviceTreeBuilder(root, model)
    builder.add(DeviceSpec("bess", INFO, device_id="bess-1", model_group="battery"))
    builder.add(DeviceSpec("pv", INFO, device_id="pv-1", model_group=lambda: "solar"))
    assert "battery" in model.groups()
    assert "solar" in model.groups()


# --- The tree shape the criteria assume -------------------------------------


def test_builder_materializes_a_parent_child_grandchild_tree(root):
    model = GroupedPropertyDict()
    builder = DeviceTreeBuilder(root, model)

    bess_spec = DeviceSpec("bess", INFO, device_id="bess-1")
    mid_spec = DeviceSpec("mid", METER, device_id="bess-1-mid", parent=bess_spec)
    bess = builder.add(bess_spec)
    mid = builder.add(mid_spec)

    assert bess.parent() is root and mid.parent() is bess
    assert mid.root() is root
    # Homie derives the description's parent/root/children from the live tree.
    assert mid.description()["root"] == "enclosure-1"
    assert mid.description()["parent"] == "bess-1"
    assert bess.description()["children"] == ["bess-1-mid"]
    assert "bess-1" in root.description()["children"]


def test_device_type_defaults_from_device_class_and_can_be_overridden(root):
    model = GroupedPropertyDict()
    builder = DeviceTreeBuilder(root, model)
    default = builder.add(DeviceSpec("circuit", INFO, device_id="c-1"))
    override = builder.add(DeviceSpec("circuit", INFO, device_id="c-2", device_type="vendor.thing"))
    assert default.type() == "energy.ebus.device.circuit"
    assert override.type() == "vendor.thing"


def test_adding_a_child_whose_parent_spec_is_not_built_builds_the_parent_first(root):
    model = GroupedPropertyDict()
    builder = DeviceTreeBuilder(root, model)
    bess_spec = DeviceSpec("bess", INFO, device_id="bess-1")
    mid_spec = DeviceSpec("mid", METER, device_id="bess-1-mid", parent=bess_spec)

    mid = builder.add(mid_spec)  # parent never added explicitly
    assert mid is not None
    assert builder.device_for(bess_spec) is not None
    assert mid.parent() is builder.device_for(bess_spec)


# --- Criterion 2: late-bound device ids are first class ----------------------


def test_a_spec_with_an_unresolved_id_defers_instead_of_publishing(root, mock_paho):
    model = GroupedPropertyDict()
    builder = DeviceTreeBuilder(root, model)
    serial = {}

    spec = DeviceSpec("bess", INFO, device_id=lambda: serial.get("value"))
    assert builder.add(spec) is None
    assert builder.deferred() == [spec]
    assert builder.device_for(spec) is None
    # Nothing was published under a placeholder id: a wrong-but-stable id leaves
    # retained topics that outlive restarts.
    assert not [t for t in _topics(mock_paho) if "/info/" in t]

    serial["value"] = "bess-serial-9"
    built = builder.resolve_deferred()
    assert [d.id() for d in built] == ["bess-serial-9"]
    assert builder.deferred() == []
    assert builder.device_for(spec).id() == "bess-serial-9"


def test_a_deferred_parent_unblocks_its_deferred_children_in_one_call(root):
    model = GroupedPropertyDict()
    builder = DeviceTreeBuilder(root, model)
    serial = {}

    bess_spec = DeviceSpec("bess", INFO, device_id=lambda: serial.get("value"))
    mid_spec = DeviceSpec(
        "mid", METER, device_id=lambda: f"{serial['value']}-mid" if serial else None, parent=bess_spec
    )
    assert builder.add(bess_spec) is None
    assert builder.add(mid_spec) is None
    assert set(builder.deferred()) == {bess_spec, mid_spec}

    serial["value"] = "tg-1"
    built = builder.resolve_deferred()
    # One call resolves the generation and everything waiting behind it.
    assert sorted(d.id() for d in built) == ["tg-1", "tg-1-mid"]
    assert builder.device_for(mid_spec).parent() is builder.device_for(bess_spec)


def test_resolving_deferred_when_nothing_can_be_built_is_a_noop(root):
    model = GroupedPropertyDict()
    builder = DeviceTreeBuilder(root, model)
    spec = DeviceSpec("bess", INFO, device_id=lambda: None)
    builder.add(spec)
    assert builder.resolve_deferred() == []
    assert builder.deferred() == [spec]


# --- Criterion 3: deletion is depth-first, grandchild before parent ----------


def test_remove_tears_down_grandchild_before_parent(root, mock_paho):
    model = GroupedPropertyDict()
    builder = DeviceTreeBuilder(root, model)
    bess_spec = DeviceSpec("bess", INFO, device_id="bess-1")
    mid_spec = DeviceSpec("mid", METER, device_id="bess-1-mid", parent=bess_spec)
    builder.add(bess_spec)
    builder.add(mid_spec)

    before = len(mock_paho.publish.call_args_list)
    builder.remove(bess_spec)
    after = _topics(mock_paho)[before:]

    # The transient matters: a settled-state comparison cannot catch an
    # observer briefly seeing a child whose parent is already gone.
    mid_state = after.index("ebus/5/bess-1-mid/$state")
    bess_state = after.index("ebus/5/bess-1/$state")
    assert mid_state < bess_state


def test_remove_drops_the_devices_and_their_model_entries(root):
    model = GroupedPropertyDict()
    builder = DeviceTreeBuilder(root, model)
    bess_spec = DeviceSpec("bess", INFO, device_id="bess-1")
    mid_spec = DeviceSpec("mid", METER, device_id="bess-1-mid", parent=bess_spec)
    builder.add(bess_spec)
    builder.add(mid_spec)
    assert "bess-1" in model.groups() and "bess-1-mid" in model.groups()

    builder.remove(bess_spec)
    assert builder.device_for(bess_spec) is None
    assert builder.device_for(mid_spec) is None  # the grandchild went with it
    assert "bess-1" not in model.groups()
    assert "bess-1-mid" not in model.groups()
    assert root.children_ids() == []


def test_remove_leaves_a_group_the_builder_did_not_create(root):
    model = GroupedPropertyDict()
    model.create_group("shared")
    builder = DeviceTreeBuilder(root, model)
    spec = PropertySpec("info", "serial-number", PropertyDatatype.STRING, model_group="shared")
    device_spec = DeviceSpec("bess", [spec], device_id="bess-1")
    builder.add(device_spec)

    builder.remove(device_spec)
    # The property this builder added is gone; the caller's group is not.
    assert "shared" in model.groups()
    assert model.get("shared", "serial-number") is None


def test_removing_a_deferred_spec_stops_waiting_for_it(root):
    model = GroupedPropertyDict()
    builder = DeviceTreeBuilder(root, model)
    spec = DeviceSpec("bess", INFO, device_id=lambda: None)
    builder.add(spec)
    builder.remove(spec)
    assert builder.deferred() == []


# --- Criterion 4: add() is idempotent ---------------------------------------


def test_add_is_idempotent(root, mock_paho):
    model = GroupedPropertyDict()
    builder = DeviceTreeBuilder(root, model)
    spec = DeviceSpec("bess", INFO, device_id="bess-1")

    first = builder.add(spec)
    quiet = len(mock_paho.publish.call_args_list)
    second = builder.add(spec)

    assert second is first
    assert len(root.children()) == 1
    # A re-fired lifecycle must not republish or duplicate the device.
    assert len(mock_paho.publish.call_args_list) == quiet


# --- Criterion 5: per-child side effects ------------------------------------


def test_on_created_runs_once_with_the_live_device(root):
    model = GroupedPropertyDict()
    builder = DeviceTreeBuilder(root, model)
    seen = []
    spec = DeviceSpec("bess", INFO, device_id="bess-1", on_created=seen.append)

    device = builder.add(spec)
    builder.add(spec)  # idempotent: no second side effect

    assert seen == [device]
    # The device is fully built when the hook runs, not a bare shell.
    assert seen[0].get_node("info") is not None


# --- Accessors ---------------------------------------------------------------


def test_homie_properties_returns_the_twins_like_the_single_device_builder(root):
    model = GroupedPropertyDict()
    builder = DeviceTreeBuilder(root, model)
    specs = [
        PropertySpec("info", "serial-number", PropertyDatatype.STRING),
        PropertySpec("info", "raw", PropertyDatatype.INTEGER, internal_only=True),
    ]
    spec = DeviceSpec("bess", specs, device_id="bess-1")
    builder.add(spec)

    props = builder.homie_properties(spec)
    assert set(props) == {("info", "serial-number")}  # internal_only has no twin
    assert builder.homie_properties(DeviceSpec("pv", INFO, device_id="pv-1")) == {}


def test_specs_are_compared_by_identity(root):
    model = GroupedPropertyDict()
    builder = DeviceTreeBuilder(root, model)
    a = DeviceSpec("circuit", INFO, device_id="c-1")
    b = DeviceSpec("circuit", INFO, device_id="c-1")
    assert a != b  # identical fields, still two declarations
    builder.add(a)
    assert builder.device_for(b) is None
