"""Tests for ebus_sdk.homie.Controller and DiscoveredDevice."""

import json
from unittest.mock import MagicMock, patch


from ebus_sdk.homie import (
    Controller,
    DiscoveredDevice,
    DeviceState,
    EBUS_HOMIE_DOMAIN,
    EBUS_HOMIE_VERSION_MAJOR,
    EBUS_HOMIE_MQTT_QOS,
    HOMIE_EFFECTIVE_STATE_TABLE,
)
import pytest

# ── DiscoveredDevice ─────────────────────────────────────────────────────


class TestDiscoveredDevice:
    def test_init_defaults(self):
        dev = DiscoveredDevice("panel-1")
        assert dev.device_id == "panel-1"
        assert dev.homie_domain == EBUS_HOMIE_DOMAIN
        assert dev.state is None
        assert dev.description is None
        assert dev.properties == {}
        assert dev.property_targets == {}
        assert dev.last_seen is None

    def test_update_state(self):
        dev = DiscoveredDevice("panel-1")
        dev.update_state("ready")
        assert dev.state == "ready"
        assert dev.last_seen is not None

    def test_update_description(self):
        dev = DiscoveredDevice("panel-1")
        desc = {"homie": "5.0", "nodes": {"core": {"name": "Core"}}}
        dev.update_description(json.dumps(desc))
        assert dev.description == desc
        assert dev.last_seen is not None

    def test_update_description_invalid_json(self):
        dev = DiscoveredDevice("panel-1")
        dev.update_description("not-json{{{")
        assert dev.description is None

    def test_update_and_get_property(self):
        dev = DiscoveredDevice("panel-1")
        dev.update_property("core", "active-power", "-500")
        assert dev.get_property("core", "active-power") == "-500"

    def test_get_property_missing(self):
        dev = DiscoveredDevice("panel-1")
        assert dev.get_property("nonexistent", "prop") is None

    def test_update_and_get_property_target(self):
        dev = DiscoveredDevice("panel-1")
        dev.update_property_target("breaker", "state", "CLOSED")
        assert dev.get_property_target("breaker", "state") == "CLOSED"

    def test_get_nodes_from_description(self):
        dev = DiscoveredDevice("panel-1")
        desc = {
            "nodes": {
                "core": {"name": "Core"},
                "circuit-1": {"name": "Kitchen"},
            }
        }
        dev.update_description(json.dumps(desc))
        nodes = dev.get_nodes()
        assert set(nodes) == {"core", "circuit-1"}

    def test_get_nodes_no_description(self):
        dev = DiscoveredDevice("panel-1")
        assert dev.get_nodes() == []

    def test_get_node_properties(self):
        dev = DiscoveredDevice("panel-1")
        desc = {
            "nodes": {
                "core": {
                    "name": "Core",
                    "properties": {"active-power": {"datatype": "float", "unit": "W"}},
                }
            }
        }
        dev.update_description(json.dumps(desc))
        props = dev.get_node_properties("core")
        assert "active-power" in props

    def test_get_node_properties_missing_node(self):
        dev = DiscoveredDevice("panel-1")
        dev.update_description(json.dumps({"nodes": {}}))
        assert dev.get_node_properties("missing") == {}


class TestDiscoveredDeviceHierarchy:
    """SDK-d1p: hierarchy fields on DiscoveredDevice."""

    def test_root_no_description_returns_self(self):
        dev = DiscoveredDevice("panel-1")
        # Before description arrives, treat the device as its own root —
        # we have no evidence otherwise.
        assert dev.root_id == "panel-1"
        assert dev.parent_id is None
        assert dev.children_ids == []
        assert dev.is_root is True

    def test_root_device_description(self):
        """A description without root/parent fields means this device is a root."""
        dev = DiscoveredDevice("panel-1")
        dev.update_description(json.dumps({"homie": "5.0", "children": ["bess-1", "evse-1"]}))
        assert dev.root_id == "panel-1"
        assert dev.parent_id is None
        assert dev.children_ids == ["bess-1", "evse-1"]
        assert dev.is_root is True

    def test_child_device_description(self):
        dev = DiscoveredDevice("bess-1")
        dev.update_description(json.dumps({"homie": "5.0", "root": "panel-1", "parent": "panel-1"}))
        assert dev.root_id == "panel-1"
        assert dev.parent_id == "panel-1"
        assert dev.children_ids == []
        assert dev.is_root is False

    def test_grandchild_distinguishes_root_from_parent(self):
        """S2: grandchild's root walks to the top while parent stays direct."""
        dev = DiscoveredDevice("mid-1")
        dev.update_description(json.dumps({"homie": "5.0", "root": "panel-1", "parent": "bess-1"}))
        assert dev.root_id == "panel-1"
        assert dev.parent_id == "bess-1"


class TestControllerHierarchyNavigation:
    """SDK-d1p: Controller tree navigation API."""

    @staticmethod
    def _discover(ctrl, device_id, description=None):
        """Push $state + (optional) $description into the controller."""
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/{device_id}/$state",
            b"ready",
        )
        if description is not None:
            ctrl._on_description_message(
                device_id,
                f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/{device_id}/$description",
                json.dumps(description).encode(),
            )

    def test_get_root_devices(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        ctrl.start_discovery()
        self._discover(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})
        self._discover(ctrl, "bess-1", {"homie": "5.0", "root": "panel-1", "parent": "panel-1"})
        self._discover(ctrl, "standalone-1", {"homie": "5.0"})

        roots = {d.device_id for d in ctrl.get_root_devices()}
        assert roots == {"panel-1", "standalone-1"}

    def test_get_root_for_child(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        ctrl.start_discovery()
        self._discover(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})
        self._discover(ctrl, "bess-1", {"homie": "5.0", "root": "panel-1", "parent": "panel-1"})

        assert ctrl.get_root("bess-1").device_id == "panel-1"
        assert ctrl.get_root("panel-1").device_id == "panel-1"
        assert ctrl.get_root("unknown") is None

    def test_get_root_for_grandchild(self, mock_paho):
        """S2: a 3-level tree's grandchild resolves to the top root."""
        ctrl, _ = _make_controller(mock_paho)
        ctrl.start_discovery()
        self._discover(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})
        self._discover(ctrl, "bess-1", {"homie": "5.0", "root": "panel-1", "parent": "panel-1", "children": ["mid-1"]})
        self._discover(ctrl, "mid-1", {"homie": "5.0", "root": "panel-1", "parent": "bess-1"})

        assert ctrl.get_root("mid-1").device_id == "panel-1"

    def test_get_children_returns_discovered_only(self, mock_paho):
        """If the parent's description lists a child that hasn't published yet, omit it."""
        ctrl, _ = _make_controller(mock_paho)
        ctrl.start_discovery()
        self._discover(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1", "evse-1"]})
        self._discover(ctrl, "bess-1", {"homie": "5.0", "root": "panel-1", "parent": "panel-1"})
        # evse-1 not yet discovered

        children = {c.device_id for c in ctrl.get_children("panel-1")}
        assert children == {"bess-1"}

    def test_get_descendants_breadth_first(self, mock_paho):
        """3-level tree: descendants of root are children + grandchildren."""
        ctrl, _ = _make_controller(mock_paho)
        ctrl.start_discovery()
        self._discover(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1", "evse-1"]})
        self._discover(ctrl, "bess-1", {"homie": "5.0", "root": "panel-1", "parent": "panel-1", "children": ["mid-1"]})
        self._discover(ctrl, "evse-1", {"homie": "5.0", "root": "panel-1", "parent": "panel-1"})
        self._discover(ctrl, "mid-1", {"homie": "5.0", "root": "panel-1", "parent": "bess-1"})

        descendants = ctrl.get_descendants("panel-1")
        ids = [d.device_id for d in descendants]
        # BFS: bess-1 and evse-1 (level 2) before mid-1 (level 3).
        assert set(ids[:2]) == {"bess-1", "evse-1"}
        assert ids[-1] == "mid-1"
        assert len(ids) == 3

    def test_get_children_unknown_device(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        assert ctrl.get_children("never-seen") == []
        assert ctrl.get_descendants("never-seen") == []


class TestEffectiveStateTable:
    """SDK-zt2: HOMIE_EFFECTIVE_STATE_TABLE shape."""

    def test_table_covers_all_states(self):
        for state in DeviceState:
            assert state in HOMIE_EFFECTIVE_STATE_TABLE, f"DeviceState.{state.name} missing from precedence table"

    def test_only_ready_maps_to_none(self):
        """Per spec: only when root is READY do children's own states stand."""
        for state, override in HOMIE_EFFECTIVE_STATE_TABLE.items():
            if state == DeviceState.READY:
                assert override is None
            else:
                assert override == state, f"non-ready root {state} should propagate as itself"


class TestControllerEffectiveState:
    """SDK-zt2: Controller.get_effective_state()."""

    @staticmethod
    def _discover(ctrl, device_id, state, description=None):
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/{device_id}/$state",
            state.encode(),
        )
        if description is not None:
            ctrl._on_description_message(
                device_id,
                f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/{device_id}/$description",
                json.dumps(description).encode(),
            )

    def test_root_returns_own_state(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        ctrl.start_discovery()
        self._discover(ctrl, "panel-1", "ready", {"homie": "5.0"})

        assert ctrl.get_effective_state("panel-1") == "ready"

    def test_unknown_device_returns_none(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        assert ctrl.get_effective_state("never-seen") is None

    @pytest.mark.parametrize(
        "root_state,child_own,expected",
        [
            ("ready", "ready", "ready"),
            ("ready", "init", "init"),
            ("ready", "sleeping", "sleeping"),
            ("ready", "lost", "lost"),
            ("init", "ready", "init"),
            ("disconnected", "ready", "disconnected"),
            ("disconnected", "lost", "disconnected"),
            ("sleeping", "ready", "sleeping"),
            ("lost", "ready", "lost"),
            ("lost", "init", "lost"),
        ],
    )
    def test_child_effective_state_per_spec(self, mock_paho, root_state, child_own, expected):
        ctrl, _ = _make_controller(mock_paho)
        ctrl.start_discovery()
        self._discover(ctrl, "panel-1", root_state, {"homie": "5.0", "children": ["bess-1"]})
        self._discover(
            ctrl,
            "bess-1",
            child_own,
            {"homie": "5.0", "root": "panel-1", "parent": "panel-1"},
        )

        assert ctrl.get_effective_state("bess-1") == expected

    def test_grandchild_uses_root_not_intermediate(self, mock_paho):
        """S2 + zt2: grandchild's effective state derives from ROOT, not parent.
        Parent in ready, root in lost → grandchild effectively lost."""
        ctrl, _ = _make_controller(mock_paho)
        ctrl.start_discovery()
        self._discover(ctrl, "panel-1", "lost", {"homie": "5.0", "children": ["bess-1"]})
        self._discover(
            ctrl,
            "bess-1",
            "ready",
            {"homie": "5.0", "root": "panel-1", "parent": "panel-1", "children": ["mid-1"]},
        )
        self._discover(
            ctrl,
            "mid-1",
            "ready",
            {"homie": "5.0", "root": "panel-1", "parent": "bess-1"},
        )

        assert ctrl.get_effective_state("mid-1") == "lost"
        assert ctrl.get_effective_state("bess-1") == "lost"
        assert ctrl.get_effective_state("panel-1") == "lost"

    def test_child_with_missing_root_falls_back_to_own_state(self, mock_paho):
        """When the root isn't discovered yet, return child's own state as best-effort."""
        ctrl, _ = _make_controller(mock_paho)
        ctrl.start_discovery()
        # Discover child first with no parent description for the root
        self._discover(
            ctrl,
            "bess-1",
            "ready",
            {"homie": "5.0", "root": "panel-1", "parent": "panel-1"},
        )
        # panel-1 not in registry yet

        assert ctrl.get_effective_state("bess-1") == "ready"

    def test_one_lost_root_makes_whole_tree_lost(self, mock_paho):
        """S5/S6 acceptance: when the panel goes LWT-lost, every descendant is effectively lost."""
        ctrl, _ = _make_controller(mock_paho)
        ctrl.start_discovery()
        # Build a 30-device tree
        self._discover(ctrl, "panel-1", "ready", {"homie": "5.0", "children": [f"c-{i}" for i in range(30)]})
        for i in range(30):
            self._discover(
                ctrl,
                f"c-{i}",
                "ready",
                {"homie": "5.0", "root": "panel-1", "parent": "panel-1"},
            )
        # Sanity: all ready
        for i in range(30):
            assert ctrl.get_effective_state(f"c-{i}") == "ready"

        # Panel goes lost (LWT fires)
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"lost",
        )

        # All children now effectively lost without re-publishing themselves
        for i in range(30):
            assert ctrl.get_effective_state(f"c-{i}") == "lost", f"c-{i} not lost"


# ── Controller ───────────────────────────────────────────────────────────


def _make_controller(mock_paho, device_id=None, auto_start=False, root_device_id=None):
    """Helper to create a Controller with mocked MQTT."""
    with patch("ebus_sdk.homie.MqttClient.from_config") as mock_from_config:
        mock_client = MagicMock()
        mock_client.sub_callbacks = {}
        mock_from_config.return_value = mock_client

        ctrl = Controller(
            mqtt_cfg={"host": "localhost", "port": 1883},
            auto_start=auto_start,
            device_id=device_id,
            root_device_id=root_device_id,
        )
        return ctrl, mock_client


class TestControllerInit:
    def test_default_init(self, mock_paho):
        ctrl, mock_client = _make_controller(mock_paho)
        assert ctrl.homie_domain == EBUS_HOMIE_DOMAIN
        assert ctrl.device_id is None
        assert ctrl.devices == {}

    def test_device_id_stored(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho, device_id="panel-1")
        assert ctrl.device_id == "panel-1"

    def test_callbacks_initially_none(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        assert ctrl._on_device_discovered is None
        assert ctrl._on_device_state_changed is None
        assert ctrl._on_device_removed is None
        assert ctrl._on_property_changed is None
        assert ctrl._on_description_received is None


class TestControllerDiscoveryWildcard:
    """Test wildcard (multi-device) discovery mode."""

    def test_start_discovery_subscribes_wildcard(self, mock_paho):
        ctrl, mock_client = _make_controller(mock_paho)
        ctrl.start_discovery()

        mock_client.subscribe.assert_called_once()
        args = mock_client.subscribe.call_args
        assert args[0][0] == f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/+/$state"

    def test_state_message_discovers_new_device(self, mock_paho):
        ctrl, mock_client = _make_controller(mock_paho)
        discovered = []
        ctrl.set_on_device_discovered_callback(lambda dev: discovered.append(dev))
        ctrl.start_discovery()

        # Simulate $state message
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )

        assert "panel-1" in ctrl.devices
        assert len(discovered) == 1
        assert discovered[0].device_id == "panel-1"
        assert discovered[0].state == "ready"

    def test_state_change_fires_callback(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        changes = []
        ctrl.set_on_device_state_changed_callback(lambda dev, old, new: changes.append((old, new)))
        ctrl.start_discovery()

        # First message — discovery
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"init",
        )
        # Second message — state change
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )

        assert len(changes) == 1
        assert changes[0] == ("init", "ready")

    def test_same_state_does_not_fire_callback(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        changes = []
        ctrl.set_on_device_state_changed_callback(lambda dev, old, new: changes.append((old, new)))
        ctrl.start_discovery()

        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )

        assert len(changes) == 0

    def test_empty_payload_removes_device(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        removed = []
        ctrl.set_on_device_removed_callback(lambda dev: removed.append(dev))
        ctrl.start_discovery()

        # Discover first
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )
        # Then remove
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"",
        )

        assert "panel-1" not in ctrl.devices
        assert len(removed) == 1


class TestControllerDiscoverySingleDevice:
    """Test single-device (device_id) discovery mode."""

    def test_start_discovery_subscribes_exact_topics(self, mock_paho):
        ctrl, mock_client = _make_controller(mock_paho, device_id="panel-1")
        ctrl.start_discovery()

        # Should subscribe to 4 exact topics (no wildcard in device-id position)
        assert mock_client.subscribe.call_count == 4
        topics = [c[0][0] for c in mock_client.subscribe.call_args_list]
        base = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1"
        assert f"{base}/$state" in topics
        assert f"{base}/$description" in topics
        assert f"{base}/+/+" in topics
        assert f"{base}/+/+/$target" in topics

    def test_pre_creates_device_entry(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho, device_id="panel-1")
        ctrl.start_discovery()

        assert "panel-1" in ctrl.devices
        assert ctrl.devices["panel-1"].state is None  # Pre-created, no state yet

    def test_first_state_fires_discovered(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho, device_id="panel-1")
        discovered = []
        ctrl.set_on_device_discovered_callback(lambda dev: discovered.append(dev))
        ctrl.start_discovery()

        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )

        assert len(discovered) == 1
        assert discovered[0].state == "ready"

    def test_no_wildcard_in_device_id_position(self, mock_paho):
        """Verify there is no '+' in the device-id segment of any subscription."""
        ctrl, mock_client = _make_controller(mock_paho, device_id="panel-1")
        ctrl.start_discovery()

        for c in mock_client.subscribe.call_args_list:
            topic = c[0][0]
            parts = topic.split("/")
            # parts[2] is the device-id position
            assert parts[2] == "panel-1", f"Wildcard found in device-id position: {topic}"


class TestControllerPropertyMessages:
    """Test property and description message handling."""

    def test_description_received(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        descriptions = []
        ctrl.set_on_description_received_callback(lambda dev: descriptions.append(dev))
        ctrl.start_discovery()

        # Discover device
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )

        desc = {"homie": "5.0", "nodes": {"core": {"name": "Core"}}}
        ctrl._on_description_message(
            "panel-1",
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$description",
            json.dumps(desc).encode(),
        )

        assert len(descriptions) == 1
        assert descriptions[0].description == desc

    def test_property_changed(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        changes = []
        ctrl.set_on_property_changed_callback(
            lambda dev_id, node, prop, val, old: changes.append((dev_id, node, prop, val, old))
        )
        ctrl.start_discovery()

        # Discover
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )

        ctrl._on_property_message(
            "panel-1",
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/core/active-power",
            b"-500",
        )

        assert len(changes) == 1
        assert changes[0] == ("panel-1", "core", "active-power", "-500", None)

    def test_property_skips_dollar_attributes(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        changes = []
        ctrl.set_on_property_changed_callback(lambda *args: changes.append(args))
        ctrl.start_discovery()

        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )

        # $description should be skipped
        ctrl._on_property_message(
            "panel-1",
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/core/$description",
            b"{}",
        )

        assert len(changes) == 0

    def test_target_message(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        ctrl.start_discovery()

        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )

        ctrl._on_target_message(
            "panel-1",
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/breaker/state/$target",
            b"CLOSED",
        )

        dev = ctrl.devices["panel-1"]
        assert dev.get_property_target("breaker", "state") == "CLOSED"

    def test_property_message_decodes_null_byte_to_empty_string(self, mock_paho):
        # Homie 5: a single 0x00 byte payload is an empty-string value, not a
        # literal "\x00" string.
        ctrl, _ = _make_controller(mock_paho)
        changes = []
        ctrl.set_on_property_changed_callback(
            lambda dev_id, node, prop, val, old: changes.append((dev_id, node, prop, val, old))
        )
        ctrl.start_discovery()
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )

        ctrl._on_property_message(
            "panel-1",
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/info/label",
            b"\x00",
        )

        assert ctrl.devices["panel-1"].get_property("info", "label") == ""
        assert changes[-1] == ("panel-1", "info", "label", "", None)

    def test_target_message_decodes_null_byte_to_empty_string(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        ctrl.start_discovery()
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )

        ctrl._on_target_message(
            "panel-1",
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/info/label/$target",
            b"\x00",
        )

        assert ctrl.devices["panel-1"].get_property_target("info", "label") == ""


class TestControllerSetProperty:
    def test_set_property_publishes(self, mock_paho):
        ctrl, mock_client = _make_controller(mock_paho)

        result = ctrl.set_property("panel-1", "breaker", "state", "CLOSED")

        assert result is True
        expected_topic = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/breaker/state/set"
        mock_client.publish.assert_called_once_with(expected_topic, "CLOSED", qos=EBUS_HOMIE_MQTT_QOS, retain=False)

    def test_set_property_empty_string_encodes_null_byte(self, mock_paho):
        ctrl, mock_client = _make_controller(mock_paho)

        result = ctrl.set_property("panel-1", "info", "label", "")

        assert result is True
        expected_topic = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/info/label/set"
        mock_client.publish.assert_called_once_with(expected_topic, "\x00", qos=EBUS_HOMIE_MQTT_QOS, retain=False)

    def test_set_property_no_connection(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        ctrl.mqttc = None

        result = ctrl.set_property("panel-1", "breaker", "state", "CLOSED")
        assert result is False


class TestControllerBroadcast:
    def test_broadcast(self, mock_paho):
        ctrl, mock_client = _make_controller(mock_paho)

        result = ctrl.broadcast("alert", "test-message")

        assert result is True
        expected_topic = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/$broadcast/alert"
        mock_client.publish.assert_called_once_with(
            expected_topic, "test-message", qos=EBUS_HOMIE_MQTT_QOS, retain=False
        )


class TestControllerStop:
    def test_stop_clears_devices(self, mock_paho):
        ctrl, mock_client = _make_controller(mock_paho)
        ctrl.start_discovery()

        # Discover a device
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )
        assert len(ctrl.devices) == 1

        ctrl.stop()

        assert ctrl.devices == {}
        assert ctrl.mqttc is None
        mock_client.stop.assert_called_once()

    def test_stop_clears_callbacks(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        ctrl.set_on_device_discovered_callback(lambda d: None)
        ctrl.set_on_property_changed_callback(lambda *a: None)
        ctrl.set_on_description_received_callback(lambda d: None)
        ctrl.set_on_device_state_changed_callback(lambda *a: None)
        ctrl.set_on_device_removed_callback(lambda d: None)

        ctrl.stop()

        assert ctrl._on_device_discovered is None
        assert ctrl._on_device_state_changed is None
        assert ctrl._on_device_removed is None
        assert ctrl._on_property_changed is None
        assert ctrl._on_description_received is None

    def test_stop_without_mqttc(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        ctrl.mqttc = None
        # Should not raise
        ctrl.stop()

    def test_get_device(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        ctrl.start_discovery()
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )

        dev = ctrl.get_device("panel-1")
        assert dev is not None
        assert dev.device_id == "panel-1"

        assert ctrl.get_device("nonexistent") is None

    def test_get_all_devices_returns_copy(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        ctrl.start_discovery()
        ctrl._on_state_message(
            f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-1/$state",
            b"ready",
        )

        all_devs = ctrl.get_all_devices()
        assert "panel-1" in all_devs
        # Mutating the copy shouldn't affect the controller
        all_devs.pop("panel-1")
        assert "panel-1" in ctrl.devices


# ── Controller QoS ────────────────────────────────────────────────────────


def _make_controller_with_qos(mock_paho, qos, device_id=None):
    """Helper to create a Controller with a custom QoS and mocked MQTT."""
    with patch("ebus_sdk.homie.MqttClient.from_config") as mock_from_config:
        mock_client = MagicMock()
        mock_client.sub_callbacks = {}
        mock_from_config.return_value = mock_client

        ctrl = Controller(
            mqtt_cfg={"host": "localhost", "port": 1883},
            device_id=device_id,
            qos=qos,
        )
        return ctrl, mock_client


class TestControllerQoS:
    """Test client-settable QoS on Controller."""

    def test_qos_defaults_to_global(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        assert ctrl.qos == EBUS_HOMIE_MQTT_QOS

    def test_qos_property_returns_custom_value(self, mock_paho):
        ctrl, _ = _make_controller_with_qos(mock_paho, qos=1)
        assert ctrl.qos == 1

    def test_wildcard_subscribe_uses_custom_qos(self, mock_paho):
        ctrl, mock_client = _make_controller_with_qos(mock_paho, qos=0)
        ctrl.start_discovery()

        mock_client.subscribe.assert_called_once()
        _, kwargs = mock_client.subscribe.call_args
        assert kwargs["qos"] == 0

    def test_single_device_subscribe_uses_custom_qos(self, mock_paho):
        ctrl, mock_client = _make_controller_with_qos(mock_paho, qos=1, device_id="panel-1")
        ctrl.start_discovery()

        assert mock_client.subscribe.call_count == 4
        for c in mock_client.subscribe.call_args_list:
            _, kwargs = c
            assert kwargs["qos"] == 1

    def test_set_property_uses_controller_qos_by_default(self, mock_paho):
        ctrl, mock_client = _make_controller_with_qos(mock_paho, qos=1)

        ctrl.set_property("panel-1", "breaker", "state", "CLOSED")

        _, kwargs = mock_client.publish.call_args
        assert kwargs["qos"] == 1

    def test_set_property_allows_qos_override(self, mock_paho):
        ctrl, mock_client = _make_controller_with_qos(mock_paho, qos=1)

        ctrl.set_property("panel-1", "breaker", "state", "CLOSED", qos=0)

        _, kwargs = mock_client.publish.call_args
        assert kwargs["qos"] == 0

    def test_broadcast_uses_controller_qos_by_default(self, mock_paho):
        ctrl, mock_client = _make_controller_with_qos(mock_paho, qos=1)

        ctrl.broadcast("alert", "test-message")

        _, kwargs = mock_client.publish.call_args
        assert kwargs["qos"] == 1

    def test_broadcast_allows_qos_override(self, mock_paho):
        ctrl, mock_client = _make_controller_with_qos(mock_paho, qos=1)

        ctrl.broadcast("alert", "test-message", qos=0)

        _, kwargs = mock_client.publish.call_args
        assert kwargs["qos"] == 0

    def test_subscribe_to_device_uses_custom_qos(self, mock_paho):
        """Verify _subscribe_to_device (wildcard re-subscribe on new device) uses controller QoS."""
        ctrl, mock_client = _make_controller_with_qos(mock_paho, qos=1)
        ctrl.start_discovery()
        mock_client.subscribe.reset_mock()

        # Simulate discovering a new device in wildcard mode
        ctrl._subscribe_to_device("panel-2")

        assert mock_client.subscribe.call_count == 3
        for c in mock_client.subscribe.call_args_list:
            _, kwargs = c
            assert kwargs["qos"] == 1


# ── Controller tree-rooted mode (SDK-o1h) ────────────────────────────────


def _push_state(ctrl, device_id, state):
    """Push a $state retained message into the controller."""
    ctrl._on_state_message(
        f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/{device_id}/$state",
        state.encode() if isinstance(state, str) else state,
    )


def _push_description(ctrl, device_id, description):
    """Push a $description retained message into the controller."""
    ctrl._on_description_message(
        device_id,
        f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/{device_id}/$description",
        json.dumps(description).encode(),
    )


def _filters_for(device_id):
    """The four exact-device topic filters subscribed for one device."""
    base = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/{device_id}"
    return {
        f"{base}/$state",
        f"{base}/$description",
        f"{base}/+/+",
        f"{base}/+/+/$target",
    }


class TestTreeRootedInit:
    def test_mutually_exclusive_with_device_id(self, mock_paho):
        with pytest.raises(ValueError):
            _make_controller(mock_paho, device_id="panel-1", root_device_id="panel-1")

    def test_root_device_id_stored(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho, root_device_id="panel-1")
        assert ctrl.root_device_id == "panel-1"
        assert ctrl.is_tree_rooted is True
        assert ctrl.device_id is None

    def test_wildcard_mode_not_tree_rooted(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)
        assert ctrl.is_tree_rooted is False

    def test_single_device_mode_not_tree_rooted(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho, device_id="panel-1")
        assert ctrl.is_tree_rooted is False


class TestTreeRootedStartDiscovery:
    def test_subscribes_four_root_filters(self, mock_paho):
        ctrl, mock_client = _make_controller(mock_paho, root_device_id="panel-1")
        ctrl.start_discovery()

        topics = {c[0][0] for c in mock_client.subscribe.call_args_list}
        assert topics == _filters_for("panel-1")

    def test_pre_creates_root_entry(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho, root_device_id="panel-1")
        ctrl.start_discovery()

        assert "panel-1" in ctrl.devices
        assert ctrl.devices["panel-1"].state is None

    def test_no_wildcard_subscription(self, mock_paho):
        """Tree-rooted mode must not subscribe to the broker-wide +/$state."""
        ctrl, mock_client = _make_controller(mock_paho, root_device_id="panel-1")
        ctrl.start_discovery()

        wildcard = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/+/$state"
        topics = [c[0][0] for c in mock_client.subscribe.call_args_list]
        assert wildcard not in topics


class TestTreeRootedBootstrap:
    """Retained-state bootstrap: tree announces all-at-once on connect."""

    def test_root_only_no_children(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho, root_device_id="panel-1")
        ctrl.start_discovery()
        # Retained $description (no children) + retained $state=ready
        _push_description(ctrl, "panel-1", {"homie": "5.0"})
        _push_state(ctrl, "panel-1", "ready")

        assert set(ctrl.devices.keys()) == {"panel-1"}

    def test_root_with_children_subscribes_each(self, mock_paho):
        ctrl, mock_client = _make_controller(mock_paho, root_device_id="panel-1")
        ctrl.start_discovery()
        # Pretend retained $description arrives first (matches MQTT typical
        # delivery order on a fresh subscription), then $state=ready.
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1", "evse-1"]})
        mock_client.subscribe.reset_mock()
        _push_state(ctrl, "panel-1", "ready")

        # init→ready edge → reconcile → subscribe to each child's 4 filters
        topics = {c[0][0] for c in mock_client.subscribe.call_args_list}
        assert _filters_for("bess-1") <= topics
        assert _filters_for("evse-1") <= topics
        assert "bess-1" in ctrl.devices
        assert "evse-1" in ctrl.devices

    def test_grandchild_cascade(self, mock_paho):
        """3-level tree bootstraps from the root via cascading state-edges."""
        ctrl, _ = _make_controller(mock_paho, root_device_id="panel-1")
        ctrl.start_discovery()
        # Root: parent of bess-1
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})
        _push_state(ctrl, "panel-1", "ready")
        # bess-1's retained state/desc arrive after subscription
        _push_description(
            ctrl,
            "bess-1",
            {
                "homie": "5.0",
                "root": "panel-1",
                "parent": "panel-1",
                "children": ["mid-1"],
            },
        )
        _push_state(ctrl, "bess-1", "ready")
        # mid-1's retained state/desc
        _push_description(
            ctrl,
            "mid-1",
            {
                "homie": "5.0",
                "root": "panel-1",
                "parent": "bess-1",
            },
        )
        _push_state(ctrl, "mid-1", "ready")

        assert set(ctrl.devices.keys()) == {"panel-1", "bess-1", "mid-1"}

    def test_discovery_callbacks_fire_per_descendant(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho, root_device_id="panel-1")
        discovered = []
        ctrl.set_on_device_discovered_callback(lambda d: discovered.append(d.device_id))
        ctrl.start_discovery()
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})
        _push_state(ctrl, "panel-1", "ready")
        _push_description(
            ctrl,
            "bess-1",
            {
                "homie": "5.0",
                "root": "panel-1",
                "parent": "panel-1",
            },
        )
        _push_state(ctrl, "bess-1", "ready")

        assert discovered == ["panel-1", "bess-1"]


class TestTreeRootedStateGate:
    """init→ready edge gates reconcile; mid-init updates are stashed."""

    def test_init_state_does_not_reconcile(self, mock_paho):
        ctrl, mock_client = _make_controller(mock_paho, root_device_id="panel-1")
        ctrl.start_discovery()
        # First message is init — no children should be subscribed
        _push_state(ctrl, "panel-1", "init")
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})
        mock_client.subscribe.reset_mock()
        # Another init refresh — still no reconcile
        _push_state(ctrl, "panel-1", "init")

        assert "bess-1" not in ctrl.devices
        assert mock_client.subscribe.call_count == 0

    def test_init_then_ready_reconciles(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho, root_device_id="panel-1")
        ctrl.start_discovery()
        _push_state(ctrl, "panel-1", "init")
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})
        _push_state(ctrl, "panel-1", "ready")

        assert "bess-1" in ctrl.devices

    def test_ready_to_ready_does_not_reconcile(self, mock_paho):
        """A retained $state=ready republish (no edge) must not re-walk."""
        ctrl, mock_client = _make_controller(mock_paho, root_device_id="panel-1")
        ctrl.start_discovery()
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})
        _push_state(ctrl, "panel-1", "ready")
        # bess-1's tree is now subscribed; reset to see whether the next
        # ready→ready refresh triggers any subscription churn.
        mock_client.subscribe.reset_mock()
        _push_state(ctrl, "panel-1", "ready")

        assert mock_client.subscribe.call_count == 0


class TestTreeRootedDynamicAdd:
    def test_mid_flight_child_addition(self, mock_paho):
        """Parent re-enters init, gets new description with extra child,
        returns to ready → new descendant is auto-subscribed."""
        ctrl, mock_client = _make_controller(mock_paho, root_device_id="panel-1")
        ctrl.start_discovery()
        # Initial steady-state with one child
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})
        _push_state(ctrl, "panel-1", "ready")
        _push_description(
            ctrl,
            "bess-1",
            {
                "homie": "5.0",
                "root": "panel-1",
                "parent": "panel-1",
            },
        )
        _push_state(ctrl, "bess-1", "ready")
        mock_client.subscribe.reset_mock()

        # Mid-flight: parent goes to init, new description includes evse-1
        _push_state(ctrl, "panel-1", "init")
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1", "evse-1"]})
        # No reconcile yet
        assert "evse-1" not in ctrl.devices
        # Parent returns to ready → reconcile fires
        _push_state(ctrl, "panel-1", "ready")

        assert "evse-1" in ctrl.devices
        topics = {c[0][0] for c in mock_client.subscribe.call_args_list}
        # evse-1's 4 filters subscribed; bess-1's were not re-subscribed
        assert _filters_for("evse-1") <= topics
        assert _filters_for("bess-1").isdisjoint(topics)


class TestTreeRootedDynamicRemove:
    def test_child_removal_unsubscribes_and_fires_callback(self, mock_paho):
        ctrl, mock_client = _make_controller(mock_paho, root_device_id="panel-1")
        removed = []
        ctrl.set_on_device_removed_callback(lambda d: removed.append(d.device_id))
        ctrl.start_discovery()
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1", "evse-1"]})
        _push_state(ctrl, "panel-1", "ready")
        _push_description(ctrl, "bess-1", {"homie": "5.0", "root": "panel-1", "parent": "panel-1"})
        _push_state(ctrl, "bess-1", "ready")
        _push_description(ctrl, "evse-1", {"homie": "5.0", "root": "panel-1", "parent": "panel-1"})
        _push_state(ctrl, "evse-1", "ready")

        # Reset only AFTER full steady-state, so we see only the unsub for evse-1
        mock_client.unsubscribe.reset_mock()

        # Parent drops evse-1 mid-flight
        _push_state(ctrl, "panel-1", "init")
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})
        _push_state(ctrl, "panel-1", "ready")

        assert "evse-1" not in ctrl.devices
        assert "bess-1" in ctrl.devices
        assert removed == ["evse-1"]
        # 4 unsubscribe calls for evse-1's four filters
        unsub_topics = {c[0][0] for c in mock_client.unsubscribe.call_args_list}
        assert unsub_topics == _filters_for("evse-1")

    def test_grandchild_dropped_recursively(self, mock_paho):
        """Removing a middle device drops its descendants too."""
        ctrl, _ = _make_controller(mock_paho, root_device_id="panel-1")
        removed = []
        ctrl.set_on_device_removed_callback(lambda d: removed.append(d.device_id))
        ctrl.start_discovery()
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})
        _push_state(ctrl, "panel-1", "ready")
        _push_description(
            ctrl,
            "bess-1",
            {
                "homie": "5.0",
                "root": "panel-1",
                "parent": "panel-1",
                "children": ["mid-1"],
            },
        )
        _push_state(ctrl, "bess-1", "ready")
        _push_description(
            ctrl,
            "mid-1",
            {
                "homie": "5.0",
                "root": "panel-1",
                "parent": "bess-1",
            },
        )
        _push_state(ctrl, "mid-1", "ready")

        # Parent drops bess-1 — mid-1 must go too
        _push_state(ctrl, "panel-1", "init")
        _push_description(ctrl, "panel-1", {"homie": "5.0"})
        _push_state(ctrl, "panel-1", "ready")

        assert "bess-1" not in ctrl.devices
        assert "mid-1" not in ctrl.devices
        # Leaves-first ordering: mid-1 fires before bess-1
        assert removed == ["mid-1", "bess-1"]


class TestTreeRootedDescriptionRace:
    """SDK-gsn: retained $state=ready may arrive before retained $description.

    paho delivers retained messages in subscription order, and we subscribe
    to $state before $description in _subscribe_device_topics. So on initial
    connect to a broker holding both retained, the state-edge reconcile in
    _on_state_message can fire while the device's description is still None,
    seeing zero children and subscribing to nothing. The fix re-runs reconcile
    from _on_description_message when the device is already ready.
    """

    def test_state_before_description_still_reconciles(self, mock_paho):
        """Retained $state=ready arrives FIRST, then $description with children."""
        ctrl, mock_client = _make_controller(mock_paho, root_device_id="panel-1")
        ctrl.start_discovery()
        # State arrives first — at this moment description is None, reconcile
        # finds zero children and is effectively a no-op.
        _push_state(ctrl, "panel-1", "ready")
        assert "bess-1" not in ctrl.devices
        mock_client.subscribe.reset_mock()

        # Description arrives second — must trigger a fresh reconcile that
        # sees the now-current children list.
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1", "evse-1"]})

        assert "bess-1" in ctrl.devices
        assert "evse-1" in ctrl.devices
        topics = {c[0][0] for c in mock_client.subscribe.call_args_list}
        assert _filters_for("bess-1") <= topics
        assert _filters_for("evse-1") <= topics

    def test_description_only_acts_when_ready(self, mock_paho):
        """A $description arriving while $state=init must NOT reconcile."""
        ctrl, mock_client = _make_controller(mock_paho, root_device_id="panel-1")
        ctrl.start_discovery()
        _push_state(ctrl, "panel-1", "init")
        mock_client.subscribe.reset_mock()
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})

        # State is still init — description-driven reconcile must be gated
        assert "bess-1" not in ctrl.devices
        assert mock_client.subscribe.call_count == 0

    def test_repeat_description_in_ready_is_idempotent(self, mock_paho):
        """A second $description with unchanged children must not re-subscribe."""
        ctrl, mock_client = _make_controller(mock_paho, root_device_id="panel-1")
        ctrl.start_discovery()
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})
        _push_state(ctrl, "panel-1", "ready")
        # bess-1's tree is established — reset and re-deliver the same
        # description (e.g. a controller resubscribe). No subscription churn.
        mock_client.subscribe.reset_mock()
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})

        assert mock_client.subscribe.call_count == 0

    def test_grandchild_race_via_intermediate(self, mock_paho):
        """The race recurs at every level — bess-1's children list may also
        arrive after its $state=ready. The same fix must cover descendants."""
        ctrl, mock_client = _make_controller(mock_paho, root_device_id="panel-1")
        ctrl.start_discovery()
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})
        _push_state(ctrl, "panel-1", "ready")
        # bess-1 announces ready first, description second
        _push_state(ctrl, "bess-1", "ready")
        assert "mid-1" not in ctrl.devices
        mock_client.subscribe.reset_mock()
        _push_description(
            ctrl,
            "bess-1",
            {
                "homie": "5.0",
                "root": "panel-1",
                "parent": "panel-1",
                "children": ["mid-1"],
            },
        )

        assert "mid-1" in ctrl.devices
        topics = {c[0][0] for c in mock_client.subscribe.call_args_list}
        assert _filters_for("mid-1") <= topics


class TestTreeRootedReconnect:
    def test_reconnect_resets_devices_and_rewalks(self, mock_paho):
        """On reconnect: registry is reset; retained state re-cascades the tree."""
        ctrl, mock_client = _make_controller(mock_paho, root_device_id="panel-1")
        ctrl.start_discovery()
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})
        _push_state(ctrl, "panel-1", "ready")
        _push_description(ctrl, "bess-1", {"homie": "5.0", "root": "panel-1", "parent": "panel-1"})
        _push_state(ctrl, "bess-1", "ready")
        assert "bess-1" in ctrl.devices

        # Simulate reconnect: paho re-subscribes our filters; controller
        # resets its registry so the retained ready triggers init→ready.
        ctrl._on_connect()

        # bess-1 is gone from the in-memory registry but root entry exists
        assert set(ctrl.devices.keys()) == {"panel-1"}
        assert ctrl.devices["panel-1"].state is None

        # Retained $state/$description re-arrive on the recovered subscriptions
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})
        _push_state(ctrl, "panel-1", "ready")
        _push_description(ctrl, "bess-1", {"homie": "5.0", "root": "panel-1", "parent": "panel-1"})
        _push_state(ctrl, "bess-1", "ready")

        assert set(ctrl.devices.keys()) == {"panel-1", "bess-1"}


class TestControllerResync:
    """resync() is the public reconnect hook a bring-your-own-transport caller wires (#13).

    An injected client bypasses MqttClient.from_config, so the SDK's on_connect
    (which resets tree-rooted bookkeeping) is never registered on it. resync()
    exposes that reset so a BYO tree-rooted caller can drive the re-walk itself.
    """

    def test_resync_resets_tree_rooted_registry(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho, root_device_id="panel-1")
        ctrl.start_discovery()
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})
        _push_state(ctrl, "panel-1", "ready")
        _push_description(ctrl, "bess-1", {"homie": "5.0", "root": "panel-1", "parent": "panel-1"})
        _push_state(ctrl, "bess-1", "ready")
        assert "bess-1" in ctrl.devices

        ctrl.resync()

        assert set(ctrl.devices.keys()) == {"panel-1"}
        assert ctrl.devices["panel-1"].state is None

    def test_resync_is_noop_when_not_tree_rooted(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho)  # wildcard mode
        ctrl.devices["some-dev"] = DiscoveredDevice("some-dev")
        ctrl.resync()
        assert "some-dev" in ctrl.devices

    def test_on_connect_delegates_to_resync(self, mock_paho):
        """The owned-client path resets via resync(), so the refactor preserved behavior (#13)."""
        ctrl, _ = _make_controller(mock_paho, root_device_id="panel-1")
        called = []
        ctrl.resync = lambda: called.append(True)
        ctrl._on_connect()
        assert called == [True]


class TestControllerBYOTransport:
    """Bring-your-own-transport: inject an MQTT client instead of constructing one (SDK-61t.6)."""

    def test_injected_client_is_used_as_is_and_not_started(self):
        fake = MagicMock()
        with patch("ebus_sdk.homie.MqttClient.from_config") as mock_from_config:
            ctrl = Controller(mqtt_cfg={"host": "x"}, mqttc=fake)
        assert ctrl.mqttc is fake
        assert ctrl._owns_client is False
        mock_from_config.assert_not_called()  # SDK does not construct its own
        fake.start.assert_not_called()  # nor start the caller's client

    def test_injected_client_not_stopped_on_stop(self):
        fake = MagicMock()
        ctrl = Controller(mqttc=fake)
        ctrl.stop()
        fake.stop.assert_not_called()  # caller owns the client's lifecycle
        assert ctrl.mqttc is None

    def test_injected_client_drives_discovery(self):
        fake = MagicMock()
        ctrl = Controller(mqttc=fake)
        ctrl.start_discovery()
        # Wildcard discovery subscribes directly on the injected client.
        fake.subscribe.assert_called_once()

    def test_owned_client_is_constructed_started_and_stopped(self):
        with patch("ebus_sdk.homie.MqttClient.from_config") as mock_from_config:
            client = MagicMock()
            client.sub_callbacks = {}
            mock_from_config.return_value = client
            ctrl = Controller(mqtt_cfg={"host": "x"})
            assert ctrl._owns_client is True
            assert ctrl.mqttc is client
            client.start.assert_called_once()  # SDK starts an owned client
            ctrl.stop()
            client.stop.assert_called_once()  # and stops it
