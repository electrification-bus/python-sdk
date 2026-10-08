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

    def test_wildcard_per_device_subscribe_uses_custom_qos(self, mock_paho):
        """The per-device filters wildcard discovery adds for a new device use controller QoS."""
        ctrl, mock_client = _make_controller_with_qos(mock_paho, qos=1)
        ctrl.start_discovery()
        mock_client.subscribe.reset_mock()

        # Discover a new device in wildcard mode, then complete its stage one
        ctrl._on_state_message(f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/panel-2/$state", b"ready")
        _push_description(ctrl, "panel-2", {"homie": "5.0"})

        assert mock_client.subscribe.call_count == 4  # $description, then +/+, +/+/$target, $description
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


def _attribute_filters_for(device_id):
    """The $state and $description filters a paced descendant is subscribed to first (GH #97)."""
    base = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/{device_id}"
    return {f"{base}/$state", f"{base}/$description"}


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

        # init→ready edge → reconcile → subscribe to each child's $state and $description
        topics = {c[0][0] for c in mock_client.subscribe.call_args_list}
        assert _attribute_filters_for("bess-1") <= topics
        assert _attribute_filters_for("evse-1") <= topics
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
        # evse-1's $state and $description subscribed; bess-1's were not re-subscribed
        assert _attribute_filters_for("evse-1") <= topics
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
    to $state before $description in _subscribe_filters. So on initial
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
        assert _attribute_filters_for("bess-1") <= topics
        assert _attribute_filters_for("evse-1") <= topics

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
        assert _attribute_filters_for("mid-1") <= topics


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


class TestMqttTransportProtocols:
    """The injection point is typed by what the SDK calls on an injected client (#8)."""

    def test_mqtt_client_satisfies_both_protocols(self):
        """The concrete client must keep satisfying the widened annotation."""
        from ebus_mqtt_client import MqttClient as ConcreteClient

        from ebus_sdk import MqttControllerTransport, MqttTransport

        assert issubclass(ConcreteClient, MqttTransport)
        assert issubclass(ConcreteClient, MqttControllerTransport)

    def test_protocols_are_exported_from_the_package_root(self):
        """A consumer who cannot name the type gets nothing from the widening."""
        import ebus_sdk

        assert "MqttTransport" in ebus_sdk.__all__
        assert "MqttControllerTransport" in ebus_sdk.__all__
        assert ebus_sdk.MqttTransport is not None
        assert ebus_sdk.MqttControllerTransport is not None

    def test_the_controller_contract_derives_from_the_shared_base(self):
        """The base carries only what both roles share, so a role-specific contract
        inherits no member its own call sites never reach. A `Device`-side protocol
        derives from `MqttTransport`, which is why it will not inherit `unsubscribe`.
        """
        from ebus_sdk import MqttControllerTransport, MqttTransport

        assert issubclass(MqttControllerTransport, MqttTransport)

        class PublishSubscribeOnly:
            def publish(self, topic, data, qos=1, retain=False):
                return None

            def subscribe(self, sub, param, qos=1):
                return None

        client = PublishSubscribeOnly()
        assert isinstance(client, MqttTransport)
        assert not isinstance(client, MqttControllerTransport)

    def test_a_three_member_client_is_a_valid_controller_transport(self):
        """publish / subscribe / unsubscribe is the whole consumer-side contract.

        Deliberately implements nothing else — no start, stop, is_connected, is_running
        or publish_and_flush — because the SDK never calls those on an injected client.
        """
        from ebus_sdk import MqttControllerTransport

        class Minimal:
            def publish(self, topic, data, qos=1, retain=False):
                return None

            def subscribe(self, sub, param, qos=1):
                return None

            def unsubscribe(self, sub):
                return True

        client = Minimal()
        assert isinstance(client, MqttControllerTransport)

        ctrl = Controller(mqttc=client)
        ctrl.start_discovery()
        ctrl.stop()  # must not reach start()/stop() on a client that has neither

    def test_owned_client_handle_is_none_when_injected(self):
        """`_owned_client` is what makes 'never stopped' structural rather than promised."""
        fake = MagicMock()
        ctrl = Controller(mqttc=fake)
        assert ctrl._owned_client is None
        ctrl.stop()
        fake.stop.assert_not_called()

    def test_owned_client_handle_is_set_and_cleared_for_an_sdk_built_client(self):
        with patch("ebus_sdk.homie.MqttClient.from_config") as mock_from_config:
            client = MagicMock()
            client.sub_callbacks = {}
            mock_from_config.return_value = client
            ctrl = Controller(mqtt_cfg={"host": "x"})
            assert ctrl._owned_client is client
            ctrl.stop()
            client.stop.assert_called_once()
            assert ctrl._owned_client is None


class TestTreeCompleteAffordance:
    """gh-37: a reconciling 'the declared tree is fully described' predicate.

    The point is that this is NOT a barrier. A Homie tree can grow at any
    moment, so the predicate must be re-evaluable and the callback must re-arm.
    """

    def test_incomplete_while_a_declared_child_has_not_described_itself(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho, root_device_id="panel-1")
        ctrl.start_discovery()
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})
        _push_state(ctrl, "panel-1", "ready")

        # The root says ready and names a child that has published nothing.
        # This is exactly the state that misleads a consumer gating on $state.
        assert ctrl.get_effective_state("panel-1") == "ready"
        assert ctrl.is_tree_complete("panel-1") is False

    def test_complete_once_every_declared_descendant_is_described(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho, root_device_id="panel-1")
        ctrl.start_discovery()
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})
        _push_state(ctrl, "panel-1", "ready")
        _push_description(
            ctrl, "bess-1", {"homie": "5.0", "root": "panel-1", "parent": "panel-1", "children": ["mid-1"]}
        )
        _push_state(ctrl, "bess-1", "ready")
        assert ctrl.is_tree_complete("panel-1") is False, "grandchild not described yet"

        _push_description(ctrl, "mid-1", {"homie": "5.0", "root": "panel-1", "parent": "bess-1"})
        assert ctrl.is_tree_complete("panel-1") is True

    def test_a_described_but_lost_child_still_counts_as_described(self, mock_paho):
        """Completeness is about description, not liveness. get_effective_state is for liveness."""
        ctrl, _ = _make_controller(mock_paho, root_device_id="panel-1")
        ctrl.start_discovery()
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})
        _push_state(ctrl, "panel-1", "ready")
        _push_description(ctrl, "bess-1", {"homie": "5.0", "root": "panel-1", "parent": "panel-1"})
        _push_state(ctrl, "bess-1", "lost")

        assert ctrl.is_tree_complete("panel-1") is True
        assert ctrl.get_effective_state("bess-1") == "lost"

    def test_unknown_or_undescribed_root_is_not_complete(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho, root_device_id="panel-1")
        ctrl.start_discovery()
        assert ctrl.is_tree_complete("never-seen") is False
        _push_state(ctrl, "panel-1", "ready")
        assert ctrl.is_tree_complete("panel-1") is False, "state without a description declares no tree"

    def test_a_declared_cycle_terminates(self, mock_paho):
        """A malformed tree must not hang the predicate."""
        ctrl, _ = _make_controller(mock_paho, root_device_id="panel-1")
        ctrl.start_discovery()
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})
        _push_state(ctrl, "panel-1", "ready")
        _push_description(
            ctrl, "bess-1", {"homie": "5.0", "root": "panel-1", "parent": "panel-1", "children": ["panel-1"]}
        )

        assert ctrl.is_tree_complete("panel-1") is True

    def test_on_tree_ready_fires_once_on_the_completing_edge(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho, root_device_id="panel-1")
        fired = []
        ctrl.set_on_tree_ready_callback(lambda root: fired.append(root.device_id))
        ctrl.start_discovery()

        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})
        _push_state(ctrl, "panel-1", "ready")
        assert fired == [], "fired before the declared child described itself"

        _push_description(ctrl, "bess-1", {"homie": "5.0", "root": "panel-1", "parent": "panel-1"})
        assert fired == ["panel-1"]

        # Edge-triggered: a redundant republish of the same shape must not refire.
        _push_description(ctrl, "bess-1", {"homie": "5.0", "root": "panel-1", "parent": "panel-1"})
        assert fired == ["panel-1"]

    def test_on_tree_ready_rearms_when_the_tree_grows(self, mock_paho):
        """The whole point: a device commissioned later must not be missed.

        A one-shot barrier is the consumer bug this API exists to prevent, so
        the callback itself must not behave like one.
        """
        ctrl, _ = _make_controller(mock_paho, root_device_id="panel-1")
        fired = []
        ctrl.set_on_tree_ready_callback(lambda root: fired.append(root.device_id))
        ctrl.start_discovery()
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})
        _push_state(ctrl, "panel-1", "ready")
        _push_description(ctrl, "bess-1", {"homie": "5.0", "root": "panel-1", "parent": "panel-1"})
        assert fired == ["panel-1"]

        # Commission a second child well after the tree first settled.
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1", "evse-1"]})
        assert ctrl.is_tree_complete("panel-1") is False, "newly declared child un-completes the tree"
        assert fired == ["panel-1"], "must not refire while incomplete"

        _push_description(ctrl, "evse-1", {"homie": "5.0", "root": "panel-1", "parent": "panel-1"})
        assert ctrl.is_tree_complete("panel-1") is True
        assert fired == ["panel-1", "panel-1"], "must fire again for the new settled shape"

    def test_on_tree_ready_survives_a_raising_callback(self, mock_paho):
        ctrl, _ = _make_controller(mock_paho, root_device_id="panel-1")
        ctrl.set_on_tree_ready_callback(MagicMock(side_effect=RuntimeError("consumer blew up")))
        ctrl.start_discovery()
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["bess-1"]})
        _push_state(ctrl, "panel-1", "ready")
        _push_description(ctrl, "bess-1", {"homie": "5.0", "root": "panel-1", "parent": "panel-1"})

        assert ctrl.is_tree_complete("panel-1") is True


# ── Subscription pacing and stuck-device healing (GH #97) ────────────────


def _make_paced_controller(root_device_id="panel-1", **kwargs):
    """A Controller on a mock transport that records calls and delivers nothing by itself."""
    client = MagicMock()
    ctrl = Controller(mqttc=client, root_device_id=root_device_id, **kwargs)
    return ctrl, client


def _topic(device_id, suffix):
    return f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}/{device_id}/{suffix}"


def _property_subscribed(client):
    """Device ids in the order their property filters were first subscribed."""
    seen = []
    for c in client.subscribe.call_args_list:
        if c[0][0].endswith("/+/+"):
            device_id = c[0][0].split("/")[2]
            if device_id not in seen:
                seen.append(device_id)
    return seen


def _final_subscriptions(client):
    """Topic filters left subscribed after replaying every subscribe and unsubscribe in order."""
    live = set()
    for name, args, _ in client.method_calls:
        if name == "subscribe":
            live.add(args[0])
        elif name == "unsubscribe":
            live.discard(args[0])
    return live


def _child_desc(child_id, props=2, parent="panel-1"):
    """A child $description declaring `props` retained properties on node n."""
    properties = {f"p{i}": {"name": f"P{i}", "datatype": "integer"} for i in range(props)}
    return {"homie": "5.0", "root": "panel-1", "parent": parent, "nodes": {"n": {"properties": properties}}}


def _announce_root(ctrl, children):
    ctrl.start_discovery()
    _push_description(ctrl, "panel-1", {"homie": "5.0", "children": children})
    _push_state(ctrl, "panel-1", "ready")


def _deliver_attributes(ctrl, child_id, props=2):
    _push_state(ctrl, child_id, "ready")
    _push_description(ctrl, child_id, _child_desc(child_id, props))


def _deliver_properties(ctrl, child_id, props=2):
    for i in range(props):
        ctrl._on_property_message(child_id, _topic(child_id, f"n/p{i}"), b"1")


def _deliver(ctrl, child_id, props=2):
    _deliver_attributes(ctrl, child_id, props)
    _deliver_properties(ctrl, child_id, props)


class TestSubscriptionPacing:
    def test_every_child_gets_state_and_description_at_once(self):
        ctrl, client = _make_paced_controller()
        children = [f"c-{i:02d}" for i in range(20)]
        _announce_root(ctrl, children)

        topics = {c[0][0] for c in client.subscribe.call_args_list}
        for child in children:
            assert _attribute_filters_for(child) <= topics
            assert child in ctrl.devices
        assert _property_subscribed(client) == ["panel-1"]  # the root is subscribed unpaced

    def test_default_batch_bounds_the_property_fan_out(self):
        ctrl, client = _make_paced_controller()
        children = [f"c-{i:02d}" for i in range(20)]
        _announce_root(ctrl, children)
        for child in children:
            _deliver_attributes(ctrl, child)

        assert _property_subscribed(client) == ["panel-1"] + children[:8]

    def test_slot_frees_when_every_retained_value_arrives(self):
        ctrl, client = _make_paced_controller(subscription_batch_size=2)
        _announce_root(ctrl, ["a", "b", "c"])
        for child in ("a", "b", "c"):
            _deliver_attributes(ctrl, child)
        assert _property_subscribed(client) == ["panel-1", "a", "b"]

        ctrl._on_property_message("a", _topic("a", "n/p0"), b"1")
        assert _property_subscribed(client) == ["panel-1", "a", "b"]  # one of two values
        ctrl._on_property_message("a", _topic("a", "n/p1"), b"1")
        assert _property_subscribed(client) == ["panel-1", "a", "b", "c"]

    def test_description_resubscribed_behind_the_property_filters(self):
        ctrl, client = _make_paced_controller(subscription_batch_size=1)
        _announce_root(ctrl, ["a"])
        _deliver_attributes(ctrl, "a")
        a_topics = [c[0][0] for c in client.subscribe.call_args_list if "/a/" in c[0][0]]
        assert a_topics == [
            _topic("a", "$state"),
            _topic("a", "$description"),
            _topic("a", "+/+"),
            _topic("a", "+/+/$target"),
            _topic("a", "$description"),
        ]

    def test_resent_description_frees_the_slot_without_a_second_callback(self):
        ctrl, client = _make_paced_controller(subscription_batch_size=1)
        received = []
        ctrl.set_on_description_received_callback(lambda d: received.append(d.device_id))
        _announce_root(ctrl, ["a", "b"])
        _deliver_attributes(ctrl, "a")
        _deliver_attributes(ctrl, "b")
        assert _property_subscribed(client) == ["panel-1", "a"]

        # a has no property values at all; the resent $description marks its burst as over.
        _push_description(ctrl, "a", _child_desc("a"))
        assert _property_subscribed(client) == ["panel-1", "a", "b"]
        assert received.count("a") == 1

    def test_changed_description_in_the_marker_slot_is_processed(self):
        ctrl, _ = _make_paced_controller(subscription_batch_size=1)
        received = []
        ctrl.set_on_description_received_callback(lambda d: received.append(d.device_id))
        _announce_root(ctrl, ["a"])
        _deliver_attributes(ctrl, "a")
        _push_description(ctrl, "a", _child_desc("a", props=3))
        assert received.count("a") == 2
        assert len(ctrl.devices["a"].description["nodes"]["n"]["properties"]) == 3

    def test_value_budget_bounds_the_batch(self):
        ctrl, client = _make_paced_controller(subscription_batch_size=8, subscription_batch_values=5)
        _announce_root(ctrl, ["a", "b", "c"])
        for child in ("a", "b", "c"):
            _deliver_attributes(ctrl, child, props=3)
        assert _property_subscribed(client) == ["panel-1", "a"]  # 3 + 3 owed would exceed 5

        _deliver_properties(ctrl, "a", props=3)
        assert _property_subscribed(client) == ["panel-1", "a", "b"]  # b fits; b and c would not
        _deliver_properties(ctrl, "b", props=3)
        assert _property_subscribed(client) == ["panel-1", "a", "b", "c"]

    def test_device_over_the_value_budget_starts_alone(self):
        ctrl, client = _make_paced_controller(subscription_batch_values=5)
        _announce_root(ctrl, ["big", "small"])
        _deliver_attributes(ctrl, "big", props=9)
        _deliver_attributes(ctrl, "small", props=1)
        assert _property_subscribed(client) == ["panel-1", "big"]
        _deliver_properties(ctrl, "big", props=9)
        assert _property_subscribed(client) == ["panel-1", "big", "small"]

    def test_no_value_budget_counts_devices_only(self):
        ctrl, client = _make_paced_controller(subscription_batch_size=3, subscription_batch_values=None)
        _announce_root(ctrl, ["a", "b", "c", "d"])
        for child in ("a", "b", "c", "d"):
            _deliver_attributes(ctrl, child, props=1000)
        assert _property_subscribed(client) == ["panel-1", "a", "b", "c"]

    def test_non_retained_properties_are_not_owed(self):
        ctrl, client = _make_paced_controller(subscription_batch_size=1)
        _announce_root(ctrl, ["a", "b"])
        desc = _child_desc("a", props=2)
        desc["nodes"]["n"]["properties"]["p1"]["retained"] = False
        _push_state(ctrl, "a", "ready")
        _push_description(ctrl, "a", desc)
        _deliver_attributes(ctrl, "b")
        ctrl._on_property_message("a", _topic("a", "n/p0"), b"1")
        assert _property_subscribed(client) == ["panel-1", "a", "b"]

    @pytest.mark.parametrize("values", [0, -1])
    def test_value_budget_below_one_rejected(self, values):
        with pytest.raises(ValueError):
            Controller(mqttc=MagicMock(), subscription_batch_values=values)

    def test_device_with_no_retained_properties_holds_no_slot(self):
        ctrl, client = _make_paced_controller(subscription_batch_size=1)
        _announce_root(ctrl, ["a", "b", "c"])
        _deliver_attributes(ctrl, "a", props=0)
        _deliver_attributes(ctrl, "b", props=0)
        _deliver_attributes(ctrl, "c")
        assert _property_subscribed(client) == ["panel-1", "a", "b", "c"]

    def test_silent_children_block_no_one(self):
        """Declared children that never publish take no batch slot, with no traffic and no heal call."""
        ctrl, client = _make_paced_controller(subscription_batch_size=2)
        _announce_root(ctrl, ["x", "y", "z", "a", "b"])  # x, y, z never publish
        _deliver_attributes(ctrl, "a")
        _deliver_attributes(ctrl, "b")
        assert _property_subscribed(client) == ["panel-1", "a", "b"]

    def test_wildcard_devices_without_description_block_no_one(self):
        client = MagicMock()
        ctrl = Controller(mqttc=client, subscription_batch_size=2)
        ctrl.start_discovery()
        for d in ("dead-0", "dead-1", "dead-2"):
            _push_state(ctrl, d, "lost")  # retained $state left behind, no $description
        _push_state(ctrl, "live", "ready")
        _push_description(ctrl, "live", {"homie": "5.0", "nodes": {"n": {"properties": {"p": {}}}}})
        assert _property_subscribed(client) == ["live"]

    def test_wildcard_discovery_is_paced(self):
        client = MagicMock()
        ctrl = Controller(mqttc=client, subscription_batch_size=2)
        ctrl.start_discovery()
        client.subscribe.reset_mock()
        desc = {"homie": "5.0", "nodes": {"n": {"properties": {"p": {}}}}}
        for d in ("d0", "d1", "d2", "d3"):
            _push_state(ctrl, d, "ready")
        assert [c[0][0] for c in client.subscribe.call_args_list] == [
            _topic(d, "$description") for d in ("d0", "d1", "d2", "d3")
        ]
        for d in ("d0", "d1", "d2", "d3"):
            _push_description(ctrl, d, desc)
        assert _property_subscribed(client) == ["d0", "d1"]

        ctrl._on_property_message("d0", _topic("d0", "n/p"), b"1")
        assert _property_subscribed(client) == ["d0", "d1", "d2"]

    def test_unbounded_subscribes_every_filter_at_once(self):
        ctrl, client = _make_paced_controller(subscription_batch_size=None)
        children = [f"c-{i:02d}" for i in range(20)]
        _announce_root(ctrl, children)
        topics = {c[0][0] for c in client.subscribe.call_args_list}
        for child in children:
            assert _filters_for(child) <= topics

    def test_zero_batch_means_unbounded(self):
        ctrl, client = _make_paced_controller(subscription_batch_size=0)
        _announce_root(ctrl, ["a", "b", "c"])
        assert _property_subscribed(client) == ["panel-1", "a", "b", "c"]

    def test_negative_batch_rejected(self):
        with pytest.raises(ValueError):
            Controller(mqttc=MagicMock(), subscription_batch_size=-1)

    def test_removed_pending_child_is_unsubscribed(self):
        ctrl, client = _make_paced_controller(subscription_batch_size=1)
        _announce_root(ctrl, ["a", "b"])
        _deliver_attributes(ctrl, "a")
        _deliver_attributes(ctrl, "b")  # waits for a's slot

        _push_state(ctrl, "panel-1", "init")
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["a"]})
        _push_state(ctrl, "panel-1", "ready")

        assert "b" not in ctrl.devices
        assert {c[0][0] for c in client.unsubscribe.call_args_list} == _filters_for("b")
        assert _property_subscribed(client) == ["panel-1", "a"]

    def test_removed_inflight_child_frees_its_slot(self):
        ctrl, client = _make_paced_controller(subscription_batch_size=1)
        _announce_root(ctrl, ["a", "b"])
        _deliver_attributes(ctrl, "a")
        _deliver_attributes(ctrl, "b")
        _push_state(ctrl, "panel-1", "init")
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["b"]})
        _push_state(ctrl, "panel-1", "ready")

        assert {c[0][0] for c in client.unsubscribe.call_args_list} == _filters_for("a")
        assert _property_subscribed(client) == ["panel-1", "a", "b"]

    def test_child_dropped_after_resync_is_unsubscribed_and_stays_gone(self):
        """A child subscribed before a reconnect keeps live filters in the transport, even
        while the re-walk has it pending; dropping it must unsubscribe them."""
        ctrl, client = _make_paced_controller(subscription_batch_size=1)
        discovered = []
        ctrl.set_on_device_discovered_callback(lambda d: discovered.append(d.device_id))
        _announce_root(ctrl, ["a", "b"])
        _deliver(ctrl, "a")
        _deliver(ctrl, "b")

        ctrl.resync()
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["a", "b"]})
        _push_state(ctrl, "panel-1", "ready")
        _deliver_attributes(ctrl, "a")
        _deliver_attributes(ctrl, "b")
        assert "b" in ctrl._pending_subscriptions
        client.reset_mock()
        discovered.clear()

        _push_state(ctrl, "panel-1", "init")
        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["a"]})
        _push_state(ctrl, "panel-1", "ready")
        assert {c[0][0] for c in client.unsubscribe.call_args_list} == _filters_for("b")

        _push_state(ctrl, "b", "ready")  # a late retained message on a filter the broker had queued
        assert "b" not in ctrl.devices
        assert discovered == []
        assert "b" not in ctrl._pending_subscriptions

    def test_untracked_state_in_tree_mode_is_unsubscribed_not_discovered(self):
        ctrl, client = _make_paced_controller()
        discovered = []
        ctrl.set_on_device_discovered_callback(lambda d: discovered.append(d.device_id))
        _announce_root(ctrl, [])
        client.reset_mock()
        discovered.clear()
        _push_state(ctrl, "ghost", "ready")
        assert "ghost" not in ctrl.devices
        assert discovered == []
        assert {c[0][0] for c in client.unsubscribe.call_args_list} == _filters_for("ghost")
        client.subscribe.assert_not_called()

    def test_resync_drops_pacing_state_and_rewalk_is_paced(self):
        ctrl, client = _make_paced_controller(subscription_batch_size=1)
        _announce_root(ctrl, ["a", "b", "c"])
        for child in ("a", "b", "c"):
            _deliver_attributes(ctrl, child)
        ctrl.resync()
        assert ctrl._pending_subscriptions == {}
        assert ctrl._watches == {}
        client.subscribe.reset_mock()

        _push_description(ctrl, "panel-1", {"homie": "5.0", "children": ["a", "b", "c"]})
        _push_state(ctrl, "panel-1", "ready")
        for child in ("a", "b", "c"):
            _deliver_attributes(ctrl, child)
        assert _property_subscribed(client) == ["a"]

    def test_stop_clears_pacing_state(self):
        ctrl, _ = _make_paced_controller(subscription_batch_size=1)
        _announce_root(ctrl, ["a", "b", "c"])
        for child in ("a", "b"):
            _deliver_attributes(ctrl, child)
        ctrl.stop()
        assert ctrl._awaiting_attributes == {}
        assert ctrl._watches == {}
        assert ctrl._pending_subscriptions == {}
        assert ctrl._description_markers == set()

    def test_wildcard_device_removed_while_pending_is_dropped(self):
        client = MagicMock()
        ctrl = Controller(mqttc=client, subscription_batch_size=1)
        ctrl.start_discovery()
        desc = {"homie": "5.0", "nodes": {"n": {"properties": {"p": {}}}}}
        for d in ("d0", "d1"):
            _push_state(ctrl, d, "ready")
            _push_description(ctrl, d, desc)
        _push_state(ctrl, "d1", "")  # retracted while pending
        ctrl._on_property_message("d0", _topic("d0", "n/p"), b"1")
        assert _property_subscribed(client) == ["d0"]


class TestStuckDeviceHealing:
    def test_withheld_child_is_resubscribed(self, caplog):
        ctrl, client = _make_paced_controller(subscription_batch_size=2)
        _announce_root(ctrl, ["a", "b", "c"])
        _deliver(ctrl, "a")
        _deliver(ctrl, "c")
        # "b" never receives its retained $state/$description.
        client.reset_mock()

        with caplog.at_level("WARNING", logger="homie"):
            assert ctrl.check_stuck_children(timeout=0) == ["b"]

        names = [c[0] for c in client.method_calls]
        assert names == ["unsubscribe"] * 2 + ["subscribe"] * 2  # unsubscribe first, then resubscribe
        assert {c[0][0] for c in client.unsubscribe.call_args_list} == _attribute_filters_for("b")
        assert {c[0][0] for c in client.subscribe.call_args_list} == _attribute_filters_for("b")
        assert "reason=stuckDeviceResubscribe,deviceID=b,missing=$state+$description,attempt=1/3" in caplog.text

    def test_resubscribe_reuses_the_device_callbacks(self):
        ctrl, client = _make_paced_controller(subscription_batch_size=1)
        _announce_root(ctrl, ["a"])
        client.reset_mock()
        ctrl.check_stuck_children(timeout=0)

        callbacks = {c[0][0]: c[1]["param"] for c in client.subscribe.call_args_list}
        callbacks[_topic("a", "$state")](_topic("a", "$state"), b"ready")
        callbacks[_topic("a", "$description")](_topic("a", "$description"), json.dumps(_child_desc("a")).encode())
        assert ctrl.devices["a"].state == "ready"
        assert ctrl.devices["a"].description is not None
        assert "a" not in ctrl._awaiting_attributes

    def test_only_the_missing_attribute_is_reported(self, caplog):
        ctrl, _ = _make_paced_controller(subscription_batch_size=1)
        _announce_root(ctrl, ["a"])
        _push_state(ctrl, "a", "ready")
        with caplog.at_level("WARNING", logger="homie"):
            ctrl.check_stuck_children(timeout=0)
        assert "deviceID=a,missing=$description,attempt=1/3" in caplog.text

    def test_not_stuck_before_the_timeout(self):
        ctrl, client = _make_paced_controller(subscription_batch_size=1)
        _announce_root(ctrl, ["a", "b"])
        _deliver_attributes(ctrl, "b")
        client.reset_mock()
        assert ctrl.check_stuck_children(timeout=60) == []
        client.unsubscribe.assert_not_called()

    def test_retry_bound(self, caplog):
        ctrl, client = _make_paced_controller(subscription_batch_size=1, max_resubscribe_attempts=2)
        _announce_root(ctrl, ["a"])

        with caplog.at_level("WARNING", logger="homie"):
            assert ctrl.check_stuck_children(timeout=0) == ["a"]
            assert ctrl.check_stuck_children(timeout=0) == ["a"]
            assert ctrl.check_stuck_children(timeout=0) == []
            assert ctrl.check_stuck_children(timeout=0) == []

        assert "reason=stuckDeviceGaveUp,deviceID=a,missing=$state+$description,attempts=2" in caplog.text
        assert caplog.text.count("reason=stuckDeviceGaveUp") == 1
        assert len(client.unsubscribe.call_args_list) == 4
        # Given up, not unsubscribed: a device that appears later is still discovered.
        _deliver(ctrl, "a")
        assert ctrl.devices["a"].description is not None

    def test_zero_attempts_only_logs(self, caplog):
        ctrl, client = _make_paced_controller(subscription_batch_size=1, max_resubscribe_attempts=0)
        _announce_root(ctrl, ["a"])
        with caplog.at_level("WARNING", logger="homie"):
            assert ctrl.check_stuck_children(timeout=0) == []
        client.unsubscribe.assert_not_called()
        assert "reason=stuckDeviceGaveUp,deviceID=a" in caplog.text

    def test_slot_holder_is_requeued_not_resubscribed_at_once(self, caplog):
        ctrl, client = _make_paced_controller(subscription_batch_size=1)
        _announce_root(ctrl, ["a", "b", "c"])
        for child in ("a", "b", "c"):
            _deliver_attributes(ctrl, child)
        assert _property_subscribed(client) == ["panel-1", "a"]
        client.reset_mock()

        with caplog.at_level("WARNING", logger="homie"):
            assert ctrl.check_stuck_children(timeout=0) == ["a"]
        assert "reason=stuckDeviceRequeue,deviceID=a,missing=properties,received=0/2,attempt=1/3" in caplog.text
        assert _property_subscribed(client) == ["b"]  # a's slot went to b; a waits its turn
        client.unsubscribe.assert_not_called()

        _deliver_properties(ctrl, "b")
        _deliver_properties(ctrl, "c")
        assert _property_subscribed(client) == ["b", "c", "a"]
        assert {c[0][0] for c in client.unsubscribe.call_args_list} == {
            _topic("a", "+/+"),
            _topic("a", "+/+/$target"),
        }

    def test_heal_pass_stays_within_the_batch(self):
        ctrl, client = _make_paced_controller(subscription_batch_size=2)
        children = [f"c{i}" for i in range(6)]
        _announce_root(ctrl, children)
        for child in children:
            _deliver_attributes(ctrl, child)
        for _ in range(3):
            client.reset_mock()
            ctrl.check_stuck_children(timeout=0)
            assert len(_property_subscribed(client)) <= 2
            assert len(ctrl._watches) <= 2

    def test_slot_holder_retry_bound(self, caplog):
        ctrl, client = _make_paced_controller(subscription_batch_size=1, max_resubscribe_attempts=1)
        _announce_root(ctrl, ["a"])
        _deliver_attributes(ctrl, "a")
        with caplog.at_level("WARNING", logger="homie"):
            assert ctrl.check_stuck_children(timeout=0) == ["a"]
            assert ctrl.check_stuck_children(timeout=0) == []
        assert "reason=stuckDeviceGaveUp,deviceID=a,missing=properties,received=0/2,attempts=1" in caplog.text
        assert ctrl._watches == {}
        assert ctrl._pending_subscriptions == {}

    def test_default_timeout_comes_from_the_constructor(self):
        ctrl, _ = _make_paced_controller(subscription_batch_size=1, stuck_device_timeout=0)
        _announce_root(ctrl, ["a"])
        assert ctrl.check_stuck_children() == ["a"]

    def test_no_default_timeout_needs_an_explicit_one(self):
        ctrl, _ = _make_paced_controller(subscription_batch_size=1, stuck_device_timeout=None)
        _announce_root(ctrl, ["a"])
        assert ctrl.check_stuck_children() == []
        assert ctrl.check_stuck_children(timeout=0) == ["a"]

    def test_message_handling_runs_the_check(self):
        ctrl, client = _make_paced_controller(subscription_batch_size=1, stuck_device_timeout=0)
        _announce_root(ctrl, ["a"])
        client.reset_mock()

        # Any inbound message (here a root property value) drives the heal.
        ctrl._on_property_message("panel-1", _topic("panel-1", "n/p"), b"1")
        assert {c[0][0] for c in client.unsubscribe.call_args_list} == _attribute_filters_for("a")

    def test_message_handling_check_is_rate_limited(self):
        ctrl, client = _make_paced_controller(subscription_batch_size=1)  # default timeout
        _announce_root(ctrl, ["a"])
        with patch.object(ctrl, "check_stuck_children") as check:
            for _ in range(100):
                ctrl._on_property_message("panel-1", _topic("panel-1", "n/p"), b"1")
        check.assert_not_called()  # under a second since construction

    def test_disabled_timeout_skips_the_message_check(self):
        ctrl, client = _make_paced_controller(subscription_batch_size=1, stuck_device_timeout=None)
        _announce_root(ctrl, ["a"])
        with patch.object(ctrl, "check_stuck_children") as check:
            ctrl._on_property_message("panel-1", _topic("panel-1", "n/p"), b"1")
        check.assert_not_called()

    def test_wildcard_resubscribe_leaves_the_state_wildcard_alone(self):
        client = MagicMock()
        ctrl = Controller(mqttc=client)
        ctrl.start_discovery()
        _push_state(ctrl, "d0", "ready")
        client.reset_mock()

        assert ctrl.check_stuck_children(timeout=0) == ["d0"]
        assert {c[0][0] for c in client.unsubscribe.call_args_list} == {_topic("d0", "$description")}

    def test_stuck_root_is_healed(self):
        ctrl, client = _make_paced_controller()
        ctrl.start_discovery()
        client.reset_mock()
        assert ctrl.check_stuck_children(timeout=0) == ["panel-1"]
        assert {c[0][0] for c in client.subscribe.call_args_list} == _filters_for("panel-1")

    def test_root_is_watched_after_resync(self):
        ctrl, _ = _make_paced_controller()
        ctrl.start_discovery()
        ctrl.resync()
        assert ctrl.check_stuck_children(timeout=0) == ["panel-1"]

    def test_heal_racing_a_drop_leaves_no_subscription(self):
        """A heal on a consumer thread interleaved with the paho thread dropping the same child."""
        ctrl, client = _make_paced_controller(subscription_batch_size=1)
        _announce_root(ctrl, ["x"])

        def drop_x_once(topic):
            # Runs between the heal's unsubscribe and its resubscribe, as the paho thread could.
            if topic == _topic("x", "$description") and "x" in ctrl.devices:
                _push_state(ctrl, "panel-1", "init")
                _push_description(ctrl, "panel-1", {"homie": "5.0", "children": []})
                _push_state(ctrl, "panel-1", "ready")

        client.unsubscribe.side_effect = drop_x_once
        assert ctrl.check_stuck_children(timeout=0) == ["x"]

        assert "x" not in ctrl.devices
        assert not {t for t in _final_subscriptions(client) if "/x/" in t}
