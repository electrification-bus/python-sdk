"""Homie 5 convention compliance of the device role (issue #95).

Line numbers in docstrings refer to homieiot/convention@7edc221 convention.md.
"""

import json
import logging
import socket
from unittest.mock import MagicMock, patch

from ebus_sdk.homie import (
    EBUS_HOMIE_DOMAIN,
    EBUS_HOMIE_VERSION_MAJOR,
    Device,
    DeviceState,
    Node,
    Property,
    PropertyDatatype,
)

BASE = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}"


def _mock_mqtt_client():
    mock = MagicMock()
    mock.is_running = True
    mock.is_connected.return_value = True
    mock.publish.return_value = MagicMock(rc=0)
    mock.subscribe.return_value = (0, 1)
    return mock


def _make_root(device_id="root", **kwargs):
    with patch("ebus_sdk.homie.MqttClient.from_config") as from_config:
        client = _mock_mqtt_client()
        from_config.return_value = client
        device = Device(id=device_id, mqtt_cfg={"host": "localhost", "port": 1883}, **kwargs)
        return device, client


def _publishes(client):
    """(topic, payload) for every plain publish, in order."""
    return [(c.args[0], c.args[1]) for c in client.publish.call_args_list]


# ── A. Lifecycle and teardown ────────────────────────────────────────────


class TestStopAnnouncesEveryDevice:
    """:280-281: every device sends `disconnected` before the clean disconnect."""

    def test_owned_stop_publishes_disconnected_descendants_first_root_last(self):
        root, client = _make_root()
        child = Device(id="child", parent=root)
        Device(id="grandchild", parent=child)
        client.publish.reset_mock()

        root.stop()

        states = [(t, p) for t, p in _publishes(client) if t.endswith("/$state")]
        assert states == [
            (f"{BASE}/grandchild/$state", "disconnected"),
            (f"{BASE}/child/$state", "disconnected"),
        ]
        # The root goes last, flushed, before the clean stop.
        client.publish_and_flush.assert_called_once()
        assert client.publish_and_flush.call_args.args[:2] == (f"{BASE}/root/$state", "disconnected")
        client.stop.assert_called_once()
        assert root.state() == child.state() == DeviceState.DISCONNECTED

    def test_stop_from_a_child_announces_the_whole_tree(self):
        root, client = _make_root()
        child = Device(id="child", parent=root)
        client.publish.reset_mock()

        child.stop()

        assert (f"{BASE}/child/$state", "disconnected") in _publishes(client)
        assert client.publish_and_flush.call_args.args[0] == f"{BASE}/root/$state"

    def test_injected_stop_publishes_disconnected_for_every_device(self):
        client = _mock_mqtt_client()
        root = Device(id="root", mqttc=client)
        Device(id="child", parent=root)
        client.reset_mock()

        root.stop()

        assert _publishes(client) == [
            (f"{BASE}/child/$state", "disconnected"),
            (f"{BASE}/root/$state", "disconnected"),
        ]
        client.publish_and_flush.assert_not_called()
        client.stop.assert_not_called()


class TestSilentStopIsABadDisconnect:
    """:281 and :284: a teardown that announced nothing must not disconnect cleanly."""

    def _recorder(self, client):
        order = MagicMock()
        order.attach_mock(client.mqttc.loop_stop, "loop_stop")
        order.attach_mock(client.mqttc.socket.return_value.shutdown, "shutdown")
        order.attach_mock(client.stop, "stop")
        return order

    def test_announce_false_drops_the_socket_before_the_client_stop(self):
        root, client = _make_root()
        order = self._recorder(client)

        root.stop(announce=False)

        names = [c[0] for c in order.mock_calls]
        assert names.index("loop_stop") < names.index("shutdown") < names.index("stop")
        client.mqttc.socket.return_value.shutdown.assert_called_once_with(socket.SHUT_RDWR)
        client.publish_and_flush.assert_not_called()

    def test_declare_lost_then_silent_stop_publishes_lost_and_lets_the_will_fire(self):
        root, client = _make_root()
        Device(id="child", parent=root)
        client.publish.reset_mock()

        root.declare_lost()
        root.stop(announce=False)

        assert client.publish_and_flush.call_args.args[:2] == (f"{BASE}/root/$state", "lost")
        assert all(p != "disconnected" for _, p in _publishes(client))
        client.mqttc.socket.return_value.shutdown.assert_called_once()

    def test_default_stop_disconnects_cleanly(self):
        root, client = _make_root()
        root.stop()
        client.mqttc.socket.return_value.shutdown.assert_not_called()
        client.stop.assert_called_once()


class TestWill:
    """:45-47 and :689-691: the will carries the tree's QoS and is retained."""

    def test_will_carries_qos_and_retain(self):
        root, _ = _make_root(qos=1)
        will = root.will()
        assert will["qos"] == 1
        assert will["retain"] is True

    def test_owned_client_is_built_with_the_full_will(self):
        with patch("ebus_sdk.homie.MqttClient.from_config") as from_config:
            from_config.return_value = _mock_mqtt_client()
            root = Device(id="root", mqtt_cfg={"host": "localhost"})
            assert from_config.call_args.kwargs["lwt"]["qos"] == root.qos


class TestChildRemovalOrder:
    """:635-641: update the parent first, then clear the child starting with `$state`."""

    def test_parent_reannounces_before_the_child_is_cleared(self):
        root, client = _make_root()
        child = Device(id="child", parent=root)
        child.add_node(child.new_node("n"))
        client.publish.reset_mock()

        child.delete()

        events = _publishes(client)
        parent_init = events.index((f"{BASE}/root/$state", "init"))
        parent_desc = next(i for i, (t, _) in enumerate(events) if t == f"{BASE}/root/$description")
        parent_ready = events.index((f"{BASE}/root/$state", "ready"))
        child_state = events.index((f"{BASE}/child/$state", ""))
        child_desc = events.index((f"{BASE}/child/$description", ""))
        assert parent_init < parent_desc < parent_ready < child_state < child_desc
        assert "child" not in json.loads(events[parent_desc][1])["children"]

    def test_batched_removal_defers_the_parent_update(self):
        root, client = _make_root()
        children = [Device(id=f"c{i}", parent=root) for i in range(3)]
        client.publish.reset_mock()

        with root.state_transition():
            for child in children:
                child.delete()

        root_states = [p for t, p in _publishes(client) if t == f"{BASE}/root/$state"]
        assert root_states == ["init", "ready"]


class TestDeletedDeviceStaysDeleted:
    """:272 and :288: a device without `$state` does not exist; nothing may re-create it."""

    def test_delete_then_stop_publishes_no_state(self):
        root, client = _make_root()
        Device(id="child", parent=root)
        root.delete()
        client.publish.reset_mock()

        root.stop()

        assert not [t for t, _ in _publishes(client) if t.endswith("/$state")]
        client.publish_and_flush.assert_not_called()
        client.stop.assert_called_once()

    def test_delete_then_silent_stop_disconnects_cleanly(self):
        """A bad disconnect would fire the will and re-create the device as `lost`."""
        root, client = _make_root()
        root.delete()

        root.stop(announce=False)

        client.mqttc.socket.return_value.shutdown.assert_not_called()
        client.stop.assert_called_once()

    def test_a_reconnect_after_delete_republishes_nothing(self):
        root, client = _make_root()
        root.add_node(root.new_node("n"))
        root.delete()
        client.publish.reset_mock()

        root.on_connect()

        assert client.publish.call_args_list == []


# ── B. $description ──────────────────────────────────────────────────────


def _descriptions(client, device_id):
    return [json.loads(p) for t, p in _publishes(client) if t == f"{BASE}/{device_id}/$description" and p]


def _states(client, device_id):
    return [p for t, p in _publishes(client) if t == f"{BASE}/{device_id}/$state"]


def _invalid_input_warnings(caplog):
    return [r for r in caplog.records if "next minor release" in r.getMessage()]


class TestDescriptionVersion:
    """:215: a new version whenever the document changes, and only then."""

    def test_version_is_a_52_bit_integer(self):
        root, _ = _make_root()
        version = root.description()["version"]
        assert isinstance(version, int)
        assert 0 <= version < 2**52

    def test_equal_content_has_equal_version(self):
        a = Device(id="same")
        b = Device(id="same")
        assert a.description()["version"] == b.description()["version"]

    def test_a_content_change_changes_the_version(self):
        root, _ = _make_root()
        before = root.description()["version"]
        root.add_node(root.new_node("n"))
        assert root.description()["version"] != before

    def test_reconnect_republishes_the_same_document_without_init(self):
        root, client = _make_root()
        root.add_node(root.new_node("n"))
        published = _descriptions(client, "root")[-1]
        client.publish.reset_mock()

        root.on_connect()

        assert _descriptions(client, "root") == [published]
        assert _states(client, "root") == ["ready"]


class TestDescriptionChangesOnlyInPermittedStates:
    """:207: `$description` may change only in `init`, `disconnected` or `lost`."""

    def test_a_change_while_sleeping_is_wrapped_in_init_and_returns_to_sleeping(self):
        root, client = _make_root()
        root.set_state(DeviceState.SLEEPING)
        client.publish.reset_mock()

        root.add_node(root.new_node("n"))

        events = [(t.rsplit("/", 1)[1], p) for t, p in _publishes(client)]
        assert events[0] == ("$state", "init")
        assert [e for e in events if e[0] == "$description"]
        assert events[-1] == ("$state", "sleeping")
        assert root.state() == DeviceState.SLEEPING

    def test_a_child_added_to_a_sleeping_parent_wraps_the_parent(self):
        root, client = _make_root()
        root.set_state(DeviceState.SLEEPING)
        client.publish.reset_mock()

        Device(id="child", parent=root)

        assert _states(client, "root") == ["init", "sleeping"]

    def test_publish_description_while_ready_wraps_only_a_change(self):
        root, client = _make_root()
        client.publish.reset_mock()
        root.publish_description()
        assert client.publish.call_args_list == []


class TestDescriptionFields:
    def test_unset_device_and_node_type_are_omitted(self):
        """:218 and :305: `type` is not nullable."""
        device = Device(id="d")
        device.add_node(Node(id="n"))
        description = device.description()
        assert "type" not in description
        assert "type" not in description["nodes"]["n"]

    def test_set_types_are_kept(self):
        device = Device(id="d", type="t")
        device.add_node(Node(id="n", type="nt"))
        assert device.description()["type"] == "t"
        assert device.description()["nodes"]["n"]["type"] == "nt"

    def test_missing_datatype_warns_and_is_omitted(self, caplog):
        """:340: `datatype` is required."""
        with caplog.at_level(logging.WARNING, logger="homie"):
            prop = Property(id="p")
        assert "datatype" not in prop.description()
        assert len(_invalid_input_warnings(caplog)) == 1

    def test_null_retained_warns_and_is_published_as_false(self, caplog):
        """:343: `retained` is a non-null boolean; None always published non-retained."""
        with caplog.at_level(logging.WARNING, logger="homie"):
            prop = Property(id="p", datatype=PropertyDatatype.FLOAT, retained=None)
        assert prop.description()["retained"] is False
        assert prop.retained() is False
        assert len(_invalid_input_warnings(caplog)) == 1

    def test_json_format_dict_is_published_as_a_string(self):
        """:395: the JSONschema is a string, not a nested object."""
        schema = {"type": "object"}
        prop = Property(id="j", datatype=PropertyDatatype.JSON, format=schema)
        assert prop.description()["format"] == json.dumps(schema)

    def test_enum_and_color_without_format_warn(self, caplog):
        """:392-393: `enum` and `color` require a format."""
        with caplog.at_level(logging.WARNING, logger="homie"):
            Property(id="e", datatype=PropertyDatatype.ENUM)
            Property(id="c", datatype=PropertyDatatype.COLOR)
        assert len(_invalid_input_warnings(caplog)) == 2

    def test_enum_format_with_empty_or_duplicate_values_warns(self, caplog):
        """:392: at least one value, none empty, no duplicates."""
        with caplog.at_level(logging.WARNING, logger="homie"):
            Property(id="e1", datatype=PropertyDatatype.ENUM, format="a,,b")
            Property(id="e2", datatype=PropertyDatatype.ENUM, format="a,b,a")
            Property(id="ok", datatype=PropertyDatatype.ENUM, format="a,b")
        reasons = [r.getMessage() for r in _invalid_input_warnings(caplog)]
        assert len(reasons) == 2
        assert "propertyID=e1" in reasons[0] and "propertyID=e2" in reasons[1]

    def test_the_warning_is_once_per_object(self, caplog):
        with caplog.at_level(logging.WARNING, logger="homie"):
            prop = Property(id="e", datatype=PropertyDatatype.ENUM)
            prop.set_format(None)
            prop.set_format("")
        assert len(_invalid_input_warnings(caplog)) == 1

    def test_set_format_checks_the_new_format(self, caplog):
        prop = Property(id="e", datatype=PropertyDatatype.ENUM, format="a,b")
        with caplog.at_level(logging.WARNING, logger="homie"):
            prop.set_format("a,a")
        assert len(_invalid_input_warnings(caplog)) == 1

    def test_extras_cannot_add_root_or_parent_to_a_root(self, caplog):
        """:220: `root` MUST be omitted on the root device."""
        with caplog.at_level(logging.WARNING, logger="homie"):
            device = Device(id="r", description_extras={"root": "x", "parent": "y", "imported-from": "z"})
        description = device.description()
        assert "root" not in description and "parent" not in description
        assert description["imported-from"] == "z"
        assert len(_invalid_input_warnings(caplog)) == 2

    def test_extras_cannot_override_core_fields(self):
        device = Device(id="r", description_extras={"version": 1, "type": "x", "homie": "4.0"})
        description = device.description()
        assert description["homie"] == "5.0"
        assert description["version"] != 1
        assert "type" not in description


class TestNewValuesPrecedeReady:
    """:279: `ready` follows every value it vouches for."""

    def test_add_property_on_a_ready_device(self):
        root, client = _make_root()
        node = root.new_node("n")
        root.add_node(node)
        client.publish.reset_mock()

        node.add_property(Property(id="p", value=5, datatype=PropertyDatatype.INTEGER))

        events = [(t.rsplit("/", 1)[1], p) for t, p in _publishes(client)]
        kinds = [e[0] if e[0].startswith("$") else "value" for e in events]
        assert kinds == ["$state", "value", "$description", "$state"]
        assert events[0][1] == "init" and events[-1][1] == "ready"

    def test_add_node_on_a_ready_device(self):
        root, client = _make_root()
        node = Node(id="n")
        node._properties["p"] = Property(id="p", value=1.5, datatype=PropertyDatatype.FLOAT, node=node)
        client.publish.reset_mock()

        root.add_node(node)

        events = [(t.rsplit("/", 1)[1], p) for t, p in _publishes(client)]
        kinds = [e[0] if e[0].startswith("$") else "value" for e in events]
        assert kinds == ["$state", "value", "$description", "$state"]

    def test_inside_a_transition_nothing_extra_is_published(self):
        root, client = _make_root()
        client.publish.reset_mock()
        with root.state_transition():
            root.add_node(root.new_node("a"))
            root.add_node(root.new_node("b"))
        assert _states(client, "root") == ["init", "ready"]
        assert len(_descriptions(client, "root")) == 1


class TestDescriptionCacheInvalidation:
    def test_clearing_the_description_topic_lets_the_next_publish_through(self):
        root, client = _make_root()
        root.clear_retained_topic(f"{BASE}/root/$description")
        client.publish.reset_mock()

        root.publish_description()

        assert len(_descriptions(client, "root")) == 1
