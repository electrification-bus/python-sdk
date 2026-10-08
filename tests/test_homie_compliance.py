"""Homie 5 convention compliance of the device role (issue #95).

Line numbers in docstrings refer to homieiot/convention@7edc221 convention.md.
"""

import datetime
import enum
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


# ── C. Property values ───────────────────────────────────────────────────


def _wired(datatype, value=None, **kwargs):
    """A property on a node on a READY root with a mock client."""
    root, client = _make_root()
    node = root.new_node("n")
    root.add_node(node)
    prop = Property(id="p", datatype=datatype, **kwargs)
    node.add_property(prop)
    client.publish.reset_mock()
    return prop, client, root


def _value_publishes(client):
    return [c for c in client.publish.call_args_list if c.args[0] == f"{BASE}/root/n/p"]


def _published(datatype, value, **kwargs):
    """The payload set_value(value) puts on the wire, or None if it publishes nothing."""
    prop, client, _ = _wired(datatype, **kwargs)
    ok = prop.set_value(value)
    calls = _value_publishes(client)
    assert ok == bool(calls)
    return calls[-1].args[1] if calls else None


class TestOutboundNumbers:
    """:85-101: integer and float payload grammar."""

    def test_non_finite_floats_are_refused(self):
        for value in (float("nan"), float("inf"), float("-inf"), "1e999"):
            assert _published(PropertyDatatype.FLOAT, value) is None

    def test_float_exponent_has_no_plus(self):
        """:97: only digits, `-`, `e`/`E` and `.`."""
        assert _published(PropertyDatatype.FLOAT, 1e20) == "1e20"
        assert _published(PropertyDatatype.FLOAT, 1.5e-7) == "1.5e-07"
        assert _published(PropertyDatatype.FLOAT, "1e+5") is None

    def test_floats_and_ints_on_a_float_property(self):
        assert _published(PropertyDatatype.FLOAT, 21.5) == "21.5"
        assert _published(PropertyDatatype.FLOAT, 5) == "5"
        assert _published(PropertyDatatype.FLOAT, "-3.25") == "-3.25"

    def test_booleans_are_refused_on_numeric_properties(self):
        assert _published(PropertyDatatype.FLOAT, True) is None
        assert _published(PropertyDatatype.INTEGER, False) is None

    def test_integers_must_be_whole_and_64_bit(self):
        """:87-90."""
        assert _published(PropertyDatatype.INTEGER, 5.0) == "5"
        assert _published(PropertyDatatype.INTEGER, 5.5) is None
        assert _published(PropertyDatatype.INTEGER, 2**63 - 1) == str(2**63 - 1)
        assert _published(PropertyDatatype.INTEGER, 2**63) is None
        assert _published(PropertyDatatype.INTEGER, -(2**63) - 1) is None
        assert _published(PropertyDatatype.INTEGER, "-") is None
        assert _published(PropertyDatatype.INTEGER, "12") == "12"

    def test_a_refused_value_leaves_the_last_good_one_on_the_broker(self):
        prop, client, _ = _wired(PropertyDatatype.FLOAT)
        prop.set_value(1.0)
        client.publish.reset_mock()
        assert prop.set_value(float("nan")) is False
        assert client.publish.call_args_list == []
        assert prop.get_last_published_value() == "1.0"


class TestOutboundEnumAndEmptyStrings:
    def test_enum_values_must_be_in_the_format(self):
        """:111."""

        class Mode(str, enum.Enum):
            AUTO = "auto"

        assert _published(PropertyDatatype.ENUM, "auto", format="auto,off") == "auto"
        assert _published(PropertyDatatype.ENUM, Mode.AUTO, format="auto,off") == "auto"
        assert _published(PropertyDatatype.ENUM, "Auto", format="auto,off") is None

    def test_empty_string_is_0x00_only_for_string(self):
        """:65-67: the empty string is valid only for string properties."""
        assert _published(PropertyDatatype.STRING, "") == "\x00"
        assert _published(PropertyDatatype.ENUM, "", format="a,b") is None
        assert _published(PropertyDatatype.INTEGER, "") is None
        assert _published(PropertyDatatype.JSON, "") is None
        assert _published(PropertyDatatype.DATETIME, "") is None


class TestOutboundEncoders:
    def test_datetime_is_iso_8601(self):
        """:132."""
        stamp = datetime.datetime(2026, 10, 8, 12, 0, tzinfo=datetime.timezone.utc)
        assert _published(PropertyDatatype.DATETIME, stamp) == "2026-10-08T12:00:00+00:00"
        assert _published(PropertyDatatype.DATETIME, datetime.date(2026, 10, 8)) == "2026-10-08"

    def test_duration_is_ptxhxmxs(self):
        """:137-144."""
        timedelta = datetime.timedelta
        assert _published(PropertyDatatype.DURATION, timedelta(hours=12, minutes=5, seconds=46)) == "PT12H5M46S"
        assert _published(PropertyDatatype.DURATION, timedelta(minutes=5)) == "PT5M"
        assert _published(PropertyDatatype.DURATION, timedelta(0)) == "PT0S"
        assert _published(PropertyDatatype.DURATION, timedelta(days=2)) == "PT48H"
        assert _published(PropertyDatatype.DURATION, timedelta(seconds=1.5)) == "PT1.5S"
        assert _published(PropertyDatatype.DURATION, timedelta(seconds=-1)) is None
        assert _published(PropertyDatatype.DURATION, "PT5M") == "PT5M"
        assert _published(PropertyDatatype.DURATION, "PT") is None
        assert _published(PropertyDatatype.DURATION, "5 minutes") is None

    def test_color_tuple_uses_the_preferred_format(self):
        """:118-123 and :393."""
        assert _published(PropertyDatatype.COLOR, (255, 0, 0), format="rgb,hsv") == "rgb,255,0,0"
        assert _published(PropertyDatatype.COLOR, (300, 50, 75.5), format="hsv") == "hsv,300,50,75.5"
        assert _published(PropertyDatatype.COLOR, (0.25, 0.34), format="xyz") == "xyz,0.25,0.34"

    def test_color_payloads_are_checked(self):
        assert _published(PropertyDatatype.COLOR, "hsv,300,50,75", format="rgb,hsv") == "hsv,300,50,75"
        assert _published(PropertyDatatype.COLOR, "rgb,256,0,0", format="rgb") is None
        assert _published(PropertyDatatype.COLOR, "rgb, 1,2,3", format="rgb") is None
        assert _published(PropertyDatatype.COLOR, "xyz,0.2,0.3", format="rgb") is None
        assert _published(PropertyDatatype.COLOR, (1, 2), format="rgb") is None

    def test_json_must_be_an_array_or_object_without_nan(self):
        """:150."""
        assert _published(PropertyDatatype.JSON, {"a": 1}) == '{"a": 1}'
        assert _published(PropertyDatatype.JSON, [1, 2]) == "[1, 2]"
        assert _published(PropertyDatatype.JSON, '{"a":1}') == '{"a":1}'
        assert _published(PropertyDatatype.JSON, "hello") is None
        assert _published(PropertyDatatype.JSON, 5) is None
        assert _published(PropertyDatatype.JSON, '"text"') is None
        assert _published(PropertyDatatype.JSON, {"a": float("nan")}) is None
        assert _published(PropertyDatatype.JSON, '{"a": NaN}') is None


class TestEventProperties:
    """:50 and :695: non-retained properties publish at QoS 0 and are never replayed."""

    def test_an_event_publishes_non_retained_at_qos_0(self):
        prop, client, _ = _wired(PropertyDatatype.STRING, retained=False)
        prop.set_value("pressed")
        (published,) = _value_publishes(client)
        assert published.kwargs == {"retain": False, "qos": 0}

    def test_a_reconnect_does_not_replay_the_last_event(self):
        prop, client, root = _wired(PropertyDatatype.STRING, retained=False)
        prop.set_value("pressed")
        client.publish.reset_mock()
        root.on_connect()
        assert _value_publishes(client) == []

    def test_adding_a_valued_event_property_publishes_no_event(self):
        root, client = _make_root()
        node = root.new_node("n")
        root.add_node(node)
        client.publish.reset_mock()
        node.add_property(Property(id="p", value="x", datatype=PropertyDatatype.STRING, retained=False))
        assert _value_publishes(client) == []

    def test_clearing_an_event_publishes_nothing(self):
        prop, client, root = _wired(PropertyDatatype.STRING, retained=False)
        prop.set_value("pressed")
        client.publish.reset_mock()
        prop.set_value(None)
        root.delete_node("n")
        assert _value_publishes(client) == []


class TestAlerts:
    """:512-527 and :289."""

    def test_publish_alert_uses_the_alert_id_level(self):
        root, client = _make_root()
        client.publish.reset_mock()
        assert root.publish_alert("battery", "Battery is low, at 8%") is True
        (published,) = client.publish.call_args_list
        assert published.args == (f"{BASE}/root/$alert/battery", "Battery is low, at 8%")
        assert published.kwargs["retain"] is True

    def test_clear_alert_deletes_the_topic(self):
        root, client = _make_root()
        root.publish_alert("battery", "low")
        client.publish.reset_mock()
        assert root.clear_alert("battery") is True
        assert _publishes(client) == [(f"{BASE}/root/$alert/battery", "")]
        assert root.alerts() == {}

    def test_delete_clears_raised_alerts(self):
        root, client = _make_root()
        child = Device(id="child", parent=root)
        child.publish_alert("childlost", "gone")
        client.publish.reset_mock()
        child.delete()
        assert (f"{BASE}/child/$alert/childlost", "") in _publishes(client)

    def test_a_reconnect_republishes_raised_alerts(self):
        root, client = _make_root()
        root.publish_alert("battery", "low")
        client.publish.reset_mock()
        root.on_connect()
        assert (f"{BASE}/root/$alert/battery", "low") in _publishes(client)

    def test_bare_alert_and_bad_input_publish_nothing(self):
        root, client = _make_root()
        client.publish.reset_mock()
        root.publish("$alert", "no id")
        assert root.publish_alert("a/b", "x") is False
        assert root.publish_alert("", "x") is False
        assert root.publish_alert("battery", "") is False
        assert client.publish.call_args_list == []
