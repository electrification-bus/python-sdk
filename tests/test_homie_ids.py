"""Homie 5 id, topic and domain validation (issue #92): warn once, still publish.

Line numbers in docstrings refer to homieiot/convention@7edc221 convention.md.
"""

import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from ebus_sdk.homie import (
    EBUS_HOMIE_DOMAIN,
    EBUS_HOMIE_VERSION_MAJOR,
    Controller,
    Device,
    Node,
    Property,
    PropertyDatatype,
    sanitize_homie_id,
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
        with device.state_transition():
            pass
        return device, client


def _topics(client):
    return [c.args[0] for c in client.publish.call_args_list]


def _warnings(caplog, reason):
    return [r for r in caplog.records if f"reason={reason}," in r.getMessage()]


def _int_property(property_id, value=1):
    return Property(id=property_id, value=value, datatype=PropertyDatatype.INTEGER)


class TestIdFormat:
    """:39: lowercase a-z, 0-9 and `-`; :41: `$` is reserved for attributes."""

    @pytest.mark.parametrize("value", ["a", "panel-1", "000010024176", "-x-"])
    def test_valid_ids_do_not_warn(self, caplog, value):
        with caplog.at_level(logging.WARNING, logger="homie"):
            Device(id=value)
            Node(id=value)
            _int_property(value)
        assert [r for r in caplog.records if "IdInvalid" in r.getMessage()] == []

    @pytest.mark.parametrize(
        "value, problem",
        [
            ("Panel", "invalidCharacter"),
            ("a_b", "invalidCharacter"),
            ("a/b", "invalidCharacter"),
            ("a+", "invalidCharacter"),
            ("$state", "reservedDollar"),
            ("", "empty"),
            (None, "empty"),
        ],
    )
    def test_property_id_warns(self, caplog, value, problem):
        with caplog.at_level(logging.WARNING, logger="homie"):
            Property(id=value, datatype=PropertyDatatype.INTEGER)
        records = _warnings(caplog, "propertyIdInvalid")
        assert len(records) == 1
        assert f"problem={problem}" in records[0].getMessage()

    def test_node_id_warns_for_both_constructor_forms(self, caplog):
        with caplog.at_level(logging.WARNING, logger="homie"):
            Node(id="$target")
            Node(from_dict={"id": "Meter_1"})
        assert len(_warnings(caplog, "nodeIdInvalid")) == 2

    def test_device_id_warns(self, caplog):
        with caplog.at_level(logging.WARNING, logger="homie"):
            Device(id="Panel_1")
        assert len(_warnings(caplog, "deviceIdInvalid")) == 1

    def test_invalid_ids_still_publish(self, caplog):
        with caplog.at_level(logging.WARNING, logger="homie"):
            root, client = _make_root("Root")
            node = Node(id="Node_1")
            root.add_node(node)
            node.add_property(_int_property("Prop", 7))
        assert f"{BASE}/Root/Node_1/Prop" in _topics(client)
        description = json.loads(
            [c.args[1] for c in client.publish.call_args_list if c.args[0] == f"{BASE}/Root/$description"][-1]
        )
        assert "Prop" in description["nodes"]["Node_1"]["properties"]

    def test_an_invalid_property_id_warns_once_across_republishes(self, caplog):
        root, client = _make_root()
        node = root.new_node("n")
        root.add_node(node)
        with caplog.at_level(logging.WARNING, logger="homie"):
            prop = node.add_property(_int_property("Bad"))
            prop.set_value(2)
            root.refresh_tree(force=True)
        assert len(_warnings(caplog, "propertyIdInvalid")) == 1


class TestDuplicateNodeAndPropertyIds:
    """:294 node ids are unique per device, :326 property ids per node."""

    def test_a_second_node_with_the_same_id_warns_and_still_replaces(self, caplog):
        root, client = _make_root()
        first = root.add_node(Node(id="n", name="first"))
        second = Node(id="n", name="second")
        with caplog.at_level(logging.WARNING, logger="homie"):
            root.add_node(second)
        assert len(_warnings(caplog, "deviceDuplicateNodeId")) == 1
        assert root.get_node("n") is second is not first
        assert root.description()["nodes"]["n"]["name"] == "second"

    def test_a_duplicate_node_id_warns_once(self, caplog):
        root, _ = _make_root()
        root.add_node(Node(id="n", name="first"))
        with caplog.at_level(logging.WARNING, logger="homie"):
            root.add_node(Node(id="n", name="second"))
            root.add_node(Node(id="n", name="third"))
        assert len(_warnings(caplog, "deviceDuplicateNodeId")) == 1

    def test_re_adding_a_node_or_an_identical_node_does_not_warn(self, caplog):
        root, _ = _make_root()
        node = root.add_node(Node(id="n", name="same"))
        with caplog.at_level(logging.WARNING, logger="homie"):
            root.add_node(node)
            root.add_node(Node(id="n", name="same"))
        assert _warnings(caplog, "deviceDuplicateNodeId") == []

    def test_a_second_property_with_the_same_id_warns_and_still_publishes(self, caplog):
        root, client = _make_root()
        node = root.add_node(root.new_node("n"))
        node.add_property(_int_property("p", 1))
        client.publish.reset_mock()
        replacement = Property(id="p", value="x", datatype=PropertyDatatype.STRING)
        with caplog.at_level(logging.WARNING, logger="homie"):
            node.add_property(replacement)
            node.add_property(Property(id="p", value=2.5, datatype=PropertyDatatype.FLOAT))
        assert len(_warnings(caplog, "nodeDuplicatePropertyId")) == 1
        assert (f"{BASE}/root/n/p", "x") in [(c.args[0], c.args[1]) for c in client.publish.call_args_list]
        assert node.get_property("p").datatype() == PropertyDatatype.FLOAT

    def test_an_identical_property_re_declaration_does_not_warn(self, caplog):
        root, _ = _make_root()
        node = root.add_node(root.new_node("n"))
        node.add_property(_int_property("p", 1))
        with caplog.at_level(logging.WARNING, logger="homie"):
            node.add_property(_int_property("p", 2))
        assert _warnings(caplog, "nodeDuplicatePropertyId") == []


class TestDuplicateDeviceIdsInTree:
    """:194: device ids are unique; the ancestor and sibling cases already raise."""

    def test_the_same_id_on_two_branches_warns_once_and_still_publishes(self, caplog):
        root, client = _make_root()
        a = Device(id="a", parent=root)
        b = Device(id="b", parent=root)
        Device(id="x", parent=a)
        with caplog.at_level(logging.WARNING, logger="homie"):
            dup = Device(id="x", parent=b)
            Device(id="x", parent=Device(id="c", parent=root))
        records = _warnings(caplog, "deviceDuplicateIdInTree")
        assert len(records) == 1
        assert "rootID=root" in records[0].getMessage()
        assert dup in b.children()
        client.publish.reset_mock()
        with dup.state_transition():
            pass
        assert f"{BASE}/x/$state" in _topics(client)

    def test_distinct_ids_do_not_warn(self, caplog):
        root, _ = _make_root()
        with caplog.at_level(logging.WARNING, logger="homie"):
            a = Device(id="a", parent=root)
            Device(id="a-1", parent=a)
            Device(id="b", parent=root)
        assert _warnings(caplog, "deviceDuplicateIdInTree") == []

    def test_ancestor_and_sibling_collisions_still_raise(self):
        root, _ = _make_root()
        Device(id="a", parent=root)
        with pytest.raises(ValueError):
            Device(id="a", parent=root)
        with pytest.raises(ValueError):
            Device(id="root", parent=root.children()[0])

    def test_a_deleted_device_id_can_be_reused_without_warning(self, caplog):
        root, _ = _make_root()
        a = Device(id="a", parent=root)
        x = Device(id="x", parent=a)
        with x.state_transition():
            pass
        x.delete()
        with caplog.at_level(logging.WARNING, logger="homie"):
            Device(id="x", parent=Device(id="b", parent=root))
        assert _warnings(caplog, "deviceDuplicateIdInTree") == []


class TestHomieDomain:
    """:156: the homie-domain is a single segment."""

    @pytest.mark.parametrize("domain", ["", "a/b", "a+", "#", "$sys"])
    def test_an_invalid_domain_warns(self, caplog, domain):
        with caplog.at_level(logging.WARNING, logger="homie"):
            Device(id="d", homie_domain=domain)
        assert len(_warnings(caplog, "deviceHomieDomainInvalid")) == 1

    @pytest.mark.parametrize("domain", [None, "ebus", "homie"])
    def test_a_valid_or_default_domain_does_not_warn(self, caplog, domain):
        with caplog.at_level(logging.WARNING, logger="homie"):
            Device(id="d", homie_domain=domain)
        assert _warnings(caplog, "deviceHomieDomainInvalid") == []

    def test_an_empty_domain_still_falls_back_to_ebus(self, caplog):
        with caplog.at_level(logging.WARNING, logger="homie"):
            device = Device(id="d", homie_domain="")
        assert device.homie_domain() == EBUS_HOMIE_DOMAIN

    def test_a_multi_segment_domain_still_publishes(self, caplog):
        with caplog.at_level(logging.WARNING, logger="homie"):
            _, client = _make_root("d", homie_domain="a/b")
        assert len(_warnings(caplog, "deviceHomieDomainInvalid")) == 1
        assert f"a/b/{EBUS_HOMIE_VERSION_MAJOR}/d/$state" in _topics(client)


class TestBroadcastSubtopic:
    """:533: every subtopic segment follows the id format."""

    def _controller(self):
        client = MagicMock()
        return Controller(mqttc=client), client

    @pytest.mark.parametrize("subtopic", ["alert", "security/alert", "a-1/b-2"])
    def test_valid_subtopics_do_not_warn(self, caplog, subtopic):
        ctrl, _ = self._controller()
        with caplog.at_level(logging.WARNING, logger="homie"):
            assert ctrl.broadcast(subtopic, "m") is True
        assert _warnings(caplog, "broadcastSubtopicInvalid") == []

    @pytest.mark.parametrize("subtopic", ["Alert", "security//alert", "a/", "$x", "a/+", "#", ""])
    def test_an_invalid_subtopic_warns_once_and_is_still_sent(self, caplog, subtopic):
        ctrl, client = self._controller()
        with caplog.at_level(logging.WARNING, logger="homie"):
            assert ctrl.broadcast(subtopic, "m") is True
            assert ctrl.broadcast(subtopic, "m") is True
        assert len(_warnings(caplog, "broadcastSubtopicInvalid")) == 1
        assert client.publish.call_count == 2
        assert client.publish.call_args.args[0] == f"{BASE}/$broadcast/{subtopic}"


class TestSanitizeHomieId:
    def test_distinct_inputs_can_map_to_the_same_id(self):
        assert (
            sanitize_homie_id("Meter_1") == sanitize_homie_id("meter.1") == sanitize_homie_id("meter--1") == "meter-1"
        )
