"""Device-side Homie domain configurability.

The Controller has always been domain-configurable; the Device was hardcoded to
`ebus`. These pin that a tree can publish under any Homie 5 domain, that the
default is unchanged, and that the domain is a property of the TREE.
"""

from unittest.mock import MagicMock, patch

import pytest

from ebus_sdk import Device, DeviceState, PropertyDatatype
from ebus_sdk.homie import EBUS_HOMIE_DOMAIN


def _mock_client():
    client = MagicMock()
    client.is_connected.return_value = True
    client.is_running = True
    client.publish.return_value = MagicMock(rc=0)
    client.subscribe.return_value = (0, 1)
    return client


def _device(device_id="dev-1", **kwargs):
    with patch("ebus_sdk.homie.MqttClient.from_config") as from_config:
        client = _mock_client()
        from_config.return_value = client
        device = Device(id=device_id, mqtt_cfg={"host": "localhost", "port": 1883}, **kwargs)
        return device, client


def _topics(client):
    return [str(c.args[0]) for c in client.publish.call_args_list if c.args]


def _subscribed(client):
    return [str(c.args[0]) for c in client.subscribe.call_args_list if c.args]


def _flushed(client):
    """Topics sent via publish_and_flush, the owned-client path stop()/declare_lost() take."""
    return [str(c.args[0]) for c in client.publish_and_flush.call_args_list if c.args]


# --- the default is unchanged ------------------------------------------------


def test_default_domain_is_ebus():
    device, client = _device()
    assert device.homie_domain() == EBUS_HOMIE_DOMAIN
    device.set_state(DeviceState.READY)
    device.publish("$state")
    assert any(t.startswith("ebus/5/dev-1/") for t in _topics(client))


def test_explicit_ebus_is_the_same_as_omitting_it():
    device, _ = _device(homie_domain="ebus")
    assert device.homie_domain() == "ebus"


# --- publishing under another domain ----------------------------------------


def test_a_root_can_publish_under_the_standard_homie_domain():
    device, client = _device(homie_domain="homie")
    assert device.homie_domain() == "homie"

    node = device.add_node_from_dict({"id": "sensor", "name": "sensor", "type": "x"})
    node.add_property_from_dict({"id": "temp", "datatype": PropertyDatatype.FLOAT, "value": 21.5})
    device.set_state(DeviceState.READY)
    device.publish("$state")

    published = _topics(client)
    assert "homie/5/dev-1/sensor/temp" in published
    assert "homie/5/dev-1/$state" in published
    # Nothing leaked onto the eBus tree.
    assert not [t for t in published if t.startswith("ebus/")]


def test_the_last_will_follows_the_domain():
    device, _ = _device(homie_domain="homie")
    assert device.will()["topic"] == "homie/5/dev-1/$state"
    assert device.will()["payload"] == DeviceState.LOST.value


def test_the_owned_client_lwt_still_matches_will_under_a_custom_domain():
    """The installed LWT and will() cannot drift, whatever the domain."""
    with patch("ebus_sdk.homie.MqttClient.from_config") as from_config:
        from_config.return_value = _mock_client()
        device = Device(id="dev-1", mqtt_cfg={"host": "localhost", "port": 1883}, homie_domain="homie")
        assert from_config.call_args[1]["lwt"] == device.will()
        assert from_config.call_args[1]["lwt"]["topic"].startswith("homie/5/")


def test_settable_properties_subscribe_under_the_domain():
    device, client = _device(homie_domain="homie")
    node = device.add_node_from_dict({"id": "control", "name": "control", "type": "x"})
    node.add_property_from_dict({"id": "mode", "datatype": PropertyDatatype.STRING, "settable": True})
    assert "homie/5/dev-1/control/mode/set" in _subscribed(client)


def test_an_inbound_set_is_accepted_on_the_configured_domain():
    device, _ = _device(homie_domain="homie")
    node = device.add_node_from_dict({"id": "control", "name": "control", "type": "x"})
    received = []
    prop = node.add_property_from_dict(
        {
            "id": "mode",
            "datatype": PropertyDatatype.STRING,
            "settable": True,
            "set_callback": received.append,
        }
    )
    prop._settable_callback("homie/5/dev-1/control/mode/set", b"manual")
    assert received == ["manual"]


def test_an_inbound_set_on_a_foreign_domain_is_rejected():
    """The topic check accepts this tree's domain, not merely 'ebus', and not anything."""
    device, _ = _device(homie_domain="homie")
    node = device.add_node_from_dict({"id": "control", "name": "control", "type": "x"})
    received = []
    prop = node.add_property_from_dict(
        {
            "id": "mode",
            "datatype": PropertyDatatype.STRING,
            "settable": True,
            "set_callback": received.append,
        }
    )
    prop._settable_callback("ebus/5/dev-1/control/mode/set", b"manual")
    assert received == []


def test_clearing_and_deleting_use_the_domain():
    device, client = _device(homie_domain="homie")
    node = device.add_node_from_dict({"id": "sensor", "name": "sensor", "type": "x"})
    node.add_property_from_dict({"id": "temp", "datatype": PropertyDatatype.FLOAT, "value": 1.0})
    device.set_state(DeviceState.READY)

    before = len(client.publish.call_args_list)
    device.delete()
    after = _topics(client)[before:]
    assert "homie/5/dev-1/$state" in after
    assert not [t for t in after if t.startswith("ebus/")]


def test_declare_lost_uses_the_domain():
    device, client = _device(homie_domain="homie")
    device.set_state(DeviceState.READY)
    device.declare_lost()
    # An owned client flushes, so the deliberate-death announcement goes out
    # through publish_and_flush rather than publish.
    assert "homie/5/dev-1/$state" in _flushed(client)
    assert not [t for t in _flushed(client) if t.startswith("ebus/")]


def test_stop_announces_disconnected_under_the_domain():
    device, client = _device(homie_domain="homie")
    device.set_state(DeviceState.READY)
    device.stop()
    assert "homie/5/dev-1/$state" in _flushed(client)


# --- the domain is a property of the TREE ------------------------------------


def test_a_child_inherits_the_roots_domain():
    root, client = _device("root-1", homie_domain="homie")
    child = Device(id="child-1", parent=root)
    grandchild = Device(id="gc-1", parent=child)

    assert child.homie_domain() == "homie"
    assert grandchild.homie_domain() == "homie"

    node = grandchild.add_node_from_dict({"id": "sensor", "name": "sensor", "type": "x"})
    node.add_property_from_dict({"id": "temp", "datatype": PropertyDatatype.FLOAT, "value": 1.0})
    assert "homie/5/gc-1/sensor/temp" in _topics(client)


def test_a_child_cannot_carry_its_own_domain():
    root, _ = _device("root-1", homie_domain="homie")
    with pytest.raises(ValueError, match="a tree shares one domain"):
        Device(id="child-1", parent=root, homie_domain="ebus")


def test_a_child_of_a_default_domain_root_is_also_refused():
    """Refused even when the value would have matched: the rule is structural."""
    root, _ = _device("root-1")
    with pytest.raises(ValueError, match="a tree shares one domain"):
        Device(id="child-1", parent=root, homie_domain="ebus")


def test_will_describes_the_roots_domain_from_a_child():
    root, _ = _device("root-1", homie_domain="homie")
    child = Device(id="child-1", parent=root)
    assert child.will()["topic"] == "homie/5/root-1/$state"


def test_two_trees_on_different_domains_do_not_interfere():
    ebus_tree, ebus_client = _device("energy-1")
    homie_tree, homie_client = _device("lamp-1", homie_domain="homie")

    for device in (ebus_tree, homie_tree):
        node = device.add_node_from_dict({"id": "info", "name": "info", "type": "x"})
        node.add_property_from_dict({"id": "vendor-name", "datatype": PropertyDatatype.STRING, "value": "Acme"})

    assert "ebus/5/energy-1/info/vendor-name" in _topics(ebus_client)
    assert "homie/5/lamp-1/info/vendor-name" in _topics(homie_client)
    assert not [t for t in _topics(homie_client) if t.startswith("ebus/")]
    assert not [t for t in _topics(ebus_client) if t.startswith("homie/")]


def test_a_transport_free_tree_still_resolves_its_domain():
    """Topic derivation is the point of a transport-free tree, so the domain must reach it."""
    device = Device(id="dev-1", homie_domain="homie")
    child = Device(id="child-1", parent=device)
    assert device.homie_domain() == "homie"
    assert child.homie_domain() == "homie"
    assert device.will()["topic"] == "homie/5/dev-1/$state"
