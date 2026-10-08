"""Homie 5 convention compliance of the device role (issue #95).

Line numbers in docstrings refer to homieiot/convention@7edc221 convention.md.
"""

import json
import socket
from unittest.mock import MagicMock, patch

from ebus_sdk.homie import (
    EBUS_HOMIE_DOMAIN,
    EBUS_HOMIE_VERSION_MAJOR,
    Device,
    DeviceState,
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
