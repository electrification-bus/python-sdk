"""Real-broker regression tests for Controller discovery on a busy bus (GH #97).

Subscribing to many devices at once makes the broker queue every matching retained
message for one client. Past mosquitto's default per-client limits
(``max_inflight_messages`` 20, ``max_queued_messages`` 1000) it drops the rest, and
retained messages are sent only at subscribe time, so the devices subscribed last never
deliver their ``$state``, ``$description`` or property values.

Skipped when no ``mosquitto`` binary is available.
"""

import json
import os
import shutil
import socket
import subprocess
import time
import uuid

import pytest

paho = pytest.importorskip("paho.mqtt.client")

from ebus_sdk.homie import EBUS_HOMIE_DOMAIN, EBUS_HOMIE_VERSION_MAJOR, Controller


def _find_mosquitto():
    for candidate in ("/opt/homebrew/sbin/mosquitto", "/usr/sbin/mosquitto", "/usr/local/sbin/mosquitto"):
        if os.access(candidate, os.X_OK):
            return candidate
    return shutil.which("mosquitto")


MOSQUITTO = _find_mosquitto()

pytestmark = pytest.mark.skipif(MOSQUITTO is None, reason="mosquitto binary not found")

CHILDREN = 30
NODES_PER_DEVICE = 3
ROOT_ID = "busy-root"
BASE = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}"


def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_port(port, proc, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"mosquitto exited early with code {proc.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("mosquitto did not start listening")


@pytest.fixture
def broker(tmp_path):
    """A throwaway mosquitto with default limits on a free 127.0.0.1 port."""
    port = _free_port()
    conf = tmp_path / "mosquitto.conf"
    conf.write_text(f"listener {port} 127.0.0.1\nallow_anonymous true\npersistence false\n")
    proc = subprocess.Popen(
        [MOSQUITTO, "-c", str(conf)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_port(port, proc)
        yield port
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def _description(device_id, properties, children=None, root=None):
    per_node = properties // NODES_PER_DEVICE
    nodes = {
        f"node-{n}": {
            "name": f"Node {n}",
            "properties": {f"p{p:03d}": {"name": f"P{p}", "datatype": "integer"} for p in range(per_node)},
        }
        for n in range(NODES_PER_DEVICE)
    }
    description = {"homie": "5.0", "version": 1, "name": device_id, "nodes": nodes}
    if children is not None:
        description["children"] = children
    if root is not None:
        description["root"] = root
        description["parent"] = root
    return description


class _Seeder:
    """Publishes retained Homie topics with a raw paho client and waits until the broker has them."""

    def __init__(self, port):
        self.client = paho.Client(paho.CallbackAPIVersion.VERSION2, client_id=f"seeder-{uuid.uuid4()}")
        self.client.max_inflight_messages_set(1000)
        self.client.connect("127.0.0.1", port)
        self.client.loop_start()
        self.infos = []

    def pub(self, topic, payload):
        self.infos.append(self.client.publish(topic, payload, qos=1, retain=True))

    def device(self, device_id, properties, children=None, root=None, targets=False):
        self.pub(f"{BASE}/{device_id}/$description", json.dumps(_description(device_id, properties, children, root)))
        for n in range(NODES_PER_DEVICE):
            for p in range(properties // NODES_PER_DEVICE):
                self.pub(f"{BASE}/{device_id}/node-{n}/p{p:03d}", str(n * 1000 + p))
                if targets:
                    self.pub(f"{BASE}/{device_id}/node-{n}/p{p:03d}/$target", str(n * 1000 + p))
        self.pub(f"{BASE}/{device_id}/$state", "ready")

    def close(self):
        try:
            for info in self.infos:
                info.wait_for_publish(timeout=10)
        finally:
            self.client.loop_stop()
            self.client.disconnect()


def _seed_tree(port, properties, silent_children=0, targets=False, children=CHILDREN):
    """A root declaring `silent_children` children that never publish, then `children` real ones."""
    seeder = _Seeder(port)
    try:
        children = [f"busy-child-{i:02d}" for i in range(children)]
        silent = [f"silent-child-{i:02d}" for i in range(silent_children)]
        for child in children:
            seeder.device(child, properties, root=ROOT_ID, targets=targets)
        seeder.device(ROOT_ID, 0, children=silent + children)
    finally:
        seeder.close()
    return children


def _seed_bus(port, properties, state_only=0):
    """CHILDREN independent devices, plus `state_only` leftovers with a retained `lost` and nothing else."""
    seeder = _Seeder(port)
    try:
        for i in range(state_only):
            seeder.pub(f"{BASE}/aaa-dead-{i:02d}/$state", "lost")
        devices = [f"busy-device-{i:02d}" for i in range(CHILDREN)]
        for device in devices:
            seeder.device(device, properties)
    finally:
        seeder.close()
    return devices


def _complete(ctrl, device_id, properties):
    device = ctrl.get_device(device_id)
    if device is None or device.description is None or device.state != "ready":
        return False
    return sum(len(values) for values in device.properties.values()) == properties


def _await_discovery(ctrl, devices, properties, timeout=30.0):
    """Wait until every device has its state, description and every retained property value."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if all(_complete(ctrl, d, properties) for d in devices):
            return
        time.sleep(0.1)
    described = [d for d in devices if (dev := ctrl.get_device(d)) is not None and dev.description is not None]
    full = [d for d in devices if _complete(ctrl, d, properties)]
    pytest.fail(
        f"{len(described)}/{len(devices)} described, {len(full)}/{len(devices)} with all {properties} values; "
        f"incomplete {sorted(set(devices) - set(full))}"
    )


@pytest.mark.parametrize("properties", [60, 150, 300])
def test_tree_rooted_controller_discovers_every_child_of_a_busy_tree(broker, properties):
    children = _seed_tree(broker, properties)
    ctrl = Controller(mqtt_cfg={"host": "127.0.0.1", "port": broker}, root_device_id=ROOT_ID)
    try:
        ctrl.start_discovery()
        _await_discovery(ctrl, children, properties)
    finally:
        ctrl.stop()


def test_wildcard_controller_discovers_every_device_of_a_busy_bus(broker):
    devices = _seed_bus(broker, 150)
    ctrl = Controller(mqtt_cfg={"host": "127.0.0.1", "port": broker})
    try:
        ctrl.start_discovery()
        _await_discovery(ctrl, devices, 150)
    finally:
        ctrl.stop()


def test_silent_declared_children_do_not_stall_a_quiet_tree(broker):
    """Children declared ahead of the real ones that never publish must not hold up the rest.

    Nothing publishes after seeding and nobody calls check_stuck_children(), so only the
    retained messages the Controller's own subscriptions bring can drive discovery.
    """
    children = _seed_tree(broker, 60, silent_children=8)
    ctrl = Controller(mqtt_cfg={"host": "127.0.0.1", "port": broker}, root_device_id=ROOT_ID)
    try:
        ctrl.start_discovery()
        _await_discovery(ctrl, children, 60, timeout=8.0)
    finally:
        ctrl.stop()


def test_state_only_leftovers_do_not_stall_a_quiet_wildcard_bus(broker):
    """Devices with a retained $state and no $description must not hold up the rest."""
    devices = _seed_bus(broker, 60, state_only=8)
    ctrl = Controller(mqtt_cfg={"host": "127.0.0.1", "port": broker})
    try:
        ctrl.start_discovery()
        _await_discovery(ctrl, devices, 60, timeout=8.0)
    finally:
        ctrl.stop()


def _targets(ctrl, device_id):
    device = ctrl.get_device(device_id)
    return 0 if device is None else sum(len(t) for t in device.property_targets.values())


def test_retained_targets_of_a_busy_tree_all_arrive(broker):
    """Every property also carries a retained $target, doubling each device's burst."""
    children = _seed_tree(broker, 150, targets=True)
    ctrl = Controller(mqtt_cfg={"host": "127.0.0.1", "port": broker}, root_device_id=ROOT_ID)
    try:
        ctrl.start_discovery()
        _await_discovery(ctrl, children, 150)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and sum(_targets(ctrl, c) for c in children) < 150 * CHILDREN:
            time.sleep(0.1)
        assert sum(_targets(ctrl, c) for c in children) == 150 * CHILDREN
    finally:
        ctrl.stop()


def test_devices_larger_than_the_broker_queue_are_discovered_on_a_quiet_tree(broker):
    """Each child alone overflows the per-client queue, so it is subscribed node by node.

    Nothing publishes after seeding and nobody calls check_stuck_children().
    """
    children = _seed_tree(broker, 1500, children=3)
    ctrl = Controller(mqtt_cfg={"host": "127.0.0.1", "port": broker}, root_device_id=ROOT_ID)
    try:
        ctrl.start_discovery()
        _await_discovery(ctrl, children, 1500, timeout=20.0)
    finally:
        ctrl.stop()


def test_child_that_appears_after_giving_up_gets_its_values(broker):
    """A declared child silent past the stuck-check retries, then published, gets its property values."""
    seeder = _Seeder(broker)
    try:
        seeder.device(ROOT_ID, 0, children=["late-child"])
    finally:
        seeder.close()
    ctrl = Controller(
        mqtt_cfg={"host": "127.0.0.1", "port": broker},
        root_device_id=ROOT_ID,
        stuck_device_timeout=0.2,
        max_resubscribe_attempts=1,
    )
    try:
        ctrl.start_discovery()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and (
            "late-child" not in ctrl.devices or "late-child" in ctrl._awaiting_attributes
        ):
            time.sleep(0.05)
        assert "late-child" not in ctrl._awaiting_attributes  # given up
        seeder = _Seeder(broker)
        try:
            seeder.device("late-child", 6, root=ROOT_ID)
        finally:
            seeder.close()
        _await_discovery(ctrl, ["late-child"], 6, timeout=5.0)
    finally:
        ctrl.stop()
