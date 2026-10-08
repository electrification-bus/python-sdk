"""Real-broker regression test for tree-rooted discovery on a busy tree (GH #97).

Subscribing to every child of a large tree at once makes the broker queue every
matching retained message for one client. Past mosquitto's default per-client
limits (``max_inflight_messages`` 20, ``max_queued_messages`` 1000) it drops the
rest, and retained messages are sent only at subscribe time, so the children
subscribed last never deliver ``$state`` or ``$description``.

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
PROPERTIES_PER_CHILD = 60
NODES_PER_CHILD = 3
ROOT_ID = "busy-root"


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


def _child_id(i):
    return f"busy-child-{i:02d}"


def _description(device_id, children=None):
    props_per_node = PROPERTIES_PER_CHILD // NODES_PER_CHILD
    nodes = {
        f"node-{n}": {
            "name": f"Node {n}",
            "properties": {f"p{p:02d}": {"name": f"P{p}", "datatype": "integer"} for p in range(props_per_node)},
        }
        for n in range(NODES_PER_CHILD)
    }
    description = {"homie": "5.0", "version": 1, "name": device_id, "nodes": nodes}
    if children is not None:
        description["children"] = children
    else:
        description["root"] = ROOT_ID
        description["parent"] = ROOT_ID
    return description


def _seed_tree(port):
    """Publish a retained tree with a raw paho client and wait until the broker has it."""
    client = paho.Client(paho.CallbackAPIVersion.VERSION2, client_id=f"seeder-{uuid.uuid4()}")
    client.max_inflight_messages_set(1000)
    client.connect("127.0.0.1", port)
    client.loop_start()
    infos = []
    base = f"{EBUS_HOMIE_DOMAIN}/{EBUS_HOMIE_VERSION_MAJOR}"

    def pub(topic, payload):
        infos.append(client.publish(topic, payload, qos=1, retain=True))

    try:
        children = [_child_id(i) for i in range(CHILDREN)]
        for child in children:
            pub(f"{base}/{child}/$description", json.dumps(_description(child)))
            for n in range(NODES_PER_CHILD):
                for p in range(PROPERTIES_PER_CHILD // NODES_PER_CHILD):
                    pub(f"{base}/{child}/node-{n}/p{p:02d}", str(n * 100 + p))
            pub(f"{base}/{child}/$state", "ready")
        pub(f"{base}/{ROOT_ID}/$description", json.dumps(_description(ROOT_ID, children=children)))
        pub(f"{base}/{ROOT_ID}/$state", "ready")
        for info in infos:
            info.wait_for_publish(timeout=10)
    finally:
        client.loop_stop()
        client.disconnect()
    return children


def test_tree_rooted_controller_discovers_every_child_of_a_busy_tree(broker):
    children = _seed_tree(broker)

    ctrl = Controller(mqtt_cfg={"host": "127.0.0.1", "port": broker}, root_device_id=ROOT_ID)
    try:
        ctrl.start_discovery()
        deadline = time.monotonic() + 30.0
        described = set()
        while time.monotonic() < deadline:
            described = {c for c in children if (d := ctrl.get_device(c)) is not None and d.description is not None}
            if len(described) == len(children):
                break
            time.sleep(0.1)
        missing = sorted(set(children) - described)
        assert not missing, f"{len(described)}/{len(children)} children described; missing {missing}"
        for child in children:
            assert ctrl.get_device(child).state == "ready"
    finally:
        ctrl.stop()
