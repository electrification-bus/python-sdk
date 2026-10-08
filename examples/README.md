# Examples

Example scripts demonstrating the ebus-sdk.

## Available Examples

### simple-device

Basic Homie device that publishes sensor data (temperature, humidity, air pressure).

```bash
./simple-device --config /path/to/broker-cfg.json
```

### simple-tree-device

A parent/child device TREE over one MQTT connection: a root `distribution-enclosure` (`panel-1`) with circuit and BESS children and a MID grandchild, all sharing the root's single connection. It shows building the tree inside one `with root.state_transition():` (so the root publishes one INIT->READY for the whole tree, not one per child), a settable property on a child whose `/set` routes back over the shared connection, and the per-device topics each tree node publishes under. Reach for this over `simple-device` when you need to model a device that contains sub-devices. `--check` builds the tree, logs the root and its direct children, and exits (it works even with the broker down, since connect is asynchronous).

```bash
./simple-tree-device --config /path/to/broker-cfg.json
./simple-tree-device --config /path/to/broker-cfg.json --check   # build + print + exit
```

### utility-meter

Publishes a single eBus utility-meter device (`energy.ebus.device.utility-meter`, per the Electrification Bus `devices/utility-meter.md`, written against v0.3 of that data model) with the data model's capabilities: the required `info` / `meter` / `status` plus the optional `grid` / `doe` / `price` / `demand` / `power-quality` a meter publishes when it has the signal. The `doe` and `price` capabilities are `json` properties (`doe/import-limit`/`export-limit`, `price/import-price`/`export-price`), each a JSON array of time-windowed objects that advertises its schema as a `$format` JSONSchema; enum properties advertise their allowed values via `$format`. A local HTTP endpoint stands in for the utility's out-of-band backhaul for runtime doe/price updates, validating each posted body against the property's `$format` before publish (install the `validation` extra to enable it: `pip install 'ebus-sdk[validation]'`).

```bash
./utility-meter --config ./utility-meter-cfg.example.json --broker-config /path/to/broker-cfg.json
```

**Need a broker to run against?** [`broker-quickstart`](https://github.com/electrification-bus/broker-quickstart) brings up a local eBus broker on macOS with one command (`python -m laptop.run`), including the mDNS advertisement that `--discover` (below) resolves. Its `scripts/laptop-bench.sh` runs this simulator against that broker with `--discover`, end to end; see the [laptop quickstart](https://github.com/electrification-bus/broker-quickstart/blob/main/docs/laptop-quickstart.md).

Add `--discover` to find the broker over mDNS, with [`ebus-service-discovery`](https://github.com/electrification-bus/python-service-discovery), instead of using the `host`/`port` in the broker config; the broker config still supplies the TLS material and credentials. A TLS broker config accepts only `_secure-mqtt._tcp`; a plain one accepts `_secure-mqtt._tcp` or `_mqtt._tcp`. When the config also names a `host`, a discovered broker with that host is preferred, and the config's `host` is the fallback after `--discover-timeout` seconds. Needs the `mdns` extra (`pip install 'ebus-sdk[mdns]'`):

```bash
./utility-meter --config ./utility-meter-cfg.example.json --broker-config /path/to/broker-cfg.json --discover
```

With the `mdns` extra installed, the meter also advertises `_ebus._tcp` and `_device-info._tcp` (role `device`; manufacturer, model, serial number and firmware version from the config's `info`), as the eBus framework requires of every entity. `--no-advertise` turns this off. On a host with several network interfaces, see [electrification-bus/python-service-discovery#3](https://github.com/electrification-bus/python-service-discovery/issues/3).

Set a DOE envelope at runtime. The body is a single envelope object or an array of them (the retained schedule); it is set atomically as one `json` value:

```bash
curl -X POST http://localhost:8765/doe/import-limit \
  -H 'Content-Type: application/json' \
  -d '[{"power-limit": 12000, "source": "GRID", "start-time": "2026-07-01T16:00:00Z", "end-time": "2026-07-01T20:00:00Z"}]'
```

Envelope fields: `power-limit` (integer W) and/or `apparent-power-limit` (integer VA) — at least one required; `source` (one of `CONTRACT` / `REGULATOR` / `EQUIPMENT` / `GRID` / `UNKNOWN`, optional); `start-time` / `end-time` (ISO-8601 UTC, optional). POST an empty body or `null` to clear the signal for that direction. The same shape is served at `/doe/export-limit`, and the price schedules at `/price/import-price` and `/price/export-price` (price-window objects: `price` + `currency`, `price-level`, `source`, `start-time` / `end-time`). A body that violates the property's `$format` schema is rejected with `400` and the validation error.

### simple-controller

Discovers eBus devices and walks them as a TREE (roots then descendants), the current parent/child model: it reports each device's EFFECTIVE state (a child of a LOST root reads LOST, not its own stale `ready`), prints per-node property values, flags settable properties, and can send one `/set` to a settable property. Pairs with `simple-tree-device`: run that against a broker, point this at the same broker, and watch the tree appear. Wildcard discovery by default; `--root <device-id>` discovers one root and its descendants.

```bash
./simple-controller --config /path/to/broker-cfg.json
./simple-controller --config broker-cfg.json --root panel-1
./simple-controller --config broker-cfg.json --set circuit-3/switch/relay=true
```

### simple-span-controller

Connects to a live SPAN panel via mDNS + MQTTS and determines whether the home is islanded from the grid, watching the MID child's `grid` capability (`islanding-state` / `grid-state` / `grid-forming-entity`) plus the enclosure's `power-flows`. Targets the current parent/child data model (the legacy flat `core/dominant-power-source` is retired).

Discovery accepts only the `_secure-mqtt._tcp` advertisement whose host is `span-<serial-number>.local`, so another broker on the network never receives this panel's credentials, and connects by host name rather than by IP address. If the panel is not heard within 5 s, it connects to `span-<serial-number>.local` directly.

Requires the `mdns` extra:

```bash
pip install ebus-sdk[mdns]
```

**Basic usage (requires password):**

```bash
./simple-span-controller <serial-number> <password>
./simple-span-controller <serial-number> <password> --broker-host 192.168.1.100
```

**With SPAN-API utilities (automatic credentials and CA cert):**

If you have access to the SPAN-API-Client-Docs repository, you can enable automatic credential and certificate management:

```bash
# Add SPAN-API-Client-Docs lib to PYTHONPATH
export PYTHONPATH=$PYTHONPATH:/path/to/SPAN-API-Client-Docs/lib

# Run without password - uses ~/.span-auth.json
./simple-span-controller <serial-number>

# Force insecure mode (skip CA cert verification)
./simple-span-controller <serial-number> --insecure
```

When SPAN-API utilities are available:

- Password is retrieved from `~/.span-auth.json` if not provided on command line
- CA certificate is fetched/cached in `~/.span-ca-certs/` for secure TLS verification
- Use `--insecure` to skip certificate verification even when CA cert is available

### ha-discovery-bridge

Bridges eBus/Homie devices to Home Assistant MQTT discovery (the reverse of the HA -> eBus path). Publishes a synthetic eBus device (meter / battery / settable control), runs a `Controller` + `HaDiscoveryBridge` that discovers it and emits `homeassistant/device/<id>/config`, then subscribes an independent client (standing in for Home Assistant) to capture and verify the emitted config, and finally shows the config being cleared on device removal. Runs against a real broker but needs no running Home Assistant. See [`doc/ha-discovery-bridge.md`](../doc/ha-discovery-bridge.md) for the full guide.

```bash
./ha-discovery-bridge --config /path/to/broker-cfg.json
./ha-discovery-bridge                 # defaults to 127.0.0.1:1883, no auth
./ha-discovery-bridge --keep-running  # leave it up for a real HA to consume
```

No broker handy? A throwaway local one works: `printf 'listener 1883 127.0.0.1\nallow_anonymous true\n' > /tmp/mosquitto.conf && mosquitto -c /tmp/mosquitto.conf`, then run with no `--config`. (mosquitto 2.x refuses a process-substitution path such as `-c <(...)`.)

## Configuration

`simple-device`, `simple-tree-device`, `simple-controller`, and `ha-discovery-bridge` accept broker configuration via:

1. **Command line**: `--config /path/to/broker-cfg.json`
2. **Environment variable**: `EBUS_BROKER_CFG=/path/to/broker-cfg.json`

`utility-meter` takes the broker config as `-b` / `--broker-config` (its `--config` is the meter config), also defaulting to `EBUS_BROKER_CFG`. `simple-span-controller` builds its own config from the panel serial number, password, and mDNS.

### Broker Config Format

```json
{
  "host": "mqtt.example.com",
  "port": 1883,
  "authentication": {
    "type": "USER_PASS",
    "username": "myuser",
    "password": "mypassword"
  }
}
```

For MQTTS (TLS) with insecure mode (no certificate verification):

```json
{
  "host": "secure-broker.example.com",
  "port": 8883,
  "use_tls": true,
  "tls_insecure": true,
  "authentication": {
    "type": "USER_PASS",
    "username": "myuser",
    "password": "mypassword"
  }
}
```

For MQTTS with CA certificate verification (secure mode):

```json
{
  "host": "secure-broker.example.com",
  "port": 8883,
  "use_tls": true,
  "tls_ca_cert": "/path/to/ca-cert.crt",
  "tls_insecure": false,
  "authentication": {
    "type": "USER_PASS",
    "username": "myuser",
    "password": "mypassword"
  }
}
```

**TLS Options:**

- `use_tls`: Enable TLS/SSL connection (required for port 8883)
- `tls_ca_cert`: Path to CA certificate file for server verification
- `tls_ca_data`: CA certificate content as PEM string or DER bytes (alternative to file)
- `tls_insecure`: Skip certificate verification (default: true for backwards compatibility)
