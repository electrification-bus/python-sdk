# Examples

Example scripts demonstrating the ebus-sdk.

## Available Examples

### simple-device

Basic Homie device that publishes sensor data (temperature, humidity, air pressure).

```bash
./simple-device --config /path/to/broker-cfg.json
```

### simple-controller

Controller that auto-discovers Homie devices and monitors property changes.

```bash
./simple-controller --config /path/to/broker-cfg.json
```

### simple-span-controller

SPAN Panel controller with mDNS discovery. Connects via MQTTS and monitors power flow properties.

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

## Configuration

All examples accept broker configuration via:

1. **Command line**: `--config /path/to/broker-cfg.json`
2. **Environment variable**: `EBUS_BROKER_CFG=/path/to/broker-cfg.json`

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
