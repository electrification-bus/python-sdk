"""Shared fixtures for ebus-sdk tests."""

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_paho():
    """Mock paho.mqtt.client to prevent real MQTT connections."""
    with patch("ebus_sdk.mqtt.mqtt.Client") as mock_client_cls:
        mock_instance = MagicMock()
        mock_instance.is_connected.return_value = True
        mock_instance.subscribe.return_value = (0, 1)  # MQTT_ERR_SUCCESS, msg_id
        mock_instance.publish.return_value = MagicMock(rc=0)  # MQTT_ERR_SUCCESS
        mock_client_cls.return_value = mock_instance
        yield mock_instance
