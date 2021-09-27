"""
Tests for the on-prem config builder
"""

from typing import Dict, Any
import tempfile
import yaml

from pydantic import ValidationError
import pytest

from driver.exceptions import DriverConfigException
from driver.driver_config_builder import (
    PartialConfigFromFile,
    DriverConfigBuilder
)

# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring


@pytest.fixture(name="test_config_data")
def _test_config_data() -> Dict[str, Any]:
    partial_config_from_file: Dict[str, Any] = {
        "server_url": "test_server_url",
        "api_key": "test_api_key",
        "db_key": "test_db_key",
        "organization_id": "test_organization",
        "database_id": 1,
        "server_http_proxy": "test_server_http_proxy",
        "server_https_proxy": "test_server_https_proxy",
        "db_type": "mysql",
        "db_host": "test_host",
        "db_port": 3306,
        "db_user": "test_user",
        "db_password": "test_password",
        "db_ssl_ca": "",
        "db_ssl_cert": "",
        "db_ssl_key": "",
        "db_enable_ssl": False,
        "db_conn_extend": {"pool_size": 10},
        "monitor_interval": 60
    }

    partial_config_from_server: Dict[str, Any] = {
        "db_provider": "on_premise",
        "api_key": "test_api_key",
        "db_key": "test_db_key",
        "organization_id": "test_organization",
        "enable_tuning": True,
        "enable_restart": False,
        "monitor_interval": 60,
        "tune_interval": 600,
    }

    config: Dict[str, Any] = {}
    config.update(partial_config_from_file)
    config.update(partial_config_from_server)

    return dict(
        file=partial_config_from_file,
        server=partial_config_from_server,
    )


# Test PartialConfigFromFile
def test_partial_config_from_file_invalid_type(
    test_config_data: Dict[str, Any]
) -> None:
    # wrong type server_url fetched from env, string is expected, but int found
    test_data_from_file = test_config_data["file"]
    test_data_from_file["server_url"] = 15213
    with pytest.raises(ValidationError) as ex:
        PartialConfigFromFile(**test_data_from_file)
    assert "server_url" in str(ex.value)


def test_partial_config_from_file_missing_value(
    test_config_data: Dict[str, Any]
) -> None:
    # missing option server_url fetched from file
    test_data_from_file = test_config_data["file"]
    test_data_from_file.pop("server_url")
    with pytest.raises(ValidationError) as ex:
        PartialConfigFromFile(**test_data_from_file)
    assert "server_url" in str(ex.value)


def test_create_driver_config_builder_invalid_config(test_config_data: Dict[str, Any]
) -> None:
    with pytest.raises(DriverConfigException):
        with tempfile.NamedTemporaryFile("w") as temp:
            test_config_data["file"]["database_id"] = "15213"
            yaml.safe_dump(test_config_data["file"], temp)
            config_builder = DriverConfigBuilder('us-east-2')
            config_builder.from_file(temp.name)


def test_create_driver_config_builder_invalid_config_path() -> None:
    # Invalid config file path
    with pytest.raises(FileNotFoundError):
        config_builder = DriverConfigBuilder('us-east-2')
        config_builder.from_file("invalid_name")


def test_create_driver_config_builder_invalid_yaml() -> None:
    # Invalid yaml config file
    with tempfile.NamedTemporaryFile("w") as temp:
        with pytest.raises(ValueError):
            temp.write("bad_format")
            temp.seek(0)
            config_builder = DriverConfigBuilder('us-east-2')
            config_builder.from_file(temp.name)
