"""
Tests for the config builder
"""

from typing import Dict, Any
import tempfile
from unittest.mock import patch

from pydantic import ValidationError
import pytest

from driver.driver_config_builder import (
    PartialConfigFromFile,
    PartialConfigFromRDS,
    DriverConfigBuilder,
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
        "monitor_interval": 60,
        "num_table_to_collect_stats": 100,
        "num_index_to_collect_stats": 1000,
        "table_level_monitor_interval": 300,
        "metric_source": [
            "cloudwatch",
        ]
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

    partial_config_from_rds: Dict[str, Any] = {
        "db_type": "mysql",
        "db_host": "test_host",
        "db_port": 3306,
        "db_version": "14_2",
        "db_non_default_parameters": ['test_parameter_1', 'test_parameter_2']
    }

    config: Dict[str, Any] = {}
    config.update(partial_config_from_file)
    config.update(partial_config_from_server)
    config.update(partial_config_from_rds)

    return dict(
        file=partial_config_from_file,
        server=partial_config_from_server,
        rds=partial_config_from_rds,
    )


def test_partial_config_from_file_success(
    test_config_data: Dict[str, Any]
) -> None:
    # wrong type server_url fetched from env, string is expected, but int found
    test_data_from_file = test_config_data["file"]
    partial_config = PartialConfigFromFile(**test_data_from_file)
    assert partial_config.monitor_interval == 60
    assert partial_config.num_table_to_collect_stats == 100
    assert partial_config.table_level_monitor_interval == 300


def test_partial_config_from_file_invalid_table_level_monitor_interval(
    test_config_data: Dict[str, Any]
) -> None:
    # wrong type server_url fetched from env, string is expected, but int found
    test_data_from_file = test_config_data["file"]
    test_data_from_file["table_level_monitor_interval"] = 60
    with pytest.raises(ValidationError) as ex:
        PartialConfigFromFile(**test_data_from_file)
    assert "table_level_monitor_interval" in str(ex.value)

def test_partial_config_from_file_invalid_num_table_to_collect_stats(
    test_config_data: Dict[str, Any]
) -> None:
    # wrong type server_url fetched from env, string is expected, but int found
    test_data_from_file = test_config_data["file"]
    test_data_from_file["num_table_to_collect_stats"] = -1
    with pytest.raises(ValidationError) as ex:
        PartialConfigFromFile(**test_data_from_file)
    assert "num_table_to_collect_stats" in str(ex.value)

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


# Test PartialConfigFromRDS
def test_partial_config_from_rds_success(
    test_config_data: Dict[str, Any]
) -> None:
    # rds config success: all key values intact
    test_data_from_rds = test_config_data["rds"]
    partial_config = PartialConfigFromRDS(**test_data_from_rds)
    assert partial_config.db_type == "mysql"
    assert partial_config.db_host == "test_host"
    assert partial_config.db_port == 3306
    assert partial_config.db_version == "14_2"
    assert partial_config.db_non_default_parameters == ['test_parameter_1', 'test_parameter_2']


def test_partial_config_from_rds_invalid_type(
    test_config_data: Dict[str, Any]
) -> None:
    # wrong type db_version fetched from env, string is expected, but int found
    test_data_from_rds = test_config_data["rds"]
    test_data_from_rds["db_version"] = 10
    with pytest.raises(ValidationError) as ex:
        PartialConfigFromRDS(**test_data_from_rds)
    assert "db_version" in str(ex.value)


def test_partial_config_from_rds_missing_value(
    test_config_data: Dict[str, Any]
) -> None:
    # missing option db_non_default_parameters fetched from rds
    test_data_from_rds = test_config_data["rds"]
    test_data_from_rds.pop("db_non_default_parameters")
    with pytest.raises(ValidationError) as ex:
        PartialConfigFromRDS(**test_data_from_rds)
    assert "db_non_default_parameters" in str(ex.value)


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

def test_get_cloudwatch_metric_file_aurora_mysql56() -> None:
    config_builder = DriverConfigBuilder('us-east-2')
    with patch('driver.driver_config_builder.get_db_version') as mocked_db_version:
        with patch('driver.driver_config_builder.get_db_type') as mocked_db_type:
            mocked_db_type.return_value = "aurora_mysql"
            mocked_db_version.return_value = "5_6_mysql_aurora_1_22_2"
            # pylint: disable=protected-access
            file_path = config_builder._get_cloudwatch_metrics_file("")
            assert file_path == (
                "./driver/config/cloudwatch_metrics/"
                "rds_aurora_mysql-5_6.json"
            )

def test_get_cloudwatch_metric_file_aurora_mysql57() -> None:
    config_builder = DriverConfigBuilder('us-east-2')
    with patch('driver.driver_config_builder.get_db_version') as mocked_db_version:
        with patch('driver.driver_config_builder.get_db_type') as mocked_db_type:
            mocked_db_type.return_value = "aurora_mysql"
            mocked_db_version.return_value = "5_7_mysql_aurora_2_10_1"
            # pylint: disable=protected-access
            file_path = config_builder._get_cloudwatch_metrics_file("")
            assert file_path == (
                "./driver/config/cloudwatch_metrics/"
                "rds_aurora_mysql-5_7.json"
            )

def test_get_cloudwatch_metric_file_aurora_postgres12() -> None:
    config_builder = DriverConfigBuilder('us-east-2')
    with patch('driver.driver_config_builder.get_db_version') as mocked_db_version:
        with patch('driver.driver_config_builder.get_db_type') as mocked_db_type:
            mocked_db_type.return_value = "aurora_postgresql"
            mocked_db_version.return_value = "12_6"
            # pylint: disable=protected-access
            file_path = config_builder._get_cloudwatch_metrics_file("")
            assert file_path == (
                "./driver/config/cloudwatch_metrics/"
                "rds_aurora_postgresql-12.json"
            )
