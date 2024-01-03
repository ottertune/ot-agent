"""
Tests for the collector factory
"""
from typing import Dict, Any, NoReturn, Optional
from unittest.mock import MagicMock, patch
import mock
import pytest
import mysql.connector.connection
import psycopg2
from driver.collector.collector_factory import (
    create_db_config_mysql,
    get_mysql_version,
    get_postgres_version,
)
from driver.collector.collector_factory import create_db_config_postgres
from driver.exceptions import (
    DriverException,
    MysqlCollectorException,
    PostgresCollectorException,
)

# pylint: disable=missing-function-docstring


@pytest.fixture(name="mock_mysql_conn")
def _mock_mysql_conn() -> MagicMock:
    return MagicMock(spec=mysql.connector.connection.MySQLConnection)


@pytest.fixture(name="mock_pg_conn")
@mock.patch("psycopg2.connect")
def _mock_pg_conn(mock_connect: MagicMock) -> MagicMock:
    return mock_connect.return_value


def test_db_config_mysql_success() -> None:
    driver_conf: Dict[str, Any] = {
        "db_host": "localhost",
        "db_port": "3306",
        "db_user": "test_user",
        "db_password": "test_password",
        "db_name": "test_db",
        "db_version": "8.0.22",
    }
    expected_db_conf: Dict[str, Any] = {
        "host": "localhost",
        "port": "3306",
        "user": "test_user",
        "password": "test_password",
        "database": "test_db",
        "charset": "utf8",
    }
    db_conf = create_db_config_mysql(driver_conf)
    assert expected_db_conf == db_conf

    driver_conf["db_name"] = ""
    expected_db_conf["database"] = "information_schema"
    db_conf_new = create_db_config_mysql(driver_conf)
    assert expected_db_conf == db_conf_new


def test_db_config_mysql_iam_auth() -> None:
    with patch(
        "driver.collector.collector_factory.get_db_auth_token"
    ) as mocked_db_auth_token:
        mocked_db_auth_token.return_value = "auth_token"

        driver_conf: Dict[str, Any] = {
            "db_host": "localhost",
            "db_port": "3306",
            "db_user": "test_user",
            "db_password": "test_password",
            "db_name": "test_db",
            "db_version": "8.0.22",
            "aws_region": "us-east-1",
            "enable_aws_iam_auth": True,
        }
        expected_db_conf: Dict[str, Any] = {
            "host": "localhost",
            "port": "3306",
            "user": "test_user",
            "password": "auth_token",
            "database": "test_db",
            "charset": "utf8",
        }
        db_conf = create_db_config_mysql(driver_conf)
        assert expected_db_conf == db_conf


def test_db_config_mysql_invalid() -> None:
    driver_conf: Dict[str, Any] = {
        "db_host": "localhost",
        "db_port": "3306",
        "db_user": "test_user",
    }
    with pytest.raises(DriverException) as ex:
        create_db_config_mysql(driver_conf)
    assert (
        "Invalid MySQL database configuration: parameter is not defined"
        in ex.value.message
    )

    driver_conf["db_password"] = "test_password"
    driver_conf["db_conf_extend"] = ["invalid db_conf_extend type"]
    with pytest.raises(DriverException) as ex:
        create_db_config_mysql(driver_conf)
    assert (
        "Invalid MySQL database configuration: db_conf_extend type" in ex.value.message
    )

    driver_conf["db_conf_extend"] = {"db_user": "duplicate_user"}
    with pytest.raises(DriverException) as ex:
        create_db_config_mysql(driver_conf)
    assert (
        "Invalid MySQL database configuration: duplicate parameters" in ex.value.message
    )


def test_db_config_postgres_success() -> None:
    driver_conf: Dict[str, Any] = {
        "db_host": "localhost",
        "db_port": "5432",
        "db_user": "test_user",
        "db_password": "test_password",
        "db_name": "test_db",
        "db_version": "9.6.20",
    }
    expected_db_conf: Dict[str, Any] = {
        "host": "localhost",
        "port": "5432",
        "user": "test_user",
        "password": "test_password",
        "dbname": "test_db",
    }
    db_conf = create_db_config_postgres(driver_conf)
    assert expected_db_conf == db_conf

    driver_conf["db_name"] = ""
    expected_db_conf["dbname"] = "postgres"
    db_conf_new = create_db_config_postgres(driver_conf)
    assert expected_db_conf == db_conf_new


def test_db_config_postgres_iam_auth() -> None:
    with patch(
        "driver.collector.collector_factory.get_db_auth_token"
    ) as mocked_db_auth_token:
        mocked_db_auth_token.return_value = "auth_token"

        driver_conf: Dict[str, Any] = {
            "db_host": "localhost",
            "db_port": "3306",
            "db_user": "test_user",
            "db_password": "test_password",
            "db_name": "test_db",
            "db_version": "8.0.22",
            "aws_region": "us-east-1",
            "enable_aws_iam_auth": True,
        }
        expected_db_conf: Dict[str, Any] = {
            "host": "localhost",
            "port": "3306",
            "user": "test_user",
            "password": "auth_token",
            "dbname": "test_db",
        }
        db_conf = create_db_config_postgres(driver_conf)
        assert expected_db_conf == db_conf


def test_db_config_postgres_invalid() -> None:
    driver_conf: Dict[str, Any] = {
        "db_host": "localhost",
        "db_port": "5432",
        "db_user": "test_user",
    }
    with pytest.raises(DriverException) as ex:
        create_db_config_postgres(driver_conf)
    assert (
        "Invalid Postgres database configuration: parameter is not defined"
        in ex.value.message
    )

    driver_conf["db_password"] = "test_password"
    driver_conf["db_conf_extend"] = ["invalid db_conf_extend type"]
    with pytest.raises(DriverException) as ex:
        create_db_config_postgres(driver_conf)
    assert (
        "Invalid Postgres database configuration: db_conf_extend type"
        in ex.value.message
    )

    driver_conf["db_conf_extend"] = {"db_user": "duplicate_user"}
    with pytest.raises(DriverException) as ex:
        create_db_config_postgres(driver_conf)
    assert (
        "Invalid Postgres database configuration: duplicate parameters"
        in ex.value.message
    )


def test_get_postgres_version_success(mock_pg_conn: MagicMock) -> Optional[NoReturn]:
    mock_cursor = mock_pg_conn.cursor.return_value
    mock_cursor.fetchall.return_value = [["9.3.10"]]
    assert get_postgres_version(mock_pg_conn) == "9.3.10"


def test_get_postgres_version_success2(mock_pg_conn: MagicMock) -> Optional[NoReturn]:
    mock_cursor = mock_pg_conn.cursor.return_value
    mock_cursor.fetchall.return_value = [["12 (Ubuntu 12.6-0ubuntu0.20.04.1)"]]
    assert get_postgres_version(mock_pg_conn) == "12"


def test_get_postgres_version_sql_failure(
    mock_pg_conn: MagicMock,
) -> Optional[NoReturn]:
    mock_cursor = mock_pg_conn.cursor.return_value
    mock_cursor.fetchall.side_effect = psycopg2.ProgrammingError("bad query")
    with pytest.raises(PostgresCollectorException) as ex:
        get_postgres_version(mock_pg_conn)
    assert "Failed to get Postgres version" in str(ex.value)


def test_get_mysql_version_success(mock_mysql_conn: MagicMock) -> Optional[NoReturn]:
    mock_cursor = mock_mysql_conn.cursor.return_value
    mock_cursor.fetchall.return_value = [["8.0.22"]]
    assert get_mysql_version(mock_mysql_conn) == "8.0.22"


def test_get_mysql_version_success2(mock_mysql_conn: MagicMock) -> Optional[NoReturn]:
    mock_cursor = mock_mysql_conn.cursor.return_value
    mock_cursor.fetchall.return_value = [["8.0.23-0ubuntu0.20.04.1"]]
    assert get_mysql_version(mock_mysql_conn) == "8.0.23"


def test_get_mysql_version_sql_failure(
    mock_mysql_conn: MagicMock,
) -> Optional[NoReturn]:
    mock_cursor = mock_mysql_conn.cursor.return_value
    mock_cursor.fetchall.side_effect = mysql.connector.ProgrammingError("bad query")
    with pytest.raises(MysqlCollectorException) as ex:
        get_mysql_version(mock_mysql_conn)
    assert "Failed to get MySQL version" in str(ex.value)
