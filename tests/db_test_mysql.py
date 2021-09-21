"""Tests for interacting with MySQL database"""

from typing import Dict, Any
import json

import mysql.connector

from driver.collector.collector_factory import get_mysql_version, connect_mysql
from driver.database import collect_data_from_database
from driver.collector.mysql_collector import MysqlCollector

# pylint: disable=missing-function-docstring


def _db_query(conn: mysql.connector.MySQLConnection, sql: str) -> None:
    conn.cursor().execute(sql)


def _create_user(
    conn: mysql.connector.MySQLConnection, user: str, password: str
) -> None:
    sql = f"CREATE USER IF NOT EXISTS '{user}' IDENTIFIED BY '{password}';"
    _db_query(conn, sql)


def _drop_user(conn: mysql.connector.MySQLConnection, user: str) -> None:
    sql = f" DROP USER IF EXISTS '{user}';"
    _db_query(conn, sql)


def _get_conf(
    mysql_user: str,
    mysql_password: str,
    mysql_host: str,
    mysql_port: str,
    mysql_database: str,
) -> Dict[str, str]:
    conf = {
        "user": mysql_user,
        "password": mysql_password,
        "host": mysql_host,
        "port": mysql_port,
        "database": mysql_database,
    }
    return conf


def _get_driver_conf(
    db_type: str,
    mysql_user: str,
    mysql_password: str,
    mysql_host: str,
    mysql_port: str,
    mysql_database: str,
) -> Dict[str, str]:
    # pylint: disable=too-many-arguments
    conf = {
        "db_user": mysql_user,
        "db_password": mysql_password,
        "db_host": mysql_host,
        "db_port": mysql_port,
        "db_name": mysql_database,
        "db_type": db_type,
        "db_provider": "on_premise",
        "db_key": "test_key",
        "organization_id": "test_organization"
    }
    return conf


def test_mysql_collector_version(
    mysql_user: str,
    mysql_password: str,
    mysql_host: str,
    mysql_port: str,
    mysql_database: str,
) -> None:
    conf = _get_conf(mysql_user, mysql_password, mysql_host, mysql_port, mysql_database)
    conn = connect_mysql(conf)
    version = get_mysql_version(conn)
    collector = MysqlCollector(conn, version)
    conn.close()
    assert collector.get_version() == version


def test_mysql_collector_permission_success(
    mysql_user: str,
    mysql_password: str,
    mysql_host: str,
    mysql_port: str,
    mysql_database: str,
) -> None:
    conf = _get_conf(mysql_user, mysql_password, mysql_host, mysql_port, mysql_database)
    conn = connect_mysql(conf)
    version = get_mysql_version(conn)
    collector = MysqlCollector(conn, version)
    perm_res = collector.check_permission()
    conn.close()
    assert perm_res[1] == []
    assert perm_res[0] is True


def test_mysql_collector_permission_failed(
    mysql_user: str,
    mysql_password: str,
    mysql_host: str,
    mysql_port: str,
    mysql_database: str,
) -> None:
    conf = _get_conf(mysql_user, mysql_password, mysql_host, mysql_port, mysql_database)
    conn = connect_mysql(conf)
    version = get_mysql_version(conn)
    new_user = "ot_test_user"
    new_password = "ot_test_password"
    # additional permissions are not granted for a new test user
    _create_user(conn, new_user, new_password)
    new_conf = _get_conf(new_user, new_password, mysql_host, mysql_port, mysql_database)
    new_conn = connect_mysql(new_conf)
    version = get_mysql_version(conn)
    new_collector = MysqlCollector(new_conn, version)
    perm_res = new_collector.check_permission()

    # drop the test user
    _drop_user(conn, new_user)
    conn.close()
    assert len(perm_res[1]) > 0
    assert perm_res[0] is False


def _verify_mysql_knobs(knobs: Dict[str, Any]) -> None:
    assert int(knobs["global"]["global"]["innodb_buffer_pool_size"]) >= 0
    assert knobs["local"] is None


def test_mysql_collector_knobs(
    mysql_user: str,
    mysql_password: str,
    mysql_host: str,
    mysql_port: str,
    mysql_database: str,
) -> None:
    conf = _get_conf(mysql_user, mysql_password, mysql_host, mysql_port, mysql_database)
    conn = connect_mysql(conf)
    version = get_mysql_version(conn)
    collector = MysqlCollector(conn, version)
    knobs = collector.collect_knobs()
    conn.close()
    # the knob json should not contain any field that cannot be converted to a string,
    # like decimal type and datetime type
    json.dumps(knobs)
    _verify_mysql_knobs(knobs)


def _verify_mysql_metrics(metrics: Dict[str, Any], version_str: str) -> None:
    version = float(".".join(version_str.split(".")[:2]))
    assert int(metrics["global"]["global"]["com_insert"]) >= 0
    assert metrics["global"]["innodb_metrics"]["trx_rw_commits"] >= 0
    assert metrics["global"]["engine"]["innodb_status"] is not None
    assert metrics["global"]["engine"]["replica_status"] is not None
    assert metrics["global"]["engine"]["master_status"] is not None
    latency_hist = metrics["global"]["performance_schema"].get(
        "events_statements_histogram_global"
    )
    if version >= 8:  # events histogram is supported since mysql 8
        assert latency_hist is not None
    else:
        assert latency_hist is None
    assert metrics["local"] is None


def test_mysql_collector_metrics(
    mysql_user: str,
    mysql_password: str,
    mysql_host: str,
    mysql_port: str,
    mysql_database: str,
) -> None:
    conf = _get_conf(mysql_user, mysql_password, mysql_host, mysql_port, mysql_database)
    conn = connect_mysql(conf)
    version_str = get_mysql_version(conn)
    collector = MysqlCollector(conn, version_str)
    metrics = collector.collect_metrics()
    conn.close()
    # the metric json should not contain any field that cannot be converted to a string,
    # like decimal type and datetime type
    json.dumps(metrics)
    _verify_mysql_metrics(metrics, version_str)


def test_collect_data_from_database(
    db_type: str,
    mysql_user: str,
    mysql_password: str,
    mysql_host: str,
    mysql_port: str,
    mysql_database: str,
) -> None:
    # pylint: disable=too-many-arguments
    driver_conf = _get_driver_conf(
        db_type, mysql_user, mysql_password, mysql_host, mysql_port, mysql_database
    )
    observation = collect_data_from_database(driver_conf)
    knobs = observation["knobs_data"]
    metrics = observation["metrics_data"]
    summary = observation["summary"]
    version_str = summary["version"]
    _verify_mysql_knobs(knobs)
    _verify_mysql_metrics(metrics, version_str)
    assert summary["observation_time"] > 0
    assert len(version_str) > 0
