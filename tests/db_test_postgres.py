"""Tests for interacting with Postgres database"""

from typing import Dict, Any
import json

from driver.collector.collector_factory import get_postgres_version, connect_postgres
from driver.database import collect_data_from_database
from driver.collector.postgres_collector import PostgresCollector

# pylint: disable=missing-function-docstring


def _get_conf(
    pg_user: str, pg_password: str, pg_host: str, pg_port: str, pg_database: str
) -> Dict[str, str]:
    conf = {
        "user": pg_user,
        "password": pg_password,
        "host": pg_host,
        "port": pg_port,
        "dbname": pg_database,
    }
    return conf


def _get_driver_conf(
    db_type: str,
    pg_user: str,
    pg_password: str,
    pg_host: str,
    pg_port: str,
    pg_database: str,
) -> Dict[str, str]:
    # pylint: disable=too-many-arguments
    conf = {
        "db_user": pg_user,
        "db_password": pg_password,
        "db_host": pg_host,
        "db_port": pg_port,
        "db_name": pg_database,
        "db_type": db_type,
        "db_provider": "on_premise",
        "db_key": "test_key",
        "organization_id": "test_organization",
    }
    return conf


def test_postgres_collector_version(
    pg_user: str, pg_password: str, pg_host: str, pg_port: str, pg_database: str
) -> None:
    conf = _get_conf(pg_user, pg_password, pg_host, pg_port, pg_database)
    conn = connect_postgres(conf)
    version = get_postgres_version(conn)
    collector = PostgresCollector(conn, version)
    conn.close()
    assert collector.get_version() == version


def test_postgres_collector_permission(
    pg_user: str, pg_password: str, pg_host: str, pg_port: str, pg_database: str
) -> None:
    conf = _get_conf(pg_user, pg_password, pg_host, pg_port, pg_database)
    conn = connect_postgres(conf)
    version = get_postgres_version(conn)
    collector = PostgresCollector(conn, version)
    perm_res = collector.check_permission()
    conn.close()
    assert perm_res[1] == []
    assert perm_res[0] is True


def _verify_postgres_knobs(knobs: Dict[str, Any]) -> None:
    assert int(knobs["global"]["global"]["shared_buffers"]) >= 0
    assert knobs["local"] is None


def test_postgres_collector_knobs(
    pg_user: str, pg_password: str, pg_host: str, pg_port: str, pg_database: str
) -> None:
    conf = _get_conf(pg_user, pg_password, pg_host, pg_port, pg_database)
    conn = connect_postgres(conf)
    version = get_postgres_version(conn)
    collector = PostgresCollector(conn, version)
    knobs = collector.collect_knobs()
    conn.close()
    # the knob json should not contain any field that cannot be converted to a string,
    # like decimal type and datetime type
    json.dumps(knobs)
    _verify_postgres_knobs(knobs)


def _verify_postgres_metrics(metrics: Dict[str, Any]) -> None:
    assert metrics["global"]["pg_stat_archiver"]["archived_count"] >= 0
    assert metrics["global"]["pg_stat_bgwriter"]["checkpoints_req"] >= 0
    assert metrics["local"]["database"]["pg_stat_database"] is not None
    assert metrics["local"]["database"]["pg_stat_database_conflicts"] is not None
    assert metrics["local"]["table"]["pg_stat_user_tables"] is not None
    assert metrics["local"]["table"]["pg_statio_user_tables"] is not None
    assert metrics["local"]["index"]["pg_stat_user_indexes"] is not None
    assert metrics["local"]["index"]["pg_statio_user_indexes"] is not None


def test_postgres_collector_metrics(
    pg_user: str, pg_password: str, pg_host: str, pg_port: str, pg_database: str
) -> None:
    conf = _get_conf(pg_user, pg_password, pg_host, pg_port, pg_database)
    conn = connect_postgres(conf)
    version = get_postgres_version(conn)
    collector = PostgresCollector(conn, version)
    metrics = collector.collect_metrics()
    conn.close()
    # the metric json should not contain any field that cannot be converted to a string,
    # like decimal type and datetime type
    json.dumps(metrics)
    _verify_postgres_metrics(metrics)


def test_collect_data_from_database(
    db_type: str,
    pg_user: str,
    pg_password: str,
    pg_host: str,
    pg_port: str,
    pg_database: str,
) -> None:
    # pylint: disable=too-many-arguments
    driver_conf = _get_driver_conf(
        db_type, pg_user, pg_password, pg_host, pg_port, pg_database
    )
    observation = collect_data_from_database(driver_conf)
    knobs = observation["knobs_data"]
    metrics = observation["metrics_data"]
    summary = observation["summary"]
    version_str = summary["version"]
    _verify_postgres_knobs(knobs)
    _verify_postgres_metrics(metrics)
    assert summary["observation_time"] > 0
    assert len(version_str) > 0
