"""Tests for interacting with Postgres database"""
import time
from typing import Dict, Any
import json

from driver.collector.collector_factory import (
    get_collector,
    get_postgres_version,
    connect_postgres,
)
from driver.database import (
    collect_db_level_data_from_database,
    collect_table_level_data_from_database,
)
from driver.collector.postgres_collector import PostgresCollector
from tests.useful_literals import (
    COLUMN_SCHEMA_POSTGRES_COLUMNS,
    FOREIGN_KEY_SCHEMA_POSTGRES_COLUMNS,
    TABLE_LEVEL_PG_STAT_USER_TABLES_COLUMNS,
    TABLE_SCHEMA_POSTGRES_COLUMNS,
    VIEW_SCHEMA_POSTGRES_COLUMNS,
    INDEX_SCHEMA_POSTGRES_COLUMNS,
)

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
    num_table_to_collect_stats: int,
    num_index_to_collect_stats: int,
    lr_query_latency_threshold_min: int,
) -> Dict[str, Any]:
    # pylint: disable=too-many-arguments
    conf = {
        "db_user": pg_user,
        "db_password": pg_password,
        "db_cluster_identifier": "",
        "db_host": pg_host,
        "db_port": pg_port,
        "db_name": pg_database,
        "db_type": db_type,
        "db_provider": "on_premise",
        "db_key": "test_key",
        "organization_id": "test_organization",
        "num_table_to_collect_stats": num_table_to_collect_stats,
        "num_index_to_collect_stats": num_index_to_collect_stats,
        "lr_query_latency_threshold_min": lr_query_latency_threshold_min,
        "db_non_default_parameters": ["test_parameter_1", "test_parameter_2"],
    }
    return conf


def test_postgres_collector_version(
    pg_user: str, pg_password: str, pg_host: str, pg_port: str, pg_database: str
) -> None:
    conf = _get_conf(pg_user, pg_password, pg_host, pg_port, pg_database)
    conn = connect_postgres(conf)
    version = get_postgres_version(conn)
    collector = PostgresCollector({pg_database: conn}, pg_database, version)
    conn.close()
    assert collector.get_version() == version


def test_postgres_collector_permission(
    pg_user: str, pg_password: str, pg_host: str, pg_port: str, pg_database: str
) -> None:
    conf = _get_conf(pg_user, pg_password, pg_host, pg_port, pg_database)
    conn = connect_postgres(conf)
    version = get_postgres_version(conn)
    collector = PostgresCollector({pg_database: conn}, pg_database, version)
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
    collector = PostgresCollector({pg_database: conn}, pg_database, version)
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
    collector = PostgresCollector({pg_database: conn}, pg_database, version)
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
        db_type, pg_user, pg_password, pg_host, pg_port, pg_database, 10, 100, 5
    )
    observation = collect_db_level_data_from_database(driver_conf)
    knobs = observation["knobs_data"]
    metrics = observation["metrics_data"]
    summary = observation["summary"]
    row_num_stats = observation["row_num_stats"]
    version_str = summary["version"]
    _verify_postgres_knobs(knobs)
    _verify_postgres_metrics(metrics)
    assert summary["observation_time"] > 0
    assert len(version_str) > 0
    assert len(row_num_stats) == 10


def test_postgres_collect_row_stats(
    pg_user: str, pg_password: str, pg_host: str, pg_port: str, pg_database: str
) -> None:
    conf = _get_conf(pg_user, pg_password, pg_host, pg_port, pg_database)
    conn = connect_postgres(conf)
    version = get_postgres_version(conn)
    collector = PostgresCollector({pg_database: conn}, pg_database, version)
    row_stats = collector.collect_table_row_number_stats()
    conn.close()
    # the metric json should not contain any field that cannot be converted to a string,
    # like decimal type and datetime type
    json.dumps(row_stats)
    assert row_stats["num_tables"] >= 0
    assert row_stats["num_empty_tables"] >= 0
    assert row_stats["num_tables_row_count_0_10k"] >= 0
    assert row_stats["num_tables_row_count_10k_100k"] >= 0
    assert row_stats["num_tables_row_count_100k_1m"] >= 0
    assert row_stats["num_tables_row_count_1m_10m"] >= 0
    assert row_stats["num_tables_row_count_10m_100m"] >= 0
    assert row_stats["num_tables_row_count_100m_inf"] >= 0
    assert (
        row_stats["num_tables"]
        == row_stats["num_empty_tables"]
        + row_stats["num_tables_row_count_0_10k"]
        + row_stats["num_tables_row_count_10k_100k"]
        + row_stats["num_tables_row_count_100k_1m"]
        + row_stats["num_tables_row_count_1m_10m"]
        + row_stats["num_tables_row_count_10m_100m"]
        + row_stats["num_tables_row_count_100m_inf"]
    )
    if row_stats["num_tables"] == 0:
        assert row_stats["min_row_num"] is None
        assert row_stats["max_row_num"] is None
    else:
        assert row_stats["min_row_num"] >= 0
        assert row_stats["max_row_num"] >= 0


def _verify_postgres_table_level_data(data: Dict[str, Any], table_nums: int) -> None:
    # pg_stat_user_tables_all_fields
    assert data["pg_stat_user_tables_all_fields"][
        "columns"
    ] == TABLE_LEVEL_PG_STAT_USER_TABLES_COLUMNS + ["logical_database_name"]
    assert len(data["pg_stat_user_tables_all_fields"]["rows"]) == table_nums
    for row in data["pg_stat_user_tables_all_fields"]["rows"]:
        assert len(row) == 23

    # pg_statio_user_tables_all_fields
    assert data["pg_statio_user_tables_all_fields"]["columns"] == [
        "relid",
        "schemaname",
        "relname",
        "heap_blks_read",
        "heap_blks_hit",
        "idx_blks_read",
        "idx_blks_hit",
        "toast_blks_read",
        "toast_blks_hit",
        "tidx_blks_read",
        "tidx_blks_hit",
        "logical_database_name",
    ]
    assert len(data["pg_statio_user_tables_all_fields"]["rows"]) == table_nums
    for row in data["pg_statio_user_tables_all_fields"]["rows"]:
        assert len(row) == 12

    # pg_stat_user_tables_table_sizes
    assert data["pg_stat_user_tables_table_sizes"]["columns"] == [
        "relid",
        "indexes_size",
        "relation_size",
        "toast_size",
        "logical_database_name",
    ]
    assert len(data["pg_stat_user_tables_table_sizes"]["rows"]) == table_nums
    for row in data["pg_stat_user_tables_table_sizes"]["rows"]:
        assert len(row) == 5

    # table_bloat_ratios
    assert data["table_bloat_ratios"]["columns"] == [
        "relid",
        "bloat_ratio",
        "logical_database_name",
    ]
    assert len(data["table_bloat_ratios"]["rows"]) == table_nums
    for row in data["table_bloat_ratios"]["rows"]:
        assert len(row) == 3


def test_collect_table_level_data_from_database(
    db_type: str,
    pg_user: str,
    pg_password: str,
    pg_host: str,
    pg_port: str,
    pg_database: str,
) -> None:
    # pylint: disable=too-many-arguments,too-many-locals
    num_table_to_collect_stats = 10
    num_index_to_collect_stats = 10
    lr_query_latency_threshold_min = 2
    conf = _get_conf(pg_user, pg_password, pg_host, pg_port, pg_database)
    conn = connect_postgres(conf)

    # create three tables
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS test1 (id serial PRIMARY KEY, num integer, data varchar);",
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS test2 (id serial PRIMARY KEY, num integer, data varchar);",
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS test3 (id serial PRIMARY KEY, num integer, data varchar);",
    )

    cur.execute(
        "INSERT INTO test1(id, num, data) values (1, 2, 'abc') ON CONFLICT DO NOTHING;"
    )
    cur.execute(
        "INSERT INTO test2(id, num, data) values (1, 2, 'abc') ON CONFLICT DO NOTHING;"
    )

    time.sleep(1)
    cur.execute("ANALYZE;")

    driver_conf = _get_driver_conf(
        db_type,
        pg_user,
        pg_password,
        pg_host,
        pg_port,
        pg_database,
        num_table_to_collect_stats,
        num_index_to_collect_stats,
        lr_query_latency_threshold_min,
    )
    # pylint: disable=too-many-function-args
    observation = collect_table_level_data_from_database(driver_conf)
    data = observation["data"]
    summary = observation["summary"]
    version_str = summary["version"]
    assert summary["observation_time"] > 0
    assert len(version_str) > 0
    # 0 as the database is empty
    _verify_postgres_table_level_data(data, 3)


def test_postgres_collect_table_level_metrics(
    pg_user: str, pg_password: str, pg_host: str, pg_port: str, pg_database: str
) -> None:
    num_table_to_collect_stats = 10
    num_index_to_collect_stats = 100
    conf = _get_conf(pg_user, pg_password, pg_host, pg_port, pg_database)
    conn = connect_postgres(conf)

    # create three tables
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS test1 (id serial PRIMARY KEY, num integer, data varchar);",
    )

    cur.execute(
        "CREATE TABLE IF NOT EXISTS test2 (id serial PRIMARY KEY, num integer, data varchar);",
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS test3 (id serial PRIMARY KEY, num integer, data varchar);",
    )

    cur.execute(
        "INSERT INTO test1(id, num, data) values (1, 2, 'abc') ON CONFLICT DO NOTHING;"
    )
    cur.execute(
        "INSERT INTO test2(id, num, data) values (1, 2, 'abc') ON CONFLICT DO NOTHING;"
    )

    time.sleep(1)
    cur.execute("ANALYZE;")

    version = get_postgres_version(conn)
    collector = PostgresCollector({pg_database: conn}, pg_database, version)
    target_table_info = collector.get_target_table_info(num_table_to_collect_stats)
    metrics = collector.collect_table_level_metrics(target_table_info)
    metrics.update(
        collector.collect_index_metrics(target_table_info, num_index_to_collect_stats)
    )

    _verify_postgres_table_level_data(metrics, 3)


def test_postgres_collect_index_metrics(
    pg_user: str, pg_password: str, pg_host: str, pg_port: str, pg_database: str
) -> None:
    num_table_to_collect_stats = 10
    num_index_to_collect_stats = 100
    conf = _get_conf(pg_user, pg_password, pg_host, pg_port, pg_database)
    conn = connect_postgres(conf)

    # create three tables
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS test1 (id serial PRIMARY KEY, num integer, data varchar);",
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS test2 (id serial PRIMARY KEY, num integer, data varchar);",
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS test3 (id serial PRIMARY KEY, num integer, data varchar);",
    )
    cur.execute(
        "INSERT INTO test1(id, num, data) values (1, 2, 'abc') ON CONFLICT DO NOTHING;"
    )
    cur.execute(
        "INSERT INTO test2(id, num, data) values (1, 2, 'abc') ON CONFLICT DO NOTHING;"
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_test1 ON test1(num);")

    time.sleep(1)
    cur.execute("ANALYZE;")
    version = get_postgres_version(conn)
    collector = PostgresCollector({pg_database: conn}, pg_database, version)
    target_table_info = collector.get_target_table_info(num_table_to_collect_stats)
    metrics = collector.collect_index_metrics(
        target_table_info, num_index_to_collect_stats
    )

    index_names = [row[4] for row in metrics["pg_stat_user_indexes_all_fields"]["rows"]]
    assert "idx_test1" in index_names


def test_postgres_collect_query_metrics(
    pg_user: str, pg_password: str, pg_host: str, pg_port: str, pg_database: str
) -> None:
    num_query_to_collect_stats = 10
    conf = _get_conf(pg_user, pg_password, pg_host, pg_port, pg_database)
    conn = connect_postgres(conf)
    version = get_postgres_version(conn)
    collector = PostgresCollector({pg_database: conn}, pg_database, version)
    metrics = collector.collect_query_metrics(num_query_to_collect_stats)

    # pg_stat_statements currently doesn't work for integration test.
    # So this is a placeholder.
    assert metrics["pg_stat_statements"]


def test_postgres_collect_schema(
    pg_user: str, pg_password: str, pg_host: str, pg_port: str, pg_database: str
) -> None:
    conf = _get_conf(pg_user, pg_password, pg_host, pg_port, pg_database)
    conn = connect_postgres(conf)

    # create three tables
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS test1 (id serial PRIMARY KEY, num integer, data varchar);",
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS test2 (id serial PRIMARY KEY, num integer, data varchar);",
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS test3 (id serial PRIMARY KEY, num integer, data varchar);",
    )
    cur.execute(
        "INSERT INTO test1(id, num, data) values (1, 2, 'abc') ON CONFLICT DO NOTHING;"
    )
    cur.execute(
        "INSERT INTO test2(id, num, data) values (1, 2, 'abc') ON CONFLICT DO NOTHING;"
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_test1 ON test1(num);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_test2 ON test2(id, num);")

    time.sleep(1)
    cur.execute("ANALYZE;")

    version = get_postgres_version(conn)
    collector = PostgresCollector({pg_database: conn}, pg_database, version)
    schema = collector.collect_schema()
    assert schema["index_columns"]["rows"][1][2] == "num"
    assert schema["index_columns"]["rows"][3][2] == "id,num"
    _verify_postgres_schema(schema)


def _verify_postgres_schema(schema: Dict[str, Any]) -> None:
    assert schema["columns"]["columns"] == COLUMN_SCHEMA_POSTGRES_COLUMNS
    for row in schema["columns"]["rows"]:
        assert len(row) == len(COLUMN_SCHEMA_POSTGRES_COLUMNS)

    assert schema["foreign_keys"]["columns"] == FOREIGN_KEY_SCHEMA_POSTGRES_COLUMNS
    for row in schema["foreign_keys"]["rows"]:
        assert len(row) == len(FOREIGN_KEY_SCHEMA_POSTGRES_COLUMNS)

    assert schema["tables"]["columns"] == TABLE_SCHEMA_POSTGRES_COLUMNS
    for row in schema["tables"]["rows"]:
        assert len(row) == len(TABLE_SCHEMA_POSTGRES_COLUMNS)

    assert schema["views"]["columns"] == VIEW_SCHEMA_POSTGRES_COLUMNS
    for row in schema["views"]["rows"]:
        assert len(row) == len(VIEW_SCHEMA_POSTGRES_COLUMNS)

    assert schema["indexes"]["columns"] == INDEX_SCHEMA_POSTGRES_COLUMNS
    for row in schema["indexes"]["rows"]:
        assert len(row) == len(INDEX_SCHEMA_POSTGRES_COLUMNS)


def test_pg_name_list_accepted(
    db_type: str,
    pg_user: str,
    pg_password: str,
    pg_host: str,
    pg_port: str,
    pg_database: str,
) -> None:
    # pylint: disable=too-many-arguments
    pg_database = "postgres, postgres"
    driver_conf = _get_driver_conf(
        db_type, pg_user, pg_password, pg_host, pg_port, pg_database, 10, 100, 5
    )
    with get_collector(driver_conf) as collector:
        # pyre-ignore[16]
        assert len(collector._conns) == 1  # pylint: disable=no-member, protected-access
        assert (
            # pylint: disable=no-member, protected-access
            collector._conns["postgres"]
            is not None
        )  # pylint: disable=no-member, protected-access

    pg_database = "postgres"
    driver_conf = _get_driver_conf(
        db_type, pg_user, pg_password, pg_host, pg_port, pg_database, 10, 100, 5
    )
    with get_collector(driver_conf) as collector:
        assert len(collector._conns) == 1  # pylint: disable=no-member, protected-access
        assert (
            collector._conns["postgres"] is not None
        )  # pylint: disable=no-member, protected-access
