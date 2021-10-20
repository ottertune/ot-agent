"""Tests for interacting with Postgres database locally"""
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, NoReturn, List, Callable
from unittest.mock import MagicMock, PropertyMock
import mock
import psycopg2
import pytest
from driver.collector.postgres_collector import PostgresCollector
from driver.collector.postgres_collector import (
    DATABASE_STAT, DATABASE_CONFLICTS_STAT, TABLE_STAT,
    TABLE_STATIO, INDEX_STAT, INDEX_STATIO
)

from driver.exceptions import PostgresCollectorException

# pylint: disable=missing-function-docstring


@dataclass
class SqlData:
    """
    Used for providing a set of mock data when collector is collecting metrics. The returned
    data doesn't reflect the real columns and rows we'd see in postgres. It is
    mainly used to validate that we are getting the appropriate response back.
    """

    views: Dict[str, List[List[Any]]]
    aggregated_views: Dict[str, List[List[int]]]
    metas: Dict[str, List[List[str]]]
    aggregated_metas: Dict[str, List[List[str]]]
    local_metrics: Dict[str, Any]
    aggregated_local_metrics: Dict[str, Any]


    def __init__(self) -> None:
        self.views = {
            "pg_stat_archiver": [["g1", 1]],
            "pg_stat_bgwriter": [["g2", 2]],
            "pg_stat_database": [["dl1", 1, 1], ["dl2", 2, 2]],
            "pg_stat_database_conflicts": [["dl3", 3, 3], ["dl4", 4, 4]],
            "pg_stat_user_tables": [["tl1", datetime(2020, 1, 1, 0, 0, 0, 0), 1]],
            "pg_statio_user_tables": [["tl2", datetime(2020, 1, 1, 0, 0, 0, 0), 2]],
            "pg_stat_user_indexes": [["il1", 1, 1]],
            "pg_statio_user_indexes": [["il2", 2, 2]],
        }
        self.aggregated_views = {
            "pg_stat_database": [[1, 1]],
            "pg_stat_database_conflicts": [[2, 2]],
            "pg_stat_user_tables": [[3, 3]],
            "pg_statio_user_tables": [[4, 4]],
            "pg_stat_user_indexes": [[5, 5]],
            "pg_statio_user_indexes": [[6, 6]],

        }
        self.metas = {
            "pg_stat_archiver": [["global"], ["global_count"]],
            "pg_stat_bgwriter": [["global"], ["global_count"]],
            "pg_stat_database": [["datname"], ["local_count"], ["datid"]],
            "pg_stat_database_conflicts": [["datname"], ["local_count"], ["datid"]],
            "pg_stat_user_tables": [["relname"], ["table_date"], ["relid"]],
            "pg_statio_user_tables": [["relname"], ["table_date"], ["relid"]],
            "pg_stat_user_indexes": [["relname"], ["local_count"], ["indexrelid"]],
            "pg_statio_user_indexes": [["relname"], ["local_count"], ["indexrelid"]],
        }
        self.aggregated_metas = {
            "pg_stat_database": [["local_count"], ["local_count2"]],
            "pg_stat_database_conflicts": [["local_count"], ["local_count2"]],
            "pg_stat_user_tables": [["local_count"], ["local_count2"]],
            "pg_statio_user_tables": [["local_count"], ["local_count2"]],
            "pg_stat_user_indexes": [["local_count"], ["local_count2"]],
            "pg_statio_user_indexes": [["local_count"], ["local_count2"]],
        }
        self.local_metrics = {
            "database": {
                "pg_stat_database": {
                    1: {
                        "datname": "dl1",
                        "local_count": 1,
                        "datid": 1
                    },
                    2: {
                        "datname": "dl2",
                        "local_count": 2,
                        "datid": 2
                    },
                },
                "pg_stat_database_conflicts": {
                    3: {
                        "datname": "dl3",
                        "local_count": 3,
                        "datid": 3
                    },
                    4: {
                        "datname": "dl4",
                        "local_count": 4,
                        "datid": 4
                    },
                },
            },
            "table": {
                "pg_stat_user_tables": {
                    1: {
                        "relname": "tl1",
                        "table_date": "2020-01-01T00:00:00",
                        "relid": 1
                    }
                },
                "pg_statio_user_tables": {
                    2: {
                        "relname": "tl2",
                        "table_date": "2020-01-01T00:00:00",
                        "relid": 2
                    }
                },
            },
            "index": {
                "pg_stat_user_indexes": {
                    1: {
                        "relname": "il1",
                        "local_count": 1,
                        "indexrelid": 1
                    }
                },
                "pg_statio_user_indexes": {
                    2: {
                        "relname": "il2",
                        "local_count": 2,
                        "indexrelid": 2
                    }
                },
            }
        }
        self.aggregated_local_metrics = {
            "database": {
                "pg_stat_database": {
                    "aggregated": {
                        "local_count": 1,
                        "local_count2": 1
                    },
                },
                "pg_stat_database_conflicts": {
                    "aggregated": {
                        "local_count": 2,
                        "local_count2": 2
                    }
                }
            },
            "table": {
                "pg_stat_user_tables": {
                    "aggregated": {
                        "local_count": 3,
                        "local_count2": 3
                    }
                },
                "pg_statio_user_tables": {
                    "aggregated": {
                        "local_count": 4,
                        "local_count2": 4
                    }
                }
            },
            "index": {
                "pg_stat_user_indexes": {
                    "aggregated": {
                        "local_count": 5,
                        "local_count2": 5
                    }
                },
                "pg_statio_user_indexes": {
                    "aggregated": {
                        "local_count": 6,
                        "local_count2": 6
                    }
                }
            }
        }

    # @staticmethod
    def expected_default_result(self) -> Dict[str, Any]:
        return {
            "global": {
                "pg_stat_archiver": {"global": "g1", "global_count": 1},
                "pg_stat_bgwriter": {"global": "g2", "global_count": 2},
            },
            "local": self.aggregated_local_metrics
        }


class Result:
    def __init__(self) -> None:
        self.value: Optional[List[Any]] = None
        self.meta: List[List[str]] = []


def get_sql_api(data: SqlData, result: Result) -> Callable[[str], NoReturn]:
    """
    Used for providing a fake sql endpoint so we can return test data
    """

    def table_to_sql(tbl: str) -> str:
        return f"SELECT * FROM {tbl};"

    def sql_fn(sql: str) -> NoReturn:
        # pylint: disable=too-many-branches

        if sql == table_to_sql("pg_stat_archiver"):
            result.value = data.views["pg_stat_archiver"]
            result.meta = data.metas["pg_stat_archiver"]
        elif sql == table_to_sql("pg_stat_bgwriter"):
            result.value = data.views["pg_stat_bgwriter"]
            result.meta = data.metas["pg_stat_bgwriter"]
        elif sql == table_to_sql("pg_stat_database"):
            result.value = data.views["pg_stat_database"]
            result.meta = data.metas["pg_stat_database"]
        elif sql == DATABASE_STAT:
            result.value = data.aggregated_views["pg_stat_database"]
            result.meta = data.aggregated_metas["pg_stat_database"]
        elif sql == table_to_sql("pg_stat_database_conflicts"):
            result.value = data.views["pg_stat_database_conflicts"]
            result.meta = data.metas["pg_stat_database_conflicts"]
        elif sql == DATABASE_CONFLICTS_STAT:
            result.value = data.aggregated_views["pg_stat_database_conflicts"]
            result.meta = data.aggregated_metas["pg_stat_database_conflicts"]
        elif sql == table_to_sql("pg_stat_user_tables"):
            result.value = data.views["pg_stat_user_tables"]
            result.meta = data.metas["pg_stat_user_tables"]
        elif sql == TABLE_STAT:
            result.value = data.aggregated_views["pg_stat_user_tables"]
            result.meta = data.aggregated_metas["pg_stat_user_tables"]
        elif sql == table_to_sql("pg_statio_user_tables"):
            result.value = data.views["pg_statio_user_tables"]
            result.meta = data.metas["pg_statio_user_tables"]
        elif sql == TABLE_STATIO:
            result.value = data.aggregated_views["pg_statio_user_tables"]
            result.meta = data.aggregated_metas["pg_statio_user_tables"]
        elif sql == table_to_sql("pg_stat_user_indexes"):
            result.value = data.views["pg_stat_user_indexes"]
            result.meta = data.metas["pg_stat_user_indexes"]
        elif sql == INDEX_STAT:
            result.value = data.aggregated_views["pg_stat_user_indexes"]
            result.meta = data.aggregated_metas["pg_stat_user_indexes"]
        elif sql == table_to_sql("pg_statio_user_indexes"):
            result.value = data.views["pg_statio_user_indexes"]
            result.meta = data.metas["pg_statio_user_indexes"]
        elif sql == INDEX_STATIO:
            result.value = data.aggregated_views["pg_statio_user_indexes"]
            result.meta = data.aggregated_metas["pg_statio_user_indexes"]
        else:
            raise Exception(f"Unknown sql: {sql}")

    return sql_fn


@pytest.fixture(name="mock_conn")
@mock.patch("psycopg2.connect")
def _mock_conn(mock_connect: MagicMock) -> MagicMock:
    return mock_connect.return_value


def test_get_version(mock_conn: MagicMock) -> NoReturn:
    collector = PostgresCollector(mock_conn, "9.6.3")
    version = collector.get_version()
    assert version == "9.6.3"


def test_collect_knobs_success(mock_conn: MagicMock) -> NoReturn:
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.return_value = [
        ["autovacuum_max_workers", 7],
        ["some_date", datetime(2020, 1, 1, 0, 0, 0, 0)],
    ]
    expected = {
        "global": {
            "global": {"autovacuum_max_workers": 7, "some_date": "2020-01-01T00:00:00"}
        },
        "local": None,
    }
    collector = PostgresCollector(mock_conn, "9.6.3")
    assert collector.collect_knobs() == expected


def test_collect_knobs_sql_failure(mock_conn: MagicMock) -> NoReturn:
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.side_effect = psycopg2.ProgrammingError("bad query")
    collector = PostgresCollector(mock_conn, "9.6.3")
    with pytest.raises(PostgresCollectorException) as ex:
        collector.collect_knobs()
    assert "Failed to execute sql" in ex.value.message


def test_check_permission_success(mock_conn: MagicMock) -> NoReturn:
    # Even with sql failure, we should still return true as we don't do anything
    # in check_permission for now
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.side_effect = psycopg2.ProgrammingError("bad query")
    collector = PostgresCollector(mock_conn, "9.6.3")
    assert collector.check_permission() == (True, [], "")


def test_collect_metrics_success(mock_conn: MagicMock) -> NoReturn:
    mock_cursor = mock_conn.cursor.return_value
    data = SqlData()
    result = Result()
    mock_cursor.execute.side_effect = get_sql_api(data, result)
    mock_cursor.fetchall.side_effect = lambda: result.value
    type(mock_cursor).description = PropertyMock(side_effect=lambda: result.meta)
    collector = PostgresCollector(mock_conn, "9.6.3")
    assert collector.collect_metrics() == data.expected_default_result()


def test_collect_metrics_sql_failure(mock_conn: MagicMock) -> NoReturn:
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.side_effect = psycopg2.ProgrammingError("bad query")
    collector = PostgresCollector(mock_conn, "9.6.3")
    with pytest.raises(PostgresCollectorException) as ex:
        collector.collect_metrics()
    assert "Failed to execute sql" in ex.value.message
