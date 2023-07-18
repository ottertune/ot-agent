"""Tests for interacting with Postgres database locally"""
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, NoReturn, List, Callable, Tuple
from unittest.mock import MagicMock, PropertyMock

import mock
import psycopg2
import pytest
from driver.collector.postgres_collector import (
    PostgresCollector,
    DATABASE_STAT,
    DATABASE_CONFLICTS_STAT,
    TABLE_STAT,
    TABLE_STATIO,
    INDEX_STAT,
    INDEX_STATIO,
    ROW_NUM_STAT,
    PG_STAT_STATEMENTS_MODULE_QUERY,
    VACUUM_PROGRESS_STAT,
    VACUUM_ACTIVITY_STAT,
    VACUUM_USER_TABLES_STAT,
    QUERY_COLUMNS_SCHEMA_SQL_TEMPLATE,
    QUERY_INDEX_SCHEMA_SQL_TEMPLATE,
    QUERY_FOREIGN_KEY_SCHEMA_SQL_TEMPLATE,
    QUERY_TABLE_SCHEMA_SQL_TEMPLATE,
    QUERY_VIEW_SCEHMA_SQL_TEMPLATE,
    QUERY_INDEX_SCHEMA_COLUMN_NAMES,
)
from driver.collector.pg_table_level_stats_sqls import (
    TOP_N_LARGEST_TABLES_SQL_TEMPLATE,
    PG_STAT_TABLE_STATS_TEMPLATE,
    PG_STATIO_TABLE_STATS_TEMPLATE,
    TABLE_SIZE_TABLE_STATS_TEMPLATE,
    TOP_N_LARGEST_INDEXES_SQL_TEMPLATE,
    PG_INDEX_TEMPLATE,
    PG_STATIO_USER_INDEXES_TEMPLATE,
    PG_STAT_USER_INDEXES_TEMPLATE,
    PG_QUERY_STATS_SQL_TEMPLATE,
)

from driver.exceptions import PostgresCollectorException
from tests.useful_literals import TABLE_LEVEL_PG_STAT_USER_TABLES_COLUMNS

# pylint: disable=missing-function-docstring,too-many-lines


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

    def __init__(self) -> None:
        self.views = {  # pyre-ignore[8]
            "pg_stat_archiver": [["g1", 1]],
            "pg_stat_bgwriter": [["g2", 2]],
            "pg_stat_database": [["dl1", 1, 1], ["dl2", 2, 2]],
            "pg_stat_database_conflicts": [["dl3", 3, 3], ["dl4", 4, 4]],
            "pg_stat_user_tables": [["tl1", datetime(2020, 1, 1, 0, 0, 0, 0), 1]],
            "pg_stat_user_tables_raw": [["tl1", datetime(2020, 1, 1, 0, 0, 0, 0), 1]],
            "pg_statio_user_tables": [["tl2", datetime(2020, 1, 1, 0, 0, 0, 0), 2]],
            "pg_stat_user_indexes": [["il1", 1, 1]],
            "pg_statio_user_indexes": [["il2", 2, 2]],
            "pg_stat_statements": [[123, 2, 1.5]],
            "pg_stat_activity_vacuum": [
                [1, "dl1", "act1", "vacuum tl1;"],
                [2, "dl2", "act2", "autovacuum: VACUUM ANALYZE dl2.tl2"],
            ],
            "pg_stat_activity_pre_pg_14": [
                [
                    7123,
                    "2023-05-09 18:30:26.616736+00",
                    "2023-05-09 18:30:26.616736+00",
                    16401,
                    "ottertunedb",
                    "idle",
                    "test_state_change",
                    "test_wait_event",
                    "test_wait_event_type",
                    "test_backend_type",
                    "2023-05-09 18:30:26.616736+00"
                ],
                [
                    7124,
                    "2023-05-09 18:30:26.616736+00",
                    "2023-05-09 18:30:26.616736+00",
                    16401,
                    "ottertunedb",
                    "active",
                    "test_state_change",
                    "test_wait_event",
                    "test_wait_event_type",
                    "test_backend_type",
                    "2023-05-09 18:30:26.616736+00"
                ],
            ],
            "pg_stat_activity": [
                [
                    7123,
                    -123456789123,
                    "2023-05-09 18:30:26.616736+00",
                    "2023-05-09 18:30:26.616736+00",
                    16401,
                    "ottertunedb",
                    "idle",
                    "test_state_change",
                    "test_wait_event",
                    "test_wait_event_type",
                    "test_backend_type",
                    "2023-05-09 18:30:26.616736+00"
                ],
                [
                    7124,
                    -123456789124,
                    "2023-05-09 18:30:26.616736+00",
                    "2023-05-09 18:30:26.616736+00",
                    16401,
                    "ottertunedb",
                    "active",
                    "test_state_change",
                    "test_wait_event",
                    "test_wait_event_type",
                    "test_backend_type",
                    "2023-05-09 18:30:26.616736+00"
                ],
            ],
            "pg_stat_progress_vacuum": [[1, "tl1", "phase1"], [2, "tl2", "phase2"]],
            "row_stats": [(2111, 1925, 72, 13, 30, 41, 30, 0, 42817478, 0)],
            "top_tables": [(1234,)],
            "table_level_pg_stat_user_tables": [
                (
                    1544350,
                    "public",
                    "partitionednumericmetric_partition_145",
                    12,
                    156742344,
                    124901,
                    8462823118,
                    15243454,
                    0,
                    0,
                    0,
                    41303336,
                    0,
                    1258272,
                    None,
                    None,
                    datetime(2022, 3, 13, 4, 58, 49, 479706, tzinfo=timezone.utc),
                    datetime(2022, 3, 28, 16, 35, 57, 602308, tzinfo=timezone.utc),
                    0,
                    0,
                    1,
                    9,
                ),
                (
                    1544351,
                    "public",
                    "partitionednumericmetric_partition_0",
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    None,
                    None,
                    datetime(2022, 3, 13, 4, 58, 49, 479706, tzinfo=timezone.utc),
                    datetime(2022, 3, 28, 16, 35, 57, 602308, tzinfo=timezone.utc),
                    0,
                    0,
                    0,
                    0,
                ),
            ],
            "table_level_pg_statio_user_tables": [
                (
                    1544350,
                    "public",
                    "partitionednumericmetric_partition_145",
                    32141302,
                    152493441,
                    10916736,
                    171887644,
                    None,
                    None,
                    None,
                    None,
                ),
                (
                    1544351,
                    "public",
                    "partitionednumericmetric_partition_0",
                    0,
                    0,
                    0,
                    0,
                    None,
                    None,
                    None,
                    None,
                ),
            ],
            "table_level_table_size": [
                (
                    1544350,
                    3761643520,
                    2487918592,
                    630784,
                ),
                (
                    1544351,
                    0,
                    0,
                    0,
                ),
            ],
            "table_level_bloat_ratio": [
                (
                    1234,
                    324633,
                    44150088.0,
                    8192.0,
                    24,
                    100,
                    False,
                    24.0,
                    23.0,
                    8,
                ),
            ],
            "pg_stat_user_indexes_top_n": [(24889, 16384)],
            "pg_stat_user_indexes_all_fields": [
                (
                    24882,
                    24889,
                    "public",
                    "test1",
                    "test1_pkey",
                    3,
                    2,
                    2,
                )
            ],
            "pg_statio_user_indexes_all_fields": [
                (
                    24889,
                    3,
                    7,
                )
            ],
            "pg_index_all_fields": [
                (
                    24889,
                    24882,
                    1,
                    1,
                    True,
                    True,
                    False,
                    True,
                    False,
                    True,
                    False,
                    True,
                    True,
                    False,
                    1,
                    0,
                    1978,
                    0,
                    None,
                    "{BOOLEXPR :boolop not :args ({VAR :varno 1 :varattno 4 "
                    ":vartype 16 :vartypmod -1 :varcollid 0 :varlevelsup 0 "
                    ":varnoold 1 :varoattno 4 :location 135}) :location 131}",
                )
            ],
            "pg_stat_statements_all_fields": [
                10,
                16384,
                2067594330036860870,
                'FETCH 50 IN "query-cursor_1"',
                6057292,
                302864600,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ],
            "column_schema": [
                [66764, "last_value", "bigint", None, True, None, "", "p", None, None]
            ],
            "index_schema": [
                [
                    33955,
                    33967,
                    "customers_pk",
                    True,
                    True,
                    False,
                    True,
                    "CREATE UNIQUE INDEX customers_pk ON customers USING btree (uuid)",
                    "PRIMARY KEY (uuid)",
                    "p",
                    False,
                    False,
                    False,
                    0,
                    "btree",
                ]
            ],
            "foreign_key_schema": [
                [
                    33961,
                    "dogs_fk",
                    "FOREIGN KEY (owner) REFERENCES customers(uuid) ON DELETE CASCADE",
                ]
            ],
            "table_schema": [
                ["public", 33955, "customers", "r", "postgres", "p", None]
            ],
            "view_schema": [["public", "pg_stat_statements", "rdsadmin", " SELECT..."]],
            "index_columns_schema": [
                [
                    33955,
                    33967,
                    "a,b,c",
                ]
            ],
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
            "pg_stat_user_tables_raw": [["relname"], ["last_autovacuum"], ["relid"]],
            "pg_statio_user_tables": [["relname"], ["table_date"], ["relid"]],
            "pg_stat_user_indexes": [["relname"], ["local_count"], ["indexrelid"]],
            "pg_statio_user_indexes": [["relname"], ["local_count"], ["indexrelid"]],
            "pg_stat_statements": [["queryid"], ["calls"], ["avg_time_ms"]],
            "pg_stat_activity_vacuum": [["pid"], ["datid"], ["state"], ["query"]],
            "pg_stat_activity_pre_pg_14": [
                ["pid"],
                ["backend_start"],
                ["query_start"],
                ["datid"],
                ["datname"],
                ["state"],
                ["state_change"],
                ["wait_event"],
                ["wait_event_type"],
                ["backend_type"],
                ["xact_start"],
            ],
            "pg_stat_activity": [
                ["pid"],
                ["query_id"],
                ["backend_start"],
                ["query_start"],
                ["datid"],
                ["datname"],
                ["state"],
                ["state_change"],
                ["wait_event"],
                ["wait_event_type"],
                ["backend_type"],
                ["xact_start"],
            ],
            "pg_stat_progress_vacuum": [["pid"], ["relid"], ["phase"]],
            "row_stats": [
                ["num_tables"],
                ["num_empty_tables"],
                ["num_tables_row_count_0_10k"],
                ["num_tables_row_count_10k_100k"],
                ["num_tables_row_count_100k_1m"],
                ["num_tables_row_count_1m_10m"],
                ["num_tables_row_count_10m_100m"],
                ["num_tables_row_count_100m_inf"],
                ["max_row_num"],
                ["min_row_num"],
            ],
            "top_tables": [
                ["relid"],
            ],
            "table_level_pg_stat_user_tables": [
                ["relid"],
                ["schemaname"],
                ["relname"],
                ["seq_scan"],
                ["seq_tup_read"],
                ["idx_scan"],
                ["idx_tup_fetch"],
                ["n_tup_ins"],
                ["n_tup_upd"],
                ["n_tup_del"],
                ["n_tup_hot_upd"],
                ["n_live_tup"],
                ["n_dead_tup"],
                ["n_mod_since_analyze"],
                ["last_vacuum"],
                ["last_autovacuum"],
                ["last_analyze"],
                ["last_autoanalyze"],
                ["vacuum_count"],
                ["autovacuum_count"],
                ["analyze_count"],
                ["autoanalyze_count"],
            ],
            "table_level_pg_statio_user_tables": [
                ["relid"],
                ["schemaname"],
                ["relname"],
                ["heap_blks_read"],
                ["heap_blks_hit"],
                ["idx_blks_read"],
                ["idx_blks_hit"],
                ["toast_blks_read"],
                ["toast_blks_hit"],
                ["tidx_blks_read"],
                ["tidx_blks_hit"],
            ],
            "table_level_table_size": [
                ["relid"],
                ["indexes_size"],
                ["relation_size"],
                ["toast_size"],
            ],
            "table_level_bloat_ratio": [
                ["relid"],
                ["tblpages"],
                ["reltuples"],
                ["bs"],
                ["page_hdr"],
                ["fillfactor"],
                ["is_na"],
                ["tpl_data_size"],
                ["tpl_hdr_size"],
                ["ma"],
            ],
            "pg_stat_user_indexes_top_n": [
                ["indexrelid"],
                ["index_size"],
            ],
            "pg_stat_user_indexes_all_fields": [
                ["relid"],
                ["indexrelid"],
                ["schemaname"],
                ["relname"],
                ["indexrelname"],
                ["idx_scan"],
                ["idx_tup_read"],
                ["idx_tup_fetch"],
            ],
            "pg_statio_user_indexes_all_fields": [
                ["indexrelid"],
                ["idx_blks_read"],
                ["idx_blks_hit"],
            ],
            "pg_index_all_fields": [
                ["indexrelid"],
                ["indrelid"],
                ["indnatts"],
                ["indnkeyatts"],
                ["indisunique"],
                ["indisprimary"],
                ["indisexclusion"],
                ["indimmediate"],
                ["indisclustered"],
                ["indisvalid"],
                ["indcheckxmin"],
                ["indisready"],
                ["indislive"],
                ["indisreplident"],
                ["indkey"],
                ["indcollation"],
                ["indclass"],
                ["indoption"],
                ["indexprs"],
                ["indpred"],
            ],
            "pg_stat_statements_all_fields": [
                ["userid"],
                ["dbid"],
                ["queryid"],
                ["query"],
                ["calls"],
                ["rows"],
                ["shared_blks_hit"],
                ["shared_blks_read"],
                ["shared_blks_dirtied"],
                ["shared_blks_written"],
                ["local_blks_hit"],
                ["local_blks_read"],
                ["local_blks_dirtied"],
                ["temp_blks_read"],
                ["temp_blks_written"],
                ["blk_read_time"],
                ["blk_write_time"],
            ],
        }
        self.aggregated_metas = {
            "pg_stat_database": [["local_count"], ["local_count2"]],
            "pg_stat_database_conflicts": [["local_count"], ["local_count2"]],
            "pg_stat_user_tables": [["local_count"], ["local_count2"]],
            "pg_statio_user_tables": [["local_count"], ["local_count2"]],
            "pg_stat_user_indexes": [["local_count"], ["local_count2"]],
            "pg_statio_user_indexes": [["local_count"], ["local_count2"]],
            "index_schema": [
                ["table_id"],
                ["index_id"],
                ["index_name"],
                ["is_primary"],
                ["is_unique"],
                ["is_clustered"],
                ["is_valid"],
                ["index_expression"],
                ["index_constraint"],
                ["constraint_type"],
                ["constraint_deferrable"],
                ["constraint_deferred_by_default"],
                ["index_replica_identity"],
                ["table_space"],
                ["index_type"],
            ],
            "column_schema": [
                ["table_id"],
                ["name"],
                ["type"],
                ["default_val"],
                ["nullable"],
                ["collation"],
                ["identity"],
                ["storage_type"],
                ["stats_target"],
                ["description"],
            ],
            "foreign_key_schema": [
                ["table_id"],
                ["constraint_name"],
                ["constraint_expression"],
            ],
            "table_schema": [
                ["schema"],
                ["table_id"],
                ["table_name"],
                ["type"],
                ["owner"],
                ["persistence"],
                ["description"],
            ],
            "view_schema": [
                ["schemaname"],
                ["viewname"],
                ["viewowner"],
                ["definition"],
            ],
            "index_columns_schema": [
                ["table_id"],
                ["index_id"],
                ["column_names"],
            ],
        }
        self.local_metrics = {
            "database": {
                "pg_stat_database": {
                    "aggregated": {"local_count": 1, "local_count2": 1}
                },
                "pg_stat_database_conflicts": {
                    "aggregated": {"local_count": 2, "local_count2": 2}
                },
            },
            "index": {
                "pg_stat_user_indexes": {
                    "aggregated": {"local_count": 5, "local_count2": 5}
                },
                "pg_statio_user_indexes": {
                    "aggregated": {"local_count": 6, "local_count2": 6}
                },
            },
            "process": {
                "pg_stat_vacuum_activity": {
                    1: {
                        "datid": "dl1",
                        "pid": 1,
                        "state": "act1",
                        "query": "vacuum tl1",
                    },
                    2: {
                        "datid": "dl2",
                        "pid": 2,
                        "state": "act2",
                        "query": "autovacuum: VACUUM ANALYZE dl2.tl2",
                    },
                },
                "pg_stat_progress_vacuum": {
                    1: {"phase": "phase1", "pid": 1, "relid": "tl1"},
                    2: {"phase": "phase2", "pid": 2, "relid": "tl2"},
                },
            },
            "table": {
                "pg_stat_vacuum_tables": {
                    1: {
                        "last_autovacuum": "2020-01-01T00:00:00",
                        "relid": 1,
                        "relname": "tl1",
                    }
                },
                "pg_stat_user_tables": {
                    "aggregated": {"local_count": 3, "local_count2": 3}
                },
                "pg_statio_user_tables": {
                    "aggregated": {"local_count": 4, "local_count2": 4}
                },
            },
        }
        self.padding_query_metas: List[List[str]] = [
            ["relid", "attname", "attalign", "avg_width"]
        ]
        self.padding_query_values: List[Tuple[int, str, str, int]] = [
            (1234, "id", "i", 4),
            (1234, "value", "d", 8),
            (1234, "fixture_id", "i", 4),
            (1234, "observation_id", "i", 4),
            (1234, "session_id", "i", 4),
        ]

    # @staticmethod
    def expected_default_result(self) -> Dict[str, Any]:
        return {
            "global": {
                "pg_stat_archiver": {"global": "g1", "global_count": 1},
                "pg_stat_bgwriter": {"global": "g2", "global_count": 2},
                "pg_stat_statements": {
                    "statements": json.dumps(
                        [{"queryid": 123, "calls": 2, "avg_time_ms": 1.5}]
                    )
                },
            },
            "local": self.local_metrics,
        }


class Result:
    def __init__(self) -> None:
        self.value: Optional[List[Any]] = None
        self.meta: List[List[str]] = []


def get_sql_api(data: SqlData, result: Result, version: str) -> Callable[[str], NoReturn]:
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
        elif (
            sql == PG_STAT_STATEMENTS_MODULE_QUERY
            or "avg_time_ms FROM pg_stat_statements;" in sql
        ):
            result.value = data.views["pg_stat_statements"]
            result.meta = data.metas["pg_stat_statements"]
        elif sql == ROW_NUM_STAT:
            result.value = data.views["row_stats"]
            result.meta = data.metas["row_stats"]
        elif (
            TOP_N_LARGEST_TABLES_SQL_TEMPLATE[
                : TOP_N_LARGEST_TABLES_SQL_TEMPLATE.index("LIMIT")
            ]
            in sql
        ):
            result.value = data.views["top_tables"]
            result.meta = data.metas["top_tables"]
        elif (
            PG_STAT_TABLE_STATS_TEMPLATE[: PG_STAT_TABLE_STATS_TEMPLATE.index("IN")]
            in sql
        ):
            result.value = data.views["table_level_pg_stat_user_tables"]
            result.meta = data.metas["table_level_pg_stat_user_tables"]
        elif (
            PG_STATIO_TABLE_STATS_TEMPLATE[: PG_STATIO_TABLE_STATS_TEMPLATE.index("IN")]
            in sql
        ):
            result.value = data.views["table_level_pg_statio_user_tables"]
            result.meta = data.metas["table_level_pg_statio_user_tables"]
        elif (
            TABLE_SIZE_TABLE_STATS_TEMPLATE[
                : TABLE_SIZE_TABLE_STATS_TEMPLATE.index("IN")
            ]
            in sql
        ):
            result.value = data.views["table_level_table_size"]
            result.meta = data.metas["table_level_table_size"]
        elif "tblpages" in sql:
            result.value = data.views["table_level_bloat_ratio"]
            result.meta = data.metas["table_level_bloat_ratio"]
        elif "attalign" in sql:
            result.value = data.padding_query_values
            result.meta = data.padding_query_metas
        elif (
            TOP_N_LARGEST_INDEXES_SQL_TEMPLATE[
                : TOP_N_LARGEST_INDEXES_SQL_TEMPLATE.index("IN")
            ]
            in sql
        ):
            result.value = data.views["pg_stat_user_indexes_top_n"]
            result.meta = data.metas["pg_stat_user_indexes_top_n"]
        elif (
            PG_STAT_USER_INDEXES_TEMPLATE[: PG_STAT_USER_INDEXES_TEMPLATE.index("IN")]
            in sql
        ):
            result.value = data.views["pg_stat_user_indexes_all_fields"]
            result.meta = data.metas["pg_stat_user_indexes_all_fields"]
        elif (
            PG_STATIO_USER_INDEXES_TEMPLATE[
                : PG_STATIO_USER_INDEXES_TEMPLATE.index("IN")
            ]
            in sql
        ):
            result.value = data.views["pg_statio_user_indexes_all_fields"]
            result.meta = data.metas["pg_statio_user_indexes_all_fields"]
        elif PG_INDEX_TEMPLATE[: PG_INDEX_TEMPLATE.index("IN")] in sql:
            result.value = data.views["pg_index_all_fields"]
            result.meta = data.metas["pg_index_all_fields"]
        elif (
            PG_QUERY_STATS_SQL_TEMPLATE[: PG_QUERY_STATS_SQL_TEMPLATE.index("LIMIT")]
            in sql
        ):
            result.value = data.views["pg_stat_statements_all_fields"]
            result.meta = data.metas["pg_stat_statements_all_fields"]
        elif "query_start < " in sql: # long running query
            version_float = float(".".join(version.split(".")[:2]))
            if version_float >= 14:
                result.value = data.views["pg_stat_activity"]
                result.meta = data.metas["pg_stat_activity"]
            else:
                result.value = data.views["pg_stat_activity_pre_pg_14"]
                result.meta = data.metas["pg_stat_activity_pre_pg_14"]
        elif sql == "CREATE EXTENSION pg_stat_statements;":
            result.value = []
            result.meta = []
        elif sql == VACUUM_ACTIVITY_STAT:
            result.value = data.views["pg_stat_activity_vacuum"]
            result.meta = data.metas["pg_stat_activity_vacuum"]
        elif sql == VACUUM_PROGRESS_STAT:
            result.value = data.views["pg_stat_progress_vacuum"]
            result.meta = data.metas["pg_stat_progress_vacuum"]
        elif sql == VACUUM_USER_TABLES_STAT:
            result.value = data.views["pg_stat_user_tables_raw"]
            result.meta = data.metas["pg_stat_user_tables_raw"]
        elif sql == QUERY_COLUMNS_SCHEMA_SQL_TEMPLATE.format(generate_query=""):
            result.value = data.views["column_schema"]
            result.meta = data.aggregated_metas["column_schema"]
        elif sql == QUERY_FOREIGN_KEY_SCHEMA_SQL_TEMPLATE.format(
            conparentid_predicate="AND conparentid = 0",
        ):
            result.value = data.views["foreign_key_schema"]
            result.meta = data.aggregated_metas["foreign_key_schema"]
        elif sql == QUERY_INDEX_SCHEMA_SQL_TEMPLATE:
            result.value = data.views["index_schema"]
            result.meta = data.aggregated_metas["index_schema"]
        elif sql == QUERY_TABLE_SCHEMA_SQL_TEMPLATE:
            result.value = data.views["table_schema"]
            result.meta = data.aggregated_metas["table_schema"]
        elif sql == QUERY_VIEW_SCEHMA_SQL_TEMPLATE:
            result.value = data.views["view_schema"]
            result.meta = data.aggregated_metas["view_schema"]
        elif sql == QUERY_INDEX_SCHEMA_COLUMN_NAMES:
            result.value = data.views["index_columns_schema"]
            result.meta = data.aggregated_metas["index_columns_schema"]
        else:
            # pyre-fixme[7]
            raise Exception(f"Unknown sql: {sql}")

    return sql_fn


@pytest.fixture(name="mock_conn")
@mock.patch("psycopg2.connect")
def _mock_conn(mock_connect: MagicMock) -> MagicMock:
    return mock_connect.return_value


def test_get_version(mock_conn: MagicMock) -> Optional[NoReturn]:
    collector = PostgresCollector({"postgres": mock_conn}, "postgres", "9.6.3")
    version = collector.get_version()
    assert version == "9.6.3"


def test_collect_knobs_success(mock_conn: MagicMock) -> Optional[NoReturn]:
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
    collector = PostgresCollector({"postgres": mock_conn}, "postgres", "9.6.3")
    assert collector.collect_knobs() == expected


def test_collect_knobs_sql_failure(mock_conn: MagicMock) -> Optional[NoReturn]:
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.side_effect = psycopg2.ProgrammingError("bad query")
    collector = PostgresCollector({"postgres": mock_conn}, "postgres", "9.6.3")
    with pytest.raises(PostgresCollectorException) as ex:
        collector.collect_knobs()
    assert "Failed to execute sql" in ex.value.message


def test_check_permission_success(mock_conn: MagicMock) -> Optional[NoReturn]:
    # Even with sql failure, we should still return true as we don't do anything
    # in check_permission for now
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.side_effect = psycopg2.ProgrammingError("bad query")
    collector = PostgresCollector({"postgres": mock_conn}, "postgres", "9.6.3")
    assert collector.check_permission() == (True, [], "")


def test_collect_metrics_success(mock_conn: MagicMock) -> Optional[NoReturn]:
    mock_cursor = mock_conn.cursor.return_value
    data = SqlData()
    result = Result()
    version = "9.6.3"
    mock_cursor.execute.side_effect = get_sql_api(data, result, version)
    mock_cursor.fetchall.side_effect = lambda: result.value
    type(mock_cursor).description = PropertyMock(side_effect=lambda: result.meta)
    collector = PostgresCollector({"postgres": mock_conn}, "postgres", version)
    assert collector.collect_metrics() == data.expected_default_result()


def test_collect_metrics_sql_failure(mock_conn: MagicMock) -> Optional[NoReturn]:
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.side_effect = psycopg2.ProgrammingError("bad query")
    collector = PostgresCollector({"postgres": mock_conn}, "postgres", "9.6.3")
    with pytest.raises(PostgresCollectorException) as ex:
        collector.collect_metrics()
    assert "Failed to execute sql" in ex.value.message


def test_collect_row_stats_success(mock_conn: MagicMock) -> Optional[NoReturn]:
    mock_cursor = mock_conn.cursor.return_value
    data = SqlData()
    result = Result()
    version = "9.6.3"
    mock_cursor.execute.side_effect = get_sql_api(data, result, version)
    mock_cursor.fetchall.side_effect = lambda: result.value
    type(mock_cursor).description = PropertyMock(side_effect=lambda: result.meta)
    collector = PostgresCollector({"postgres": mock_conn}, "postgres", version)
    assert collector.collect_table_row_number_stats() == {
        "num_tables": 2111,
        "num_empty_tables": 1925,
        "num_tables_row_count_0_10k": 72,
        "num_tables_row_count_10k_100k": 13,
        "num_tables_row_count_100k_1m": 30,
        "num_tables_row_count_1m_10m": 41,
        "num_tables_row_count_10m_100m": 30,
        "num_tables_row_count_100m_inf": 0,
        "max_row_num": 42817478,
        "min_row_num": 0,
    }


def test_collect_row_stats_failure(mock_conn: MagicMock) -> Optional[NoReturn]:
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.side_effect = psycopg2.ProgrammingError("bad query")
    collector = PostgresCollector({"postgres": mock_conn}, "postgres", "9.6.3")
    with pytest.raises(PostgresCollectorException) as ex:
        collector.collect_table_row_number_stats()
    assert "Failed to execute sql" in ex.value.message


def test_collect_table_level_metrics_success(mock_conn: MagicMock) -> Optional[NoReturn]:
    mock_cursor = mock_conn.cursor.return_value
    data = SqlData()
    result = Result()
    version = "9.6.3"
    mock_cursor.execute.side_effect = get_sql_api(data, result, version)
    mock_cursor.fetchall.side_effect = lambda: result.value
    type(mock_cursor).description = PropertyMock(side_effect=lambda: result.meta)
    collector = PostgresCollector({"postgres": mock_conn}, "postgres", version)
    target_table_info = collector.get_target_table_info(num_table_to_collect_stats=1)
    assert collector.collect_table_level_metrics(
        target_table_info=target_table_info
    ) == {
        "pg_stat_user_tables_all_fields": {
            "columns": TABLE_LEVEL_PG_STAT_USER_TABLES_COLUMNS
            + ["logical_database_name"],
            "rows": [
                [
                    1544350,
                    "public",
                    "partitionednumericmetric_partition_145",
                    12,
                    156742344,
                    124901,
                    8462823118,
                    15243454,
                    0,
                    0,
                    0,
                    41303336,
                    0,
                    1258272,
                    None,
                    None,
                    datetime(2022, 3, 13, 4, 58, 49, 479706, tzinfo=timezone.utc),
                    datetime(2022, 3, 28, 16, 35, 57, 602308, tzinfo=timezone.utc),
                    0,
                    0,
                    1,
                    9,
                    "postgres",
                ],
                [
                    1544351,
                    "public",
                    "partitionednumericmetric_partition_0",
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    None,
                    None,
                    datetime(2022, 3, 13, 4, 58, 49, 479706, tzinfo=timezone.utc),
                    datetime(2022, 3, 28, 16, 35, 57, 602308, tzinfo=timezone.utc),
                    0,
                    0,
                    0,
                    0,
                    "postgres",
                ],
            ],
        },
        "pg_statio_user_tables_all_fields": {
            "columns": [
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
            ],
            "rows": [
                [
                    1544350,
                    "public",
                    "partitionednumericmetric_partition_145",
                    32141302,
                    152493441,
                    10916736,
                    171887644,
                    None,
                    None,
                    None,
                    None,
                    "postgres",
                ],
                [
                    1544351,
                    "public",
                    "partitionednumericmetric_partition_0",
                    0,
                    0,
                    0,
                    0,
                    None,
                    None,
                    None,
                    None,
                    "postgres",
                ],
            ],
        },
        "pg_stat_user_tables_table_sizes": {
            "columns": [
                "relid",
                "indexes_size",
                "relation_size",
                "toast_size",
                "logical_database_name",
            ],
            "rows": [
                [1544350, 3761643520, 2487918592, 630784, "postgres"],
                [1544351, 0, 0, 0, "postgres"],
            ],
        },
        "table_bloat_ratios": {
            "columns": ["relid", "bloat_ratio", "logical_database_name"],
            "rows": [
                [
                    1234,
                    0.09764872948837611,
                    "postgres",
                ],
            ],
        },
    }
    assert collector.collect_index_metrics(
        target_table_info=target_table_info, num_index_to_collect_stats=10
    ) == {
        "indexes_size": {
            "columns": ["indexrelid", "index_size", "logical_database_name"],
            "rows": [[24889, 16384, "postgres"]],
        },
        "pg_index_all_fields": {
            "columns": [
                "indexrelid",
                "indrelid",
                "indnatts",
                "indnkeyatts",
                "indisunique",
                "indisprimary",
                "indisexclusion",
                "indimmediate",
                "indisclustered",
                "indisvalid",
                "indcheckxmin",
                "indisready",
                "indislive",
                "indisreplident",
                "indkey",
                "indcollation",
                "indclass",
                "indoption",
                "indexprs",
                "indpred",
                "logical_database_name",
            ],
            "rows": [
                [
                    24889,
                    24882,
                    1,
                    1,
                    True,
                    True,
                    False,
                    True,
                    False,
                    True,
                    False,
                    True,
                    True,
                    False,
                    1,
                    0,
                    1978,
                    0,
                    None,
                    "{BOOLEXPR :boolop not :args ({VAR :varno 1 "
                    ":varattno 4 :vartype 16 :vartypmod -1 "
                    ":varcollid 0 :varlevelsup 0 :varnoold 1 "
                    ":varoattno 4 :location 135}) :location "
                    "131}",
                    "postgres",
                ]
            ],
        },
        "pg_stat_user_indexes_all_fields": {
            "columns": [
                "relid",
                "indexrelid",
                "schemaname",
                "relname",
                "indexrelname",
                "idx_scan",
                "idx_tup_read",
                "idx_tup_fetch",
                "logical_database_name",
            ],
            "rows": [
                [24882, 24889, "public", "test1", "test1_pkey", 3, 2, 2, "postgres"]
            ],
        },
        "pg_statio_user_indexes_all_fields": {
            "columns": [
                "indexrelid",
                "idx_blks_read",
                "idx_blks_hit",
                "logical_database_name",
            ],
            "rows": [[24889, 3, 7, "postgres"]],
        },
    }


def test_collect_table_level_metrics_failure(mock_conn: MagicMock) -> Optional[NoReturn]:
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.side_effect = psycopg2.ProgrammingError("bad query")
    collector = PostgresCollector({"postgres": mock_conn}, "postgres", "9.6.3")
    with pytest.raises(PostgresCollectorException) as ex:
        target_table_info = collector.get_target_table_info(10)
        collector.collect_table_level_metrics(target_table_info)
    assert "Failed to execute sql" in ex.value.message


def test_postgres_padding_calculator(mock_conn: MagicMock) -> Optional[NoReturn]:
    collector = PostgresCollector({"postgres": mock_conn}, "postgres", "9.6.3")
    # pylint: disable=protected-access
    assert (
        collector._calculate_padding_size_for_table(
            [
                (1234, "id", "i", 4),
                (1234, "value", "d", 8),
                (1234, "fixture_id", "i", 4),
                (1234, "observation_id", "i", 4),
                (1234, "session_id", "i", 4),
            ]
        )
        == 4
    )

    # pylint: disable=protected-access
    assert (
        collector._calculate_padding_size_for_table(
            [
                (1234, "id", "i", 5),
                (1234, "value", "d", 8),
                (1234, "fixture_id", "d", 19),
                (1234, "observation_id", "i", 121),
                (1234, "session_id", "s", 2),
                (1234, "session_id2", "s", 2),
                (1234, "session_id3", "c", 1),
                (1234, "session_id4", "c", 2),
            ]
        )
        == 8
    )

    # pylint: disable=protected-access
    assert (
        collector._calculate_padding_size_for_table(
            [
                (1234, "id", "i", 8),
                (1234, "value", "d", 8),
                (1234, "fixture_id", "d", 3),
                (1234, "observation_id", "c", 121),
                (1234, "session_id", "i", 8),
            ]
        )
        == 0
    )

    # pylint: disable=protected-access
    assert collector._calculate_padding_size_for_tables(
        [
            (1234, "id", "i", 5),
            (1234, "value", "i", 3),
            (1234, "fixture_id", "d", 3),
            (1234, "observation_id", "c", 7),
            (1234, "session_id", "i", 2),
            (2234, "id", "i", 1),
            (2234, "value", "d", 2),
            (2234, "fixture_id", "d", 3),
            (2234, "observation_id", "d", 4),
            (2234, "session_id", "i", 5),
        ]
    ) == {
        1234: 12,
        2234: 21,
    }

def test_collect_long_running_query_success_pre_pg_14(mock_conn: MagicMock) -> Optional[NoReturn]:
    mock_cursor = mock_conn.cursor.return_value
    data = SqlData()
    result = Result()
    version = "12.4"
    mock_cursor.execute.side_effect = get_sql_api(data, result, version)
    mock_cursor.fetchall.side_effect = lambda: result.value
    type(mock_cursor).description = PropertyMock(side_effect=lambda: result.meta)
    collector = PostgresCollector({"postgres": mock_conn}, "postgres", version)
    assert collector.collect_long_running_query(1) == {
        "pg_stat_activity": {
            "columns": [
                "pid",
                "backend_start",
                "query_start",
                "datid",
                "datname",
                "state",
                "state_change",
                "wait_event",
                "wait_event_type",
                "backend_type",
                "xact_start"
            ],
            "rows": [
                [
                    7123,
                    "2023-05-09 18:30:26.616736+00",
                    "2023-05-09 18:30:26.616736+00",
                    16401,
                    "ottertunedb",
                    "idle",
                    "test_state_change",
                    "test_wait_event",
                    "test_wait_event_type",
                    "test_backend_type",
                    "2023-05-09 18:30:26.616736+00"
                ],
                [
                    7124,
                    "2023-05-09 18:30:26.616736+00",
                    "2023-05-09 18:30:26.616736+00",
                    16401,
                    "ottertunedb",
                    "active",
                    "test_state_change",
                    "test_wait_event",
                    "test_wait_event_type",
                    "test_backend_type",
                    "2023-05-09 18:30:26.616736+00"
                ],
            ],
        }
    }

def test_collect_long_running_query_success_pg_14(mock_conn: MagicMock) -> Optional[NoReturn]:
    mock_cursor = mock_conn.cursor.return_value
    data = SqlData()
    result = Result()
    version = "14.2"
    mock_cursor.execute.side_effect = get_sql_api(data, result, version)
    mock_cursor.fetchall.side_effect = lambda: result.value
    type(mock_cursor).description = PropertyMock(side_effect=lambda: result.meta)
    collector = PostgresCollector({"postgres": mock_conn}, "postgres", version)
    assert collector.collect_long_running_query(1) == {
        "pg_stat_activity": {
            "columns": [
                "pid",
                "query_id",
                "backend_start",
                "query_start",
                "datid",
                "datname",
                "state",
                "state_change",
                "wait_event",
                "wait_event_type",
                "backend_type",
                "xact_start"
            ],
            "rows": [
                [
                    7123,
                    -123456789123,
                    "2023-05-09 18:30:26.616736+00",
                    "2023-05-09 18:30:26.616736+00",
                    16401,
                    "ottertunedb",
                    "idle",
                    "test_state_change",
                    "test_wait_event",
                    "test_wait_event_type",
                    "test_backend_type",
                    "2023-05-09 18:30:26.616736+00",
                ],
                [
                    7124,
                    -123456789124,
                    "2023-05-09 18:30:26.616736+00",
                    "2023-05-09 18:30:26.616736+00",
                    16401,
                    "ottertunedb",
                    "active",
                    "test_state_change",
                    "test_wait_event",
                    "test_wait_event_type",
                    "test_backend_type",
                    "2023-05-09 18:30:26.616736+00",
                ],
            ],
        }
    }

def test_collect_query_metrics_success(mock_conn: MagicMock) -> Optional[NoReturn]:
    mock_cursor = mock_conn.cursor.return_value
    data = SqlData()
    result = Result()
    version = "9.6.3"
    mock_cursor.execute.side_effect = get_sql_api(data, result, version)
    mock_cursor.fetchall.side_effect = lambda: result.value
    type(mock_cursor).description = PropertyMock(side_effect=lambda: result.meta)
    collector = PostgresCollector({"postgres": mock_conn}, "postgres", version)
    assert collector.collect_query_metrics(1) == {
        "pg_stat_statements": {
            "columns": [
                "userid",
                "dbid",
                "queryid",
                "query",
                "calls",
                "rows",
                "shared_blks_hit",
                "shared_blks_read",
                "shared_blks_dirtied",
                "shared_blks_written",
                "local_blks_hit",
                "local_blks_read",
                "local_blks_dirtied",
                "temp_blks_read",
                "temp_blks_written",
                "blk_read_time",
                "blk_write_time",
            ],
            "rows": [
                10,
                16384,
                2067594330036860870,
                'FETCH 50 IN "query-cursor_1"',
                6057292,
                302864600,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ],
        }
    }


def test_anonymize_query(mock_conn: MagicMock) -> Optional[NoReturn]:
    collector = PostgresCollector({"postgres": mock_conn}, "postgres", "9.6.3")
    # pylint: disable=protected-access
    assert (
        collector._anonymize_query({"query": "vacuum oorder;"})["query"]
        == "vacuum oorder"
    )
    # pylint: disable=protected-access
    assert (
        collector._anonymize_query({"query": "--some comments\nvacuum oorder;"})[
            "query"
        ]
        == "vacuum oorder"
    )
    # pylint: disable=protected-access
    assert (
        collector._anonymize_query({"query": "--some comments\n vacuum oorder;"})[
            "query"
        ]
        == "vacuum oorder"
    )
    # pylint: disable=protected-access
    assert (
        collector._anonymize_query({"query": "--some comments\nVACUUM oorder;"})[
            "query"
        ]
        == "vacuum oorder"
    )
    # pylint: disable=protected-access
    assert (
        collector._anonymize_query({"query": "--some comments\nVACUUM OORDER;"})[
            "query"
        ]
        == "vacuum oorder"
    )
    # pylint: disable=protected-access
    assert (
        collector._anonymize_query({"query": "VACUUM OORDER"})["query"]
        == "vacuum oorder"
    )
    # pylint: disable=protected-access
    assert (
        collector._anonymize_query(
            {"query": "--some comments\n\t\t VACUUM TPCC.OORDER\t\n--comment"}
        )["query"]
        == "vacuum tpcc.oorder"
    )
    # pylint: disable=protected-access
    assert collector._anonymize_query({"query": "vacuum tl1;"})["query"] == "vacuum tl1"


def test_collect_schema_success(mock_conn: MagicMock) -> Optional[NoReturn]:
    mock_cursor = mock_conn.cursor.return_value
    data = SqlData()
    result = Result()
    version = "11"
    mock_cursor.execute.side_effect = get_sql_api(data, result, version)
    mock_cursor.fetchall.side_effect = lambda: result.value
    type(mock_cursor).description = PropertyMock(side_effect=lambda: result.meta)
    collector = PostgresCollector({"postgres": mock_conn}, "postgres", version)
    schema = collector.collect_schema()
    assert schema == {
        "columns": {
            "columns": [
                "table_id",
                "name",
                "type",
                "default_val",
                "nullable",
                "collation",
                "identity",
                "storage_type",
                "stats_target",
                "description",
            ],
            "rows": [
                [
                    66764,
                    "last_value",
                    "bigint",
                    None,
                    True,
                    None,
                    "",
                    "p",
                    None,
                    None,
                ],
            ],
        },
        "indexes": {
            "columns": [
                "table_id",
                "index_id",
                "index_name",
                "is_primary",
                "is_unique",
                "is_clustered",
                "is_valid",
                "index_expression",
                "index_constraint",
                "constraint_type",
                "constraint_deferrable",
                "constraint_deferred_by_default",
                "index_replica_identity",
                "table_space",
                "index_type",
            ],
            "rows": [
                [
                    33955,
                    33967,
                    "customers_pk",
                    True,
                    True,
                    False,
                    True,
                    "CREATE UNIQUE INDEX customers_pk ON customers USING btree (uuid)",
                    "PRIMARY KEY (uuid)",
                    "p",
                    False,
                    False,
                    False,
                    0,
                    "btree",
                ],
            ],
        },
        "foreign_keys": {
            "columns": [
                "table_id",
                "constraint_name",
                "constraint_expression",
            ],
            "rows": [
                [
                    33961,
                    "dogs_fk",
                    "FOREIGN KEY (owner) REFERENCES customers(uuid) ON DELETE CASCADE",
                ],
            ],
        },
        "tables": {
            "columns": [
                "schema",
                "table_id",
                "table_name",
                "type",
                "owner",
                "persistence",
                "description",
            ],
            "rows": [
                [
                    "public",
                    33955,
                    "customers",
                    "r",
                    "postgres",
                    "p",
                    None,
                ],
            ],
        },
        "views": {
            "columns": [
                "schemaname",
                "viewname",
                "viewowner",
                "definition",
            ],
            "rows": [
                [
                    "public",
                    "pg_stat_statements",
                    "rdsadmin",
                    " SELECT...",
                ],
            ],
        },
        "index_columns": {
            "columns": [
                "table_id",
                "index_id",
                "column_names",
            ],
            "rows": [
                [
                    33955,
                    33967,
                    "a,b,c",
                ],
            ]
        }
    }


def test_add_logical_db_columns(mock_conn: MagicMock) -> Optional[NoReturn]:
    mock_cursor = mock_conn.cursor.return_value
    data = SqlData()
    result = Result()
    version = "11"
    mock_cursor.execute.side_effect = get_sql_api(data, result, version)
    mock_cursor.fetchall.side_effect = lambda: result.value
    type(mock_cursor).description = PropertyMock(side_effect=lambda: result.meta)
    collector = PostgresCollector({"postgres": mock_conn}, "postgres", version)
    results = {
        "postgres": {
            "pg_statio_user_tables_all_fields": {
                "columns": [
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
                ],
                "rows": [
                    [
                        1544350,
                        "public",
                        "partitionednumericmetric_partition_145",
                        32141302,
                        152493441,
                        10916736,
                        171887644,
                        None,
                        None,
                        None,
                        None,
                    ],
                    [
                        1544351,
                        "public",
                        "partitionednumericmetric_partition_0",
                        0,
                        0,
                        0,
                        0,
                        None,
                        None,
                        None,
                        None,
                    ],
                ],
            },
            "pg_stat_user_tables_table_sizes": {
                "columns": ["relid", "indexes_size", "relation_size", "toast_size"],
                "rows": [[1544350, 3761643520, 2487918592, 630784], [1544351, 0, 0, 0]],
            },
            "table_bloat_ratios": {
                "columns": ["relid", "bloat_ratio"],
                "rows": [[1234, 0.09764872948837611]],
            },
        },
        "postgres_2": {
            "pg_statio_user_tables_all_fields": {
                "columns": [
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
                ],
                "rows": [
                    [
                        1544350,
                        "public",
                        "partitionednumericmetric_partition_145",
                        32141302,
                        152493441,
                        10916736,
                        171887644,
                        None,
                        None,
                        None,
                        None,
                    ],
                    [
                        1544351,
                        "public",
                        "partitionednumericmetric_partition_0",
                        0,
                        0,
                        0,
                        0,
                        None,
                        None,
                        None,
                        None,
                    ],
                ],
            },
            "pg_stat_user_tables_table_sizes": {
                "columns": ["relid", "indexes_size", "relation_size", "toast_size"],
                "rows": [[1544350, 3761643520, 2487918592, 630784], [1544351, 0, 0, 0]],
            },
            "table_bloat_ratios": {
                "columns": ["relid", "bloat_ratio"],
                "rows": [[1234, 0.09764872948837611]],
            },
        },
    }
    expected_modded_results = {
        "pg_statio_user_tables_all_fields": {
            "columns": [
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
            ],
            "rows": [
                [
                    1544350,
                    "public",
                    "partitionednumericmetric_partition_145",
                    32141302,
                    152493441,
                    10916736,
                    171887644,
                    None,
                    None,
                    None,
                    None,
                    "postgres",
                ],
                [
                    1544351,
                    "public",
                    "partitionednumericmetric_partition_0",
                    0,
                    0,
                    0,
                    0,
                    None,
                    None,
                    None,
                    None,
                    "postgres",
                ],
                [
                    1544350,
                    "public",
                    "partitionednumericmetric_partition_145",
                    32141302,
                    152493441,
                    10916736,
                    171887644,
                    None,
                    None,
                    None,
                    None,
                    "postgres_2",
                ],
                [
                    1544351,
                    "public",
                    "partitionednumericmetric_partition_0",
                    0,
                    0,
                    0,
                    0,
                    None,
                    None,
                    None,
                    None,
                    "postgres_2",
                ],
            ],
        },
        "pg_stat_user_tables_table_sizes": {
            "columns": [
                "relid",
                "indexes_size",
                "relation_size",
                "toast_size",
                "logical_database_name",
            ],
            "rows": [
                [1544350, 3761643520, 2487918592, 630784, "postgres"],
                [1544351, 0, 0, 0, "postgres"],
                [1544350, 3761643520, 2487918592, 630784, "postgres_2"],
                [1544351, 0, 0, 0, "postgres_2"],
            ],
        },
        "table_bloat_ratios": {
            "columns": ["relid", "bloat_ratio", "logical_database_name"],
            "rows": [
                [1234, 0.09764872948837611, "postgres"],
                [1234, 0.09764872948837611, "postgres_2"],
            ],
        },
    }
    modded_results = collector._add_logical_db_columns( # pylint: disable=protected-access
        results, "postgres"
    )  # pylint: disable=protected-access
    assert modded_results == expected_modded_results


def test_get_target_table_info_success_multi_db(mock_conn: MagicMock) -> Optional[NoReturn]:
    mock_cursor = mock_conn.cursor.return_value
    data = SqlData()
    result = Result()
    version = "9.6.3"
    mock_cursor.execute.side_effect = get_sql_api(data, result, version)
    mock_cursor.fetchall.side_effect = lambda: result.value
    type(mock_cursor).description = PropertyMock(side_effect=lambda: result.meta)
    collector = PostgresCollector(
        {"postgres": mock_conn, "postgres_2": mock_conn}, "postgres", version
    )
    expected_result = {
        "postgres": {"target_tables": (1234,), "target_tables_str": "(1234)"},
        "postgres_2": {"target_tables": (1234,), "target_tables_str": "(1234)"},
    }
    target_table_info = collector.get_target_table_info(num_table_to_collect_stats=1)
    assert target_table_info == expected_result


def test_collect_table_level_metrics_success_multi_db(mock_conn: MagicMock) -> Optional[NoReturn]:
    mock_cursor = mock_conn.cursor.return_value
    data = SqlData()
    result = Result()
    version = "9.6.3"
    mock_cursor.execute.side_effect = get_sql_api(data, result, version)
    mock_cursor.fetchall.side_effect = lambda: result.value
    type(mock_cursor).description = PropertyMock(side_effect=lambda: result.meta)
    collector = PostgresCollector(
        {"postgres": mock_conn, "postgres_2": mock_conn}, "postgres", version
    )
    target_table_info = collector.get_target_table_info(num_table_to_collect_stats=1)
    expected_results = {
        "pg_stat_user_tables_all_fields": {
            "columns": TABLE_LEVEL_PG_STAT_USER_TABLES_COLUMNS
            + ["logical_database_name"],
            "rows": [
                [
                    1544350,
                    "public",
                    "partitionednumericmetric_partition_145",
                    12,
                    156742344,
                    124901,
                    8462823118,
                    15243454,
                    0,
                    0,
                    0,
                    41303336,
                    0,
                    1258272,
                    None,
                    None,
                    datetime(2022, 3, 13, 4, 58, 49, 479706, tzinfo=timezone.utc),
                    datetime(2022, 3, 28, 16, 35, 57, 602308, tzinfo=timezone.utc),
                    0,
                    0,
                    1,
                    9,
                    "postgres",
                ],
                [
                    1544351,
                    "public",
                    "partitionednumericmetric_partition_0",
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    None,
                    None,
                    datetime(2022, 3, 13, 4, 58, 49, 479706, tzinfo=timezone.utc),
                    datetime(2022, 3, 28, 16, 35, 57, 602308, tzinfo=timezone.utc),
                    0,
                    0,
                    0,
                    0,
                    "postgres",
                ],
                [
                    1544350,
                    "public",
                    "partitionednumericmetric_partition_145",
                    12,
                    156742344,
                    124901,
                    8462823118,
                    15243454,
                    0,
                    0,
                    0,
                    41303336,
                    0,
                    1258272,
                    None,
                    None,
                    datetime(2022, 3, 13, 4, 58, 49, 479706, tzinfo=timezone.utc),
                    datetime(2022, 3, 28, 16, 35, 57, 602308, tzinfo=timezone.utc),
                    0,
                    0,
                    1,
                    9,
                    "postgres_2",
                ],
                [
                    1544351,
                    "public",
                    "partitionednumericmetric_partition_0",
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    None,
                    None,
                    datetime(2022, 3, 13, 4, 58, 49, 479706, tzinfo=timezone.utc),
                    datetime(2022, 3, 28, 16, 35, 57, 602308, tzinfo=timezone.utc),
                    0,
                    0,
                    0,
                    0,
                    "postgres_2",
                ],
            ],
        },
        "pg_statio_user_tables_all_fields": {
            "columns": [
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
            ],
            "rows": [
                [
                    1544350,
                    "public",
                    "partitionednumericmetric_partition_145",
                    32141302,
                    152493441,
                    10916736,
                    171887644,
                    None,
                    None,
                    None,
                    None,
                    "postgres",
                ],
                [
                    1544351,
                    "public",
                    "partitionednumericmetric_partition_0",
                    0,
                    0,
                    0,
                    0,
                    None,
                    None,
                    None,
                    None,
                    "postgres",
                ],
                [
                    1544350,
                    "public",
                    "partitionednumericmetric_partition_145",
                    32141302,
                    152493441,
                    10916736,
                    171887644,
                    None,
                    None,
                    None,
                    None,
                    "postgres_2",
                ],
                [
                    1544351,
                    "public",
                    "partitionednumericmetric_partition_0",
                    0,
                    0,
                    0,
                    0,
                    None,
                    None,
                    None,
                    None,
                    "postgres_2",
                ],
            ],
        },
        "pg_stat_user_tables_table_sizes": {
            "columns": [
                "relid",
                "indexes_size",
                "relation_size",
                "toast_size",
                "logical_database_name",
            ],
            "rows": [
                [1544350, 3761643520, 2487918592, 630784, "postgres"],
                [1544351, 0, 0, 0, "postgres"],
                [1544350, 3761643520, 2487918592, 630784, "postgres_2"],
                [1544351, 0, 0, 0, "postgres_2"],
            ],
        },
        "table_bloat_ratios": {
            "columns": ["relid", "bloat_ratio", "logical_database_name"],
            "rows": [
                [1234, 0.09764872948837611, "postgres"],
                [1234, 0.09764872948837611, "postgres_2"],
            ],
        },
    }
    table_level_metrics = collector.collect_table_level_metrics(
        target_table_info=target_table_info
    )
    assert table_level_metrics == expected_results


def test_collect_index_metrics_success_multi_db(mock_conn: MagicMock) -> Optional[NoReturn]:
    mock_cursor = mock_conn.cursor.return_value
    data = SqlData()
    result = Result()
    version = "9.6.3"
    mock_cursor.execute.side_effect = get_sql_api(data, result, version)
    mock_cursor.fetchall.side_effect = lambda: result.value
    type(mock_cursor).description = PropertyMock(side_effect=lambda: result.meta)
    collector = PostgresCollector(
        {"postgres": mock_conn, "postgres_2": mock_conn}, "postgres", version
    )
    target_table_info = collector.get_target_table_info(num_table_to_collect_stats=1)
    expected_results = {
        "pg_stat_user_indexes_all_fields": {
            "columns": [
                "relid",
                "indexrelid",
                "schemaname",
                "relname",
                "indexrelname",
                "idx_scan",
                "idx_tup_read",
                "idx_tup_fetch",
                "logical_database_name",
            ],
            "rows": [
                [24882, 24889, "public", "test1", "test1_pkey", 3, 2, 2, "postgres"],
                [24882, 24889, "public", "test1", "test1_pkey", 3, 2, 2, "postgres_2"],
            ],
        },
        "pg_statio_user_indexes_all_fields": {
            "columns": [
                "indexrelid",
                "idx_blks_read",
                "idx_blks_hit",
                "logical_database_name",
            ],
            "rows": [[24889, 3, 7, "postgres"], [24889, 3, 7, "postgres_2"]],
        },
        "pg_index_all_fields": {
            "columns": [
                "indexrelid",
                "indrelid",
                "indnatts",
                "indnkeyatts",
                "indisunique",
                "indisprimary",
                "indisexclusion",
                "indimmediate",
                "indisclustered",
                "indisvalid",
                "indcheckxmin",
                "indisready",
                "indislive",
                "indisreplident",
                "indkey",
                "indcollation",
                "indclass",
                "indoption",
                "indexprs",
                "indpred",
                "logical_database_name",
            ],
            "rows": [
                [
                    24889,
                    24882,
                    1,
                    1,
                    True,
                    True,
                    False,
                    True,
                    False,
                    True,
                    False,
                    True,
                    True,
                    False,
                    1,
                    0,
                    1978,
                    0,
                    None,
                    "{BOOLEXPR :boolop not :args ({VAR :varno 1 :varattno 4 :vartype 16 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 1 :varoattno 4 :location 135}) :location 131}",  # pylint: disable=line-too-long
                    "postgres",
                ],
                [
                    24889,
                    24882,
                    1,
                    1,
                    True,
                    True,
                    False,
                    True,
                    False,
                    True,
                    False,
                    True,
                    True,
                    False,
                    1,
                    0,
                    1978,
                    0,
                    None,
                    "{BOOLEXPR :boolop not :args ({VAR :varno 1 :varattno 4 :vartype 16 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnoold 1 :varoattno 4 :location 135}) :location 131}",  # pylint: disable=line-too-long
                    "postgres_2",
                ],
            ],
        },
        "indexes_size": {
            "columns": ["indexrelid", "index_size", "logical_database_name"],
            "rows": [[24889, 16384, "postgres"], [24889, 16384, "postgres_2"]],
        },
    }
    index_metrics = collector.collect_index_metrics(
        target_table_info=target_table_info, num_index_to_collect_stats=1
    )
    assert index_metrics == expected_results
