"""Postgres database collector to get knob and metric data from the target database"""
import re
import math
from datetime import datetime
from decimal import Decimal
from itertools import groupby
from typing import Dict, List, Any, Tuple, Optional, Union
import logging
import json

from driver.exceptions import PostgresCollectorException
from driver.collector.base_collector import BaseDbCollector, PermissionInfo
from driver.collector.pg_table_level_stats_sqls import (
    TABLE_SIZE_TABLE_STATS_TEMPLATE,
    TABLE_BLOAT_RATIO_FACTOR_TEMPLATE,
    PADDING_HELPER_TEMPLATE,
    ALIGNMENT_DICT,
    PG_STATIO_TABLE_STATS_TEMPLATE,
    PG_STAT_TABLE_STATS_TEMPLATE,
    TOP_N_LARGEST_TABLES_SQL_TEMPLATE,
    TOP_N_LARGEST_INDEXES_SQL_TEMPLATE,
    PG_STAT_USER_INDEXES_TEMPLATE,
    PG_STATIO_USER_INDEXES_TEMPLATE,
    PG_INDEX_TEMPLATE,
    PG_QUERY_STATS_SQL_TEMPLATE,
)

# database-wide statistics from pg_stat_database view
DATABASE_STAT = """
SELECT
  sum(numbackends) as numbackends,
  sum(xact_commit) as xact_commit,
  sum(xact_rollback) as xact_rollback,
  sum(blks_read) as blks_read,
  sum(blks_hit) as blks_hit,
  sum(tup_returned) as tup_returned,
  sum(tup_fetched) as tup_fetched,
  sum(tup_inserted) as tup_inserted,
  sum(tup_updated) as tup_updated,
  sum(tup_deleted) as tup_deleted,
  sum(conflicts) as conflicts,
  sum(temp_files) as temp_files,
  sum(temp_bytes) as temp_bytes,
  sum(deadlocks) as deadlocks,
  sum(blk_read_time) as blk_read_time,
  sum(blk_write_time) as blk_write_time
FROM
  pg_stat_database;
"""

# database-wide statistics about query cancels occurring due to conflicts
# from pg_stat_database_conflicts view
DATABASE_CONFLICTS_STAT = """
SELECT
  sum(confl_tablespace) as confl_tablespace,
  sum(confl_lock) as confl_lock,
  sum(confl_snapshot) as confl_snapshot,
  sum(confl_bufferpin) as confl_bufferpin,
  sum(confl_deadlock) as confl_deadlock
FROM
  pg_stat_database_conflicts;
"""

# table statistics from pg_stat_user_tables view
TABLE_STAT = """
SELECT
  sum(seq_scan) as seq_scan,
  sum(seq_tup_read) as seq_tup_read,
  sum(idx_scan) as idx_scan,
  sum(idx_tup_fetch) as idx_tup_fetch,
  sum(n_tup_ins) as n_tup_ins,
  sum(n_tup_upd) as n_tup_upd,
  sum(n_tup_del) as n_tup_del,
  sum(n_tup_hot_upd) as n_tup_hot_upd,
  sum(n_live_tup) as n_live_tup,
  sum(n_dead_tup) as n_dead_tup,
  sum(n_mod_since_analyze) as n_mod_since_analyze,
  sum(vacuum_count) as vacuum_count,
  sum(autovacuum_count) as autovacuum_count,
  sum(analyze_count) as analyze_count,
  sum(autoanalyze_count) as autoanalyze_count
FROM
  pg_stat_user_tables;
"""

# table statistics about I/O from pg_statio_user_tables view
TABLE_STATIO = """
SELECT
  sum(heap_blks_read) as heap_blks_read,
  sum(heap_blks_hit) as heap_blks_hit,
  sum(idx_blks_read) as idx_blks_read,
  sum(idx_blks_hit) as idx_blks_hit,
  sum(toast_blks_read) as toast_blks_read,
  sum(toast_blks_hit) as toast_blks_hit,
  sum(tidx_blks_read) as tidx_blks_read,
  sum(tidx_blks_hit) as tidx_blks_hit
FROM
  pg_statio_user_tables;
"""

# index statistics from pg_stat_user_indexes view
INDEX_STAT = """
SELECT
  sum(idx_scan) as idx_scan,
  sum(idx_tup_read) as idx_tup_read,
  sum(idx_tup_fetch) as idx_tup_fetch
FROM
  pg_stat_user_indexes;
"""

# index statistics about I/O from pg_statio_user_indexes view
INDEX_STATIO = """
SELECT
  sum(idx_blks_read) as idx_blks_read,
  sum(idx_blks_hit) as idx_blks_hit
FROM
  pg_statio_user_indexes;
"""

# row number distribution collecting from pg_stat_user_tables
ROW_NUM_STAT = """
SELECT
  count(*) as num_tables,
  count(nullif(n_live_tup = 0, false)) as num_empty_tables,
  count(nullif(n_live_tup > 0 and n_live_tup <= 1e4, false)) as num_tables_row_count_0_10k,
  count(nullif(n_live_tup > 1e4 and n_live_tup <= 1e5, false)) as num_tables_row_count_10k_100k,
  count(nullif(n_live_tup > 1e5 and n_live_tup <= 1e6, false)) as num_tables_row_count_100k_1m,
  count(nullif(n_live_tup > 1e6 and n_live_tup <= 1e7, false)) as num_tables_row_count_1m_10m,
  count(nullif(n_live_tup > 1e7 and n_live_tup <= 1e8, false)) as num_tables_row_count_10m_100m,
  count(nullif(n_live_tup > 1e8, false)) as num_tables_row_count_100m_inf,
  max(n_live_tup) as max_row_num,
  min(n_live_tup) as min_row_num
FROM
  pg_stat_user_tables;
"""

# Query to check pg_stat_statements module
PG_STAT_STATEMENTS_MODULE_QUERY = """
SELECT count(*) FROM pg_extension where extname='pg_stat_statements';
"""

# process statistics for vacuum processes from pg_stat_activity view
VACUUM_ACTIVITY_STAT = """
SELECT
  usename,
  datid,
  datname,
  pid,
  wait_event_type,
  wait_event,
  state,
  query
FROM
  pg_stat_activity
WHERE
  query ilike '%vacuum %'
  and pid not in (select pg_backend_pid());
"""

# autovacuum progress statistics
VACUUM_PROGRESS_STAT = """
SELECT * FROM pg_stat_progress_vacuum;
"""

# User table information for autovacuum monitoring
VACUUM_USER_TABLES_STAT = """
SELECT 
  relid,
  last_autovacuum,
  n_dead_tup,
  relname
FROM
  pg_stat_user_tables
WHERE
  relid in (SELECT relid from pg_stat_progress_vacuum);
"""

QUERY_COLUMNS_SCHEMA_SQL_TEMPLATE = """
SELECT
    a.attrelid as table_id,
    a.attname as name,
    format_type(a.atttypid, a.atttypmod) as type,
    (SELECT
        pg_get_expr(d.adbin, d.adrelid, true)
    FROM
        pg_attrdef d
    WHERE
        d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef
    ) as default_val,
    a.attnotnull as nullable,
    (SELECT
        c.collname
    FROM
        pg_collation c, pg_type t
    WHERE
  c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <>      t.typcollation
             ) as collation,
    a.attidentity as identity,
    a.attstorage as storage_type,
    {generate_query}
    CASE WHEN
        a.attstattarget=-1
    THEN NULL
    ELSE
        a.attstattarget
    END AS stats_target,
    col_description(a.attrelid, a.attnum) as description
FROM
    pg_attribute a 
    LEFT JOIN pg_class c ON c.oid = a.attrelid
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE
    a.attnum > 0
    AND NOT a.attisdropped
    AND c.relkind IN ('r','p','v','m','f','')
    AND n.nspname <> 'pg_catalog'
    AND n.nspname <> 'information_schema'
    AND n.nspname !~ '^pg_toast'
ORDER BY
    a.attnum;
"""

QUERY_INDEX_SCHEMA_SQL_TEMPLATE = """
SELECT
    c.oid as table_id,
    i.indexrelid as index_id,
    c2.relname as index_name,
    i.indisprimary as is_primary,
    i.indisunique as is_unique,
    i.indisclustered as is_clustered,
    i.indisvalid as is_valid,
    pg_get_indexdef(i.indexrelid, 0, true) as index_expression,
    pg_get_constraintdef(con.oid, true) as index_constraint,
    contype as constraint_type,
    condeferrable as constraint_deferrable,
    condeferred as constraint_deferred_by_default,
    i.indisreplident as index_replica_identity,
    c2.reltablespace as table_space,
    am.amname as index_type
FROM
    pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace,
    pg_class c2 LEFT JOIN pg_am am ON am.oid=c2.relam,
    pg_index i LEFT JOIN
    pg_constraint con
ON
    (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN   ('p','u','x'))
WHERE
    c.oid = i.indrelid
    AND i.indexrelid = c2.oid
    AND n.nspname <> 'pg_catalog'
    AND n.nspname <> 'information_schema'
    AND n.nspname !~ '^pg_toast'
ORDER BY
    i.indisprimary DESC, c2.relname;
"""

QUERY_FOREIGN_KEY_SCHEMA_SQL_TEMPLATE = """
SELECT
    conrelid as table_id,
    conname as constraint_name,
    pg_get_constraintdef(r.oid, true) as constraint_expression
FROM
    pg_constraint r
    LEFT JOIN pg_class c ON c.oid = r.conrelid
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE
    r.contype = 'f' {conparentid_predicate}
    AND n.nspname <> 'pg_catalog'
    AND n.nspname <> 'information_schema'
    AND n.nspname !~ '^pg_toast'
ORDER BY
    conrelid, conname;
"""

QUERY_TABLE_SCHEMA_SQL_TEMPLATE = """
SELECT
    n.nspname as schema,
    c.oid as table_id,
    c.relname as table_name,
    c.relkind as type,
    pg_get_userbyid(c.relowner) as owner,
    c.relpersistence as persistence,
    obj_description(c.oid, 'pg_class') as description
FROM pg_class c
     LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','p','v','m','f','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname <> 'information_schema'
      AND n.nspname !~ '^pg_toast'
ORDER BY 1,2;
"""

QUERY_VIEW_SCEHMA_SQL_TEMPLATE = """
SELECT
    schemaname, viewname, viewowner, definition
FROM
    pg_views
WHERE
    schemaname <> 'pg_catalog'
AND schemaname <> 'information_schema'
AND schemaname !~ '^pg_toast';
"""
class PostgresCollector(BaseDbCollector):
    """Postgres connector to collect knobs/metrics from the Postgres database"""

    # Getting knobs from pg_settings does not contain the units, e.g.,
    # it will be 2 instead of 2min
    KNOBS_SQL = "SELECT name, setting From pg_settings;"
    ROW_NUMS_SQL: str = ROW_NUM_STAT
    TABLE_LEVEL_STATS_SQLS: Dict[str, Any] = {
        "pg_stat_user_tables_all_fields": PG_STAT_TABLE_STATS_TEMPLATE,
        "pg_statio_user_tables_all_fields": PG_STATIO_TABLE_STATS_TEMPLATE,
        "pg_stat_user_tables_table_sizes": TABLE_SIZE_TABLE_STATS_TEMPLATE,
    }
    INDEX_STATS_SQLS: Dict[str, Any] = {
        "pg_stat_user_indexes_all_fields": PG_STAT_USER_INDEXES_TEMPLATE,
        "pg_statio_user_indexes_all_fields": PG_STATIO_USER_INDEXES_TEMPLATE,
        "pg_index_all_fields": PG_INDEX_TEMPLATE,
    }
    PG_STAT_VIEWS_LOCAL_AGGREGATED = {
        "database": ["pg_stat_database", "pg_stat_database_conflicts"],
        "table": ["pg_stat_user_tables", "pg_statio_user_tables"],
        "index": ["pg_stat_user_indexes", "pg_statio_user_indexes"],
    }
    PG_STAT_VIEWS_LOCAL_RAW = {
        "table": ["pg_stat_vacuum_tables"],
        "process": ["pg_stat_vacuum_activity", "pg_stat_progress_vacuum"],
    }
    PG_STAT_VIEWS_LOCAL_KEY = {
        "database": "datid",
        "table": "relid",
        "index": "indexrelid",
        "process": "pid",
    }
    PG_STAT_LOCAL_QUERY_AGGREGATED: Dict[str, str] = {
        "pg_stat_database": DATABASE_STAT,
        "pg_stat_database_conflicts": DATABASE_CONFLICTS_STAT,
        "pg_stat_user_tables": TABLE_STAT,
        "pg_statio_user_tables": TABLE_STATIO,
        "pg_stat_user_indexes": INDEX_STAT,
        "pg_statio_user_indexes": INDEX_STATIO,
    }
    PG_STAT_LOCAL_QUERY_RAW: Dict[str, str] = {
        "pg_stat_progress_vacuum": VACUUM_PROGRESS_STAT,
        "pg_stat_vacuum_activity": VACUUM_ACTIVITY_STAT,
        "pg_stat_vacuum_tables": VACUUM_USER_TABLES_STAT,
    }

    VIEW_DATA_POST_PROCESS_FUNCTIONS: Dict[str, str] = {
        "pg_stat_vacuum_activity": "_anonymize_query",
    }

    def __init__(
        self,
        conn,
        version: str,
    ) -> None:
        """
        Callers should make sure that the connection object is closed after using
        the collector. This likely means that callers should not instantiate this class
        directly and instead use the collector_factory.get_collector method instead.

        Args:
            conn: The connection to the database
            options: Options used to define which tables to use for metric collection
        """
        self._conn = conn
        self._version_str = version
        version_float = float(".".join(version.split(".")[:2]))
        # pylint: disable=invalid-name
        if version_float >= 9.4:
            self.PG_STAT_VIEWS: List[str] = [
                "pg_stat_archiver",
                "pg_stat_bgwriter",
            ]
        else:
            self.PG_STAT_VIEWS: List[str] = ["pg_stat_bgwriter"]

        if version_float >= 13:
            self.PG_STAT_STATEMENTS_SQL: str = (
                "SELECT CONCAT(userid, '_', dbid, '_', queryid) as queryid, "
                "calls, mean_exec_time as avg_time_ms "
                "FROM pg_stat_statements;"
            )
        else:
            self.PG_STAT_STATEMENTS_SQL: str = (
                "SELECT CONCAT(userid, '_', dbid, '_', queryid) as queryid, "
                "calls, mean_time as avg_time_ms "
                "FROM pg_stat_statements;"
            )

    def _cmd(self, sql: str):  # type: ignore
        """Run the command line (sql query), and fetch the returned results

        Args:
            sql: Sql query which is executed
        Returns:
            Fetched results of the query, as well as table meta data
        Raises:
            PostgresCollectorException: Failed to execute the sql query
        """

        try:
            cursor = self._conn.cursor()
            cursor.execute(sql)
            res = cursor.fetchall()
            columns = cursor.description
            meta = [col[0] for col in columns]
            return res, meta
        except Exception as ex:  # pylint: disable=broad-except
            msg = f"Failed to execute sql {sql}"
            raise PostgresCollectorException(msg, ex) from ex

    def get_version(self) -> str:
        """Get database version"""

        return self._version_str

    def check_permission(
        self,
    ) -> Tuple[bool, List[PermissionInfo], str]:  # pylint: disable=no-self-use
        """Check the permissions of running all collector queries

        Returns:
            True if the user has all expected permissions. If errors appear, return False,
        as well as the information about how to grant corresponding permissions. Currently,
        running Postgres collector queries does not require any additional permissions.
        The function simply returns True
        """

        success = True
        results = []
        text = ""
        return success, results, text

    def collect_knobs(self) -> Dict[str, Any]:
        """Collect database knobs information

        Returns:
            Database knob data
        Raises:
            PostgresCollectorException: Failed to execute the sql query to get knob data
        """

        knobs: Dict[str, Any] = {"global": {"global": {}}, "local": None}

        knobs_info = self._cmd(self.KNOBS_SQL)[0]
        knobs_json = {}
        for knob_tuple in knobs_info:
            val = knob_tuple[1]
            if isinstance(val, datetime):
                val = val.isoformat()
            knobs_json[knob_tuple[0]] = val
        knobs["global"]["global"] = knobs_json
        return knobs

    def collect_metrics(self) -> Dict[str, Any]:
        """Collect database metrics information

        Returns:
            Database metric data
        Raises:
            PostgresCollectorException: Failed to execute the sql query to get metric data
        """

        metrics: Dict[str, Any] = {
            "global": {},
            "local": {"database": {}, "table": {}, "index": {}, "process": {}},
        }

        # global
        for view in self.PG_STAT_VIEWS:
            query = f"SELECT * FROM {view};"
            rows = self._get_metrics(query)
            # A global view can only have one row
            assert len(rows) == 1
            metrics["global"][view] = rows[0]
        metrics["global"]["pg_stat_statements"] = {
            "statements": json.dumps(self._get_stat_statements())
        }
        # local
        self._aggregated_local_stats(metrics["local"])
        self._raw_local_stats(metrics["local"])

        return metrics

    def collect_table_row_number_stats(self) -> Dict[str, Any]:
        """Collect statistics about the number of rows of different tables

        Returns:
            {
                "num_tables": <int>,
                "num_empty_tables": <int>,
                "num_tables_row_count_0_10k": <int>,
                "num_tables_row_count_10k_100k": <int>,
                "num_tables_row_count_100k_1m": <int>,
                "num_tables_row_count_1m_10m": <int>,
                "num_tables_row_count_10m_100m: <int>,
                "num_tables_row_count_100m_inf": <int>,
                "min_row_num": <int>,
                "max_row_num": <int>,
            }
        Raises:
            PostgresCollectorException: Failed to execute the sql query to get row stats data
        """
        raw_stats = self._cmd(self.ROW_NUMS_SQL)
        return {entry[0]: entry[1] for entry in zip(raw_stats[1], raw_stats[0][0])}

    def get_target_table_info(self,
                              num_table_to_collect_stats: int) -> Dict[str, Any]:
        target_tables_tuple = self._cmd(
            TOP_N_LARGEST_TABLES_SQL_TEMPLATE.format(n=num_table_to_collect_stats),
        )[0]
        target_tables = tuple(table[0] for table in target_tables_tuple)
        target_tables_str = str(target_tables) if len(target_tables) > 1 else (
            f"({target_tables[0]})" if len(target_tables) == 1 else "(0)"
        )
        return {
            "target_tables": target_tables,
            "target_tables_str": target_tables_str,
        }

    def collect_table_level_metrics(self,
                                    target_table_info: Dict[str, Any]) -> Dict[str, Any]:
        """Collect table level statistics
        Returns:
            {
                "pg_stat_user_tables_all_fields": {
                    "columns": [
                        "relid",
                        "schemaname",
                        "relname",
                        "seq_scan",
                        "seq_tup_read",
                        "idx_scan",
                        "idx_tup_fetch",
                        "n_tup_ins",
                        "n_tup_upd",
                        "n_tup_del",
                        "n_tup_hot_upd",
                        "n_live_tup",
                        "n_dead_tup",
                        "n_mod_since_analyze",
                        "last_vacuum",
                        "last_autovacuum",
                        "last_analyze",
                        "last_autoanalyze",
                        "vacuum_count",
                        "autovacuum_count",
                        "analyze_count",
                        "autoanalyze_count",
                    ],
                    "rows": List[List[Any]],
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
                    ],
                    "rows": List[List[Any]],
                },
                "pg_stat_user_tables_table_sizes": {
                    "columns": [
                        "relid",
                        "indexes_size",
                        "relation_size",
                        "toast_size",
                    ],
                    "row": List[List[Any]],
                },
                "table_bloat_ratios": {
                    "columns": [
                        "relid",
                        "bloat_ratio",
                    ],
                    "rows": List[List[Any]],
                },
            }
        Raises:
            PostgresCollectorException: Failed to execute the sql query to get metrics
        """
        metrics = {}
        target_tables = target_table_info["target_tables"]
        target_tables_str = target_table_info["target_tables_str"]

        for field, sql_template in self.TABLE_LEVEL_STATS_SQLS.items():
            rows, columns = self._cmd(
                sql_template.format(table_list=target_tables_str),
            )
            metrics[field] = {
                "columns": columns,
                "rows": [list(row) for row in rows],
            }

        # calculate bloat ratio
        metrics["table_bloat_ratios"] = {
            "columns": ["relid", "bloat_ratio"],
            "rows": [],
        }
        if target_tables:
            raw_padding_info, _ = self._cmd(
                PADDING_HELPER_TEMPLATE.format(
                    table_list=target_tables_str,
                )
            )
            padding_size_dict = self._calculate_padding_size_for_tables(raw_padding_info)
            bloat_ratio_factors_dict = self._retrive_bloat_ratio_factors_for_tables(
                target_tables_str,
            )
            metrics["table_bloat_ratios"]["rows"] = self._calculate_bloat_ratios(
                padding_size_dict, bloat_ratio_factors_dict,
            )

        return metrics

    def collect_index_metrics(self,
                              target_table_info: Dict[str, Any],
                              num_index_to_collect_stats: int) -> Dict[str, Any]:
        """Collect index statistics
        Returns:
            {
                'indexes_size': {
                    'columns': [
                        'indexrelid',
                        'index_size'
                    ],
                    'rows': List[List[Any]],
                },
                'pg_index_all_fields': {
                    'columns': [
                        'indexrelid',
                        'indrelid',
                        'indnatts',
                        'indnkeyatts',
                        'indisunique',
                        'indisprimary',
                        'indisexclusion',
                        'indimmediate',
                        'indisclustered',
                        'indisvalid',
                        'indcheckxmin',
                        'indisready',
                        'indislive',
                        'indisreplident',
                        'indkey',
                        'indcollation',
                        'indclass',
                        'indoption',
                        'indexprs',
                        'indpred'
                    ],
                    'rows': List[List[Any]],
                },
                'pg_stat_user_indexes_all_fields': {
                    'columns': [
                        'relid',
                        'indexrelid',
                        'schemaname',
                        'relname',
                        'indexrelname',
                        'idx_scan',
                        'idx_tup_read',
                        'idx_tup_fetch'
                    ],
                    'rows': List[List[Any]],
                },
                'pg_statio_user_indexes_all_fields': {
                    'columns': [
                        'indexrelid',
                        'idx_blks_read',
                        'idx_blks_hit'
                    ],
                  'rows': List[List[Any]],
                },
            }
            Raises:
            PostgresCollectorException: Failed to execute the sql query to get metrics
        """
        metrics = {}
        target_tables_str = target_table_info["target_tables_str"]

        target_indexes_tuple: List[List[int]] = self._cmd(
            TOP_N_LARGEST_INDEXES_SQL_TEMPLATE.format(table_list=target_tables_str,
                                                      n=num_index_to_collect_stats),
        )[0]
        # pyre-ignore[9]
        target_indexes: Tuple[int] = tuple(index[0] for index in target_indexes_tuple)
        target_indexes_str = str(target_indexes) if len(target_indexes) > 1 else (
            f"({target_indexes[0]})" if len(target_indexes) == 1 else "(0)"
        )

        for field, sql_template in self.INDEX_STATS_SQLS.items():
            rows, columns = self._cmd(
                sql_template.format(index_list = target_indexes_str),
            )
            metrics[field] = {
                "columns": columns,
                "rows": [list(row) for row in rows],
            }

        metrics["indexes_size"] = {
            "columns": ["indexrelid", "index_size"],
            "rows": [],
        }

        if target_indexes:
            metrics["indexes_size"]["rows"] = [
                [index[0], index[1]] for index in target_indexes_tuple]
        return metrics

    def collect_query_metrics(self, num_query_to_collect_stats: int):
        """Collect query statistics
        Returns:
            {
                'pg_stat_statements': {
                    'columns':[
                    ],
                    'rows': [
                    ],
                },
            }
        Raises:
            PostgresCollectorException: Failed to execute the sql query to get metrics
        """
        columns = []
        rows = []
        try:
            rows, columns = self._cmd(
                PG_QUERY_STATS_SQL_TEMPLATE.format(n=num_query_to_collect_stats),
            )
        except PostgresCollectorException as ex:
            logging.error(
                "Failed to load pg_stat_statements module, you need to add "
                "pg_stat_statements in parameter shared_preload_libraries: %s",
                ex,
            )

        return {
            "pg_stat_statements": {
                "columns": columns,
                "rows": rows
            },
        }

    def collect_schema(self) -> Dict[str, Any]:
        """Collect schema"""
        version_float = float(".".join(self._version_str.split(".")[:2]))
        generate_query= ""
        conparentid_predicate = ""
        if version_float >= 13:
            generate_query = "a.attgenerated as generated,"
        if version_float >= 11:
            conparentid_predicate = "AND conparentid = 0"

        column_schema_rows, column_schema_columns = self._cmd(QUERY_COLUMNS_SCHEMA_SQL_TEMPLATE.format(generate_query=generate_query))
        index_schema_rows, index_schema_columns = self._cmd(QUERY_INDEX_SCHEMA_SQL_TEMPLATE)
        foreign_key_schema_rows, foreign_key_schema_columns = self._cmd(QUERY_FOREIGN_KEY_SCHEMA_SQL_TEMPLATE.format(conparentid_predicate=conparentid_predicate))
        table_schema_rows, table_schema_columns = self._cmd(QUERY_TABLE_SCHEMA_SQL_TEMPLATE)
        view_schema_rows, view_schema_columns = self._cmd(QUERY_VIEW_SCEHMA_SQL_TEMPLATE)

        return {
            "columns" : {
                "columns" : column_schema_columns,
                "rows" : column_schema_rows
            },
            "indexes" : {
                "columns" : index_schema_columns,
                "rows" : index_schema_rows
            },
            "foreign_keys" : {
                "columns" :  foreign_key_schema_columns,
                "rows" : foreign_key_schema_rows
            },
            "tables" : {
                "columns" : table_schema_columns,
                "rows" : table_schema_rows
            },
            "views" : {
                "columns": view_schema_columns,
                "rows" : view_schema_rows
            }
        }



    def _calculate_bloat_ratios(
        self,
        padding_size_dict: Dict[int, int],
        bloat_ratio_factors_dict: Dict[int, Dict[str, Any]],
    ) -> List[List[Union[int, float]]]:
        res = []
        for relid, factors in bloat_ratio_factors_dict.items():
            bloat_ratio = self._calculate_bloat_ratio_for_table(
                padding_size=padding_size_dict[relid] if relid in padding_size_dict else 0,
                **factors)
            res.append([relid, bloat_ratio])
        return res

    # pylint: disable=no-self-use, invalid-name, too-many-arguments
    def _calculate_bloat_ratio_for_table(
        self,
        is_na: bool,
        padding_size: int,
        tpl_data_size: float,
        tpl_hdr_size: float,
        ma: int,
        tblpages: float,
        reltuples: float,
        bs: float,
        page_hdr: float,
        fillfactor: float,
    ) -> Optional[float]:
        if is_na:
            return None
        tpl_data_size = tpl_data_size + padding_size
        tpl_size = 4 + tpl_hdr_size + tpl_data_size + 2 * ma - (
            ma if tpl_hdr_size % ma == 0 else tpl_hdr_size % ma
        ) - (
            # pylint: disable=c-extension-no-member
            ma if math.ceil(tpl_data_size) % ma == 0 else math.ceil(tpl_data_size) % ma
        )
        est_tblpages_ff = math.ceil( # pylint: disable=c-extension-no-member
            reltuples / ((bs - page_hdr) * fillfactor / (tpl_size * 100.0))
        )
        return 100.0 * (
            tblpages - est_tblpages_ff
        ) / tblpages if (tblpages - est_tblpages_ff) > 0 else 0

    def _retrive_bloat_ratio_factors_for_tables(
        self,
        target_tables_str: str,
    ) -> Dict[int, Dict[str, Any]]:
        factors, columns = self._cmd(
            TABLE_BLOAT_RATIO_FACTOR_TEMPLATE.format(
                table_list=target_tables_str,
            )
        )
        return {
            factor[0]: dict(zip(columns[1:], factor[1:]))
            for factor in factors
        }

    def _calculate_padding_size_for_tables(
        self,
        raw_fields_info: List[Tuple[int, str, str, int]],
    ) -> Dict[int, int]:
        # Note that groupby requires that the list is sorted by the group key
        # which is done currently by the query
        return {
            relid : self._calculate_padding_size_for_table(list(field_info))
            for relid, field_info in groupby(raw_fields_info, lambda x: x[0])
        }

    # pylint: disable=no-self-use
    def _calculate_padding_size_for_table(
        self,
        table_fields_info: List[Tuple[int, str, str, int]],
    ) -> int:
        # Note that table_fields_info will not be empty
        # field: ('relid', 'attname', 'attalign', 'avg_width')
        # example: (1544350, 'fixture_id', 'i', 4)
        padding = 0
        offset = table_fields_info[0][3]
        for field in table_fields_info[1:]:
            cur_alignment = ALIGNMENT_DICT[field[2]]
            cur_alignment_ = cur_alignment - 1
            padded_size = ((offset + cur_alignment_) & ~cur_alignment_)
            padding += padded_size - offset
            offset = padded_size + field[3]

        # Assume tuples align to 4 bytes, process the last field
        padded_size = ((offset + 3) & ~3)
        padding += padded_size - offset
        return padding

    def _aggregated_local_stats(self, local_metric: Dict[str, Any]) -> Dict[str, Any]:
        """Get Aggregated local metrics by summing all values"""

        for category, data in local_metric.items():
            views = self.PG_STAT_VIEWS_LOCAL_AGGREGATED.get(category, [])
            for view in views:
                query = self.PG_STAT_LOCAL_QUERY_AGGREGATED[view]
                rows = self._get_metrics(query)
                if view not in data:
                    data[view] = {}
                if len(rows) > 0:
                    data[view]["aggregated"] = rows[0]
        return local_metric

    def _raw_local_stats(self, local_metric: Dict[str, Any]) -> Dict[str, Any]:
        """Get raw local metrics without aggregation"""

        for category, data in local_metric.items():
            views = self.PG_STAT_VIEWS_LOCAL_RAW.get(category, [])
            views_key = self.PG_STAT_VIEWS_LOCAL_KEY[category]
            for view in views:
                query = self.PG_STAT_LOCAL_QUERY_RAW[view]
                rows = self._get_metrics(query)
                data[view] = {}

                for row in rows:
                    key = row.get(views_key)
                    if view in self.VIEW_DATA_POST_PROCESS_FUNCTIONS:
                        row = getattr(
                            self, self.VIEW_DATA_POST_PROCESS_FUNCTIONS[view]
                        )(row)
                    data[view][key] = row
        return local_metric

    def _get_metrics(self, query: str):  # type: ignore
        """Get data given a query

        Args:
            query: sql query executed to get data
        Returns:
            Table data. A list of table row (dict format). For example, the table has two
        columns: database, and tup_inserted. And it has two rows: ('db1', 1), and
        ('db2', 2). The returned result will be [{'database': 'db1', 'tup_inserted': 1},
        {'database': 'db2', 'tup_inserted': 2}]
        Raises:
            PostgresCollectorException: Failed to execute the sql query to get table data
        """
        metrics = []
        ret, col = self._cmd(query)
        if len(ret) > 0:
            for data in ret:
                row = {}
                for idx, val in enumerate(data):
                    if val is not None:
                        if isinstance(val, datetime):
                            val = val.isoformat()
                        elif isinstance(val, Decimal):
                            val = float(val)
                        row[col[idx]] = val
                metrics.append(row)
        return metrics

    def _load_stat_statements(self) -> bool:
        """
        Load pg_stat_statements module if it does not exist.
        Returns:
            True if module is loaded successfully, otherwise return False.
        """

        load_module_sql = "CREATE EXTENSION pg_stat_statements;"
        module_exists = self._cmd(PG_STAT_STATEMENTS_MODULE_QUERY)[0][0][0] == 1
        if not module_exists:
            try:
                self._conn.cursor().execute(load_module_sql)
                self._conn.commit()
            except Exception as ex:  # pylint: disable=broad-except
                logging.error("Failed to load pg_stat_statements module: %s", ex)
                self._conn.rollback()
                return False
        return True

    def _get_stat_statements(self) -> List[Dict[str, Any]]:
        """
        Get statement statistics from pg_stat_statements module.
        """

        res = []
        success = self._load_stat_statements()
        if success:
            try:
                res = self._get_metrics(self.PG_STAT_STATEMENTS_SQL)
            except PostgresCollectorException as ex:
                logging.error(
                    "Failed to load pg_stat_statements module, you need to add "
                    "pg_stat_statements in parameter shared_preload_libraries: %s",
                    ex,
                )
        return res

    @staticmethod
    def _anonymize_query(row: Dict):
        """
        Keeps only autovacuum/vacuum relevant portion of query strings,
        to prevent driver from sending sensitive data to ottertune.
        """
        raw_query: str = row["query"]
        if "autovacuum: " in raw_query:
            row["query"] = raw_query
        elif "vacuum " in raw_query.lower():
            # Example:
            # -- a sql comment
            # VACUUM tpcc.oorder\t;
            #
            # re.search("/vacuum .*[^;]/gmi"): VACUUM tpcc.oorder\t
            # query.strip(): VACUUM tpcc.oorder
            query = re.search(
                r"vacuum .*[^;]", raw_query, flags=re.MULTILINE | re.IGNORECASE
            )
            if query is not None:
                query = query.group(0)
                query = query.strip()
                row["query"] = query.lower()
        return row
