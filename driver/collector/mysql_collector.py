"""MySQL database collector to get knob and metric data from the target database"""
import json
from decimal import Decimal
from typing import Dict, List, Any, Tuple
import logging
import mysql.connector
import mysql.connector.connection as mysql_conn
from mysql.connector import errorcode

from driver.exceptions import MysqlCollectorException
from driver.collector.base_collector import BaseDbCollector, PermissionInfo

TABLE_LEVEL_STATS_SQL_TEMPLATE = """
SELECT
  TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE,
  ENGINE, ROW_FORMAT, TABLE_ROWS,
  AVG_ROW_LENGTH, DATA_LENGTH, INDEX_LENGTH,
  DATA_FREE
FROM
  information_schema.TABLES
ORDER BY 
  TABLE_ROWS
DESC LIMIT 
  {n};
"""

class MysqlCollector(BaseDbCollector):  # pylint: disable=too-many-instance-attributes
    """Mysql connector to collect knobs/metrics from the MySQL database"""

    VERSION_SQL = "SELECT VERSION();"
    KNOBS_SQL = "SHOW GLOBAL VARIABLES;"
    METRICS_SQL = "SHOW GLOBAL STATUS;"
    METRICS_INNODB_SQL = (
        "SELECT name, count FROM information_schema.innodb_metrics "
        "WHERE subsystem = 'transaction';"
    )

    # convert the time unit from ps to ms by dividing 1,000,000,000
    METRICS_LATENCY_HIST_SQL = (
        "SELECT bucket_number, bucket_timer_low / 1000000000 as bucket_timer_low, "
        "bucket_timer_high / 1000000000 as bucket_timer_high, count_bucket, "
        "count_bucket_and_lower, bucket_quantile FROM "
        "performance_schema.events_statements_histogram_global;"
    )
    QUERY_DIGEST_TIME = (
        "SELECT CONCAT(IFNULL(schema_name, 'NULL'), '_', digest) as queryid, "
        "count_star as calls, "
        "round(avg_timer_wait/1000000000, 6) as avg_time_ms "
        "FROM performance_schema.events_statements_summary_by_digest;"
    )

    ENGINE_INNODB_SQL = "SHOW ENGINE INNODB STATUS;"
    ENGINE_MASTER_SQL = "SHOW MASTER STATUS;"

    def __init__(self, conn: mysql_conn.MySQLConnection, version: str) -> None:
        """
        Callers should make sure that the connection object is closed after using
        the collector. This likely means that callers should not insantiate this class
        directly and instead use the collector_factory.get_collector method instead.
        Args:
            conn: The connection to the database
            version: DB version (e.g. 5.7.3)
        """
        self._conn = conn
        self._version_str = version
        self._version = float(".".join(version.split(".")[:2]))
        self._innodb_status: str = ""
        self._global_status: Dict[str, Any] = {}
        # From MySQL 8.0.22, SHOW REPLICA STATUS is available to use.
        if self._version > 8.0:
            # pylint: disable=invalid-name
            self.ENGINE_REPLICA_SQL: str = "SHOW REPLICA STATUS;"
        else:
            self.ENGINE_REPLICA_SQL: str = "SHOW SLAVE STATUS;"

    def _cmd(self, sql: str):  # type: ignore
        """Run the command line (sql query), and fetch the returned results.
        Args:
            sql: Sql query which is executed
        Returns:
            Fetched results of the query, as well as table meta data
        Raises:
            MysqlCollectorException: Failed to execute the sql query
        """
        try:
            cursor = self._conn.cursor(dictionary=False)
            cursor.execute(sql)
            res = cursor.fetchall()
            columns = cursor.description
            meta = [col[0] for col in columns]
            return res, meta
        except Exception as ex:  # pylint: disable=broad-except
            msg = f"Failed to execute sql {sql}"
            raise MysqlCollectorException(msg, ex) from ex

    def get_version(self) -> str:
        """Get database version"""

        return self._version_str

    def check_permission(self) -> Tuple[bool, List[PermissionInfo], str]:
        """Check the permissions of running all collector queries.
        Returns:
            True if the user has all expected permissions. If errors appear, return False,
            as well as the information about how to grant corresponding permissions.
        Raises:
            MysqlCollectorException: Failed to connect to the database
        """
        success = True
        # The SHOW STATUS and SHOW VARIABLES statements do not need any privileges
        sql_priv_map = {
            self.ENGINE_INNODB_SQL: "PROCESS",
            self.KNOBS_SQL: "",
            self.ENGINE_MASTER_SQL: "REPLICATION CLIENT",
            self.ENGINE_REPLICA_SQL: "REPLICATION CLIENT",
            self.METRICS_INNODB_SQL: "PROCESS",
            self.METRICS_SQL: "",
            self.VERSION_SQL: "",
        }
        if self._version >= 8.0:
            sql_priv_map[
                self.METRICS_LATENCY_HIST_SQL
            ] = "performance_schema.events_statements_histogram_global"

        results = []
        for sql, priv in sql_priv_map.items():
            try:
                cursor = self._conn.cursor(dictionary=False)
                cursor.execute(sql)
                cursor.fetchall()
            except mysql.connector.Error as err:
                example = "unknown"
                if err.errno in (
                    errorcode.ER_SPECIFIC_ACCESS_DENIED_ERROR,
                    errorcode.ER_ACCESS_DENIED_ERROR,
                ):
                    example = f"GRANT {priv} ON *.* TO <user>@<host>;"
                elif err.errno == errorcode.ER_TABLEACCESS_DENIED_ERROR:
                    example = f"GRANT SELECT ON {priv} TO <user>@<'host'>;"
                info = {}
                info["query"] = sql
                info["success"] = False
                # example of how to grant the privilege
                info["example"] = example
                results.append(info)
                success = False
        # debug info
        # TODO(bohan) (from nappelson) I think debug information like this should be
        # propgated in a different way For instance, this kind of information
        # should be pushed somewhere. For now, we can leave as is.
        text_lines = []
        for res in results:
            text_lines.append("-----------------------------------------------\n")
            text_lines.append(f"Permissions check failed for SQL: {res['query']}\n")
            text_lines.append(
                f"Please grant the privilege. For example: {res['example']}\n"
            )
        text = "".join(text_lines)
        return success, results, text

    def collect_knobs(self) -> Dict[str, Any]:
        """Collect database knobs information
        Returns:
            Database knob data
        Raises:
            MysqlCollectorException: Failed to execute the sql query to get knob data
        """

        knobs: Dict[str, Any] = {"global": {"global": {}}, "local": None}

        knobs["global"]["global"] = dict(self._cmd(self.KNOBS_SQL)[0])
        return knobs

    def collect_metrics(self) -> Dict[str, Any]:
        """Collect database metrics information
        Returns:
            Database metric data
        Raises:
            MysqlCollectorException: Failed to execute the sql query to get metric data
        """

        metrics: Dict[str, Any] = {
            "global": {
                "global": {},
                "innodb_metrics": {},
                "performance_schema": {},
                "engine": {},
                "derived": {},
            },
            "local": None,
        }
        self._global_status = {
            x[0].lower(): x[1] for x in self._cmd(self.METRICS_SQL)[0]
        }
        metrics["global"]["global"] = self._global_status
        metrics["global"]["innodb_metrics"] = dict(
            self._cmd(self.METRICS_INNODB_SQL)[0]
        )
        status_raw = self._cmd(self.ENGINE_INNODB_SQL)[0]
        if len(status_raw) > 0:
            self._innodb_status = self._truncate_innodb_status(status_raw[0][-1])

        metrics["global"]["engine"]["innodb_status"] = self._innodb_status
        metrics["global"]["derived"] = self._collect_derived_metrics()
        # replica status and master status
        replica_metrics, replica_meta = self._cmd(self.ENGINE_REPLICA_SQL)
        if len(replica_metrics) > 0:
            replica_metrics = replica_metrics[0]
            replica_json = dict(zip(replica_meta, replica_metrics))
            metrics["global"]["engine"]["replica_status"] = json.dumps(replica_json)
        else:
            metrics["global"]["engine"]["replica_status"] = ""

        master_metrics, master_meta = self._cmd(self.ENGINE_MASTER_SQL)
        if len(master_metrics) > 0:
            master_metrics = master_metrics[0]
            master_json = dict(zip(master_meta, master_metrics))
            metrics["global"]["engine"]["master_status"] = json.dumps(master_json)
        else:
            metrics["global"]["engine"]["master_status"] = ""

        try:
            digest_data, digest_meta = self._cmd(self.QUERY_DIGEST_TIME)
            summary_by_digest = self._make_list(digest_data, digest_meta)
        except Exception as ex:  # pylint: disable=broad-except
            logging.error("Failed to collect query latency metrics: %s", ex)
            summary_by_digest = []

        metrics["global"]["performance_schema"][
            "events_statements_summary_by_digest"
        ] = json.dumps(summary_by_digest)

        if self._version >= 8.0:
            # latency histogram
            histogram_data, histogram_meta = self._cmd(self.METRICS_LATENCY_HIST_SQL)
            metrics["global"]["performance_schema"][
                "events_statements_histogram_global"
            ] = json.dumps(self._make_list(histogram_data, histogram_meta))
        return metrics

    def collect_table_row_number_stats(self) -> Dict[str, Any]:
        """Collect statistics about the number of rows of different tables"""
        return {}

    def collect_table_level_metrics(self, num_table_to_collect_stats: int) -> Dict[str, Any]:
        """Collect table level statistics

        Returns:
        {
            "information_schema_TABLES": {
                "columns": [
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "TABLE_TYPE",
                    "ENGINE",
                    "ROW_FORMAT",
                    "TABLE_ROWS",
                    "AVG_ROW_LENGTH",
                    "DATA_LENGTH",
                    "INDEX_LENGTH",
                    "DATA_FREE",
                ],
                "rows": List[List[Any]],
            }
        }
        """
        values, columns = self._cmd(
            TABLE_LEVEL_STATS_SQL_TEMPLATE.format(n=num_table_to_collect_stats),
        )
        return {
            "information_schema_TABLES": {
                "columns": columns,
                "rows": [list(row) for row in values],
            }
        }

    def _collect_derived_metrics(self) -> Dict[str, Any]:
        """Collect metrics derived from base metrics
        Calculate derived metrics from collected base metrics. We may want to move it to the server
        side to calculate derived metrics. We can revisit this when saas server is implemented
        Returns:
            Database calculated derived metrics
        """

        # buffer pool miss ratio
        innodb_buffer_pool_reads = int(
            self._global_status.get("innodb_buffer_pool_reads", 0)
        )
        innodb_buffer_pool_read_requests = int(
            self._global_status.get("innodb_buffer_pool_read_requests", 0)
        )
        if innodb_buffer_pool_read_requests == 0:
            buffer_miss_ratio = 0.0
        else:
            buffer_miss_ratio = (
                round(innodb_buffer_pool_reads / innodb_buffer_pool_read_requests, 4)
                * 100
            )

        # read write query ratio
        read_counts = int(self._global_status.get("com_select", 0))
        write_counts = (
            int(self._global_status.get("com_insert", 0))
            + int(self._global_status.get("com_update", 0))
            + int(self._global_status.get("com_delete", 0))
            + int(self._global_status.get("com_replace", 0))
        )
        read_counts = 1 if read_counts == 0 else read_counts
        write_counts = 1 if write_counts == 0 else write_counts
        read_write_ratio = round(read_counts / write_counts, 4)  # keep 4 decimals here

        # merge metrics
        derived_metrics = dict(
            buffer_miss_ratio=buffer_miss_ratio, read_write_ratio=read_write_ratio
        )
        return derived_metrics

    @staticmethod
    def _truncate_innodb_status(status: str) -> str:
        """
        If the innodb status has too many lines, we truncate it and keep a limited number of lines.
        Currently, we keep the first 50 lines and the last 100 lines.
        """

        lines = status.splitlines()
        size = len(lines)
        if size <= 150:
            return status
        new_lines = lines[:50]
        new_lines.append(f"...ignore {size-150} lines here...")
        new_lines.extend(lines[-100:])
        truncated_status = "\n".join(new_lines)
        return truncated_status

    @staticmethod
    def _make_list(data, meta: List[str]) -> List[Dict[str, Any]]:  # type: ignore
        """
        Convert fetched data to a list.
        Args:
            data: row values, e.g., [(val1_1, val1_2), (val2_1, val2_2)]
            meta: column names, e.g., [col1, col2]
        Returns:
            converted data, e.g., [{"col1": val1_1, "col2": val1_2},
                                   {"col1": val2_1, "col2": val2_2}]
        """

        res = []
        for row in data:
            row_new = [
                float(elem) if isinstance(elem, Decimal) else elem for elem in row
            ]
            res.append(dict(zip(meta, row_new)))
        return res
