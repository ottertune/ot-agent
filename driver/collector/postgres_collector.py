"""Postgres database collector to get knob and metric data from the target database"""
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Any, Tuple
from driver.exceptions import PostgresCollectorException
from driver.collector.base_collector import BaseDbCollector, PermissionInfo

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


class PostgresCollector(BaseDbCollector):
    """Postgres connector to collect knobs/metrics from the Postgres database"""

    # Getting knobs from pg_settings does not contain the units, e.g.,
    # it will be 2 instead of 2min
    KNOBS_SQL = "SELECT name, setting From pg_settings;"
    PG_STAT_VIEWS_LOCAL = {
        "database": ["pg_stat_database", "pg_stat_database_conflicts"],
        "table": ["pg_stat_user_tables", "pg_statio_user_tables"],
        "index": ["pg_stat_user_indexes", "pg_statio_user_indexes"],
    }
    PG_STAT_VIEWS_LOCAL_KEY = {
        "database": "datid",
        "table": "relid",
        "index": "indexrelid",
    }
    PG_STAT_LOCAL_QUERY: Dict[str, str] = {
        "pg_stat_database": DATABASE_STAT,
        "pg_stat_database_conflicts": DATABASE_CONFLICTS_STAT,
        "pg_stat_user_tables": TABLE_STAT,
        "pg_statio_user_tables": TABLE_STATIO,
        "pg_stat_user_indexes": INDEX_STAT,
        "pg_statio_user_indexes": INDEX_STATIO,
    }

    def __init__(
        self,
        # pyre-ignore[2] no postgres type for conn
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
        self._conn = conn  # pyre-ignore[4] no postgres type for conn
        self._version_str = version
        if float(".".join(version.split(".")[:2])) >= 9.4:
            # pylint: disable=invalid-name
            self.PG_STAT_VIEWS: List[str] = [
                "pg_stat_archiver",
                "pg_stat_bgwriter",
            ]
        else:
            self.PG_STAT_VIEWS: List[str] = ["pg_stat_bgwriter"]

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
            "local": {"database": {}, "table": {}, "index": {}},
        }

        # global
        for view in self.PG_STAT_VIEWS:
            query = f"SELECT * FROM {view};"
            rows = self._get_metrics(query)
            # A global view can only have one row
            assert len(rows) == 1
            metrics["global"][view] = rows[0]
        # local
        self._aggregated_local_stats(metrics['local'])

        return metrics

    def _aggregated_local_stats(self, local_metric: Dict[str, Any]) -> Dict[str, Any]:
        """Get Aggregated local metrics by summing all values"""

        for category, data in local_metric.items():
            views = self.PG_STAT_VIEWS_LOCAL[category]
            for view in views:
                query = self.PG_STAT_LOCAL_QUERY[view]
                rows = self._get_metrics(query)
                data[view] = {}
                if len(rows) > 0:
                    data[view]['aggregated'] = rows[0]
        return local_metric

    def _raw_local_stats(self, local_metric: Dict[str, Any]) -> Dict[str, Any]:
        """Get raw local metrics without aggregation"""

        for category, data in local_metric.items():
            views = self.PG_STAT_VIEWS_LOCAL[category]
            views_key = self.PG_STAT_VIEWS_LOCAL_KEY[category]
            for view in views:
                query = f"SELECT * FROM {view};"
                rows = self._get_metrics(query)
                data[view] = {}
                for row in rows:
                    key = row.get(views_key)
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
