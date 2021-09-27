"""Postgres database collector to get knob and metric data from the target database"""
from datetime import datetime
from typing import Dict, List, Any, Tuple
from driver.exceptions import PostgresCollectorException
from driver.collector.base_collector import BaseDbCollector, PermissionInfo


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

    def __init__(self, conn, version: str) -> None:
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
            rows = self._get_metrics(view)
            # A global view can only have one row
            assert len(rows) == 1
            metrics["global"][view] = rows[0]
        # local
        for category, data in metrics["local"].items():
            views = self.PG_STAT_VIEWS_LOCAL[category]
            views_key = self.PG_STAT_VIEWS_LOCAL_KEY[category]
            for view in views:
                rows = self._get_metrics(view)
                data[view] = {}
                for row in rows:
                    key = row.get(views_key)
                    data[view][key] = row
        return metrics

    def _get_metrics(self, tbl: str):  # type: ignore
        """Get data from a table

        Args:
            tbl: table name in a query
        Returns:
            Table data. A list of table row (dict format). For example, the table has two
        columns: database, and tup_inserted. And it has two rows: ('db1', 1), and
        ('db2', 2). The returned result will be [{'database': 'db1', 'tup_inserted': 1},
        {'database': 'db2', 'tup_inserted': 2}]
        Raises:
            PostgresCollectorException: Failed to execute the sql query to get table data
        """
        metrics = []
        sql = f"SELECT * FROM {tbl};"
        ret, col = self._cmd(sql)
        if len(ret) > 0:
            for data in ret:
                row = {}
                for idx, val in enumerate(data):
                    if val is not None:
                        if isinstance(val, datetime):
                            val = val.isoformat()
                        row[col[idx]] = val
                metrics.append(row)
        return metrics
