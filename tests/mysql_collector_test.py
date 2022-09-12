"""Tests for interacting with Mysql database locally"""
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, NoReturn, Union, Optional
from unittest.mock import MagicMock, PropertyMock
import pytest
import mysql.connector.connection
from mysql.connector import errorcode
from driver.collector.mysql_collector import MysqlCollector
from driver.exceptions import MysqlCollectorException
from tests.useful_literals import TABLE_LEVEL_MYSQL_COLUMNS
# pylint: disable=missing-function-docstring,too-many-instance-attributes


@dataclass()
class SqlData:
    """
    Used for providing a set of mock data when collector is collecting metrics
    """

    global_status: List[List[Union[int, str]]]
    innodb_metrics: List[List[Union[int, str]]]
    innodb_status: List[List[str]]
    latency_hist: List[List[float]]
    latency_hist_meta: List[List[str]]
    digest_time: List[List[Union[int, str, float]]]
    digest_time_meta: List[List[str]]
    master_status: List[List[Union[int, str]]]
    master_status_meta: List[List[str]]
    replica_status: List[List[Union[int, str]]]
    replica_status_meta: List[List[str]]
    table_level_stats: List[List[Union[int, str]]]
    table_level_stats_meta: List[List[str]]
    column_schema: List[List[Union[int, str]]]
    column_schema_meta: List[List[str]]
    foreign_keys_schema: List[List[Union[int, str]]]
    foreign_keys_schema_meta: List[List[str]]
    view_schema: List[List[Union[int, str]]]
    view_schema_meta: List[List[str]]


    def __init__(self) -> None:
        self.global_status = [
            ["Innodb_buffer_pool_reads", 25],
            ["Innodb_buffer_pool_read_requests", 100],
            ["com_select", 1],
            ["com_insert", 1],
            ["com_update", 1],
            ["com_delete", 1],
            ["com_replace", 1],
        ]
        self.innodb_metrics = [["trx_rw_commits", 0]]
        self.innodb_status = [["ndbcluster", "connection", "cluster_node_id=7"]]
        self.latency_hist = [[2, 1, 5, 3, 1, 0.0588]]
        self.latency_hist_meta = [["bucket_number"], ["bucket_timer_low"],
                                  ["bucket_timer_high"], ["count_bucket"],
                                  ["count_bucket_and_lower"], ["bucket_quantile"]]
        self.digest_time = [["abc", 10, 1.5]]
        self.digest_time_meta = [["queryid"], ["calls"], ["avg_time_ms"]]
        self.master_status = [[1307, "test"]]
        self.master_status_meta = [["Position"], ["Binlog_Do_DB"]]
        self.replica_status = [["localhost", 60]]
        self.replica_status_meta = [["Source_Host"], ["Connect_Retry"]]
        self.table_level_stats = [[
            "mysql",
            "time_zone_transition",
            "BASE TABLE",
            "InnoDB",
            "Dynamic",
            119074,
            39,
            4734976,
            0,
            4194304,
        ]]
        self.table_level_stats_meta = [
            ["TABLE_SCHEMA"],
            ["TABLE_NAME"],
            ["TABLE_TYPE"],
            ["ENGINE"],
            ["ROW_FORMAT"],
            ["TABLE_ROWS"],
            ["AVG_ROW_LENGTH"],
            ["DATA_LENGTH"],
            ["INDEX_LENGTH"],
            ["DATA_FREE"],
        ]
        self.index_stats = [
            [
                "tpcc",
                "OORDER",
                0,
                "tpcc",
                "O_W_ID",
                1,
                "O_W_ID",
                "A",
                4,
                None,
                "",
                "BTREE"
            ],
            [
                "tpcc",
                "OORDER",
                0,
                "tpcc",
                "O_W_ID",
                2,
                "O_D_ID",
                "A",
                48,
                None,
                "",
                "BTREE"
            ]
        ]

        self.index_stats_meta = [
            ["TABLE_SCHEMA"],
            ["TABLE_NAME"],
            ["NON_UNIQUE"],
            ["INDEX_SCHEMA"],
            ["INDEX_NAME"],
            ["SEQ_IN_INDEX"],
            ["COLUMN_NAME"],
            ["COLLATION"],
            ["CARDINALITY"],
            ["SUB_PART"],
            ["NULLABLE"],
            ["INDEX_TYPE"]
        ]
        self.index_usage = [[
            "TABLE",
            "tpcc",
            "CUSTOMER",
            "PRIMARY",
            60000,
            2834031795200,
            1307086781600,
            47233600,
            1526945013600,
            60000,
            2834031795200,
            1307086781600,
            47233600,
            1526945013600,
            0,
            0,
            0,
            0,
        ]]
        self.index_usage_meta = [
            ["OBJECT_TYPE"],
            ["OBJECT_SCHEMA"],
            ["OBJECT_NAME"],
            ["INDEX_NAME"],
            ["COUNT_STAR"],
            ["SUM_TIMER_WAIT"],
            ["COUNT_READ"],
            ["SUM_TIMER_READ"],
            ["COUNT_WRITE"],
            ["SUM_TIMER_WRITE"],
            ["COUNT_FETCH"],
            ["SUM_TIMER_FETCH"],
            ["COUNT_INSERT"],
            ["SUM_TIMER_INSERT"],
            ["COUNT_UPDATE"],
            ["SUM_TIMER_UPDATE"],
            ["COUNT_DELETE"],
            ["SUM_TIMER_DELETE"]
        ]
        self.index_size = [[
            "tpcc",
            "CUSTOMER",
            "IDX_CUSTOMER_NAME",
            675,
            11059200
        ]]
        self.index_size_meta = [
            ["DATABASE_NAME"],
            ["TABLE_NAME"],
            ["INDEX_NAME"],
            ["STAT_VALUE"],
            ["SIZE_IN_BYTE"]
        ]
        self.query_stats = [[
            None,
            "SELECT COUNT ( * ) FROM `information_schema` . `PLUGINS` WHERE `PLUGIN_NAME` = ?",
            435321,
            282100585546000,
            427483000,
            648028000,
            51135477000,
            679796000000,
            0,
            0,
            0,
            435321,
            20895408,
            435321,
            435321,
            0,
            0,
            0,
            0,
            435321,
            0,
            0,
            0,
            0,
            435321,
            0,
            datetime.strptime("2022-07-28 19:21:48.350718", "%Y-%m-%d %H:%M:%S.%f"),
            datetime.strptime("2022-08-22 23:58:23.005279", "%Y-%m-%d %H:%M:%S.%f"),
        ]]
        self.query_stats_meta = [
            ["SCHEMA_NAME"],
            ["DIGEST_TEXT"],
            ["COUNT_STAR"],
            ["SUM_TIMER_WAIT"],
            ["MIN_TIMER_WAIT"],
            ["AVG_TIMER_WAIT"],
            ["MAX_TIMER_WAIT"],
            ["SUM_LOCK_TIME"],
            ["SUM_ERRORS"],
            ["SUM_WARNINGS"],
            ["SUM_ROWS_AFFECTED"],
            ["SUM_ROWS_SENT"],
            ["SUM_ROWS_EXAMINED"],
            ["SUM_CREATED_TMP_DISK_TABLES"],
            ["SUM_CREATED_TMP_TABLES"],
            ["SUM_SELECT_FULL_JOIN"],
            ["SUM_SELECT_FULL_RANGE_JOIN"],
            ["SUM_SELECT_RANGE"],
            ["SUM_SELECT_RANGE_CHECK"],
            ["SUM_SELECT_SCAN"],
            ["SUM_SORT_MERGE_PASSES"],
            ["SUM_SORT_RANGE"],
            ["SUM_SORT_ROWS"],
            ["SUM_SORT_SCAN"],
            ["SUM_NO_INDEX_USED"],
            ["SUM_NO_GOOD_INDEX_USED"],
            ["FIRST_SEEN"],
            ["LAST_SEEN"],
        ]
        self.table_schema_meta = [
            ["TABLE_SCHEMA"],
            ["TABLE_NAME"],
            ["TABLE_TYPE"],
            ["ENGINE"],
            ["VERSION"],
            ["ROW_FORMAT"],
            ["TABLE_ROWS"],
            ["MAX_DATA_LENGTH"],
            ["TABLE_COLLATION"],
            ["CREATE_OPTIONS"],
            ["TABLE_COMMENT"],
        ]
        self.table_schema = [[
            "main",
            "Customers",
            "BASE TABLE",
            "InnoDB",
            10,
            "Dynamic",
            0,
            0,
            "utf8mb4_0900_ai_ci",
            "",
            ""
        ]]
        self.column_schema_meta = [
            ["TABLE_NAME"],
            ["TABLE_SCHEMA"],
            ["COLUMN_NAME"],
            ["ORDINAL_POSITION"],
            ["COLUMN_DEFAULT"],
            ["IS_NULLABLE"],
            ["DATA_TYPE"],
            ["COLLATION_NAME"],
            ["COLUMN_COMMENT"],
        ]
        self.column_schema = [[
          "CHECK_CONSTRAINTS",
          "information_schema",
          "CHECK_CLAUSE",
          4,
          None,
          "NO",
          "longtext",
          "utf8_bin",
          ""
        ]]
        self.foreign_keys_schema_meta = [
            ["CONSTRAINT_SCHEMA"],
            ["TABLE_NAME"],
            ["CONSTRAINT_NAME"],
            ["UNIQUE_CONSTRAINT_SCHEMA"],
            ["UNIQUE_CONSTRAINT_NAME"],
            ["UPDATE_RULE"],
            ["DELETE_RULE"],
            ["REFERENCED_TABLE_NAME"],
        ]
        self.foreign_keys_schema = [[
            "main",
            "Pets",
            "Pets_FK",
            "main",
            "PRIMARY",
            "NO ACTION",
            "NO ACTION",
            "Customers"
        ]]
        self.view_schema_meta = [
            ["TABLE_SCHEMA"],
            ["TABLE_NAME"],
            ["VIEW_DEFINITION"],
            ["IS_UPDATABLE"],
            ["CHECK_OPTION"],
            ["SECURITY_TYPE"],
        ]
        self.view_schema = [[
            "sys",
            "schema_redundant_indexes",
            "select `sys`.`redundant_keys`.`ta",
            "NO",
            "NONE",
            "INVOKER"
        ]]
        self.index_schema_meta = [
            ["INDEX_NAME"],
            ["NON_UNIQUE"],
            ["COLUMN_NAME"],
            ["COLLATION"],
            ["SUB_PART"],
            ["INDEX_TYPE"],
            ["NULLABLE"],
            ["PACKED"],
            ["TABLE_SCHEMA"],
            ["TABLE_NAME"],
        ]
        self.index_schema = [[
            "PRIMARY",
            0,
            "O_D_ID",
            "A",
            None,
            "BTREE",
            "",
            None,
            "tpcc",
            "WAREHOUSE",
        ]]



    def expected_default_result(self) -> Dict[str, Any]:
        """
        The expected default format of the metrics dictionary based on the data above
        (assuming mysql > 8.0 and that there is master and replica status information)
        """
        return {
            "global": {
                # pyre-ignore[16] we know first element is string
                "global": {x[0].lower(): x[1] for x in self.global_status},
                "innodb_metrics": {"trx_rw_commits": 0},
                "engine": {
                    "innodb_status": "cluster_node_id=7",
                    "master_status": json.dumps(
                        {
                            "Position": 1307,
                            "Binlog_Do_DB": "test",
                        }
                    ),
                    "replica_status": json.dumps(
                        {"Source_Host": "localhost", "Connect_Retry": 60}
                    ),
                },
                "derived": {
                    "buffer_miss_ratio": 25.0,
                    "read_write_ratio": 0.25,
                },
                "performance_schema": {
                    "events_statements_summary_by_digest": json.dumps(
                        [
                            {
                                "queryid": "abc",
                                "calls": 10,
                                "avg_time_ms": 1.5
                            }
                        ]
                    ),
                    "events_statements_histogram_global": json.dumps(
                        [
                            {
                                "bucket_number": 2,
                                "bucket_timer_low": 1,
                                "bucket_timer_high": 5,
                                "count_bucket": 3,
                                "count_bucket_and_lower": 1,
                                "bucket_quantile": 0.0588,
                            }
                        ]
                    )
                },
            },
            "local": None,
        }


class Result:
    def __init__(self) -> None:
        self.value: Optional[List[Any]] = None
        self.meta: List[List[str]] = []


@pytest.fixture(name="mock_conn")
def _mock_conn() -> MagicMock:
    return MagicMock(spec=mysql.connector.connection.MySQLConnection)


def get_sql_api(data: SqlData, result: Result) -> Callable[[str], NoReturn]:
    """
    Used for providing a fake sql endpoint so we can return test data
    """

    def sql_fn(sql: str) -> NoReturn:
        # pylint: disable=too-many-branches
        if sql == MysqlCollector.METRICS_SQL:
            result.value = data.global_status
        elif sql == MysqlCollector.METRICS_INNODB_SQL:
            result.value = data.innodb_metrics
        elif sql == MysqlCollector.ENGINE_INNODB_SQL:
            result.value = data.innodb_status
        elif sql == MysqlCollector.METRICS_LATENCY_HIST_SQL:
            result.value = data.latency_hist
            result.meta = data.latency_hist_meta
        elif sql == MysqlCollector.QUERY_DIGEST_TIME:
            result.value = data.digest_time
            result.meta = data.digest_time_meta
        elif sql == MysqlCollector.ENGINE_MASTER_SQL:
            result.value = data.master_status
            result.meta = data.master_status_meta
        elif sql in ("SHOW REPLICA STATUS;", "SHOW SLAVE STATUS;"):
            result.value = data.replica_status
            result.meta = data.replica_status_meta
        elif "information_schema.TABLES".lower() in sql.lower() and "data_free" in sql.lower():
            result.value = data.table_level_stats
            result.meta = data.table_level_stats_meta
        elif "information_schema.STATISTICS".lower() in sql.lower() and "packed" not in sql.lower():
            result.value = data.index_stats
            result.meta = data.index_stats_meta
        elif "mysql.innodb_index_stats".lower() in sql.lower():
            result.value = data.index_size
            result.meta = data.index_size_meta
        elif "performance_schema.events_statements_summary_by_digest".lower() in sql.lower():
            result.value = data.query_stats
            result.meta = data.query_stats_meta
        elif "information_schema.tables".lower() in sql.lower() and "table_comment" in sql.lower():
            result.value = data.table_schema
            result.meta = data.table_schema_meta
        elif "information_schema.column" in sql.lower():
            result.value = data.column_schema
            result.meta = data.column_schema_meta
        elif "information_schema.referential_constraints" in sql.lower():
            result.value = data.foreign_keys_schema
            result.meta = data.foreign_keys_schema_meta
        elif "information_schema.views" in sql.lower():
            result.value = data.view_schema
            result.meta = data.view_schema_meta
        elif "information_schema.statistics" in sql.lower() and "packed" in sql.lower():
            result.value = data.index_schema
            result.meta = data.index_schema_meta

    return sql_fn


def test_collect_knobs_success(mock_conn: MagicMock) -> NoReturn:
    collector = MysqlCollector(mock_conn, "5.7.3")
    mock_cursor = mock_conn.cursor.return_value
    expected = [["bulk_insert_buffer_size", 5000], ["tmpdir", "/tmp"]]
    mock_cursor.fetchall.return_value = expected
    result = collector.collect_knobs()
    assert result == {
        "global": {"global": dict(expected)},  # pyre-ignore[6] we know size of list
        "local": None,
    }


def test_get_version(mock_conn: MagicMock) -> NoReturn:
    collector = MysqlCollector(mock_conn, "5.7.3")
    version = collector.get_version()
    assert version == "5.7.3"


def test_collect_knobs_sql_failure(mock_conn: MagicMock) -> NoReturn:
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.side_effect = mysql.connector.ProgrammingError("bad query")
    collector = MysqlCollector(mock_conn, "5.7.3")
    with pytest.raises(MysqlCollectorException) as ex:
        collector.collect_knobs()
    assert "Failed to execute sql" in ex.value.message


def test_collect_metrics_success_with_latency_hist(mock_conn: MagicMock) -> NoReturn:
    mock_cursor = mock_conn.cursor.return_value
    data = SqlData()
    res = Result()
    mock_cursor.execute.side_effect = get_sql_api(data, res)
    mock_cursor.fetchall.side_effect = lambda: res.value
    type(mock_cursor).description = PropertyMock(side_effect=lambda: res.meta)
    collector = MysqlCollector(mock_conn, "8.0.0")
    metrics = collector.collect_metrics()
    assert metrics == data.expected_default_result()


def test_collect_metrics_success_no_latency_hist(mock_conn: MagicMock) -> NoReturn:
    mock_cursor = mock_conn.cursor.return_value
    data = SqlData()
    res = Result()
    mock_cursor.execute.side_effect = get_sql_api(data, res)
    mock_cursor.fetchall.side_effect = lambda: res.value
    type(mock_cursor).description = PropertyMock(side_effect=lambda: res.meta)
    collector = MysqlCollector(mock_conn, "7.9.9")
    metrics = collector.collect_metrics()
    result = data.expected_default_result()
    result["global"]["performance_schema"].pop('events_statements_histogram_global')
    assert metrics == result


def test_collect_metrics_success_no_master_status(mock_conn: MagicMock) -> NoReturn:
    mock_cursor = mock_conn.cursor.return_value
    data = SqlData()
    data.master_status = []
    res = Result()
    mock_cursor.execute.side_effect = get_sql_api(data, res)
    mock_cursor.fetchall.side_effect = lambda: res.value
    type(mock_cursor).description = PropertyMock(side_effect=lambda: res.meta)
    collector = MysqlCollector(mock_conn, "8.0.0")
    metrics = collector.collect_metrics()
    result = data.expected_default_result()
    result["global"]["engine"]["master_status"] = ""
    assert metrics == result


def test_collect_metrics_success_no_replica_status(mock_conn: MagicMock) -> NoReturn:
    mock_cursor = mock_conn.cursor.return_value
    data = SqlData()
    data.replica_status = []
    res = Result()
    mock_cursor.execute.side_effect = get_sql_api(data, res)
    mock_cursor.fetchall.side_effect = lambda: res.value
    type(mock_cursor).description = PropertyMock(side_effect=lambda: res.meta)
    collector = MysqlCollector(mock_conn, "8.0.0")
    metrics = collector.collect_metrics()
    result = data.expected_default_result()
    result["global"]["engine"]["replica_status"] = ""
    assert metrics == result


def test_collect_metrics_sql_failure(mock_conn: MagicMock) -> NoReturn:
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.side_effect = mysql.connector.ProgrammingError("bad query")
    collector = MysqlCollector(mock_conn, "5.7.3")
    with pytest.raises(MysqlCollectorException) as ex:
        collector.collect_metrics()
    assert "Failed to execute sql" in ex.value.message


def test_check_permissions_success(mock_conn: MagicMock) -> NoReturn:
    collector = MysqlCollector(mock_conn, "8.0.0")
    assert collector.check_permission() == (True, [], "")


# pyre-ignore[56]
@pytest.mark.parametrize(
    "code",
    [
        errorcode.ER_SPECIFIC_ACCESS_DENIED_ERROR,
        errorcode.ER_ACCESS_DENIED_ERROR,  # cannot infer type w/pytest
        errorcode.ER_TABLEACCESS_DENIED_ERROR,
        errorcode.ER_UNKNOWN_ERROR,
    ],
)
def test_check_permissions_specific_access_denied(
    mock_conn: MagicMock, code: int
) -> NoReturn:
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.side_effect = mysql.connector.Error(errno=code)
    collector = MysqlCollector(mock_conn, "8.0.0")
    success, results, _ = collector.check_permission()
    assert not success
    for info in results:
        assert not info["success"]
        if code in [
            errorcode.ER_SPECIFIC_ACCESS_DENIED_ERROR,
            errorcode.ER_ACCESS_DENIED_ERROR,
            errorcode.ER_TABLEACCESS_DENIED_ERROR,
        ]:
            assert "GRANT" in info["example"]
        else:
            assert "unknown" in info["example"]


def test_collect_table_level_metrics_success(mock_conn: MagicMock) -> NoReturn:
    mock_cursor = mock_conn.cursor.return_value
    data = SqlData()
    res = Result()
    mock_cursor.execute.side_effect = get_sql_api(data, res)
    mock_cursor.fetchall.side_effect = lambda: res.value
    type(mock_cursor).description = PropertyMock(side_effect=lambda: res.meta)
    collector = MysqlCollector(mock_conn, "7.9.9")
    target_table_info = collector.get_target_table_info(num_table_to_collect_stats=1)
    assert collector.collect_table_level_metrics(target_table_info) == {
        "information_schema_TABLES": {
            "columns":TABLE_LEVEL_MYSQL_COLUMNS,
            "rows": [
                [
                    'mysql',
                    'time_zone_transition',
                    'BASE TABLE',
                    'InnoDB',
                    'Dynamic',
                    119074,
                    39,
                    4734976,
                    0,
                    4194304,
                ],
            ],
        },
    }
    assert collector.collect_index_metrics(target_table_info=target_table_info,
                                           num_index_to_collect_stats=10) == {
        'indexes_size': {
            'columns': [
                'DATABASE_NAME',
                'TABLE_NAME',
                'INDEX_NAME',
                'STAT_VALUE',
                'SIZE_IN_BYTE'],
            'rows': [[
                'tpcc',
                'CUSTOMER',
                'IDX_CUSTOMER_NAME',
                675,
                11059200]]},
        'information_schema_STATISTICS': {
            'columns': [
                'TABLE_SCHEMA',
                'TABLE_NAME',
                'NON_UNIQUE',
                'INDEX_SCHEMA',
                'INDEX_NAME',
                'SEQ_IN_INDEX',
                'COLUMN_NAME',
                'COLLATION',
                'CARDINALITY',
                'SUB_PART',
                'NULLABLE',
                'INDEX_TYPE'],
            'rows': [[
                'tpcc',
                'OORDER',
                0,
                'tpcc',
                'O_W_ID',
                1,
                'O_W_ID',
                'A',
                4,
                None,
                '',
                'BTREE'],
               ['tpcc',
                'OORDER',
                0,
                'tpcc',
                'O_W_ID',
                2,
                'O_D_ID',
                'A',
                48,
                None,
                '',
                'BTREE']]},
        'performance_schema_table_io_waits_summary_by_index_usage':
            {'columns': ['TABLE_SCHEMA',
                         'TABLE_NAME',
                         'NON_UNIQUE',
                         'INDEX_SCHEMA',
                         'INDEX_NAME',
                         'SEQ_IN_INDEX',
                         'COLUMN_NAME',
                         'COLLATION',
                         'CARDINALITY',
                         'SUB_PART',
                         'NULLABLE',
                         'INDEX_TYPE'],
             'rows': [['tpcc',
                       'OORDER',
                       0,
                       'tpcc',
                       'O_W_ID',
                       1,
                       'O_W_ID',
                       'A',
                       4,
                       None,
                       '',
                       'BTREE'],
                      ['tpcc',
                       'OORDER',
                       0,
                       'tpcc',
                       'O_W_ID',
                       2,
                       'O_D_ID',
                       'A',
                       48,
                       None,
                       '',
                       'BTREE']]}
    }


def test_collect_table_level_metrics_failure(mock_conn: MagicMock) -> NoReturn:
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.side_effect = mysql.connector.ProgrammingError("bad query")
    collector = MysqlCollector(mock_conn, "5.7.3")
    with pytest.raises(MysqlCollectorException) as ex:
        target_table_info = collector.get_target_table_info(10)
        collector.collect_table_level_metrics(target_table_info)
    assert "Failed to execute sql" in ex.value.message


def test_collect_query_metric_success(mock_conn: MagicMock) -> NoReturn:
    mock_cursor = mock_conn.cursor.return_value
    data = SqlData()
    res = Result()
    mock_cursor.execute.side_effect = get_sql_api(data, res)
    mock_cursor.fetchall.side_effect = lambda: res.value
    type(mock_cursor).description = PropertyMock(side_effect=lambda: res.meta)
    collector = MysqlCollector(mock_conn, "7.9.9")
    assert collector.collect_query_metrics(num_query_to_collect_stats=1) == \
           {'events_statements_summary_by_digest': {
               'columns': [
                   'SCHEMA_NAME',
                   'DIGEST_TEXT',
                   'COUNT_STAR',
                   'SUM_TIMER_WAIT',
                   'MIN_TIMER_WAIT',
                   'AVG_TIMER_WAIT',
                   'MAX_TIMER_WAIT',
                   'SUM_LOCK_TIME',
                   'SUM_ERRORS',
                   'SUM_WARNINGS',
                   'SUM_ROWS_AFFECTED',
                   'SUM_ROWS_SENT',
                   'SUM_ROWS_EXAMINED',
                   'SUM_CREATED_TMP_DISK_TABLES',
                   'SUM_CREATED_TMP_TABLES',
                   'SUM_SELECT_FULL_JOIN',
                   'SUM_SELECT_FULL_RANGE_JOIN',
                   'SUM_SELECT_RANGE',
                   'SUM_SELECT_RANGE_CHECK',
                   'SUM_SELECT_SCAN',
                   'SUM_SORT_MERGE_PASSES',
                   'SUM_SORT_RANGE',
                   'SUM_SORT_ROWS',
                   'SUM_SORT_SCAN',
                   'SUM_NO_INDEX_USED',
                   'SUM_NO_GOOD_INDEX_USED',
                   'FIRST_SEEN',
                   'LAST_SEEN',],
               'rows': [
                   [
                       None,
                       'SELECT COUNT ( * ) FROM '
                       '`information_schema` . '
                       '`PLUGINS` WHERE '
                       '`PLUGIN_NAME` = ?',
                       435321,
                       282100585546000,
                       427483000,
                       648028000,
                       51135477000,
                       679796000000,
                       0,
                       0,
                       0,
                       435321,
                       20895408,
                       435321,
                       435321,
                       0,
                       0,
                       0,
                       0,
                       435321,
                       0,
                       0,
                       0,
                       0,
                       435321,
                       0,
                       datetime(2022, 7, 28, 19, 21, 48, 350718),
                       datetime(2022, 8, 22, 23, 58, 23, 5279),
                   ]
               ]}
           }


def test_collect_schema_success(mock_conn: MagicMock) -> NoReturn:
    mock_cursor = mock_conn.cursor.return_value
    data = SqlData()
    res = Result()
    mock_cursor.execute.side_effect = get_sql_api(data, res)
    mock_cursor.fetchall.side_effect = lambda: res.value
    type(mock_cursor).description = PropertyMock(side_effect=lambda: res.meta)
    collector = MysqlCollector(mock_conn, "7.9.9")
    schema = collector.collect_schema()
    assert schema == {
        "columns" : {
            "columns" : [
                "TABLE_NAME", "TABLE_SCHEMA", "COLUMN_NAME", "ORDINAL_POSITION",
                "COLUMN_DEFAULT", "IS_NULLABLE", "DATA_TYPE", "COLLATION_NAME", "COLUMN_COMMENT",
            ],
            "rows" : [
                [
                    "CHECK_CONSTRAINTS",
                    "information_schema",
                    "CHECK_CLAUSE",
                    4,
                    None,
                    "NO",
                    "longtext",
                    "utf8_bin",
                    "",
                ],
            ]
        },
        "indexes" : {
            "columns" : [
                "INDEX_NAME", "NON_UNIQUE", "COLUMN_NAME", "COLLATION", "SUB_PART",
                "INDEX_TYPE", "NULLABLE", "PACKED", "TABLE_SCHEMA", "TABLE_NAME",
            ],
            "rows" : [
                [
                    "PRIMARY",
                    0,
                    "O_D_ID",
                    "A",
                    None,
                    "BTREE",
                    "",
                    None,
                    "tpcc",
                    "WAREHOUSE",
                ],
            ]
        },
        "foreign_keys" : {
            "columns" : [
                "CONSTRAINT_SCHEMA", "TABLE_NAME", "CONSTRAINT_NAME", "UNIQUE_CONSTRAINT_SCHEMA",
                "UNIQUE_CONSTRAINT_NAME", "UPDATE_RULE", "DELETE_RULE", "REFERENCED_TABLE_NAME",
            ],
            "rows" : [
                [
                    "main",
                    "Pets",
                    "Pets_FK",
                    "main",
                    "PRIMARY",
                    "NO ACTION",
                    "NO ACTION",
                    "Customers",
                ],
            ]
        },
        "tables" : {
            "columns" : [
                "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "ENGINE", "VERSION", "ROW_FORMAT",
                "TABLE_ROWS", "MAX_DATA_LENGTH", "TABLE_COLLATION", "CREATE_OPTIONS",
                "TABLE_COMMENT",
            ],
            "rows" : [
                [
                    "main",
                    "Customers",
                    "BASE TABLE",
                    "InnoDB",
                    10,
                    "Dynamic",
                    0,
                    0,
                    "utf8mb4_0900_ai_ci",
                    "",
                    "",
                ],
            ]
        },
        "views" : {
            "columns" : [
                "TABLE_SCHEMA", "TABLE_NAME", "VIEW_DEFINITION", "IS_UPDATABLE", "CHECK_OPTION",
                "SECURITY_TYPE",
            ],
            "rows" : [
                [
                    "sys",
                    "schema_redundant_indexes",
                    "select `sys`.`redundant_keys`.`ta",
                    "NO",
                    "NONE",
                    "INVOKER",
                ],
            ]
        }
    }
