"""Useful literals"""

TABLE_LEVEL_PG_STAT_USER_TABLES_COLUMNS = [
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
]

TABLE_LEVEL_MYSQL_COLUMNS = [
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
]
