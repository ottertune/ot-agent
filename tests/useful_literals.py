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

TABLE_SCHEMA_MYSQL_COLUMNS = [
    "TABLE_SCHEMA",
    "TABLE_NAME",
    "TABLE_TYPE",
    "ENGINE",
    "VERSION",
    "ROW_FORMAT",
    "TABLE_ROWS",
    "MAX_DATA_LENGTH",
    "TABLE_COLLATION",
    "CREATE_OPTIONS",
    "TABLE_COMMENT"
]


COLUMN_SCHEMA_MYSQL_COLUMNS = [
    "TABLE_SCHEMA",
    "TABLE_NAME",
    "COLUMN_NAME",
    "ORDINAL_POSITION",
    "COLUMN_DEFAULT",
    "IS_NULLABLE",
    "DATA_TYPE",
    "COLLATION_NAME",
    "COLUMN_COMMENT",
]

FOREIGN_KEY_SCHEMA_MYSQL_COLUMNS = [
    "CONSTRAINT_SCHEMA",
    "TABLE_NAME",
    "CONSTRAINT_NAME",
    "UNIQUE_CONSTRAINT_SCHEMA",
    "UNIQUE_CONSTRAINT_NAME",
    "UPDATE_RULE",
    "DELETE_RULE",
    "REFERENCED_TABLE_NAME"
]

VIEW_SCHEMA_MYSQL_COLUMNS = [
    "TABLE_SCHEMA",
    "TABLE_NAME",
    "VIEW_DEFINITION",
    "IS_UPDATABLE",
    "CHECK_OPTION",
    "SECURITY_TYPE"
]

TABLE_SCHEMA_POSTGRES_COLUMNS = [
    "schema",
    "table_id",
    "table_name",
    "type",
    "owner",
    "persistence",
    "description"
]

COLUMN_SCHEMA_POSTGRES_COLUMNS = [
    "table_id",
    "name",
    "type",
    "default_val",
    "nullable",
    "collation",
    "identity",
    "storage_type",
    "stats_target",
    "description"
]

FOREIGN_KEY_SCHEMA_POSTGRES_COLUMNS = [
    "table_id",
    "constraint_name",
    "constraint_expression"
]
VIEW_SCHEMA_POSTGRES_COLUMNS = [
    "schemaname",
    "viewname",
    "viewowner",
    "definition"
]