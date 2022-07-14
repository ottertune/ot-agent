"""SQL strings for table level stats"""

TOP_N_LARGEST_TABLES_SQL_TEMPLATE = """
SELECT
  relid
FROM
  pg_stat_user_tables
WHERE
  n_live_tup > 0
ORDER BY
  n_live_tup
DESC LIMIT
  {n};
"""

PG_STAT_TABLE_STATS_TEMPLATE = """
SELECT
  *
FROM
  pg_stat_user_tables
WHERE 
  relid
IN
  {table_list};
"""

PG_STATIO_TABLE_STATS_TEMPLATE = """
SELECT
  *
FROM
  pg_statio_user_tables
WHERE 
  relid
IN
  {table_list};
"""

TABLE_SIZE_TABLE_STATS_TEMPLATE = """
SELECT
  relid,
  pg_indexes_size(relid) as indexes_size,
  pg_relation_size(relid) as relation_size,
  pg_table_size(relid) - pg_relation_size(relid) as toast_size
FROM
  pg_stat_user_tables
WHERE 
  relid
IN
  {table_list};
"""

ALIGNMENT_DICT = {
  "c": 1,
  "s": 2,
  "i": 4,
  "d": 8,
}

PADDING_HELPER_TEMPLATE = """
SELECT
  tbl.oid as relid, att.attname, attalign, avg_width
FROM
  pg_attribute AS att
    JOIN pg_class AS tbl ON att.attrelid = tbl.oid
    JOIN pg_namespace AS ns ON ns.oid = tbl.relnamespace
    JOIN pg_stats AS s ON s.schemaname=ns.nspname
      AND s.tablename = tbl.relname
      AND s.inherited=false
      AND s.attname=att.attname
WHERE
  tbl.oid in {table_list}
ORDER BY
  tbl.oid, att.attnum
ASC;
"""
  
TABLE_BLOAT_RATIO_FACTOR_TEMPLATE = """
SELECT
  relid, heappages AS tblpages,
  reltuples, bs::float, page_hdr, fillfactor, is_na, tpl_data_size, tpl_hdr_size::float, ma
FROM (
  SELECT
    tbl.oid AS relid, tbl.reltuples,
    tbl.relpages AS heappages,
    coalesce(substring(
      array_to_string(tbl.reloptions, ' ')
      FROM 'fillfactor=([0-9]+)')::smallint, 100) AS fillfactor,
    current_setting('block_size')::numeric AS bs,
    CASE WHEN version()~'mingw32' OR version()~'64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END AS ma,
    24 AS page_hdr,
    23 + CASE WHEN MAX(coalesce(s.null_frac,0)) > 0 THEN ( 7 + count(s.attname) ) / 8.0 ELSE 0::int END
        + CASE WHEN bool_or(att.attname = 'oid' and att.attnum < 0) THEN 4 ELSE 0 END AS tpl_hdr_size,
    sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 0) ) AS tpl_data_size,
    bool_or(att.atttypid = 'pg_catalog.name'::regtype)
      OR sum(CASE WHEN att.attnum > 0 THEN 1 ELSE 0 END) <> count(s.attname) AS is_na
  FROM pg_attribute AS att
    JOIN pg_class AS tbl ON att.attrelid = tbl.oid
    JOIN pg_namespace AS ns ON ns.oid = tbl.relnamespace
    LEFT JOIN pg_stats AS s ON s.schemaname=ns.nspname
      AND s.tablename = tbl.relname AND s.inherited=false AND s.attname=att.attname
  WHERE NOT att.attisdropped AND tbl.oid in {table_list}
    AND tbl.relkind in ('r','m')
  GROUP BY 1,2,3,4,5,6
) AS s;
"""

TOP_N_LARGEST_INDEXES_SQL_TEMPLATE = """
SELECT
  indexrelid, pg_relation_size(indexrelid) as index_size
FROM
  pg_stat_user_indexes
WHERE
  relid IN {table_list}
ORDER BY
  index_size
DESC LIMIT {n};
"""

PG_STAT_USER_INDEXES_TEMPLATE = """
SELECT
  *
FROM
  pg_stat_user_indexes
WHERE
  indexrelid IN {index_list};
"""

PG_STATIO_USER_INDEXES_TEMPLATE = """
SELECT
  indexrelid, idx_blks_read, idx_blks_hit
FROM
  pg_statio_user_indexes
WHERE
  indexrelid IN {index_list};
"""

PG_INDEX_TEMPLATE = """
SELECT
  *
FROM
  pg_index
WHERE indexrelid IN {index_list};
"""
