"""
Package for testing driver without needing a real collector
"""

import pytz
from datetime import datetime
from typing import Dict, Tuple, List, Any

from driver.collector.base_collector import BaseDbCollector, PermissionInfo


class MockCollector(BaseDbCollector):
    """
    Mocks functionality required for collecting data from databases
    """

    def check_permission(self) -> Tuple[bool, List[PermissionInfo], str]:
        """Check the permissions of running all collector queries"""
        success = True
        results = []
        debug_txt = ""
        return success, results, debug_txt

    def collect_knobs(self) -> Dict[str, Any]:
        """Collect database knobs information"""
        knobs: Dict[str, Any] = {"global": {"global": {}}, "local": None}
        return knobs

    def collect_metrics(self) -> Dict[str, Any]:
        """Collect database metrics information"""
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
        return metrics

    def collect_table_row_number_stats(self) -> Dict[str, Any]:
        return {}

    def get_target_table_info(self,
                          num_table_to_collect_stats: int) -> Dict[str, Any]:
        return {}

    def collect_table_level_metrics(self,
                                    target_table_info: Dict[str, Any]) -> Dict[str, Any]:
        """Collect table level statistics"""
        return {
            "pg_stat_user_tables_all_fields": {
                "columns": [
                    "relid", "schemaname", "relname", "seq_scan", "seq_tup_read", "idx_scan", "idx_tup_fetch",
                    "n_tup_ins", "n_tup_upd", "n_tup_del", "n_tup_hot_upd", "n_live_tup", "n_dead_tup", "n_mod_since_analyze",
                    "last_vacuum", "last_autovacuum", "last_analyze", "last_autoanalyze", "vacuum_count", "autovacuum_count",
                    "analyze_count", "autoanalyze_count"],
                "rows": [[1, "public", "table_1", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
                    datetime.now(pytz.utc), datetime.now(pytz.utc),
                    datetime.now(pytz.utc), datetime.now(pytz.utc), 12, 13, 14, 15],
                    [2, "public", "table_2", 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
                        datetime.now(pytz.utc), datetime.now(pytz.utc),
                        datetime.now(pytz.utc), datetime.now(pytz.utc), 22, 23, 24, 25]]
            },
            "pg_statio_user_tables_all_fields": {
                "columns": ["relid", "schemaname", "relname", "heap_blks_read", "heap_blks_hit",
                    "idx_blks_read", "idx_blks_hit", "toast_blks_read", "toast_blks_hit",
                    "tidx_blks_read", "tidx_blks_hit"],
                "rows": [[1, "public", "table_1", 1, 2, 3, 4, 5, 6, 7, 8],
                    [2, "public", "table_2", 11, 12, 13, 14, 15, 16, 17, 18]]
            },
            "pg_stat_user_tables_table_sizes": {
                "columns": ["relid", "indexes_size", "relation_size", "toast_size"],
                "rows": [[1, 1, 2, 3],
                    [2, 11, 12, 13]]
            },
            "table_bloat_ratios": {
                "columns": ["relid", "bloat_ratio"],
                "rows": [[1, 0.5],
                    [2, 0.6]]
            },
        }

    def collect_index_metrics(self,
                              target_table_info: Dict[str, Any],
                              num_index_to_collect_stats: int) -> Dict[str, Any]:
        return {}

    def get_version(self) -> str:
        """Get database version"""
        return "0.0"
