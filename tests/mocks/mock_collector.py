"""
Package for testing driver without needing a real collector
"""

from typing import Dict, Tuple, List, Any
import logging

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

    def collect_table_level_metrics(self, num_table_to_collect_stas: int) -> Dict[str, Any]:
        """Collect table level statistics"""
        return {
            "data": {},
        }

    def get_version(self) -> str:
        """Get database version"""
        return "0.0"
