"""Abstract base class for the database collector"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Tuple, List, TypedDict


class PermissionInfo(TypedDict):
    query: str
    success: bool
    example: str


class BaseDbCollector(ABC):
    """Abstract base class for the database collector"""

    @abstractmethod
    def check_permission(self) -> Tuple[bool, List[PermissionInfo], str]:
        """Check the permissions of running all collector queries"""

    @abstractmethod
    def collect_knobs(self) -> Dict[str, Any]:
        """Collect database knobs information"""

    @abstractmethod
    def collect_metrics(self) -> Dict[str, Any]:
        """Collect database metrics information"""

    @abstractmethod
    def collect_table_row_number_stats(self) -> Dict[str, Any]:
        """Collect statistics about the number of rows of different tables"""
    
    @abstractmethod
    def get_target_table_info(self,
                          num_table_to_collect_stats: int) -> Dict[str, Any]:
        """Get the information of tables to collect table and index stats"""

    @abstractmethod
    def collect_table_level_metrics(self,
                                    target_table_info: Dict[str, Any]) -> Dict[str, Any]:
        """Collect table level statistics"""

    @abstractmethod
    def collect_index_metrics(self,
                              target_table_info: Dict[str, Any],
                              num_index_to_collect_stats: int) -> Dict[str, Any]:
        """Collect index statistics"""

    @abstractmethod
    def get_version(self) -> str:
        """Get database version"""
