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
    def get_version(self) -> str:
        """Get database version"""
