"""
Defines the base class DriverConfigBuilder to build the driver configuraton.
It fetches the necessary information to run the driver pipeline.
"""
from abc import ABC, abstractmethod
from typing import Any


class DriverConfigBuilder(ABC):
    """Defines the driver config builder."""

    @abstractmethod
    def get_config(
        self,
    ) -> Any:  # pyre-ignore[3]: dataclasses and typing not happy together
        """Returns a dictionary containing everything necessary to run the driver pipeline."""
        return {}
