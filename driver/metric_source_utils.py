"""Util functions for retrieving from different metric source """

from typing import Any, Callable, Dict
from driver.cloudwatch_utils import cloudwatch_collector

METRIC_SOURCE_COLLECTOR: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]] = {
    "cloudwatch": cloudwatch_collector,
}
