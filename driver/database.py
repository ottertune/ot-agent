"""
The driver pipeline function. It's responsible for a single tuning/monitoring loop.
"""

import time
from typing import Dict, Any

from driver.compute_server_client import Observation
from driver.collector.collector_factory import get_collector
from driver.driver_config_builder import DriverConfig
from driver.metric_source_utils import METRIC_SOURCE_COLLECTOR


def collect_observation_for_on_prem(config: DriverConfig) -> Observation:
    """
    Get the observation data for the target cloud database. It may have multiple sources,
    e.g., runtime knobs/metrics data from the database, metrics from cloudwatch, etc.

    Args:
        config: driver configuration for cloud deployment.
    Returns:
        Observation data from the target database.
    Raises:
        DriverConfigException: invalid database configuration.
        DbCollectorException: database type is not supported.
        MysqlCollectorException: unable to connect to MySQL database or get version.
        PostgresCollectorException: unable to connect to Postgres database or get version.
    """
    observation = collect_data_from_database(config._asdict())
    metrics_from_sources = collect_data_from_metric_sources(config._asdict())
    observation["metrics_data"]["global"].update(metrics_from_sources)
    return observation


def collect_data_from_metric_sources(driver_conf: Dict[str, Any]) -> Dict[str, Any]:
    """Get data from various metric sources"""

    metrics: Dict[str, Any] = {}
    for metric_source in driver_conf["metric_source"]:
        metrics[metric_source] = METRIC_SOURCE_COLLECTOR[metric_source](
            driver_conf,
        )
    return metrics


def collect_data_from_database(driver_conf: Dict[str, Any]) -> Observation:
    """
    Get the knobs, metrics, summary data from the target database.

    Args:
        config: driver configuration.
    Returns:
        Collected data from the target database.
    Raises:
        DriverConfigException: invalid database configuration.
        DbCollectorException: database type is not supported.
        MysqlCollectorException: unable to connect to MySQL database or get version.
        PostgresCollectorException: unable to connect to Postgres database or get version.
    """

    with get_collector(driver_conf) as collector:
        observation_time = int(time.time())
        knobs = collector.collect_knobs()
        metrics = collector.collect_metrics()
        version = collector.get_version()
        summary: Dict[str, Any] = {
            "version": version,
            "observation_time": observation_time,
        }

    observation: Observation = {
        "knobs_data": knobs,
        "metrics_data": metrics,
        "summary": summary,
        "db_key": driver_conf["db_key"],
        "organization_id": driver_conf["organization_id"],
    }
    return observation
