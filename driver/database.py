"""
The driver pipeline function. It's responsible for a single tuning/monitoring loop.
"""
import logging
import time
from typing import Dict, Any

from driver.compute_server_client import (
    DBLevelObservation,
    SchemaObservation,
    TableLevelObservation,
    QueryObservation,
)
from driver.collector.collector_factory import get_collector
from driver.driver_config_builder import DriverConfig
from driver.exceptions import DbCollectorException
from driver.metric_source_utils import METRIC_SOURCE_COLLECTOR


def collect_db_level_observation_for_on_prem(config: DriverConfig) -> DBLevelObservation:
    """
    Get the db level observation data for the target cloud database. It may have multiple sources,
    e.g., runtime knobs/metrics data from the database, metrics from cloudwatch, etc.

    Args:
        config: driver configuration for cloud deployment.
    Returns:
        DB level observation data from the target database.
    Raises:
        DriverConfigException: invalid database configuration.
        DbCollectorException: database type is not supported.
        MysqlCollectorException: unable to connect to MySQL database or get version.
        PostgresCollectorException: unable to connect to Postgres database or get version.
    """
    observation = collect_db_level_data_from_database(config._asdict())
    metrics_from_sources = collect_data_from_metric_sources(config._asdict())
    observation["metrics_data"]["global"].update(metrics_from_sources)
    return observation


def collect_table_level_observation_for_on_prem(config: DriverConfig) -> TableLevelObservation:
    """
    Get the table level observation data for the target cloud database.

    Args:
        config: driver configuration for cloud deployment.
    Returns:
        Table level observation data from the target database.
    Raises:
        DriverConfigException: invalid database configuration.
        DbCollectorException: database type is not supported.
        MysqlCollectorException: unable to connect to MySQL database or get version.
        PostgresCollectorException: unable to connect to Postgres database or get version.
    """
    observation = collect_table_level_data_from_database(config._asdict())
    return observation


def collect_query_observation_for_on_prem(config: DriverConfig) -> QueryObservation:
    """
    Get the query observation data for the target cloud database.

    Args:
        config: driver configuration for cloud deployment.
    Returns:
        Query observation data from the target database.
    Raises:
        DriverConfigException: invalid database configuration.
        DbCollectorException: database type is not supported.
        MysqlCollectorException: unable to connect to MySQL database or get version.
        PostgresCollectorException: unable to connect to Postgres database or get version.
    """
    observation = collect_query_observation_from_database(config._asdict())
    return observation

def collect_schema_observation_for_on_prem(config: DriverConfig) -> SchemaObservation:
    """
    Get the query observation data for the target cloud database.

    Args:
        config: driver configuration for cloud deployment.
    Returns:
        Query observation data from the target database.
    Raises:
        DriverConfigException: invalid database configuration.
        DbCollectorException: database type is not supported.
        MysqlCollectorException: unable to connect to MySQL database or get version.
        PostgresCollectorException: unable to connect to Postgres database or get version.
    """
    observation = collect_schema_observation_from_database(config._asdict())
    return observation

def collect_data_from_metric_sources(driver_conf: Dict[str, Any]) -> Dict[str, Any]:
    """Get data from various metric sources"""

    metrics: Dict[str, Any] = {}
    for metric_source in driver_conf["metric_source"]:
        metrics[metric_source] = METRIC_SOURCE_COLLECTOR[metric_source](
            driver_conf,
        )
    return metrics


def collect_db_level_data_from_database(driver_conf: Dict[str, Any]) -> DBLevelObservation:
    """
    Get the db level knobs, metrics, summary data from the target database.

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
        row_num_stats = collector.collect_table_row_number_stats()
        version = collector.get_version()
        summary: Dict[str, Any] = {
            "version": version,
            "observation_time": observation_time,
        }

    observation: DBLevelObservation = {
        "knobs_data": knobs,
        "metrics_data": metrics,
        "summary": summary,
        "row_num_stats": row_num_stats,
        "db_key": driver_conf["db_key"],
        "organization_id": driver_conf["organization_id"],
        "non_default_knobs": driver_conf["db_non_default_parameters"]
    }
    return observation


def collect_table_level_data_from_database(driver_conf: Dict[str, Any]) -> TableLevelObservation:
    """
    Get the table level metrics data from the target database.

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
        target_table_info = collector.get_target_table_info(
            driver_conf["num_table_to_collect_stats"])
        data: Dict[str, Any] = {}
        if not driver_conf.get("disable_table_level_stats", False):
            table_level_data = collector.collect_table_level_metrics(target_table_info)
            data.update(table_level_data)
        if not driver_conf.get("disable_index_stats", False):
            try:
                index_data = collector.collect_index_metrics(
                    target_table_info,
                    driver_conf["num_index_to_collect_stats"])
                data.update(index_data)
            except DbCollectorException:
                logging.exception("Error raised during index stats collection.")
        version = collector.get_version()
        summary: Dict[str, Any] = {
            "version": version,
            "observation_time": observation_time,
        }

    observation: TableLevelObservation = {
        "data": data,
        "summary": summary,
        "db_key": driver_conf["db_key"],
        "organization_id": driver_conf["organization_id"],
    }
    return observation


def collect_query_observation_from_database(config: Dict[str, Any]) -> QueryObservation:
    """
    Collect query metrics from databases and compress the data

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
    with get_collector(config) as collector:
        observation_time = int(time.time())
        query_metrics = collector.collect_query_metrics(int(config["num_query_to_collect"]))
        version = collector.get_version()
        summary: Dict[str, Any] = {
            "version": version,
            "observation_time": observation_time,
        }

    observation: QueryObservation = {
        "data": query_metrics,
        "summary": summary,
        "db_key": config["db_key"],
        "organization_id": config["organization_id"],
    }
    return observation

def collect_schema_observation_from_database(config: Dict[str, Any]) -> QueryObservation:
    """
    Collect schema metrics from databases and compress the data

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
    with get_collector(config) as collector:
        observation_time = int(time.time())
        schema = collector.collect_schema()
        version = collector.get_version()
        summary: Dict[str, Any] = {
            "version": version,
            "observation_time": observation_time,
        }

    observation: SchemaObservation = {
        "data": schema,
        "summary": summary,
        "db_key": config["db_key"],
        "organization_id": config["organization_id"],
    }
    return observation
