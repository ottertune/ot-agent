""" Module containing pipelines (reoccuring function calls) for use with apscheduler. """
import traceback
from datetime import datetime
import logging

import requests
from requests import Session

from apscheduler.schedulers.background import BlockingScheduler

from driver.driver_config import DriverConfig
from driver.compute_server_client import ComputeServerClient
from driver.agent_health_heartbeat import add_error_to_global
from driver.exceptions import DriverException
from driver.s3_client import S3Client, ObservationType
from driver.database import (
    collect_db_level_observation_for_on_prem,
    collect_long_running_query_observation_for_on_prem,
    collect_table_level_observation_for_on_prem,
    collect_query_observation_for_on_prem,
    collect_schema_observation_for_on_prem,
)


TUNE_JOB_ID = "tune_job"
DB_LEVEL_MONITOR_JOB_ID = "db_level_monitor_job"
APPLY_EVENT_JOB_ID = "apply_event_job"
TABLE_LEVEL_MONITOR_JOB_ID = "table_level_monitor_job"
LONG_RUNNING_QUERY_MONITOR_JOB_ID = "long_running_query_monitor_job"
QUERY_MONITOR_JOB_ID = "query_monitor_job"
SCHEMA_MONITOR_JOB_ID = "schema_monitor_job"


def driver_pipeline(
    config: DriverConfig,
    job_id: str,  # pylint: disable=unused-argument
) -> None:
    """
    Run the core pipeline for the driver deployment
    """
    try:
        logging.info("Running driver pipeline deployment!")

        compute_server_client = ComputeServerClient(
            config.server_url, Session(), config.api_key
        )
        s3_client = S3Client(config.enable_s3, config.organization_id, config.api_key)

        if job_id == DB_LEVEL_MONITOR_JOB_ID:
            _db_level_monitor_driver_pipeline_for_on_prem(
                config, compute_server_client, s3_client
            )
        elif job_id == TABLE_LEVEL_MONITOR_JOB_ID:
            _table_level_monitor_driver_pipeline_for_on_prem(
                config, compute_server_client, s3_client
            )
        elif job_id == LONG_RUNNING_QUERY_MONITOR_JOB_ID:
            _long_running_query_monitor_driver_pipeline_for_on_prem(
                config, compute_server_client, s3_client
            )
        elif job_id == QUERY_MONITOR_JOB_ID:
            _query_monitor_driver_pipeline_for_on_prem(
                config, compute_server_client, s3_client
            )
        elif job_id == SCHEMA_MONITOR_JOB_ID:
            _schema_monitor_driver_pipeline_for_on_prem(
                config, compute_server_client, s3_client
            )
        else:
            raise DriverException(f"Unknown job id {job_id}")
    except requests.RequestException as e:
        logging.error(f"Network error during driver pipeline for job_id {job_id}: {str(e)}")
        stacktrace = traceback.format_exc()
        add_error_to_global(e, stacktrace)
    except Exception as e:
        logging.error(f"Unexpected error during driver pipeline for job_id {job_id}: {str(e)}")
        stacktrace = traceback.format_exc()
        add_error_to_global(e, stacktrace)


def _db_level_monitor_driver_pipeline_for_on_prem(
    config: DriverConfig,
    compute_server_client: ComputeServerClient,
    s3_client: S3Client,
) -> None:
    """
    Regular monitoring pipeline that collects database level metrics and configs every minute

    Args:
        config: Driver configuration.
        compute_server_client: Client interacting with server in Ottertune.
    Raises:
        DriverException: Driver error.
        Exception: Other unknown exceptions that are not caught as DriverException.
    """
    logging.debug("Collecting db level observation data.")
    db_level_observation = collect_db_level_observation_for_on_prem(config)

    logging.debug("Posting db level observation data to the server.")
    if config.enable_s3:
        s3_client.post_observation(db_level_observation, ObservationType.DB)
        logging.info("Posted db level observation data to S3.")
    else:
        compute_server_client.post_db_level_observation(db_level_observation)


def _table_level_monitor_driver_pipeline_for_on_prem(
    config: DriverConfig,
    compute_server_client: ComputeServerClient,
    s3_client: S3Client,
) -> None:
    """
    Regular monitoring pipeline that collects table level metrics every hour

    Args:
        config: Driver configuration.
        comppute_server_client: Client interacting with server in Ottertune.
    Raises:
        DriverException: Driver error.
        Exception: Other unknown exceptions that are not caught as DriverException.
    """
    logging.debug("Collecting table level observation data")
    table_level_observation = collect_table_level_observation_for_on_prem(config)

    logging.debug("Posting table level observation data to the server.")

    if config.enable_s3:
        s3_client.post_observation(table_level_observation, ObservationType.TABLE)
        logging.info("Posted table level observation data to S3.")
    else:
        compute_server_client.post_table_level_observation(table_level_observation)


def _long_running_query_monitor_driver_pipeline_for_on_prem(
    config: DriverConfig,
    compute_server_client: ComputeServerClient,
    s3_client: S3Client,
) -> None:
    """
    Regular monitoring pipeline that collects long running query instances every minute
    Args:
        config: Driver configuration.
        compute_server_client: Client interacting with server in Ottertune.
    Raises:
        DriverException: Driver error.
        Exception: Other unknown exceptions that are not caught as DriverException.
    """
    query_observation = collect_long_running_query_observation_for_on_prem(config)
    if config.enable_s3:
        s3_client.post_observation(
            query_observation, ObservationType.LONG_RUNNING_QUERY
        )
        logging.info("Posted long running query observation data to S3.")
    else:
        compute_server_client.post_long_running_query_observation(query_observation)


def _query_monitor_driver_pipeline_for_on_prem(
    config: DriverConfig,
    compute_server_client: ComputeServerClient,
    s3_client: S3Client,
) -> None:
    """
    Regular monitoring pipeline that collects queries every day
    Args:
        config: Driver configuration.
        compute_server_client: Client interacting with server in Ottertune.
    Raises:
        DriverException: Driver error.
        Exception: Other unknown exceptions that are not caught as DriverException.
    """
    query_observation = collect_query_observation_for_on_prem(config)
    if config.enable_s3:
        s3_client.post_observation(query_observation, ObservationType.QUERY)
        logging.info("Posted query observation data to S3.")
    else:
        compute_server_client.post_query_observation(query_observation)


def _schema_monitor_driver_pipeline_for_on_prem(
    config: DriverConfig,
    compute_server_client: ComputeServerClient,
    s3_client: S3Client,
) -> None:
    """
    Regular monitoring pipeline that collects schemas every day
    Args:
        config: Driver configuration.
        compute_server_client: Client interacting with server in Ottertune.
    Raises:
        DriverException: Driver error.
        Exception: Other unknown exceptions that are not caught as DriverException.
    """
    schema_observation = collect_schema_observation_for_on_prem(config)
    if config.enable_s3:
        s3_client.post_observation(schema_observation, ObservationType.SCHEMA)
        logging.info("Posted schema observation data to S3.")
    else:
        compute_server_client.post_schema_observation(schema_observation)


def _get_interval(config: DriverConfig, job_id: str) -> int:
    """Get the scheduled time interval (sec) based on job id."""

    if job_id == DB_LEVEL_MONITOR_JOB_ID:
        interval_s = int(config.monitor_interval)
    elif job_id == TABLE_LEVEL_MONITOR_JOB_ID:
        interval_s = int(config.table_level_monitor_interval)
    elif job_id == LONG_RUNNING_QUERY_MONITOR_JOB_ID:
        interval_s = int(config.long_running_query_monitor_interval)
    elif QUERY_MONITOR_JOB_ID in job_id:
        interval_s = int(config.query_monitor_interval)
    elif SCHEMA_MONITOR_JOB_ID in job_id:
        interval_s = int(config.schema_monitor_interval)
    else:
        raise ValueError(f"Job {job_id} is not supported")
    return interval_s


def _start_job(
    scheduler: BlockingScheduler, config: DriverConfig, job_id: str, interval: int
) -> None:
    "Helper to start new job"
    logging.info("Initializing driver pipeline (job %s)...", job_id)

    kwargs = {}
    if job_id in (
        DB_LEVEL_MONITOR_JOB_ID,
        TABLE_LEVEL_MONITOR_JOB_ID,
        LONG_RUNNING_QUERY_MONITOR_JOB_ID,
        QUERY_MONITOR_JOB_ID,
        SCHEMA_MONITOR_JOB_ID,
    ):
        kwargs["next_run_time"] = datetime.now()

    scheduler.add_job(
        driver_pipeline,
        "interval",
        seconds=interval,
        args=[config, job_id],
        id=job_id,
        **kwargs,
    )
    logging.info("Running driver pipeline every %d seconds (job %s).", interval, job_id)


def _update_job(
    scheduler: BlockingScheduler,
    old_config: DriverConfig,
    new_config: DriverConfig,
    job_id: str,
    interval: int,
) -> None:
    "Helper to update pre-existing job"
    logging.info("Found new config (job %s)...", job_id)
    # grab old interval before modification
    old_interval = _get_interval(old_config, job_id)
    scheduler.modify_job(job_id, args=[new_config, job_id])
    if old_interval != interval:
        scheduler.reschedule_job(job_id, trigger="interval", seconds=interval)
        logging.info(
            "Running driver pipeline every %d seconds (job %s).", interval, job_id
        )


def schedule_or_update_job(
    scheduler: BlockingScheduler, config: DriverConfig, job_id: str
) -> None:
    """
    Apply configuration change to the job. If the configuration does not change, it will do nothing.
    If the job is not scheduled, it will start a job.

    Args:
        config: Driver configuration.
        job_id: Job ID.
    Raises:
        DriverException: Driver error.
        Exception: Other unknown exceptions that are not caught as DriverException.
    """
    interval = _get_interval(config, job_id)
    job = scheduler.get_job(job_id)

    if not job:
        # NB: first invocation is at current_time + interval
        _start_job(scheduler=scheduler, config=config, job_id=job_id, interval=interval)
    else:
        old_config = job.args[0]
        if old_config != config:
            _update_job(
                scheduler=scheduler,
                old_config=old_config,
                new_config=config,
                job_id=job_id,
                interval=interval,
            )
