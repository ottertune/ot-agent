""" Module containing pipelines (reoccuring function calls) for use with apscheduler. """

from datetime import datetime
import logging

from apscheduler.schedulers.background import BlockingScheduler

from driver.onprem_driver_config_builder import OnPremDriverConfig
from driver.request import build_request_session
from driver.compute_server_client import ComputeServerClient
from driver.database import collect_observation_for_on_prem

TUNE_JOB_ID = "tune_job"
MONITOR_JOB_ID = "monitor_job"
APPLY_EVENT_JOB_ID = "apply_event_job"


def driver_pipeline_for_onprem(
    config: OnPremDriverConfig,
    job_id: str, # pylint: disable=unused-argument
) -> None:
    """
    Run the core pipeline for the driver in on-prem deployment
    """
    logging.info("Running driver pipeline for on-prem deployment!")

    logging.debug("Collecting observation data.")
    observation = collect_observation_for_on_prem(config)

    logging.debug("Posting observation data to the server.")

    req_session = build_request_session()
    compute_server_client = ComputeServerClient(
        config.server_url, req_session, config.api_key
    )

    compute_server_client.post_observation(observation)


def _get_interval(config: OnPremDriverConfig, job_id: str) -> int:
    """Get the scheduled time interval (sec) based on job id."""

    if job_id == MONITOR_JOB_ID:
        interval_s = int(config.monitor_interval)
    else:
        raise ValueError(f"Job {job_id} is not supported")
    return interval_s


def _start_job(
    scheduler: BlockingScheduler, config: OnPremDriverConfig, job_id: str, interval: int
) -> None:
    "Helper to start new job"
    logging.info("Initializing driver pipeline (job %s)...", job_id)
    driver_pipeline = driver_pipeline_for_onprem

    kwargs = {}
    if job_id == MONITOR_JOB_ID:
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
    old_config: OnPremDriverConfig,
    new_config: OnPremDriverConfig,
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
    scheduler: BlockingScheduler, config: OnPremDriverConfig, job_id: str
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
