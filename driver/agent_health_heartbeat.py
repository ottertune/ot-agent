"""
Agent Health reporting
"""
import datetime
import logging
import queue

from apscheduler.schedulers.background import BackgroundScheduler
from requests import Session

from driver.compute_server_client import AgentHealthData, ComputeServerClient
from driver.driver_config import DriverConfig

error_queue_global = queue.Queue()  # thread safe queue

SERVER_ENDPOINT = "https://api.ottertune.com/agent_health"


def add_error_to_global(error, stacktrace=None):
    """
    Add error to the global error queue.
    """
    error_queue_global.put((error, datetime.datetime.utcnow(), stacktrace))


def schedule_agent_health_job(
    config: DriverConfig, agent_starttime: datetime.datetime, agent_version: str
):
    """
    Run the heartbeat sender.
    """
    scheduler = BackgroundScheduler()
    interval_seconds = config.agent_health_report_interval
    kwargs = {
        "next_run_time": datetime.datetime.now()
        + datetime.timedelta(seconds=interval_seconds),
    }

    scheduler.add_job(
        send_heartbeat,
        "interval",
        seconds=interval_seconds,
        args=[config, agent_starttime, agent_version],
        **kwargs
    )
    scheduler.start()
    logging.info("Agent health job scheduled")


def send_heartbeat(
    config: DriverConfig,
    agent_starttime: datetime.datetime,
    agent_version: str,
    terminating: bool = False,
):
    """
    Send heartbeat to the compute-service.
    """
    if terminating:
        if not error_queue_global.empty():
            status = "terminating_error"
        else:
            status = "terminating_ok"
    elif not error_queue_global.empty():
        status = "error"
    else:
        status = "ok"

    data: AgentHealthData = {
        "organization_id": config.organization_id,
        "db_key": config.db_key,
        "agent_status": status,
        "agent_starttime": agent_starttime.isoformat(),
        "heartbeat_time": datetime.datetime.utcnow().isoformat(),
        "agent_version": agent_version,
        "errors": construct_error_list_and_clear(),
    }
    compute_server_client = ComputeServerClient(
        config.server_url, Session(), config.api_key
    )
    compute_server_client.post_agent_health_heartbeat(data)


def construct_error_list_and_clear(error_queue=error_queue_global):
    """
    Construct the error objects from the error queue with stacktrace.
    Then clear the error queue.
    """
    errors = []
    while not error_queue.empty():
        error, timestamp, stacktrace = error_queue.get()
        errors.append(
            {
                "data": {
                    "name": error.__class__.__name__,
                    "message": str(error),
                    "stacktrace": stacktrace,
                },
                "timestamp": timestamp,
            }
        )
    error_queue.queue.clear()
    return errors
