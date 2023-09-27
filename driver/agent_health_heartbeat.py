"""
Agent Health reporting
"""
import datetime
import queue

from apscheduler.schedulers.background import BackgroundScheduler
from requests import Session

from driver.compute_server_client import AgentHealthData, ComputeServerClient
from driver.driver_config import DriverConfig

error_queue_global = queue.Queue()  # thread safe queue

SERVER_ENDPOINT = 'https://api.ottertune.com/agent_health'


def add_error_to_global(error, stacktrace=None):
    """
    Add error to the global error queue.
    """
    error_queue_global.put((error, datetime.datetime.utcnow(), stacktrace))


def schedule_agent_health_job(config: DriverConfig,
                              agent_starttime: datetime.datetime,
                              agent_version: str):
    """
    Run the heartbeat sender.
    """
    scheduler = BackgroundScheduler()
    heartbeat_interval_minutes = 1
    kwargs = {
        "next_run_time":
            datetime.datetime.now() + datetime.timedelta(minutes=heartbeat_interval_minutes),
    }

    scheduler.add_job(send_heartbeat,
                      'interval',
                      minutes=heartbeat_interval_minutes,
                      args=[config, agent_starttime, agent_version],
                      kwargs=kwargs)
    scheduler.start()


def send_heartbeat(config: DriverConfig,
                   agent_starttime: datetime.datetime,
                   agent_version: str):
    """
    Send heartbeat to the compute-service.
    """
    data: AgentHealthData = {
        "organization_id": config.organization_id,
        "db_key": config.db_key,
        "agent_status": "Error" if error_queue_global else "OK",
        "agent_starttime": agent_starttime,
        "heartbeat_time": datetime.datetime.now().isoformat(),
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
        errors.append({
            "data": {
                "name": error.__class__.__name__,
                "message": str(error),
                "stacktrace": stacktrace
            },
            "timestamp": timestamp,
        })
    error_queue.queue.clear()
    return errors
