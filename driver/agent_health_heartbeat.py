"""
Agent Health reporting
"""
import datetime
import queue
import traceback

from apscheduler.schedulers.background import BackgroundScheduler

from driver.compute_server_client import AgentHealthData, ComputeServerClient
from driver.driver_config_builder import DriverConfig

error_queue_global = queue.Queue()  # thread safe queue

SERVER_ENDPOINT = 'https://api.ottertune.com/agent_health'


def add_error_to_global(error):
    """
    Add error to the global error queue.
    """
    error_queue_global.put((error, datetime.datetime.now()))


def schedule_agent_health_job(config: DriverConfig,
                              agent_starttime: datetime.datetime,
                              agent_version: str):
    """
    Run the heartbeat sender.
    """
    scheduler = BackgroundScheduler()
    HEARTBEAT_INTERVAL_MINUTES = 1
    kwargs = {
        "next_run_time": datetime.datetime.now() + datetime.timedelta(minutes=HEARTBEAT_INTERVAL_MINUTES),
    }
    scheduler.add_job(send_heartbeat,
                      'interval',
                      minutes=HEARTBEAT_INTERVAL_MINUTES,
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
        "errors": construct_error_list_and_clear(error_queue_global),
    }
    ComputeServerClient.send_heartbeat(data)


def construct_error_list_and_clear(error_queue):
    """
    Construct the error objects from the error queue with stacktrace.
    Then clear the error queue.
    """
    errors = []
    while not error_queue.empty():
          error, timestamp = error_queue.get()
          errors.append({
                "data": {
                    "name": error.__class__.__name__,
                    "message": str(error),
                    "stacktrace": traceback.format_exc(),
                },
                "timestamp": timestamp,
            })
    error_queue.queue.clear()
    return errors
