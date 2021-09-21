"""
The main entrypoint for the driver. The driver will poll for new configurations and schedule
executions of monitoring and tuning pipeline.
"""

from typing import NamedTuple
import argparse
import logging

from apscheduler.schedulers.background import BlockingScheduler

from driver.onprem_driver_config_builder import create_onprem_driver_config_builder
from driver.onprem_driver_config_builder import OnPremDriverConfigBuilder
from driver.pipeline import (
    schedule_or_update_job,
    MONITOR_JOB_ID,
)

# Setup the scheduler that will poll for new configs and run the core pipeline
scheduler = BlockingScheduler()


def _get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Provide driver configuration")
    parser.add_argument(
        "--polling-interval",
        type=int,
        default=10,
        help="How often to check for new driver configuration (in seconds)",
    )
    parser.add_argument(
        "--monitor-interval",
        type=int,
        default=1,
        help="How often to collect new data (in seconds)",
    )
    parser.add_argument(
        "--log",
        type=str,
        default="INFO",
        help="Logging level, DEBUG,INFO,WARNING, etc.",
    )
    parser.add_argument(
        "--deployment",
        type=str,
        choices=["cloud", "onprem"],
        help="Cloud deployment (SaaS) or On-Prem deployment",
        required=True,
    )
    parser.add_argument(
        "--config",
        type=str,
        help="Currently required path to configuration file."
        " Required for on-prem deployment",
    )
    parser.add_argument(
        "--override-db-username",
        type=str,
        help="override username used for db",
        default=None,
    )
    parser.add_argument(
        "--override-db-password",
        type=str,
        help="override password used for db",
        default=None,
    )
    parser.add_argument(
        "--override-hostname",
        type=str,
        help="override hostname used for db",
        default=None,
    )
    parser.add_argument(
        "--override-port", type=str, help="override port used for db", default=None
    )
    parser.add_argument(
        "--override-api-key",
        type=str,
        help="override api key used to identify user",
        default=None,
    )
    parser.add_argument(
        "--override-db-key",
        type=str,
        help="override key used to identify database",
        default=None,
    )
    parser.add_argument(
        "--override-db-type",
        type=str,
        help="override db type (postgres or mysql)",
        default=None,
    )
    parser.add_argument(
        "--override-organization-id",
        type=str,
        help="override organization-id",
        default=None,
    )
    return parser.parse_args()


class Overrides(NamedTuple):
    """
    Runtime overrides for configurations in files, useful for when running in container
    """

    override_db_username: str
    override_db_password: str
    override_hostname: str
    override_port: str
    override_api_key: str
    override_db_key: str
    override_organization_id: str
    override_db_type: str
    monitor_interval: int


def poll_config_and_schedule_monitor_job(
    driver_config_builder: OnPremDriverConfigBuilder, overrides: Overrides
) -> None:
    """
    The outer polling loop for the driver
    """
    config = driver_config_builder.get_config()

    config.monitor_interval = int(overrides.monitor_interval)

    if overrides.override_db_username:
        config.db_user = overrides.override_db_username
    if overrides.override_db_password:
        config.db_password = overrides.override_db_password
    if overrides.override_hostname:
        config.db_host = overrides.override_hostname
    if overrides.override_port:
        config.db_port = int(overrides.override_port)
    if overrides.override_api_key:
        config.api_key = overrides.override_api_key
    if overrides.override_db_key:
        config.db_key = overrides.override_db_key
    if overrides.override_organization_id:
        config.override_organization_id = overrides.override_organization_id
    if overrides.override_db_type:
        config.db_type = overrides.override_db_type

    schedule_or_update_job(scheduler, config, MONITOR_JOB_ID)


def run() -> None:
    """
    The main entrypoint for the driver
    """

    args = _get_args()

    loglevel = args.log
    numeric_level = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError("Invalid log level: %s" % loglevel)
    logging.basicConfig(level=numeric_level)

    driver_config_builder = create_onprem_driver_config_builder(args.config)
    overrides = Overrides(
        override_db_username=args.override_db_username,
        override_db_password=args.override_db_password,
        override_hostname=args.override_hostname,
        override_port=args.override_port,
        override_api_key=args.override_api_key,
        override_db_key=args.override_db_key,
        override_organization_id=args.override_organization_id,
        override_db_type=args.override_db_type,
        monitor_interval=args.monitor_interval,
    )

    scheduler.add_job(
        poll_config_and_schedule_monitor_job,
        "interval",
        seconds=args.polling_interval,
        id="polling_loop",
        args=[driver_config_builder, overrides],
    )
    scheduler.start()


if __name__ == "__main__":
    run()
