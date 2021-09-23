"""
The main entrypoint for the driver. The driver will poll for new configurations and schedule
executions of monitoring and tuning pipeline.
"""

from typing import NamedTuple
import argparse
import logging

from apscheduler.schedulers.background import BlockingScheduler

from driver.onprem_driver_config_builder import OnPremDriverConfigBuilder, Overrides
from driver.pipeline import schedule_or_update_job, MONITOR_JOB_ID

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
        "--aws-region",
        type=str,
        default="INFO",
        help="aws region, eg: us-east-2",
    )
    parser.add_argument(
        "--override-db-identifier",
        type=str,
        help="override aws rds database identifier",
        default=None,
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


def schedule_monitor_job(config) -> None:
    """
    The outer polling loop for the driver
    """
    schedule_or_update_job(scheduler, config, MONITOR_JOB_ID)


def get_config(args):
    """
    Build configuration from file, command line overrides, rds info, 
    """    
    driver_config_builder = OnPremDriverConfigBuilder(args.aws_region)
    overrides = Overrides(
        db_username=args.override_db_username,
        db_password=args.override_db_password,
        api_key=args.override_api_key,
        db_key=args.override_db_key,
        organization_id=args.override_organization_id,
        db_type=args.override_db_type,
        db_identifier=args.override_db_identifier
        monitor_interval=args.monitor_interval,
    )

    # build partial config to get db_identifier
    driver_config_builder.from_file(args.config).from_overrides(overrides)
    db_identifier = driver_config_builder.get_config()["db_identifier"]

    # finish building config
    driver_config_builder.from_rds(db_identifier).from_cloudwatch_metrics(db_identifier)
    config = driver_config_builder.from_overrides(overrides).get_config()


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

    scheduler.add_job(
        schedule_monitor_job,
        "interval",
        seconds=args.polling_interval,
        id="polling_loop",
        args=[config],
    )
    scheduler.start()


if __name__ == "__main__":
    run()
