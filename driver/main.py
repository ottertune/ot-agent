"""
The main entrypoint for the driver. The driver will poll for new configurations and schedule
executions of monitoring and tuning pipeline.
"""

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
        "--log-verbosity",
        type=str,
        default="INFO",
        help="Logging level, DEBUG,INFO,WARNING, etc.",
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
        required=True
    )
    parser.add_argument(
        "--db-identifier",
        type=str,
        help="AWS rds database identifier",
        required=True
    )
    parser.add_argument(
        "--db-username",
        type=str,
        help="Username used for db connection",
        required=True
    )
    parser.add_argument(
        "--db-password",
        type=str,
        help="Password used for db connection",
        required=True
    )
    parser.add_argument(
        "--api-key",
        type=str,
        help="API key used to identify OtterTune user",
        required=True
    )
    parser.add_argument(
        "--db-key",
        type=str,
        help="Key used to identify database to OtterTune",
        required=True
    )
    parser.add_argument(
        "--organization-id",
        type=str,
        help="Organization Id in Ottertune",
        required=True
    )
    parser.add_argument(
        "--override-monitor-interval",
        type=int,
        help="Override file setting for how often to collect new data (in seconds)",
    )
    parser.add_argument(
        "--override-server-url",
        type=str,
        help="Override file setting for endpoint to post observation data",
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
    config_builder = OnPremDriverConfigBuilder(args.aws_region)
    overrides = Overrides(monitor_interval=args.override_monitor_interval,
                          server_url=args.override_server_url,
    )

    config_builder.from_file(args.config)\
                  .from_overrides(overrides)\
                  .from_rds(args.db_identifier)\
                  .from_cloudwatch_metrics(args.db_identifier)\
                  .from_command_line(args)\
                  .from_overrides(overrides)

    config = config_builder.get_config()

    return config


def run() -> None:
    """
    The main entrypoint for the driver
    """

    args = _get_args()

    loglevel = args.log_verbosity
    numeric_level = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {loglevel}")
    logging.basicConfig(level=numeric_level)

    config = get_config(args)

    schedule_monitor_job(config)
    scheduler.start()


if __name__ == "__main__":
    run()
