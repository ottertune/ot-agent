"""
Defines the DriverConfigBuilder to build the driver configuraton for on-prem deployment.
It fetches the necessary information to run the driver pipeline from the local file and server.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, NamedTuple, List, Optional
import json
import logging
import os

from pydantic import (
    BaseModel,
    StrictBool,
    StrictInt,
    StrictStr,
    validator,
    ValidationError,
)
import yaml

from driver.aws.rds import get_db_version, get_db_port, get_db_hostname, get_db_type
from driver.aws.wrapper import AwsWrapper
from driver.exceptions import DriverConfigException


class BaseDriverConfigBuilder(ABC):
    """Defines the driver config builder."""

    @abstractmethod
    def get_config(self) -> Any:
        """Returns a dictionary containing everything necessary to run the driver pipeline."""
        return {}


class PartialConfigFromFile(BaseModel):  # pyre-ignore[13]: pydantic uninitialized variables
    """Driver options fetched from local file for on-prem deployment.

    Such options are part of the complete driver options (defined in DriverConfig).
    It validates that options do not have missing values, wrong types or invalid values.
    """
    server_url: StrictStr
    monitor_interval: StrictInt
    num_table_to_collect_stats: StrictInt
    table_level_monitor_interval: StrictInt
    metric_source: List[str]

    @validator("table_level_monitor_interval")
    def check_table_level_monitor_interval(cls, val: int) -> int:  # pylint: disable=no-self-argument, no-self-use
        """Validate that table_level_monitor_interval is greater than 5 minutes"""
        if val < 300:
            raise ValueError(
                "Invalid driver option table_level_monitor_interval, value >= 300"
                f" is expected, but {val} is found"
            )
        return val

    @validator("monitor_interval")
    def check_monitor_interval(cls, val: int) -> int:  # pylint: disable=no-self-argument, no-self-use
        """Validate that monitor_interval is positive and at least 60 seconds."""
        if val < 60:
            raise ValueError(
                "Invalid driver option monitor_interval, positive value"
                f" is expected, but {val} is found"
            )
        return val

    @validator("num_table_to_collect_stats")
    def check_num_table_to_collect_stats(cls, val: int) -> int:  # pylint: disable=no-self-argument, no-self-use
        """Validate that num_table_to_collect_stats is not negative"""
        if val < 0:
            raise ValueError(
                "Invalid driver option num_table_to_collect_stats, non-negative value"
                f" is expected, but {val} is found"
            )
        return val

class Overrides(NamedTuple):
    """
    Runtime overrides for configurations in files, useful for when running in container
    """
    monitor_interval: int
    server_url: str
    num_table_to_collect_stats: int
    table_level_monitor_interval: int


class PartialConfigFromEnvironment(BaseModel):  # pyre-ignore[13]: pydantic uninitialized variables
    """Driver options fetched from RDS for agent deployment.

    Such options are part of the complete driver options (defined in DriverConfig).
    """
    db_name: Optional[StrictStr]


class PartialConfigFromCommandline(BaseModel):  # pyre-ignore[13]: pydantic uninitialized variables
    """Driver options fetched from RDS for agent deployment.

    Such options are part of the complete driver options (defined in DriverConfig).
    """
    api_key: StrictStr
    db_key: StrictStr
    organization_id: StrictStr

    aws_region: StrictStr
    db_identifier: StrictStr

    db_user: StrictStr
    db_password: StrictStr

    disable_table_level_stats: StrictBool


class PartialConfigFromRDS(BaseModel):  # pyre-ignore[13]: pydantic uninitialized variables
    """Driver options fetched from RDS for agent deployment.

    Such options are part of the complete driver options (defined in DriverConfig).
    """
    db_host: StrictStr
    db_port: StrictInt
    db_version: StrictStr
    db_type: StrictStr


class PartialConfigFromCloudwatchMetrics(BaseModel):  # pyre-ignore[13]: uninitialized variables
    """Driver options fetched from RDS for agent deployment.

    Such options are part of the complete driver options (defined in DriverConfig).
    """
    metrics_to_retrieve_from_source: Dict[StrictStr, List[StrictStr]]


class DriverConfig(NamedTuple):  # pylint: disable=too-many-instance-attributes
    """Driver Config for on-prem deployment."""
    server_url: str  # OtterTune server url, required

    db_identifier: str  # AWS RDS Database identifier, required
    aws_region: str  # AWS Region of Database and cloudwatch logs, required

    db_type: str  # Database type (mysql or postgres), required
    db_host: str  # Database host address, required
    db_port: int  # Database port, required
    db_version: str  # Database version number, key for what metrics to fetch, required
    db_user: str  # Database username, required
    db_password: str  # Database password, required

    db_name: str  # Database name in DBMS to focus on, optional

    api_key: str  # API key handed to agent proxy to identify user
    db_key: str  # DB key handed to agent proxy to identify database
    organization_id: str  # Org id handed to agent proxy to identify database

    monitor_interval: int  # how frequently to query database for metrics

    metric_source: List[
        str
    ]  # Extra metric sources where we want to collect metric data
    metrics_to_retrieve_from_source: Dict[
        str, List[str]
    ]  # A list of target metric names
    disable_table_level_stats: bool
    num_table_to_collect_stats: int
    table_level_monitor_interval: int


class DriverConfigBuilder(BaseDriverConfigBuilder):
    """Defines the builder to build driver config for on-prem deployment."""

    def __init__(self, aws_region) -> None:
        self.config = {}
        self.rds_client = AwsWrapper.rds_client(aws_region)
        self.has_determined_db_type = False

    def from_file(self, config_path: str) -> BaseDriverConfigBuilder:
        """build config options from config file"""
        with open(config_path, "r", encoding="utf-8") as config_file:
            data = yaml.safe_load(config_file)
            print(f"??? data is {data}")
            if not isinstance(data, dict):
                raise ValueError("Invalid data in the driver configuration YAML file")

            try:
                partial_config_from_file: PartialConfigFromFile = (
                    PartialConfigFromFile(**data)
                )
            except ValidationError as ex:
                msg = (
                    "Invalid driver configuration for On-Prem deployment: "
                    "the driver option from file is missing or invalid"
                )
                raise DriverConfigException(msg, ex) from ex
            self.config.update(partial_config_from_file)
        return self

    def from_command_line(self, args) -> BaseDriverConfigBuilder:
        """build config options from command line arguments that aren't overriding other builders"""
        try:
            from_cli = PartialConfigFromCommandline(aws_region=args.aws_region,
                                                    db_identifier=args.db_identifier,
                                                    db_user=args.db_username,
                                                    db_password=args.db_password,
                                                    api_key=args.api_key,
                                                    db_key=args.db_key,
                                                    organization_id=args.organization_id,
                                                    disable=args.disable_table_level_stats)
        except ValidationError as ex:
            msg = (
                "Invalid driver configuration for On-Prem deployment: "
                "the driver option from commandline is missing or invalid"
            )
            raise DriverConfigException(msg, ex) from ex

        self.config.update(from_cli)
        return self

    def from_env_vars(self):
        """build config options from environment variables"""
        db_name = os.getenv("POSTGRES_OTTERTUNE_DB_NAME", None)

        if not self.has_determined_db_type:
            msg = "Builder must know db type before from_env_vars, try running from_rds first"
            raise DriverConfigException(msg)

        if self.config["db_type"] == "postgres":
            if not db_name:
                msg = ("Must supply database name for Postgres via environment variable: "
                       "POSTGRES_OTTERTUNE_DB_NAME")
                raise DriverConfigException(msg)
        elif self.config["db_type"] == "mysql":
            if db_name:
                msg = "Ignoring POSTGRES_OTTERTUNE_DB_NAME as this agent is connected to a MySql db"
                logging.warning(msg)
                db_name = None

        config_from_env = {
            "db_name": db_name
        }

        try:
            partial_config_from_env: PartialConfigFromEnvironment = (
                PartialConfigFromEnvironment(**config_from_env)
            )
        except ValidationError as ex:
            msg = (
                "Invalid driver configuration the driver option from env vars is missing or invalid"
            )
            raise DriverConfigException(msg, ex) from ex

        self.config.update(partial_config_from_env)
        return self

    def from_rds(self, db_instance_identifier) -> BaseDriverConfigBuilder:
        """build config options from rds description of database"""
        config_from_rds = {
            "db_host": get_db_hostname(db_instance_identifier, self.rds_client),
            "db_port": get_db_port(db_instance_identifier, self.rds_client),
            "db_version": get_db_version(db_instance_identifier, self.rds_client),
            "db_type": get_db_type(db_instance_identifier, self.rds_client)
        }

        try:
            partial_config_from_rds: PartialConfigFromRDS = (
                PartialConfigFromRDS(**config_from_rds)
            )
        except ValidationError as ex:
            msg = (
                "Invalid driver configuration for On-Prem deployment: "
                "the driver option from rds is missing or invalid"
            )
            raise DriverConfigException(msg, ex) from ex

        self.has_determined_db_type = True
        self.config.update(partial_config_from_rds)
        return self

    def _get_cloudwatch_metrics_file(self, db_instance_identifier):
        """
         For aurora mysql 5.6:
           db_version = 5_6_mysql_aurora_1_22_2
           db_type = aurora_mysql
         For aurora mysql 5.7:
           db_version = 5_7_mysql_aurora_2_10_1
           db_type = aurora_mysql
         For aurora postgres
           db_version = 10_x / 11_x / 12_x
           db_type = aurora_postgresql
        """
        db_version = get_db_version(db_instance_identifier, self.rds_client)
        db_type = get_db_type(db_instance_identifier, self.rds_client)

        if "aurora" in db_type:
            db_version_breakdown = db_version.split("_")
            if db_type == "aurora_postgresql":
                db_version = db_version_breakdown[0]
            else:
                db_version = f"{db_version_breakdown[0]}_{db_version_breakdown[1]}"
        else:
            if "postgres" in db_type:
                # drop minor version except for 9_6
                if "9_6" in db_version:
                    major, minor, _ = db_version.split("_")
                    db_version = f"{major}_{minor}"
                else:
                    db_version, _ = db_version.split("_")
            if "mysql" in db_type:
                # drop minor version if present
                try:
                    release, major, _ = db_version.split("_")
                except ValueError:
                    release, major = db_version.split("_")
                db_version = f"{release}_{major}"

        folder_path = "./driver/config/cloudwatch_metrics"
        return f"{folder_path}/rds_{db_type}-{db_version}.json"

    def from_cloudwatch_metrics(self, db_instance_identifier) -> BaseDriverConfigBuilder:
        """Build config options from cloudwatch metrics configurations"""
        metric_names = []
        file_path = self._get_cloudwatch_metrics_file(db_instance_identifier)
        with open(file_path, "r", encoding="utf-8") as metrics_file:
            metrics = json.load(metrics_file)
            for metric in metrics:
                metric_names.append(metric["name"])

        self.config.update(
            {"metrics_to_retrieve_from_source": {"cloudwatch": metric_names}}
        )
        return self

    def from_overrides(self, overrides) -> BaseDriverConfigBuilder:
        """Override config options supplied from other builder steps"""
        supplied_overrides = {
            k: v for k, v in overrides._asdict().items() if v is not None
        }
        self.config.update(supplied_overrides)
        return self

    def get_config(self) -> DriverConfig:
        """Get driver configuration for on-prem deployment.

        Returns:
            driver configuration for on-prem deployment.
        Raises:
            DriverConfigException: Invalid driver configuration for on-prem deployment.
        """
        driver_config: DriverConfig = DriverConfig(**self.config)
        return driver_config
