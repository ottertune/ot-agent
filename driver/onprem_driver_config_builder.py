"""
Defines the OnPremDriverConfigBuilder to build the driver configuraton for on-prem deployment.
It fetches the necessary information to run the driver pipeline from the local file and server.
"""

from typing import Dict, NamedTuple, List
import json

from pydantic import (
    BaseModel,
    StrictInt,
    StrictStr,
    validator,
    ValidationError,
)
import yaml

from driver.aws.rds import get_db_version, get_db_port, get_db_hostname, get_db_type
from driver.aws.wrapper import AwsWrapper
from driver.driver_config_builder import DriverConfigBuilder
from driver.exceptions import DriverConfigException


class PartialOnPremConfigFromFile(
    BaseModel
):  # pyre-ignore[13]: pydantic uninitialized variables
    """Driver options fetched from local file for on-prem deployment.

    Such options are part of the complete driver options (defined in OnPremDriverConfig).
    It validates that options do not have missing values, wrong types or invalid values.
    """
    server_url: StrictStr
    monitor_interval: StrictInt
    metric_source: List[str]

    @validator("monitor_interval")
    def check_monitor_interval(  # pylint: disable=no-self-argument, no-self-use
        cls, val: int
    ) -> int:
        """Validate that monitor_interval is positive and at least 60 seconds."""
        if val < 60:
            raise ValueError(
                "Invalid driver option monitor_interval, positive value"
                f" is expected, but {val} is found"
            )
        return val


class Overrides(NamedTuple):
    """
    Runtime overrides for configurations in files, useful for when running in container
    """
    monitor_interval: int
    server_url: str


class PartialOnPremConfigFromCommandline(
    BaseModel
):  # pyre-ignore[13]: pydantic uninitialized variables
    """Driver options fetched from RDS for agent deployment.

    Such options are part of the complete driver options (defined in OnPremDriverConfig).
    """
    api_key: StrictStr
    db_key: StrictStr
    organization_id: StrictStr

    aws_region: StrictStr
    db_identifier: StrictStr

    db_user: StrictStr
    db_password: StrictStr

class PartialOnPremConfigFromRDS(
    BaseModel
):  # pyre-ignore[13]: pydantic uninitialized variables
    """Driver options fetched from RDS for agent deployment.

    Such options are part of the complete driver options (defined in OnPremDriverConfig).
    """
    db_host: StrictStr
    db_port: StrictInt
    db_version: StrictStr
    db_type: StrictStr


class PartialOnPremConfigFromCloudwatchMetrics(
    BaseModel
):  # pyre-ignore[13]: pydantic uninitialized variables
    """Driver options fetched from RDS for agent deployment.

    Such options are part of the complete driver options (defined in OnPremDriverConfig).
    """
    metrics_to_retrieve_from_source: Dict[StrictStr, List[StrictStr]]


class OnPremDriverConfig(NamedTuple):  # pylint: disable=too-many-instance-attributes
    """Driver Config for on-prem deployment."""
    server_url: str  # OtterTune server url, required

    db_identifier: str  # AWS RDS Database identifier, required
    aws_region: str # AWS Region of Database and cloudwatch logs, required

    db_type: str  # Database type (mysql or postgres), required
    db_host: str  # Database host address, required
    db_port: int  # Database port, required
    db_version: str  # Database version number, key for what metrics to fetch, required
    db_user: str  # Database username, required
    db_password: str  # Database password, required

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



class OnPremDriverConfigBuilder(DriverConfigBuilder):
    """Defines the builder to build driver config for on-prem deployment."""

    def __init__(self, aws_region) -> None:
        self.config = {}
        self.rds_client = AwsWrapper.rds_client(aws_region)

    def from_file(self, config_path: str) -> DriverConfigBuilder:
        """build config options from config file"""
        with open(config_path, "r") as config_file:
            data = yaml.safe_load(config_file)
            if not isinstance(data, dict):
                raise ValueError("Invalid data in the driver configuration YAML file")

            try:
                partial_config_from_file: PartialOnPremConfigFromFile = (
                    PartialOnPremConfigFromFile(**data)
                )
            except ValidationError as ex:
                msg = (
                    "Invalid driver configuration for On-Prem deployment: "
                    "the driver option from file is missing or invalid"
                )
                raise DriverConfigException(msg, ex) from ex
            self.config.update(partial_config_from_file)
        return self

    def from_command_line(self, args) -> DriverConfigBuilder:
        """build config options from command line arguments that aren't overriding other builders"""
        try:
            from_cli = PartialOnPremConfigFromCommandline(aws_region=args.aws_region,
                                                          db_identifier=args.db_identifier,
                                                          db_user=args.db_username,
                                                          db_password=args.db_password,
                                                          api_key=args.api_key,
                                                          db_key=args.db_key,
                                                          organization_id=args.organization_id)
        except ValidationError as ex:
            msg = (
                "Invalid driver configuration for On-Prem deployment: "
                "the driver option from commandline is missing or invalid"
            )
            raise DriverConfigException(msg, ex) from ex
        self.config.update(from_cli)
        return self

    def from_rds(self, db_instance_identifier) -> DriverConfigBuilder:
        """build config options from rds description of database"""
        config_from_rds = {
            "db_host": get_db_hostname(db_instance_identifier, self.rds_client),
            "db_port": get_db_port(db_instance_identifier, self.rds_client),
            "db_version": get_db_version(db_instance_identifier, self.rds_client),
            "db_type": get_db_type(db_instance_identifier, self.rds_client)
        }

        try:
            partial_config_from_rds: PartialOnPremConfigFromRDS = (
                PartialOnPremConfigFromRDS(**config_from_rds)
            )
        except ValidationError as ex:
            msg = (
                "Invalid driver configuration for On-Prem deployment: "
                "the driver option from rds is missing or invalid"
            )
            raise DriverConfigException(msg, ex) from ex

        self.config.update(partial_config_from_rds)
        return self

    def _get_cloudwatch_metrics_file(self, db_instance_identifier):
        db_version = get_db_version(db_instance_identifier, self.rds_client)
        db_version_formatted = db_version.replace(".", "_").replace("-", "_")
        db_type = get_db_type(db_instance_identifier, self.rds_client)
        db_type_formatted = db_type.replace(".", "_").replace("-", "_")

        if db_type_formatted == "aurora_mysql":
            db_version_formatted, _ = db_version_formatted.split("_mysql")
        if db_type_formatted == "aurora_postgres":
            db_version_formatted, _ = db_version_formatted.split("_postgres")

        if "postgres" in db_type_formatted:
            # drop minor version except for 9_6
            if db_version_formatted != "9_6":
                db_version_formatted, _ = db_version_formatted.split("_")
        if "mysql" in db_type_formatted:
            # drop minor version
            release, major, _ = db_version_formatted.split("_")
            db_version_formatted = f"{release}_{major}"

        folder_path = "./driver/config/cloudwatch_metrics"
        return f"{folder_path}/rds_{db_type_formatted}-{db_version_formatted}.json"

    def from_cloudwatch_metrics(self, db_instance_identifier) -> DriverConfigBuilder:
        """Build config options from cloudwatch metrics configurations"""
        metric_names = []
        with open(
            self._get_cloudwatch_metrics_file(db_instance_identifier)
        ) as metrics_file:
            metrics = json.load(metrics_file)
            for metric in metrics:
                metric_names.append(metric["name"])

        self.config.update(
            {"metrics_to_retrieve_from_source": {"cloudwatch": metric_names}}
        )
        return self

    def from_overrides(self, overrides) -> DriverConfigBuilder:
        """Override config options supplied from other builder steps"""
        supplied_overrides = {
            k: v for k, v in overrides._asdict().items() if v is not None
        }
        self.config.update(supplied_overrides)
        return self

    def get_config(self) -> OnPremDriverConfig:
        """Get driver configuration for on-prem deployment.

        Returns:
            driver configuration for on-prem deployment.
        Raises:
            DriverConfigException: Invalid driver configuration for on-prem deployment.
        """
        driver_config: OnPremDriverConfig = OnPremDriverConfig(**self.config)
        return driver_config
