"""
Defines the OnPremDriverConfigBuilder to build the driver configuraton for on-prem deployment.
It fetches the necessary information to run the driver pipeline from the local file and server.
"""

from typing import Dict, Any, Optional, NamedTuple, List
import json

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
    server_http_proxy: StrictStr
    server_https_proxy: StrictStr
    database_id: StrictInt

    db_identifier: StrictStr

    db_type: StrictStr
    db_user: StrictStr
    db_password: StrictStr

    db_ssl_ca: StrictStr
    db_ssl_cert: StrictStr
    db_ssl_key: StrictStr
    db_enable_ssl: StrictBool

    db_conn_extend: Optional[Dict[str, Any]]

    api_key: StrictStr
    db_key: StrictStr
    organization_id: StrictStr

    monitor_interval: StrictInt

    metric_source: List[str]
    aws_region: StrictStr

    @validator("db_enable_ssl")
    def check_db_ssl(  # pylint: disable=no-self-argument, no-self-use
        cls, val: bool, values: Dict[str, Any]
    ) -> bool:
        """Validate that database ssl.

        When database ssl is enabled, db_ssl_ca, db_ssl_cert and db_ssl_key should not be empty
        strings at the same time. We should specify at least one of these options. Such options
        will be further validated when connecting to the database.
        """

        if (
            val is True
            and values["db_ssl_ca"] == ""
            and values["db_ssl_cert"] == ""
            and values["db_ssl_key"] == ""
        ):
            raise ValueError(
                "Invalid driver options for database ssl. When database ssl is "
                "enabled, options 'db_ssl_ca', 'db_ssl_cert' or 'db_ssl_key' must be "
                "specified. But all of these options are empty strings"
            )
        return val

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

    db_user: str
    db_password: str
    api_key: str
    db_key: str
    organization_id: str
    db_type: str
    monitor_interval: int
    db_identifier: str


class PartialOnPremConfigFromRDS(
    BaseModel
):  # pyre-ignore[13]: pydantic uninitialized variables
    """Driver options fetched from RDS for agent deployment.

    Such options are part of the complete driver options (defined in OnPremDriverConfig).
    """

    db_host: StrictStr
    db_port: StrictInt
    db_version: StrictStr


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
    server_http_proxy: str  # HTTP proxy to connect to the server
    server_https_proxy: str  # HTTPS proxy to connect to the server
    database_id: int  # Database primary key in the server model, required

    db_identifier: str  # AWS RDS Database identifier

    db_type: str  # Database type (mysql or postgres), required
    db_host: str  # Database host address, required
    db_port: int  # Database port, required
    db_version: str  # Database version number, key for what metrics to fetch
    db_user: str  # Database username, required
    db_password: str  # Database password, required

    db_enable_ssl: bool  # Enable SSL connection to the database
    db_ssl_ca: str  # File containing the SSL certificate authority
    db_ssl_cert: str  # File containing the SSL certificate file
    db_ssl_key: str  # File containing the SSL key

    # If you need more parameters to connect to the database besides to the above ones, you can
    # add options in db_conn_extend, e.g., db_conn_extend = {"pool_size":10, "autocommit":False}
    # For MySQL, we use mysql-connector-python, supported connection parameters can be found in:
    # https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html
    # For Postgres, we use psycopg2, supported parameters can be found here:
    # https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
    db_conn_extend: Optional[Dict[str, Any]]

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

    aws_region: str


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

    def from_rds(self, db_instance_identifier) -> DriverConfigBuilder:
        """build config options from rds description of database"""
        config_from_rds = {
            "db_host": get_db_hostname(db_instance_identifier, self.rds_client),
            "db_port": get_db_port(db_instance_identifier, self.rds_client),
            "db_version": get_db_version(db_instance_identifier, self.rds_client),
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

    def _get_cloudwatch_metrics_file(self):
        # TODO - actually select metrics file based on db info
        print(self)
        return "./driver/config/cloudwatch_metrics/rds_postgres-13.json"

    def from_cloudwatch_metrics(self, db_instance_identifier) -> DriverConfigBuilder:
        """Build config options from cloudwatch metrics configurations"""
        db_version = get_db_version(db_instance_identifier, self.rds_client)
        db_type = get_db_type(db_instance_identifier, self.rds_client)
        print(db_version)
        print(db_type)

        metric_names = []
        with open(
            self._get_cloudwatch_metrics_file()
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
