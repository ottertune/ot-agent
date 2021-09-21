"""
Defines the OnPremDriverConfigBuilder to build the driver configuraton for on-prem deployment.
It fetches the necessary information to run the driver pipeline from the local file and server.
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional
import yaml


from pydantic import (
    BaseModel,
    StrictBool,
    StrictInt,
    StrictStr,
    validator,
    ValidationError,
)

from driver.driver_config_builder import DriverConfigBuilder
from driver.exceptions import DriverConfigException


class PartialOnPremConfigFromFile(BaseModel): # pyre-ignore[13]: pydantic uninitialized variables
    """Driver options fetched from local file for on-prem deployment.

    Such options are part of the complete driver options (defined in OnPremDriverConfig).
    It validates that options do not have missing values, wrong types or invalid values.
    """

    server_url: StrictStr
    server_http_proxy: StrictStr
    server_https_proxy: StrictStr
    database_id: StrictInt

    db_type: StrictStr
    db_host: StrictStr
    db_port: StrictInt
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

    @validator("db_enable_ssl")
    def check_db_ssl( # pylint: disable=no-self-argument, no-self-use
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


class PartialOnPremConfigFromServer(BaseModel):  # pyre-ignore[13]: pydantic uninitialized variables
    """Driver options fetched from OtterTune server for on-prem deployment.

    Such options are part of the complete driver options (defined in OnPremDriverConfig).
    It validates that options do not have missing values, wrong types or invalid values.
    """
    db_provider: StrictStr
    enable_tuning: StrictBool
    enable_restart: StrictBool
    tune_interval: StrictInt

    @validator("db_provider")
    def check_db_provider(  # pylint: disable=no-self-argument, no-self-use
        cls, val: str
    ) -> str:
        """Validate that db_provider is on_premise."""
        if val != "on_premise":
            raise ValueError(
                "Invalid driver option db_provider, on_premise "
                f" is expected, but {val} is found"
            )
        return val

    @validator("tune_interval")
    def check_tune_interval(  # pylint: disable=no-self-argument, no-self-use
        cls, val: int, values: Dict[str, Any]
    ) -> int:
        """Validate that tune_interval is positive when tuning is enabled."""
        if values["enable_tuning"] and val <= 0:
            raise ValueError(
                "Invalid driver option tune_interval, positive value "
                f"is expected when tuning is enabled, but {val} is found"
            )
        return val


@dataclass
class OnPremDriverConfig: # pylint: disable=too-many-instance-attributes
    """Driver Config for on-prem deployment."""

    server_url: str  # OtterTune server url, required
    server_http_proxy: str  # HTTP proxy to connect to the server
    server_https_proxy: str  # HTTPS proxy to connect to the server
    database_id: int  # Database primary key in the server model, required

    db_type: str  # Database type (mysql or postgres), required
    db_host: str  # Database host address, required
    db_port: int  # Database port, required
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
    organization_id: str # Org id handed to agent proxy to identify database

    monitor_interval: int # how frequently to query database for metrics


class OnPremDriverConfigBuilder(DriverConfigBuilder):
    """Defines the builder to build driver config for on-prem deployment."""

    def __init__(
        self,
        partial_config_from_file: PartialOnPremConfigFromFile,
    ) -> None:
        """Initialze config class for on-prem deployment.

        Args:
            partial_config_from_file: part of driver options fetched from the local file.
            compute_server_client: client that communicates with the compute server.
        """

        self._partial_config_from_file = partial_config_from_file

    def get_config(self) -> OnPremDriverConfig:
        """Get driver configuration for on-prem deployment.

        Returns:
            driver configuration for on-prem deployment.
        Raises:
            DriverConfigException: Invalid driver configuration for on-prem deployment.
        """
        config = {}
        config.update(self._partial_config_from_file.dict())
        driver_config: OnPremDriverConfig = OnPremDriverConfig(**config)
        return driver_config


def _load_file_config(config_path: str) -> Dict[str, Any]:
    """Load the config from a local file for on-prem deployment."""

    with open(config_path, "r") as config_file:
        data = yaml.safe_load(config_file)
        if not isinstance(data, dict):
            raise ValueError("Invalid data in the driver configuration YAML file")
        return data


def create_onprem_driver_config_builder(config_path: str) -> OnPremDriverConfigBuilder:
    """Create driver configuration builder for on-prem deployment.

    Args:
        config_path: driver config file path for on-prem deployment.
    Returns:
        driver configuration builder for on-prem deployment.
    Raises:
        DriverConfigException: Invalid driver configuration for on-prem deployment.
        ComputeServerClientException: Failed to build the request session.
    """
    try:
        partial_config = _load_file_config(config_path)
    except Exception as ex:
        msg = (
            "Invalid driver configuration for On-Prem deployment: "
            "cannot load configuration from the local file"
        )
        raise DriverConfigException(msg, ex) from ex
    try:
        partial_config_from_file: PartialOnPremConfigFromFile = (
            PartialOnPremConfigFromFile(**partial_config)
        )
    except ValidationError as ex:
        msg = (
            "Invalid driver configuration for On-Prem deployment: "
            "the driver option from file is missing or invalid"
        )
        raise DriverConfigException(msg, ex) from ex

    driver_config_builder = OnPremDriverConfigBuilder(partial_config_from_file)
    return driver_config_builder
