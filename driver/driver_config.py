"""
DriverConfig definition and test helpers
"""
from typing import NamedTuple, List, Dict

import factory


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
    db_password: str  # Database password, required if not using IAM auth
    enable_aws_iam_auth: bool  # Flag for using IAM auth instead of password auth

    db_name: str  # Database name in DBMS to focus on, optional

    api_key: str  # API key handed to agent proxy to identify user
    db_key: str  # DB key handed to agent proxy to identify database
    organization_id: str  # Org id handed to agent proxy to identify database

    # how frequently to query database for events and activities
    long_running_query_monitor_interval: int
    lr_query_latency_threshold_min: int  # latency threshold for long running queries in minutes
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
    disable_index_stats: bool
    num_index_to_collect_stats: int
    disable_long_running_query_monitoring: bool
    disable_query_monitoring: bool
    query_monitor_interval: int
    num_query_to_collect: int
    disable_schema_monitoring: bool
    schema_monitor_interval: int
    db_non_default_parameters: List[str]
    enable_s3: bool


class DriverConfigFactory(factory.Factory):
    """
    Factory for DriverConfig
    """
    class Meta:
        model = DriverConfig

    server_url = factory.Faker('url')
    db_identifier = factory.Faker('pystr')
    aws_region = factory.Faker('pystr')
    db_type = factory.Faker('pystr')
    db_host = factory.Faker('pystr')
    db_port = factory.Faker('pyint')
    db_version = factory.Faker('pystr')
    db_user = factory.Faker('pystr')
    db_password = factory.Faker('pystr')
    enable_aws_iam_auth = factory.Faker('pybool')
    db_name = factory.Faker('pystr')
    api_key = factory.Faker('pystr')
    db_key = factory.Faker('pystr')
    organization_id = factory.Faker('pystr')
    long_running_query_monitor_interval = factory.Faker('pyint')
    lr_query_latency_threshold_min = factory.Faker('pyint')
    monitor_interval = factory.Faker('pyint')
    metric_source = factory.Faker('pylist', nb_elements=5, variable_nb_elements=True)
    metrics_to_retrieve_from_source = factory.Faker(
        'pydict', nb_elements=5, variable_nb_elements=True)
    disable_table_level_stats = factory.Faker('pybool')
    num_table_to_collect_stats = factory.Faker('pyint')
    table_level_monitor_interval = factory.Faker('pyint')
    disable_index_stats = factory.Faker('pybool')
    num_index_to_collect_stats = factory.Faker('pyint')
    disable_long_running_query_monitoring = factory.Faker('pybool')
    disable_query_monitoring = factory.Faker('pybool')
    query_monitor_interval = factory.Faker('pyint')
    num_query_to_collect = factory.Faker('pyint')
    disable_schema_monitoring = factory.Faker('pybool')
    schema_monitor_interval = factory.Faker('pyint')
    db_non_default_parameters = factory.Faker('pylist', nb_elements=5, variable_nb_elements=True)
    enable_s3 = factory.Faker('pybool')
