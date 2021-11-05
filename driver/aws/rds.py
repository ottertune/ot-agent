"""Library for aws Relational Database Service (sts) methods"""

from typing import List, Optional
from functools import lru_cache

import boto3
import botocore
from mypy_boto3_ec2.client import EC2Client
from mypy_boto3_ec2.type_defs import InstanceTypeInfoTypeDef
from mypy_boto3_iam.client import IAMClient
from mypy_boto3_iam.type_defs import (
    EvaluationResultTypeDef,
    SimulatePolicyResponseTypeDef,
)
from mypy_boto3_rds.client import RDSClient
from mypy_boto3_rds.type_defs import DBInstanceTypeDef
from mypy_boto3_sts.client import STSClient
from mypy_boto3_sts.type_defs import CredentialsTypeDef
from mypy_boto3_cloudwatch.client import CloudWatchClient

from driver.aws.exceptions import InvalidCustomerSettingsError


@lru_cache
def get_db_instance_info(
    db_instance_identifier: str, client: RDSClient
) -> DBInstanceTypeDef:
    """
    Ensures that we can connect to the target AWS RDS instance by calling describe_db_instances.
    Returns:
        The DBInstance object returned by AWS
    Raises:
        botocore.exceptions.ClientError: If we receive an error from AWS
        InvalidCustomerSettingsError: If customer supplied an invalid db_instance_identifier
        InvalidPermissionError: If it's not allowed to describe db instances
        DBInstanceNotFound: If the database instance is not found
    """
    resp = client.describe_db_instances(DBInstanceIdentifier=db_instance_identifier)

    if len(resp["DBInstances"]) == 0:
        raise InvalidCustomerSettingsError(
            "No instance was found for provided db identifier"
        )
    if len(resp["DBInstances"]) > 1:
        raise InvalidCustomerSettingsError(
            "Multiple instances found for " "provided db identifier. Expected only one"
        )
    return resp["DBInstances"][0]


def get_db_hostname(db_instance_identifier: str, client: RDSClient) -> str:
    """
    Ensures that we can connect to the target AWS RDS instance by calling describe_db_instances.
    Returns:
        The DBInstance object returned by AWS
    Raises:
        botocore.exceptions.ClientError: If we receive an error from AWS
        InvalidCustomerSettingsError: If customer supplied an invalid db_instance_identifier
        InvalidPermissionError: If it's not allowed to describe db instances
        DBInstanceNotFound: If the database instance is not found
    """
    instance_info = get_db_instance_info(db_instance_identifier, client)
    return instance_info["Endpoint"]["Address"]


def get_db_port(db_instance_identifier: str, client: RDSClient) -> str:
    """
    Ensures that we can connect to the target AWS RDS instance by calling describe_db_instances.
    Returns:
        The DBInstance object returned by AWS
    Raises:
        botocore.exceptions.ClientError: If we receive an error from AWS
        InvalidCustomerSettingsError: If customer supplied an invalid db_instance_identifier
        InvalidPermissionError: If it's not allowed to describe db instances
        DBInstanceNotFound: If the database instance is not found
    """
    instance_info = get_db_instance_info(db_instance_identifier, client)
    return instance_info["Endpoint"]["Port"]


def get_db_version(db_instance_identifier: str, client: RDSClient) -> str:
    """
    Get's database version information
    """
    instance_info = get_db_instance_info(db_instance_identifier, client)
    return instance_info["EngineVersion"].replace(".", "_").replace("-", "_")


def get_db_type(db_instance_identifier: str, client: RDSClient) -> str:
    """
    Get's database type information
    """
    instance_info = get_db_instance_info(db_instance_identifier, client)
    db_type = instance_info["Engine"].replace(".", "_").replace("-", "_")
    if db_type == "aurora":  # for aurora mysql 5.6
        db_type = "aurora_mysql"
    return db_type
