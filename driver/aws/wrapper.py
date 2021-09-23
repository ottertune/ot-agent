"""Library for performing aws methods"""
from typing import List, Optional
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

PARAMETER_GROUP_ACTIONS = [
    "rds:ModifyDBParameterGroup",
]
CLUSTER_PARAMETER_GROUP_ACTIONS = [
    "rds:ModifyDBClusterParameterGroup",
]
REBOOT_ACTION = [
    "rds:RebootDBInstance",
]


class AwsWrapper:
    """
    Wrapper class that returns boto client libraries.
    """

    @staticmethod
    def ec2_client(region_name: str) -> EC2Client:
        """
        Wrapper for EC2 client
        """
        return boto3.client("ec2")

    @staticmethod
    def sts_client(region_name: str) -> STSClient:
        """
        Wrapper for STS client
        """
        return boto3.client("sts")

    @staticmethod
    def rds_client(region_name: str) -> RDSClient:
        """
        Wrapper for RDS client
        """
        return boto3.client("rds", region_name=region_name)

    @staticmethod
    def iam_client(region_name: str) -> IAMClient:
        """
        Wrapper for IAM client
        """
        return boto3.client("iam", region_name=region_name)

    @staticmethod
    def cloudwatch_client(region_name: str) -> CloudWatchClient:
        """Return a cloudwatch client"""

        return boto3.client("cloudwatch", region_name=region_name)
