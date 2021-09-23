"""Library for aws related exceptions"""
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
from driver_lib import exception
from driver_lib.secret_redaction import (
    ErrorMessage,
    ErrorCode,
    remove_sensitive_info_in_error_msg,
)


class DriverAwsLibException(exception.DriverLibException):
    """
    Custom exception for driver library. It is up to callers to appropriately
    format errors (e.g. HTTP response for driver API)
    """

    def __init__(
        self, message: str, error_code: Optional[str] = None
    ) -> None:  # pylint: disable=super-init-not-called
        super().__init__(remove_sensitive_info_in_error_msg(message), error_code)

    def __str__(self) -> str:
        return self.message


class InvalidCustomerSettingsError(DriverAwsLibException):
    pass


class InvalidDBInstanceClass(DriverAwsLibException):
    pass


class DBInstanceTypeNotFound(DriverAwsLibException):
    pass


class UnexpectedAwsResponseError(DriverAwsLibException):
    pass


class InvalidPermissionError(DriverAwsLibException):
    """Invalid permission error"""


class DBInstanceNotFound(DriverAwsLibException):
    """Database instance not found"""
