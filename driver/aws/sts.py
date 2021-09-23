"""Library for aws Security Token Service (sts) methods"""
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


def get_sts_credentials(
    role_arn: str, external_id: str, client: STSClient
) -> CredentialsTypeDef:
    """
    Assumes the target role and fetches necessarily credentials from STS
    Returns:
        STS credentials
    Raises:
        botocore.exceptions.ClientError: If we receive an error from AWS
        InvalidCustomerSettingsError: If customer did not setup role appropriately
        InvalidPermissionError: If it's denied to assume the role
    """
    resp = None
    try:
        resp = client.assume_role(
            RoleArn=role_arn,
            RoleSessionName="Check_Connection_No_ExternalId",
            DurationSeconds=900,
        )
    except botocore.exceptions.ClientError:
        pass
    # We should always ensure client has role setup appropriately.
    # If we received a response, we assumed the role when we shouldn't have.
    if resp:
        raise InvalidCustomerSettingsError(
            "Assuming a role without an external ID was allowed. "
            "Role must enforce usage of an external ID",
            ErrorCode.ASSUME_ROLE_REQUIRE_EXTERNAL_ID.value,
        )
    try:
        resp = client.assume_role(
            RoleArn=role_arn,
            ExternalId=external_id,
            RoleSessionName="Check_Connection",
            DurationSeconds=900,
        )
    except botocore.exceptions.ClientError as ex:
        msg = str(ex)
        if ErrorMessage.ASSUME_ROLE_NOT_ALLOWED.value in msg:
            raise InvalidPermissionError(
                msg, ErrorCode.ASSUME_ROLE_NOT_ALLOWED.value
            ) from None
        raise ex
    return resp["Credentials"]
