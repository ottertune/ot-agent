"""Defines the S3 client that interacts with the OtterTune S3 bucket."""

import json
from enum import Enum
import boto3
import datetime
import zlib


class ObservationType(Enum):
    """Possible observation types."""

    DB = "db"
    TABLE = "table"
    SCHEMA = "schema"
    LONG_RUNNING_QUERY = "long_running_query"
    QUERY = "query"


S3_BUCKET_SHARING_ROLE = (
    "arn:aws:iam::691523222388:role/CrossAccountS3BucketSharingRole"
)
BUCKET_NAME = "customer-database-observations"


class S3Client:
    """S3 client that interacts with the OtterTune S3 bucket."""

    def __init__(self, enable_s3, organization_id) -> None:
        self._enable_s3 = enable_s3
        self._organization_id = organization_id

    def _get_s3_session(self):
        """Get the S3 session."""

        session = boto3.Session()
        client = session.client("sts")
        resp = client.assume_role(
            RoleArn=S3_BUCKET_SHARING_ROLE, RoleSessionName="s3", DurationSeconds=900
        )
        creds = resp["Credentials"]
        session = boto3.Session(
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
        )

        return session

    def _get_s3_bucket_object_key(self, type: ObservationType):
        """Get the S3 bucket object key for the observation data."""

        current_time = datetime.datetime.now(datetime.timezone.utc)
        timestamp_folder = current_time.strftime("%Y%m%d/%H")
        object_key = timestamp_folder + "/" + "data"
        if type.value == ObservationType.DB.value:
            object_key = "DB/" + object_key
        elif type.value == ObservationType.TABLE.value:
            object_key = "TABLE/" + object_key
        elif type.value == ObservationType.SCHEMA.value:
            object_key = "SCHEMA/" + object_key
        elif type.value == ObservationType.LONG_RUNNING_QUERY.value:
            object_key = "LONG_RUNNING_QUERY/" + object_key
        elif type.value == ObservationType.QUERY.value:
            object_key = "QUERY/" + object_key
        else:
            object_key = "UNKNOWN/" + object_key

        return self._organization_id + "/" + object_key

    def _process_observation_data(self, data, type: ObservationType):
        """Process the observation data before posting to S3."""

        if (
            type.value == ObservationType.QUERY.value
            or type.value == ObservationType.LONG_RUNNING_QUERY.value
            or type.value == ObservationType.SCHEMA.value
        ):
            compressed_data = zlib.compress(
                json.dumps(data, default=str).encode("utf-8")
            )
            return compressed_data
        else:
            serialized_data = json.dumps(data).encode("utf-8")
            return serialized_data

    def post_observation(self, data, type: ObservationType) -> None:
        """Post the observation to S3."""

        if not self._enable_s3:
            return

        object_key = self._get_s3_bucket_object_key(type)
        self._s3_session = self._get_s3_session()
        self._s3_client = self._s3_session.client("s3")
        processed_data = self._process_observation_data(data, type)
        self._s3_client.put_object(
            Bucket=BUCKET_NAME, Key=object_key, Body=processed_data
        )
