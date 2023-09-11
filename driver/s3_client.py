"""Defines the S3 client that interacts with the OtterTune S3 bucket."""

import json
from enum import Enum
import datetime
import zlib
import boto3

# pylint: disable=attribute-defined-outside-init

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

    @staticmethod
    def get_s3_session():
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

    @staticmethod
    def process_observation_data(data, obs_type: ObservationType):
        """Process the observation data before posting to S3."""

        if obs_type.value in (
            ObservationType.QUERY.value,
            ObservationType.LONG_RUNNING_QUERY.value,
            ObservationType.SCHEMA.value,
        ):
            compressed_data = zlib.compress(
                json.dumps(data, default=str).encode("utf-8")
            )
            return compressed_data

        serialized_data = json.dumps(data).encode("utf-8")
        return serialized_data

    def get_s3_bucket_object_key(self, obs_type: ObservationType):
        """Get the S3 bucket object key for the observation data."""

        current_time = datetime.datetime.now(datetime.timezone.utc)
        timestamp_folder = current_time.strftime("%Y%m%d/%H")
        object_key = timestamp_folder + "/" + "data"
        if obs_type.value == ObservationType.DB.value:
            object_key = "DB/" + object_key
        elif obs_type.value == ObservationType.TABLE.value:
            object_key = "TABLE/" + object_key
        elif obs_type.value == ObservationType.SCHEMA.value:
            object_key = "SCHEMA/" + object_key
        elif obs_type.value == ObservationType.LONG_RUNNING_QUERY.value:
            object_key = "LONG_RUNNING_QUERY/" + object_key
        elif obs_type.value == ObservationType.QUERY.value:
            object_key = "QUERY/" + object_key
        else:
            object_key = "UNKNOWN/" + object_key

        return self._organization_id + "/" + object_key


    def post_observation(self, data, obs_type: ObservationType) -> None:
        """Post the observation to S3."""

        if not self._enable_s3:
            return

        object_key = self.get_s3_bucket_object_key(obs_type)
        s3_session = self.get_s3_session()
        s3_client = s3_session.client("s3")
        processed_data = self.process_observation_data(data, obs_type)
        s3_client.put_object(
            Bucket=BUCKET_NAME, Key=object_key, Body=processed_data
        )
