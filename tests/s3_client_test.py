"""
Tests for the S3 client
"""
import zlib
import json

from agent_version import AGENT_VERSION
from driver.s3_client import S3Client, ObservationType

# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring


def test_get_s3_bucket_object_key() -> None:
    client = S3Client(
        enable_s3=True,
        organization_id="81072a87-ef79-4a24-86be-84b2efd84688",
        api_key="test_api_key",
    )
    object_key = client.get_s3_bucket_object_key(ObservationType.DB)
    assert "81072a87-ef79-4a24-86be-84b2efd84688/DB/" in object_key
    assert "data" in object_key


def test_process_observation_data() -> None:
    client = S3Client(
        enable_s3=True,
        organization_id="81072a87-ef79-4a24-86be-84b2efd84688",
        api_key="test_api_key",
    )
    observation_data = {
        "key1": "value1",
    }
    processed_observation_data = client.process_observation_data(
        observation_data, ObservationType.QUERY
    )
    decompressed_data = zlib.decompress(processed_observation_data).decode("utf-8")
    assert isinstance(processed_observation_data, bytes), True
    assert decompressed_data, json.dumps(observation_data, default=str)

    processed_observation_data = client.process_observation_data(
        observation_data, ObservationType.SCHEMA
    )
    decompressed_data = zlib.decompress(processed_observation_data).decode("utf-8")
    assert isinstance(processed_observation_data, bytes), True
    assert decompressed_data, json.dumps(observation_data, default=str)

    processed_observation_data = client.process_observation_data(
        observation_data, ObservationType.LONG_RUNNING_QUERY
    )
    decompressed_data = zlib.decompress(processed_observation_data).decode("utf-8")
    assert isinstance(processed_observation_data, bytes), True
    assert decompressed_data, json.dumps(observation_data, default=str)

    processed_observation_data = client.process_observation_data(
        observation_data, ObservationType.DB
    )
    assert isinstance(processed_observation_data, bytes), True
    assert processed_observation_data, json.dumps(observation_data)

    processed_observation_data = client.process_observation_data(
        observation_data, ObservationType.TABLE
    )
    assert isinstance(processed_observation_data, bytes), True
    assert processed_observation_data, json.dumps(observation_data)


def test_generate_headers() -> None:
    client = S3Client(
        enable_s3=True,
        organization_id="81072a87-ef79-4a24-86be-84b2efd84688",
        api_key="test_api_key",
    )
    observation_data = {"key1": "value1", "headers": client.generate_headers()}
    assert (
        observation_data["headers"]["organization_id"]
        == "81072a87-ef79-4a24-86be-84b2efd84688"
    )
    assert observation_data["headers"]["AgentVersion"] == AGENT_VERSION
    assert observation_data["headers"]["ApiKey"] == "test_api_key"
