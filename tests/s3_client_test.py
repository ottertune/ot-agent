"""
Tests for the compute server client
"""
from typing import Dict, Any
import pytest
import responses
import requests

from driver.exceptions import ComputeServerClientException
from unittest.mock import Mock, patch

# Code under test
from driver.s3_client import S3Client, ObservationType
import zlib
import json

# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring


def test_get_s3_bucket_object_key() -> None:
    client = S3Client(
        enable_s3=True,
        organization_id="81072a87-ef79-4a24-86be-84b2efd84688",
    )
    object_key = client._get_s3_bucket_object_key(ObservationType.DB)
    assert "81072a87-ef79-4a24-86be-84b2efd84688/DB/" in object_key
    assert "data" in object_key


def test_process_observation_data() -> None:
    client = S3Client(
        enable_s3=True,
        organization_id="81072a87-ef79-4a24-86be-84b2efd84688",
    )
    observation_data = {
        "key1": "value1",
    }
    processed_observation_data = client._process_observation_data(
        observation_data, ObservationType.QUERY
    )
    decompressed_data = zlib.decompress(processed_observation_data).decode("utf-8")
    assert isinstance(processed_observation_data, bytes), True
    assert decompressed_data, json.dumps(observation_data, default=str)

    processed_observation_data = client._process_observation_data(
        observation_data, ObservationType.SCHEMA
    )
    decompressed_data = zlib.decompress(processed_observation_data).decode("utf-8")
    assert isinstance(processed_observation_data, bytes), True
    assert decompressed_data, json.dumps(observation_data, default=str)

    processed_observation_data = client._process_observation_data(
        observation_data, ObservationType.LONG_RUNNING_QUERY
    )
    decompressed_data = zlib.decompress(processed_observation_data).decode("utf-8")
    assert isinstance(processed_observation_data, bytes), True
    assert decompressed_data, json.dumps(observation_data, default=str)

    processed_observation_data = client._process_observation_data(
        observation_data, ObservationType.DB
    )
    assert isinstance(processed_observation_data, bytes), True
    assert processed_observation_data, json.dumps(observation_data)

    processed_observation_data = client._process_observation_data(
        observation_data, ObservationType.TABLE
    )
    assert isinstance(processed_observation_data, bytes), True
    assert processed_observation_data, json.dumps(observation_data)
