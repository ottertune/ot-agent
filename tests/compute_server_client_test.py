"""
Tests for the compute server client
"""
from typing import Dict, Any
import pytest
import responses
import requests

from driver.exceptions import ComputeServerClientException

# Code under test
from driver.compute_server_client import ComputeServerClient

# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring


@pytest.fixture(name="test_data")
def _test_response_data() -> Dict[str, Any]:

    server_url = "https://server_url:8000"
    api_key = "test_api_key"

    observation_knobs_data: Dict[str, Any] = {
        "global": {
            "global": {
                "innodb_adaptive_hash_index": "ON",
                "innodb_buffer_pool_size": "134217728",
            }
        },
        "local": None,
    }
    observation_metrics_data: Dict[str, Any] = {
        "global": {
            "global": {"com_select": "475", "com_insert": "123"},
            "innodb_metrics": {
                "trx_rw_commits": 10,
                "trx_ro_commits": 20,
            },
            "performance_schema": {},
            "engine": {"replica_status": ""},
            "derived": {"buffer_miss_ratio": 2.77},
        },
        "local": None,
    }
    observation_summary: Dict[str, Any] = {"observation_time": "2021-01-14 18:20:33"}
    observation = dict(
        knobs_data=observation_knobs_data,
        metrics_data=observation_metrics_data,
        summary=observation_summary,
        organization_id="test_org"
    )
    schema: Dict[str,Any] = dict(
        summary=observation_summary,
        organization_id="test_org"
    )
    return dict(
        server_url=server_url,
        api_key=api_key,
        observation=observation,
        schema=schema
    )


@responses.activate
def test_post_db_level_observation_success(test_data: Dict[str, Any]) -> None:
    responses.add(
        responses.POST,
        f"{test_data['server_url']}/observation/",
        status=200,
    )
    session = requests.Session()
    client = ComputeServerClient(
        server_url=test_data["server_url"],
        req_session=session,
        api_key=test_data["api_key"],
    )
    client.post_db_level_observation(test_data["observation"])


@responses.activate
def test_post_db_level_observation_session_not_found(test_data: Dict[str, Any]) -> None:
    responses.add(
        responses.POST,
        f"{test_data['server_url']}/observation/",
        status=404,
    )
    session = requests.Session()
    client = ComputeServerClient(
        server_url=test_data["server_url"],
        req_session=session,
        api_key=test_data["api_key"],
    )
    with pytest.raises(ComputeServerClientException) as ex:
        client.post_db_level_observation(test_data["observation"])
    assert "404" in str(ex.value)


@responses.activate
def test_post_db_level_observation_connection_error(test_data: Dict[str, Any]) -> None:
    responses.add(
        responses.POST,
        f"{test_data['server_url']}/observation/",
        body=ConnectionError("Connection Error"),
    )
    session = requests.Session()
    client = ComputeServerClient(
        server_url=test_data["server_url"],
        req_session=session,
        api_key=test_data["api_key"],
    )
    with pytest.raises(ComputeServerClientException) as ex:
        client.post_db_level_observation(test_data["observation"])
    assert "Connection Error" in str(ex.value)


@responses.activate
def test_post_schema_observation_success(test_data: Dict[str, Any]) -> None:
    responses.add(
        responses.POST,
        f"{test_data['server_url']}/schema_observation/",
        status=200,
    )
    session = requests.Session()
    client = ComputeServerClient(
        server_url=test_data["server_url"],
        req_session=session,
        api_key=test_data["api_key"],
    )
    client.post_schema_observation(test_data["schema"])



@responses.activate
def test_post_schema_observation_session_not_found(test_data: Dict[str, Any]) -> None:
    responses.add(
        responses.POST,
        f"{test_data['server_url']}/schema_observation/",
        status=404,
    )
    session = requests.Session()
    client = ComputeServerClient(
        server_url=test_data["server_url"],
        req_session=session,
        api_key=test_data["api_key"],
    )
    with pytest.raises(ComputeServerClientException) as ex:
        client.post_schema_observation(test_data["schema"])
    assert "404" in str(ex.value)


@responses.activate
def test_post_schema_observation_connection_error(test_data: Dict[str, Any]) -> None:
    responses.add(
        responses.POST,
        f"{test_data['server_url']}/schema_observation/",
        body=ConnectionError("Connection Error"),
    )
    session = requests.Session()
    client = ComputeServerClient(
        server_url=test_data["server_url"],
        req_session=session,
        api_key=test_data["api_key"],
    )
    with pytest.raises(ComputeServerClientException) as ex:
        client.post_schema_observation(test_data["schema"])
    assert "Connection Error" in str(ex.value)
