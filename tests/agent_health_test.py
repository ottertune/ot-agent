"""
Tests for the agent health heartbeat calls
"""
import datetime
import json
import traceback
import responses

from driver.agent_health_heartbeat import add_error_to_global, construct_error_list_and_clear, send_heartbeat
from driver.driver_config import DriverConfigFactory


def test_add_error():
    """
    Test adding an error to the global error queue.
    """
    try:
        raise Exception("Test error")
    except Exception as e:
        stacktrace = traceback.format_exc()
        add_error_to_global(e, stacktrace)

    error_list = construct_error_list_and_clear()
    assert len(error_list) == 1
    assert "Traceback (most recent call last):" in error_list[0]["data"]["stacktrace"]
    assert type(error_list[0]["timestamp"]) == datetime.datetime
    assert error_list == [
        {
            "data": {
                "name": "Exception",
                "message": "Test error",
                "stacktrace": error_list[0]["data"]["stacktrace"],
            },
            "timestamp": error_list[0]["timestamp"],
        }
    ]


@responses.activate
def test_heartbeat_without_errors():
    """
    Test heartbeat when no errors are present.
    """
    config = DriverConfigFactory()
    agent_version = "1.0.0"
    url = f"{config.server_url}/agent_health/"

    responses.add(
        responses.POST,
        url,
        status=200,
    )
    send_heartbeat(config, datetime.datetime.now(), agent_version)
    assert len(responses.calls) == 1
    assert responses.calls[0].request.url == url
    body = json.loads(responses.calls[0].request.body)
    assert body == {
        "organization_id": config.organization_id,
        "db_key": config.db_key,
        "agent_status": "Error",
        "agent_starttime": body["agent_starttime"],
        "heartbeat_time": body["heartbeat_time"],
        "agent_version": agent_version, "errors": []}


@responses.activate
def test_heartbeat_with_errors():
    """
    Test heartbeat when errors are present.
    """
    config = DriverConfigFactory()
    agent_version = "1.0.0"
    try:
        raise Exception("Test error")
    except Exception as e:
        stacktrace = traceback.format_exc()
        add_error_to_global(e, stacktrace)

    url = f"{config.server_url}/agent_health/"
    responses.add(
        responses.POST,
        url,
        status=200,
    )
    send_heartbeat(config, datetime.datetime.now(), agent_version)
    assert len(responses.calls) == 1
    assert responses.calls[0].request.url == url
    body = json.loads(responses.calls[0].request.body)
    assert body == {
        "organization_id": config.organization_id,
        "db_key": config.db_key,
        "agent_status": "Error",
        "agent_starttime": body["agent_starttime"],
        "heartbeat_time": body["heartbeat_time"],
        "agent_version": agent_version,
        "errors": [
            {
                "data": {
                    "name": "Exception",
                    "message": "Test error",
                    "stacktrace": body["errors"][0]["data"]["stacktrace"],
                },
                "timestamp": body["errors"][0]["timestamp"],
            }
        ],
    }
