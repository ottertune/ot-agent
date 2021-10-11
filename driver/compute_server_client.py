"""Defines the compute server client that interacts with the server with http requests"""

from typing import Dict, Any, TypedDict, Set
from http import HTTPStatus
from requests import Session

from driver.exceptions import ComputeServerClientException

SECONDS_TO_MS = 1000
RETRYABLE_HTTP_STATUS: Set[int] = {
    HTTPStatus.REQUEST_TIMEOUT,
    HTTPStatus.INTERNAL_SERVER_ERROR,
    HTTPStatus.BAD_GATEWAY,
    HTTPStatus.SERVICE_UNAVAILABLE,
    HTTPStatus.GATEWAY_TIMEOUT,
}

# TODO: move this elsewhere and have it pull from git tags as source of truth
AGENT_VERSION = "0.1.4"


class Observation(TypedDict):
    """Observation data collected from the target database."""

    knobs_data: Dict[str, Any]  # collected knobs data
    metrics_data: Dict[str, Any]  # collected metrics data
    summary: Dict[
        str, Any
    ]  # summary information like observation time, database version, etc
    db_key: str
    organization_id: str


class DriverStatus(TypedDict):
    """Driver status information."""

    status: str  # error or success
    message: str  # error message or success message


class ComputeServerClient:
    """Defines the compute server client which communicates with the server."""

    def __init__(self, server_url: str, req_session: Session, api_key: str) -> None:
        """Initialze the compute server client.

        Args:
            server_url: url of OtterTune server that the driver can connect to.
            req_session: Request session used to get/post data to the server.
        """
        self._server_url: str = server_url
        self._req_session: Session = req_session
        self._api_key = api_key

    def post_observation(self, data: Observation) -> None:
        """Post the observation to the server.

        Args:
            data: Collected data from the target database.
        Raises:
            ComputeServerClientException: Failed to post the observation.
        """
        headers = {}
        headers["ApiKey"] = self._api_key
        headers["organization_id"] = data["organization_id"]
        headers["agent_version"] = AGENT_VERSION
        url = f"{self._server_url}/observation/"
        print(url)
        print(headers)
        print(data)
        try:
            response = self._req_session.post(
                url, json=data, headers=headers, timeout=10
            )
            response.raise_for_status()
        except Exception as ex:
            msg = "Failed to post the observation to the server"
            raise ComputeServerClientException(msg, ex) from ex
