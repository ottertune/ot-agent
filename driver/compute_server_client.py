"""Defines the compute server client that interacts with the server with http requests"""

import json
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
AGENT_VERSION = "0.3.10"


class DBLevelObservation(TypedDict):
    """DB level observation data collected from the target database."""

    knobs_data: Dict[str, Any]  # collected knobs data
    metrics_data: Dict[str, Any]  # collected metrics data
    row_num_stats: Dict[str, Any]
    summary: Dict[
        str, Any
    ]  # summary information like observation time, database version, etc
    db_key: str
    organization_id: str


class TableLevelObservation(TypedDict):
    """Table level observation data collected from the target database."""

    data: Dict[str, Any]
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

    def _generate_headers(self, org_id) -> Dict[str, Any]:
        headers = {}
        headers["ApiKey"] = self._api_key
        headers["organization_id"] = org_id
        headers["AgentVersion"] = AGENT_VERSION
        return headers

    def post_db_level_observation(self, data: DBLevelObservation) -> None:
        """Post the observation to the server.

        Args:
            data: Collected data from the target database.
        Raises:
            ComputeServerClientException: Failed to post the observation.
        """
        headers = self._generate_headers(data["organization_id"])
        url = f"{self._server_url}/observation/"
        try:
            response = self._req_session.post(
                url, json=data, headers=headers, timeout=10
            )
            response.raise_for_status()
        except Exception as ex:
            msg = "Failed to post the observation to the server"
            raise ComputeServerClientException(msg, ex) from ex

    def post_table_level_observation(self, data: TableLevelObservation) -> None:
        """Post the observation to the server.

        Args:
            data: Collected data from the target database.
        Raises:
            ComputeServerClientException: Failed to post the observation.
        """
        # pylint: disable=unused-variable
        headers = self._generate_headers(data["organization_id"])

        url = f"{self._server_url}/table_level_observation/"
        data_str = json.dumps(data, default=str)
        headers["Content-Type"] = "application/json"
        try:
            response = self._req_session.post(
                url, data=data_str, headers=headers, timeout=10
            )
            response.raise_for_status()
        except Exception as ex:
            msg = "Failed to post the table level observation to the server"
            raise ComputeServerClientException(msg, ex) from ex
