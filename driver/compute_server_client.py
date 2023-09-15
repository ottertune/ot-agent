"""Defines the compute server client that interacts with the server with http requests"""

import zlib
from typing import List, Dict, Any, TypedDict, Set
from http import HTTPStatus
from requests import Session
import simplejson as json

from driver.exceptions import ComputeServerClientException


TIMEOUT_SEC = 30
SECONDS_TO_MS = 1000
RETRYABLE_HTTP_STATUS: Set[int] = {
    HTTPStatus.REQUEST_TIMEOUT,
    HTTPStatus.INTERNAL_SERVER_ERROR,
    HTTPStatus.BAD_GATEWAY,
    HTTPStatus.SERVICE_UNAVAILABLE,
    HTTPStatus.GATEWAY_TIMEOUT,
}

# TODO: move this elsewhere and have it pull from git tags as source of truth
AGENT_VERSION = "0.4.9"


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
    non_default_knobs: List[str]


class LongRunningQueryObservation(TypedDict):
    """Observation of long running query instances"""

    data: Dict[str, Any]
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


class QueryObservation(TypedDict):
    """Query observation data collected from the target database."""

    data: Dict[str, Any]
    summary: Dict[
        str, Any
    ]  # summary information like observation time, database version, etc
    db_key: str
    organization_id: str


class SchemaObservation(TypedDict):
    """Schema observation data collected from the target database."""

    data: Dict[str, Any]
    summary: Dict[
        str, Any
    ]  # summary information like observation time, database version, etc
    db_key: str
    organization_id: str


class AgentHealthData(TypedDict):
    organization_id: str
    db_key: str
    agent_status: str
    agent_starttime: str
    heartbeat_time: str
    agent_version: str
    agent_hostname: str
    errors: List[Dict[str, Any]]


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
                url, json=data, headers=headers, timeout=TIMEOUT_SEC
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
                url, data=data_str, headers=headers, timeout=TIMEOUT_SEC
            )
            response.raise_for_status()
        except Exception as ex:
            msg = "Failed to post the table level observation to the server"
            raise ComputeServerClientException(msg, ex) from ex

    def post_long_running_query_observation(
        self, data: LongRunningQueryObservation
    ) -> None:
        """Post **COMPRESSED** long running query observation to the server
        Args:
            session_id: Session that the data is uploaded to.
            data: Collected data from the target database.
        Raises:
            ComputeServerClientException: Failed to post the observation.
        """
        url = f"{self._server_url}/long_running_query_observation/"
        headers = self._generate_headers(data["organization_id"])
        headers["Content-Type"] = "application/json; charset=utf-8"
        headers["Content-Encoding"] = "gzip"
        # pylint: disable=c-extension-no-member
        compressed_data = zlib.compress(json.dumps(data, default=str).encode("utf-8"))
        # long running query observation use its own timeout settings
        query_observation_timeout = 60
        try:
            response = self._req_session.post(
                url,
                data=compressed_data,
                timeout=query_observation_timeout,
                headers=headers,
            )
            response.raise_for_status()
        except Exception as ex:
            msg = "Failed to post the long running query observation to the server"
            raise ComputeServerClientException(msg, ex) from ex

    def post_query_observation(self, data: QueryObservation) -> None:
        """Post **COMPRESSED** query observation to the server
        Args:
            session_id: Session that the data is uploaded to.
            data: Collected data from the target database.
        Raises:
            ComputeServerClientException: Failed to post the observation.
        """
        url = f"{self._server_url}/query_observation/"
        headers = self._generate_headers(data["organization_id"])
        headers["Content-Type"] = "application/json; charset=utf-8"
        headers["Content-Encoding"] = "gzip"
        # pylint: disable=c-extension-no-member
        compressed_data = zlib.compress(json.dumps(data, default=str).encode("utf-8"))
        # query observation use its own timeout settings due to the potential large data volume
        query_observation_timeout = 90
        try:
            response = self._req_session.post(
                url,
                data=compressed_data,
                timeout=query_observation_timeout,
                headers=headers,
            )
            response.raise_for_status()
        except Exception as ex:
            msg = "Failed to post the query observation to the server"
            raise ComputeServerClientException(msg, ex) from ex

    def post_schema_observation(self, data: SchemaObservation) -> None:
        """Post **COMPRESSED** schema observation to the server
        Args:
            session_id: Session that the data is uploaded to.
            data: Collected data from the target database.
        Raises:
            ComputeServerClientException: Failed to post the observation.
        """
        url = f"{self._server_url}/schema_observation/"
        headers = self._generate_headers(data["organization_id"])
        headers["Content-Type"] = "application/json; charset=utf-8"
        headers["Content-Encoding"] = "gzip"
        # pylint: disable=c-extension-no-member
        compressed_data = zlib.compress(json.dumps(data, default=str).encode("utf-8"))
        # schema observation use its own timeout settings due to the potential large data volume
        schema_observation_timeout = 90
        try:
            response = self._req_session.post(
                url,
                data=compressed_data,
                timeout=schema_observation_timeout,
                headers=headers,
            )
            response.raise_for_status()
        except Exception as ex:
            msg = "Failed to post the schema observation to the server"
            raise ComputeServerClientException(msg, ex) from ex

    def post_agent_health_heartbeat(self, data: AgentHealthData):
        url = f"{self._server_url}/agent_health/"
        headers = self._generate_headers(data["organization_id"])
        headers["Content-Type"] = "application/json; charset=utf-8"
        try:
            response = self._req_session.post(
                url, data=json.dumps(data, default=str), headers=headers
            )
            response.raise_for_status()
        except Exception as ex:
            msg = "Failed to post the agent health heartbeat to the server"
            raise ComputeServerClientException(msg, ex) from ex
