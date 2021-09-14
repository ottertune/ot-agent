"""Defines the http/https request session builder"""
import ssl
from typing import Any
import requests
from requests.adapters import HTTPAdapter

# pylint: disable=import-error
from requests.packages.urllib3.poolmanager import (
    PoolManager,
)
from requests import Session
from driver.exceptions import ComputeServerClientException


class SSLAdapter(HTTPAdapter):
    """SSL Adapter used to build the ssl connection with the latest SSL version."""

    def init_poolmanager(
        self, connections: int, maxsize: int, block: bool = False, **pool_kwargs: Any
    ) -> None:
        context = ssl.create_default_context()
        # Force to use the latest TLS/SSL version (TLS v1.3)
        context.options |= (
            ssl.OP_NO_TLSv1
            | ssl.OP_NO_TLSv1_1
            | ssl.OP_NO_TLSv1_2
            | ssl.OP_NO_SSLv2
            | ssl.OP_NO_SSLv3
        )
        self.poolmanager = PoolManager(
            num_pools=connections, maxsize=maxsize, block=block, ssl_context=context
        )


def build_request_session() -> Session:
    """Build the request session.

    Returns:
        A request session built for connection.
    Raises:
        ComputeServerClientException: Failed to build the request session.
    """

    try:
        session = requests.Session()
        # Force to use the latest TLS version for https requests
        session.mount("https://", SSLAdapter())
    except Exception as ex:
        msg = "Failed to build a request session"
        raise ComputeServerClientException(msg, ex) from ex
    # Disable request proxy by ignoring proxy environment variables like http_proxy and https_proxy
    session.trust_env = False
    return session
