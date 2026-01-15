import json
import logging
import os
import socket
import sys
from typing import Any, Optional

import aiohttp
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Connection keep-alive configuration
CONNECT_TIMEOUT = 3  # seconds
KEEP_ALIVE_INTERVAL = 5  # seconds
KEEP_ALIVE_TIMEOUT = 5  # seconds


# Configure logging to output to stdout
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
)


# Custom socket options for TCP keep-alive
class TCPKeepAliveAdapter(HTTPAdapter):
    def __init__(self, *args, logger=None, **kwargs):
        self._logger = logger or logging.getLogger("restate_client_adapter")
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, *args, **kwargs):
        if kwargs.get('socket_options') is None:
            socket_options = [
                (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),  # Enable keep-alive
                (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, KEEP_ALIVE_TIMEOUT),  # Start probes after 5s idle
                (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, KEEP_ALIVE_INTERVAL),  # Interval between probes (5s)
                (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 2),  # Number of failed probes before closing
            ]
            kwargs['socket_options'] = socket_options
            self._logger.debug(
                f"TCP keep-alive configured: OS will send keep-alive pings "
                f"after {KEEP_ALIVE_TIMEOUT}s idle, every {KEEP_ALIVE_INTERVAL}s, max 2 failed probes"
            )
        poolmanager = super().init_poolmanager(*args, **kwargs)
        return poolmanager

    def send(self, request, *args, **kwargs):
        """
        Send a request and log keep-alive activity.
        TCP keep-alive pings are sent by the OS at the configured intervals.
        """
        # Log that keep-alive is active for this request
        # The OS will send keep-alive pings on idle connections
        self._logger.debug(
            f"Keep-alive: Sending request to {request.url} " f"(TCP keep-alive pings configured and active)"
        )
        return super().send(request, *args, **kwargs)


def parse_data(data: Any) -> dict:
    """
    Parse the data to a dictionary.
    """
    if isinstance(data, dict):
        return data
    elif hasattr(data, "model_dump_json"):
        # This is to convert enum values to strings
        return json.loads(data.model_dump_json())
    else:
        return data


class Singleton(type):
    """
    Singleton metaclass.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class RestateBase:
    def __init__(
        self,
        debug: bool = False,
        base_url: str = "",
        connect_timeout: Optional[int] = None,
        keep_alive_interval: Optional[int] = None,
        keep_alive_timeout: Optional[int] = None,
    ):
        self._logger = logging.getLogger("restate_client")
        self.base_url = ""
        # console handler
        self._logger.setLevel(logging.DEBUG if debug else logging.INFO)
        if not self._logger.hasHandlers():
            self._logger.addHandler(logging.StreamHandler())

        self._logger.propagate = False
        self._debug = debug
        self._base_url = base_url

        # Connection keep-alive configuration (use provided values or defaults)
        self._connect_timeout = connect_timeout if connect_timeout is not None else CONNECT_TIMEOUT
        self._keep_alive_interval = keep_alive_interval if keep_alive_interval is not None else KEEP_ALIVE_INTERVAL
        self._keep_alive_timeout = keep_alive_timeout if keep_alive_timeout is not None else KEEP_ALIVE_TIMEOUT

        if "PYTEST_CURRENT_TEST" in os.environ or "PYTEST_VERSION" in os.environ:
            self._session = None
            self._async_session = None
        else:
            self._session = self._create_session()
            try:
                self._async_session = self._create_async_session()
            except Exception as e:
                self._logger.warning(f"Failed to create async session: {e}, will be initialized later.")
                self._async_session = None

    def _create_session(self):
        """
        Create a requests session with connection keep-alive and pooling.
        Uses:
        - connect-timeout: CONNECT_TIMEOUT
        - keep-alive-interval: KEEP_ALIVE_INTERVAL
        - keep-alive-timeout: KEEP_ALIVE_TIMEOUT
        """
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )

        # Create HTTP adapter with connection pooling and keep-alive
        # Connection keep-alive is handled automatically by urllib3
        adapter = TCPKeepAliveAdapter(
            pool_connections=10,  # Number of connection pools to cache
            pool_maxsize=20,  # Maximum number of connections to save in the pool
            max_retries=retry_strategy,
            pool_block=False,  # Don't block when pool is full
            logger=self._logger,  # Pass logger for keep-alive logging
        )

        # Set connect-timeout (tuple format: (connect_timeout, read_timeout))
        # Using a reasonable read timeout (60s) so it doesn't interfere with normal operations
        session.timeout = (self._connect_timeout, 60)

        # Mount adapter for both HTTP and HTTPS
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def _create_async_session(self):
        """
        Create an async session with connection keep-alive and pooling.
        Uses:
        - connect-timeout: CONNECT_TIMEOUT
        - keep-alive-interval: KEEP_ALIVE_INTERVAL
        - keep-alive-timeout: KEEP_ALIVE_TIMEOUT
        """
        if type(self._async_session) == aiohttp.ClientSession:
            return self._async_session

        # Create TCP connector with keep-alive settings
        connector = aiohttp.TCPConnector(
            limit=100,  # Total connection pool size
            limit_per_host=30,  # Max connections per host
            keepalive_timeout=self._keep_alive_timeout,
            enable_cleanup_closed=True,  # Clean up closed connections
        )

        # Create session with connector
        async_session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(
                total=60,  # Total timeout for requests
                connect=self._connect_timeout,
            ),
        )

        return async_session
