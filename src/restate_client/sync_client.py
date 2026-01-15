from typing import Optional

import requests
from restate.exceptions import TerminalError

from .base import RestateBase, parse_data, Singleton


class RestateService(RestateBase):
    def __init__(
            self,
            *args,
            **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._url = ""

    def __getattr__(self, name):
        def _get_handler(data=None, key=None) -> dict:
            if key:
                self._url = f"{self._base_url}/{key}/{name}"
            else:
                self._url = f"{self._base_url}/{name}"
            response = self._request(data)
            if response.text:
                return response.json()
            else:
                return {}

        return _get_handler

    def _request(self, data: Optional[dict] = None):
        """
        Send a POST request to the specified endpoint with the given data.
        """
        if self._session is None:
            return None
        if data:
            response = self._session.post(self._url, json=parse_data(data), headers={"Content-Type": "application/json"})
        else:
            response = self._session.get(self._url)
        if response.status_code != 200:
            self._logger.error(f"Response: {response.text}")
            raise requests.RequestException(f"Request failed: {response.status_code}")
        return response


class RestateClient(RestateBase, metaclass=Singleton):
    """
    A client for interacting with the Restate API.
    """

    def __getattr__(self, name):
        self._url = f"{self._base_url}/{name}"
        return RestateService(
            base_url=self._url,
            debug=self._debug,
        )

    def service_send(
            self,
            service_name: str,
            handler: str,
            payload: dict,
            delay_seconds: int = 0,
            idempotency_key: Optional[str] = None,
    ):
        """
        Invoke a service handler without expecting a return value.

        Args:
            service_name (str): The name of the service.
            handler (str): The name of the handler.
            payload (dict): The payload to send.
            delay_seconds (int, optional): Delay in seconds before sending the request. Defaults to 0.
            idempotency_key (str, optional): The idempotency key to send. Defaults to None.
        """
        try:
            if not self._session:
                return
            url = f"{self._base_url}/{service_name}/{handler}/send"

            if delay_seconds > 0:
                url = f"{url}?delay={delay_seconds}s"

            headers = {"Content-Type": "application/json"}
            if idempotency_key:
                headers["idempotency-key"] = idempotency_key
            response = self._session.post(url, json=parse_data(payload), headers=headers)
            response.raise_for_status()
            self._logger.debug(f"Sent payload to {url} successfully.")
        except requests.RequestException as e:
            self._logger.error(f"Failed to send payload to {url}: {e}")
            raise TerminalError(str(e)) from e

    def object_send(
            self,
            service_name: str,
            handler: str,
            key: str,
            payload: dict,
            delay_seconds: int = 0,
            idempotency_key: Optional[str] = None,
    ):
        """
        Invoke a virtual object handler with a key, without expecting a return value.

        Args:
            service_name (str): The name of the service.
            handler (str): The name of the handler.
            key (str): The key for the virtual object.
            payload (dict): The payload to send.
            delay_seconds (int, optional): Delay in seconds before sending the request. Defaults to 0.
            idempotency_key (str, optional): The idempotency key to send. Defaults to None.
        """

        try:
            if not self._session:
                return
            url = f"{self._base_url}/{service_name}/{key}/{handler}/send"

            if delay_seconds > 0:
                url = f"{url}?delay={delay_seconds}s"

            headers = {"Content-Type": "application/json"}
            if idempotency_key:
                headers["idempotency-key"] = idempotency_key
            response = self._session.post(url, json=parse_data(payload), headers=headers)
            response.raise_for_status()
            self._logger.debug(f"Sent payload to {url} successfully.")
        except requests.RequestException as e:
            self._logger.error(f"Failed to send payload to {url}: {e}")
            raise TerminalError(str(e)) from e

    def generic_send(
            self,
            service_name: str,
            handler: str,
            payload: dict,
            key: Optional[str] = None,
            delay_seconds: int = 0,
            idempotency_key: Optional[str] = None,
    ):
        """
        Generic handle for sending payload to a service or virtual object.
        """
        if key:
            return self.object_send(service_name, handler, key, payload, delay_seconds, idempotency_key)
        else:
            return self.service_send(service_name, handler, payload, delay_seconds, idempotency_key)

    def service_attach(self, service_name: str, handler: str, idempotency_key: str):
        """
        Attach a service to a virtual object.
        """
        if not self._session:
            return
        if not service_name:
            raise ValueError("Service name is required")
        if not handler:
            raise ValueError("Handler is required")
        if not idempotency_key:
            raise ValueError("Idempotency key is required")
        try:
            url = f"{self._base_url}/restate/invocation/{service_name}/{handler}/{idempotency_key}/attach"
            response = self._session.get(url)
            response.raise_for_status()
            if response.text:
                return response.json()
            else:
                return {}
        except requests.RequestException as e:
            self._logger.error(
                f"Failed to attach service {service_name} to {handler} with idempotency key {idempotency_key}: {e}"
            )
            raise

    def object_attach(self, service_name: str, handler: str, key: str, idempotency_key: str):
        """
        Attach a virtual object to a service.
        """
        if not self._session:
            return
        if not service_name:
            raise ValueError("Service name is required")
        if not handler:
            raise ValueError("Handler is required")
        if not key:
            raise ValueError("Key is required")
        if not idempotency_key:
            raise ValueError("Idempotency key is required")
        try:
            url = f"{self._base_url}/restate/invocation/{service_name}/{key}/{handler}/{idempotency_key}/attach"
            response = self._session.get(url)
            response.raise_for_status()
            if response.text:
                return response.json()
            else:
                return {}
        except requests.RequestException as e:
            self._logger.error(
                f"Failed to attach virtual object {service_name} to {handler} with idempotency key {idempotency_key}: {e}"
            )
            raise

    def generic_attach(self, service_name: str, handler: str, idempotency_key: str, key: Optional[str] = None):
        """
        Generic handle for attaching a service or virtual object.
        """
        if key:
            return self.object_attach(service_name, handler, key, idempotency_key)
        else:
            return self.service_attach(service_name, handler, idempotency_key)

    def service_output(self, service_name: str, handler: str, idempotency_key: str):
        """
        Get the output of a service.
        """
        if not self._session:
            return
        if not service_name:
            raise ValueError("Service name is required")
        if not handler:
            raise ValueError("Handler is required")
        if not idempotency_key:
            raise ValueError("Idempotency key is required")
        try:
            url = f"{self._base_url}/restate/invocation/{service_name}/{handler}/{idempotency_key}/output"
            response = self._session.get(url)
            if response.text:
                return response.json()
            else:
                return {}
        except requests.RequestException as e:
            self._logger.error(
                f"Failed to get output of service {service_name} with idempotency key {idempotency_key}: {e}"
            )
            raise

    def object_output(self, service_name: str, handler: str, key: str, idempotency_key: str):
        """
        Get the output of a virtual object.
        """
        if not self._session:
            return
        if not service_name:
            raise ValueError("Service name is required")
        if not handler:
            raise ValueError("Handler is required")
        if not key:
            raise ValueError("Key is required")
        if not idempotency_key:
            raise ValueError("Idempotency key is required")
        try:
            url = f"{self._base_url}/restate/invocation/{service_name}/{key}/{handler}/{idempotency_key}/output"
            response = self._session.get(url)
            if response.text:
                return response.json()
            else:
                return {}
        except requests.RequestException as e:
            self._logger.error(
                f"Failed to get output of virtual object {service_name} with idempotency key {idempotency_key}: {e}"
            )
            raise

    def generic_output(self, service_name: str, handler: str, idempotency_key: str, key: Optional[str] = None):
        """
        Generic handle for getting the output of a service or virtual object.
        """
        if key:
            return self.object_output(service_name, handler, key, idempotency_key)
        else:
            return self.service_output(service_name, handler, idempotency_key)

    def delete_invocation(self, invocation_id: str) -> dict:
        """
        Delete an invocation.
        """
        try:
            url = f"{self._base_url}/invocation/{invocation_id}"
            response = self._session.delete(url)
            response.raise_for_status()
            if response.text:
                return response.json()
            else:
                return {}
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                self._logger.warning(f"Invocation {invocation_id} not found")
                return None
            raise
        except requests.RequestException as e:
            self._logger.error(f"Failed to delete invocation {invocation_id}: {e}")
            raise
