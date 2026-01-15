from typing import Optional

import aiohttp
from restate.exceptions import TerminalError

from .base import RestateBase, parse_data, Singleton


class RestateAsyncService(RestateBase):
    """
    A class to handle Restate service requests.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._url = ""
        self._async_session = self._create_async_session()

    def __getattr__(self, name):
        async def _get_handler(data=None, key=None) -> dict:
            if key:
                self._url = f"{self._base_url}/{key}/{name}"
            else:
                self._url = f"{self._base_url}/{name}"

            response = await self._request(data)
            content_type = response.headers.get('Content-Type', '').lower()
            if 'application/json' in content_type:
                try:
                    return await response.json()
                except (aiohttp.ClientPayloadError, ValueError):
                    return await response.text()
            else:
                return await response.text()

        return _get_handler

    async def _request(self, data: Optional[dict] = None, key: Optional[str] = None):
        """
        Send a POST request to the specified endpoint with the given data.
        """
        
        self._async_session = self._create_async_session()
        if data:
            response = await self._async_session.post(
                self._url,
                json=parse_data(data),
                headers={"Content-Type": "application/json"}
            )
        else:
            response = await self._async_session.get(self._url)
        if response.status != 200:
            self._logger.error(f"Response: {await response.text()}")
            raise aiohttp.ClientError(f"Request failed: {response.status}")
        return response


class RestateAsyncClient(RestateBase, metaclass=Singleton):
    """
    A client for interacting with the Restate API.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __getattr__(self, name):
        self._url = f"{self._base_url}/{name}"
        return RestateAsyncService(
            base_url=self._url,
            debug=self._debug,
        )

    async def service_send(
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
            self._async_session = self._create_async_session()
            url = f"{self._base_url}/{service_name}/{handler}/send"

            if delay_seconds > 0:
                url = f"{url}?delay={delay_seconds}s"

            headers = {"Content-Type": "application/json"}
            if idempotency_key:
                headers["idempotency-key"] = idempotency_key
            response = await self._async_session.post(url, json=parse_data(payload), headers=headers)
            response.raise_for_status()
            self._logger.debug(f"Sent payload to {url} successfully.")
        except aiohttp.ClientError as e:
            self._logger.error(f"Failed to send payload to {url}: {e}")
            raise TerminalError(str(e)) from e
        except RuntimeError:
            self._logger.error("Failed to create async session")
            raise TerminalError("Failed to create async session") from None
        except Exception as e:
            self._logger.error(f"Failed to send payload to {url}: {e}")
            raise TerminalError(str(e)) from e

    async def object_send(
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
            self._async_session = self._create_async_session()
            url = f"{self._base_url}/{service_name}/{key}/{handler}/send"

            if delay_seconds > 0:
                url = f"{url}?delay={delay_seconds}s"

            headers = {"Content-Type": "application/json"}
            if idempotency_key:
                headers["idempotency-key"] = idempotency_key
            response = await self._async_session.post(url, json=parse_data(payload), headers=headers)
            response.raise_for_status()
            self._logger.debug(f"Sent payload to {url} successfully.")
        except aiohttp.ClientError as e:
            self._logger.error(f"Failed to send payload to {url}: {e}")
            raise TerminalError(str(e)) from e
        except RuntimeError:
            self._logger.error("Failed to create async session")
            raise TerminalError("Failed to create async session") from None
        except Exception as e:
            self._logger.error(f"Failed to send payload to {url}: {e}")
            raise TerminalError(str(e)) from e

    async def generic_send(
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
            return await self.object_send(service_name, handler, key, payload, delay_seconds, idempotency_key)
        else:
            return await self.service_send(service_name, handler, payload, delay_seconds, idempotency_key)

    async def service_attach(self, service_name: str, handler: str, idempotency_key: str):
        """
        Attach a service to a virtual object.
        """
        self._async_session = self._create_async_session()
        if not service_name:
            raise ValueError("Service name is required")
        if not handler:
            raise ValueError("Handler is required")
        if not idempotency_key:
            raise ValueError("Idempotency key is required")
        try:
            url = f"{self._base_url}/restate/invocation/{service_name}/{handler}/{idempotency_key}/attach"
            response = await self._async_session.get(url)
            response.raise_for_status()
            content_type = response.headers.get('Content-Type', '').lower()
            if 'application/json' in content_type:
                try:
                    return await response.json()
                except (aiohttp.ClientPayloadError, ValueError):
                    return await response.text()
            else:
                text = await response.text()
                return text if text else {}
        except aiohttp.ClientError as e:
            self._logger.error(
                f"Failed to attach service {service_name} to {handler} with idempotency key {idempotency_key}: {e}"
            )
            raise TerminalError(str(e)) from e
        except RuntimeError:
            self._logger.error("Failed to create async session")
            raise TerminalError("Failed to create async session") from None
        except Exception as e:
            self._logger.error(
                f"Failed to attach service {service_name} to {handler} with idempotency key {idempotency_key}: {e}"
            )
            raise TerminalError(str(e)) from e

    async def object_attach(self, service_name: str, handler: str, key: str, idempotency_key: str):
        """
        Attach a virtual object to a service.
        """
        self._async_session = self._create_async_session()
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
            response = await self._async_session.get(url)
            response.raise_for_status()
            content_type = response.headers.get('Content-Type', '').lower()
            if 'application/json' in content_type:
                try:
                    return await response.json()
                except (aiohttp.ClientPayloadError, ValueError):
                    return await response.text()
            else:
                text = await response.text()
                return text if text else {}
        except aiohttp.ClientError as e:
            self._logger.error(
                f"Failed to attach virtual object {service_name} to {handler} with idempotency key {idempotency_key}: {e}"
            )
            raise TerminalError(str(e)) from e
        except RuntimeError:
            self._logger.error("Failed to create async session")
            raise TerminalError("Failed to create async session") from None
        except Exception as e:
            self._logger.error(
                f"Failed to attach virtual object {service_name} to {handler} with idempotency key {idempotency_key}: {e}"
            )
            raise TerminalError(str(e)) from e

    async def generic_attach(self, service_name: str, handler: str, idempotency_key: str, key: Optional[str] = None):
        """
        Generic handle for attaching a service or virtual object.
        """
        if key:
            return await self.object_attach(service_name, handler, key, idempotency_key)
        else:
            return await self.service_attach(service_name, handler, idempotency_key)

    async def service_output(self, service_name: str, handler: str, idempotency_key: str):
        """
        Get the output of a service.
        """
        self._async_session = self._create_async_session()
        if not service_name:
            raise ValueError("Service name is required")
        if not handler:
            raise ValueError("Handler is required")
        if not idempotency_key:
            raise ValueError("Idempotency key is required")
        try:
            url = f"{self._base_url}/restate/invocation/{service_name}/{handler}/{idempotency_key}/output"
            response = await self._async_session.get(url)
            content_type = response.headers.get('Content-Type', '').lower()
            if 'application/json' in content_type:
                try:
                    return await response.json()
                except (aiohttp.ClientPayloadError, ValueError):
                    return await response.text()
            else:
                text = await response.text()
                return text if text else {}
        except aiohttp.ClientError as e:
            self._logger.error(
                f"Failed to get output of service {service_name} with idempotency key {idempotency_key}: {e}"
            )
            raise TerminalError(str(e)) from e
        except RuntimeError:
            self._logger.error("Failed to create async session")
            raise TerminalError("Failed to create async session") from None
        except Exception as e:
            self._logger.error(
                f"Failed to get output of service {service_name} with idempotency key {idempotency_key}: {e}"
            )
            raise TerminalError(str(e)) from e

    async def object_output(self, service_name: str, handler: str, key: str, idempotency_key: str):
        """
        Get the output of a virtual object.
        """
        self._async_session = self._create_async_session()
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
            response = await self._async_session.get(url)
            content_type = response.headers.get('Content-Type', '').lower()
            if 'application/json' in content_type:
                try:
                    return await response.json()
                except (aiohttp.ClientPayloadError, ValueError):
                    return await response.text()
            else:
                text = await response.text()
                return text if text else {}
        except aiohttp.ClientError as e:
            self._logger.error(
                f"Failed to get output of virtual object {service_name} with idempotency key {idempotency_key}: {e}"
            )
            raise TerminalError(str(e)) from e
        except RuntimeError:
            self._logger.error("Failed to create async session")
            raise TerminalError("Failed to create async session") from None
        except Exception as e:
            self._logger.error(
                f"Failed to get output of virtual object {service_name} with idempotency key {idempotency_key}: {e}"
            )
            raise TerminalError(str(e)) from e

    async def generic_output(self, service_name: str, handler: str, idempotency_key: str, key: Optional[str] = None):
        """
        Generic handle for getting the output of a service or virtual object.
        """
        if key:
            return await self.object_output(service_name, handler, key, idempotency_key)
        else:
            return await self.service_output(service_name, handler, idempotency_key)

    async def delete_invocation(self, invocation_id: str) -> dict:
        """
        Delete an invocation.
        """
        try:
            self._async_session = self._create_async_session()
            url = f"{self._base_url}/invocation/{invocation_id}"
            response = await self._async_session.delete(url)
            response.raise_for_status()
            content_type = response.headers.get('Content-Type', '').lower()
            if 'application/json' in content_type:
                try:
                    return await response.json()
                except (aiohttp.ClientPayloadError, ValueError):
                    return await response.text()
            else:
                return await response.text()
        except aiohttp.ClientError as e:
            if e.status == 404:
                self._logger.warning(f"Invocation {invocation_id} not found")
                return None
            self._logger.error(f"Failed to delete invocation {invocation_id}: {e}")
            raise TerminalError(str(e)) from e
        except RuntimeError:
            self._logger.error("Failed to create async session")
            raise TerminalError("Failed to create async session") from None
        except Exception as e:
            self._logger.error(f"Failed to delete invocation {invocation_id}: {e}")
            raise TerminalError(str(e)) from e
