import logging
import os
from typing import Any
from typing import Self

import ydb
from testcontainers.core.generic import DbContainer
from testcontainers.core.waiting_utils import wait_container_is_ready

logger = logging.getLogger(__name__)


def _load_image() -> str:
    return os.environ.get("YDB_DOCKER_IMAGE", "ydbplatform/local-ydb")


class YDBContainer(DbContainer):
    def __init__(
        self,
        name: str | None = None,
        port: str = "2135",
        image: str | None = None,
        **kwargs: Any,
    ) -> None:
        image = image or _load_image()
        docker_client_kw: dict[str, Any] = kwargs.pop("docker_client_kw", {})
        docker_client_kw["timeout"] = docker_client_kw.get("timeout") or 300
        super().__init__(image=image, hostname="localhost", docker_client_kw=docker_client_kw, **kwargs)
        self.port_to_expose = port
        self._name = name

    def start(self) -> Self:
        self._maybe_stop_old_container()
        super().start()
        return self

    def get_connection_url(self, driver: str = "ydb") -> str:
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.port_to_expose)
        return f"yql+{driver}://{host}:{port}/local"

    def get_connection_string(self) -> str:
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.port_to_expose)
        return f"grpc://{host}:{port}/?database=/local"

    @wait_container_is_ready(ydb.ConnectionError)
    def _connect(self) -> None:
        with ydb.Driver(connection_string=self.get_connection_string()) as driver:
            driver.wait(fail_fast=True)
            try:
                driver.scheme_client.describe_path("/local/.sys_health/test")
            except ydb.SchemeError as e:
                msg = "Database is not ready"
                raise ydb.ConnectionError(msg) from e

    def _configure(self) -> None:
        self.with_bind_ports(self.port_to_expose, self.port_to_expose)
        if self._name:
            self.with_name(self._name)
        self.with_env("YDB_USE_IN_MEMORY_PDISKS", "true")
        self.with_env("YDB_DEFAULT_LOG_LEVEL", "DEBUG")
        self.with_env("GRPC_PORT", self.port_to_expose)
        self.with_env("GRPC_TLS_PORT", self.port_to_expose)

    def _maybe_stop_old_container(self) -> None:
        if not self._name:
            return
        docker_client = self.get_docker_client()
        running_container = docker_client.client.api.containers(filters={"name": self._name})
        if running_container:
            logger.info("Stop existing container")
            docker_client.client.api.remove_container(running_container[0], force=True, v=True)
