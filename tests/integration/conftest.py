import os

import pytest
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(str(pytestconfig.rootdir), "docker-compose.yml")


@pytest.fixture(scope="session")
def broker_host(docker_services) -> str:
    host_port = docker_services.port_for("kafka", 9092)
    return f"localhost:{host_port}"


@pytest.fixture(scope="session")
def registry_url(docker_services) -> str:
    host_port = docker_services.port_for("registry", 8081)
    return f"http://localhost:{host_port}"


@pytest.fixture(scope="session")
def admin(broker_host: str, docker_services) -> AdminClient:
    admin_client = AdminClient({"bootstrap.servers": broker_host})

    def check() -> bool:
        try:
            admin_client.list_topics()
            return True
        except Exception:
            return False

    docker_services.wait_until_responsive(check, timeout=60, pause=1)
    return admin_client


@pytest.fixture(scope="session")
def registry(registry_url: str, docker_services) -> SchemaRegistryClient:
    registry_client = SchemaRegistryClient({"url": registry_url})

    def check() -> bool:
        try:
            registry_client.get_subjects()
            return True
        except Exception:
            return False

    docker_services.wait_until_responsive(check, timeout=60, pause=1)
    return registry_client
