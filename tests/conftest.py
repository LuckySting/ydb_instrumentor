import shutil
from collections.abc import Iterator

import pytest

from tests.ydb_container import YDBContainer


@pytest.fixture(scope="session")
def ydb_container() -> Iterator[YDBContainer]:
    if shutil.which("docker") is None:
        pytest.skip("Docker is not available")
    container = YDBContainer()
    container.start()
    try:
        yield container
    finally:
        container.stop()
