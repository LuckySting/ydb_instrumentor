import shutil
from collections.abc import Iterator

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from tests.ydb_container import YDBContainer
from ydb_instrumentor import YDBInstrumentor


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


@pytest.fixture
def instrument_ydb() -> Iterator[InMemorySpanExporter]:
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    previous_provider = trace.get_tracer_provider()
    trace.set_tracer_provider(provider)

    instrumentor = YDBInstrumentor(True, True)
    instrumentor.instrument()
    try:
        yield exporter
    finally:
        instrumentor.uninstrument()
        trace.set_tracer_provider(previous_provider)
        exporter.clear()
