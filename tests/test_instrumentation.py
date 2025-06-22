import pytest
import ydb
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from tests.ydb_container import YDBContainer


@pytest.mark.asyncio
async def test_query_session_is_traced(ydb_container: YDBContainer, instrument_ydb: InMemorySpanExporter) -> None:
    connection_string = ydb_container.get_connection_string()
    async with (
        ydb.aio.Driver(connection_string=connection_string) as driver,
        ydb.aio.QuerySessionPool(driver, size=1) as pool,
        pool.checkout() as session,
    ):
        await driver.wait()
        cursor = await session.execute("SELECT 1 + 1")
        _ = [row async for rs in cursor for row in rs.rows]

    spans = instrument_ydb.get_finished_spans()
    assert len(spans) == 1  # noqa: S101
    span = spans[0]
    assert span.name == "YdbQuery"  # noqa: S101
    assert span.attributes["ydb.query.text"] == "SELECT 1 + 1"  # noqa: S101
    assert "ydb.session.id" in span.attributes  # noqa: S101
    assert "ydb.query.stats.total_duration" in span.attributes  # noqa: S101
    instrument_ydb.clear()


@pytest.mark.asyncio
async def test_query_tx_is_traced(ydb_container: YDBContainer, instrument_ydb: InMemorySpanExporter) -> None:
    connection_string = ydb_container.get_connection_string()
    async with (
        ydb.aio.Driver(connection_string=connection_string) as driver,
        ydb.aio.QuerySessionPool(driver, size=1) as pool,
        pool.checkout() as session,
        session.transaction() as tx,
    ):
        await driver.wait()
        cursor = await tx.execute("SELECT 1 + 1")
        _ = [row async for rs in cursor for row in rs.rows]
        await tx.commit()

    spans = instrument_ydb.get_finished_spans()
    assert len(spans) == 1  # noqa: S101
    span = spans[0]
    assert span.attributes["ydb.query.text"] == "SELECT 1 + 1"  # noqa: S101
    assert "ydb.session.id" in span.attributes  # noqa: S101
    assert "ydb.tx.id" in span.attributes  # noqa: S101
    assert "ydb.tx.mode" in span.attributes  # noqa: S101
    instrument_ydb.clear()
