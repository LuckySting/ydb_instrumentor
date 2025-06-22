import pytest
import ydb

from tests.ydb_container import YDBContainer


@pytest.mark.asyncio
async def test_can_select(ydb_container: YDBContainer) -> None:
    connection_string = ydb_container.get_connection_string()
    async with (
        ydb.aio.Driver(connection_string=connection_string) as driver,
        ydb.aio.QuerySessionPool(driver, size=1) as pool,
        pool.checkout() as session,
    ):
        await driver.wait()
        cursor = await session.execute("SELECT 1 + 1")
        results = [row async for rs in cursor for row in rs.rows]
        expected_result = 2
        assert results[0][0] == expected_result  # noqa: S101

