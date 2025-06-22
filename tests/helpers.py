import contextlib
from collections.abc import AsyncIterator

import ydb


async def make_query(pool: ydb.aio.QuerySessionPool) -> None:
    session: ydb.aio.QuerySession
    async with pool.checkout() as session, session.transaction() as tx:
        cursor = await tx.execute("SELECT 1+1")
        _ = [result_set.rows[0] async for result_set in cursor]
        await tx.commit()


@contextlib.asynccontextmanager
async def ydb_pool(
    endpoint: str = "localhost:2135",
    database: str = "/local",
) -> AsyncIterator[ydb.aio.QuerySessionPool]:
    config = ydb.DriverConfig(
        endpoint=endpoint,
        database=database,
    )
    async with ydb.aio.Driver(driver_config=config) as driver:
        await driver.wait()
        async with ydb.aio.QuerySessionPool(driver, size=200) as pool:
            yield pool
