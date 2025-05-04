import asyncio

from opentelemetry import trace

from tests.helpers import make_query
from tests.helpers import ydb_pool
from tests.o11y import init_o11y

TRACE = trace.get_tracer(__name__)


async def main() -> None:
    init_o11y()
    async with ydb_pool() as pool:
        with TRACE.start_as_current_span("YdbQueries") as span:
            async with pool.checkout() as session:
                cursor = await session.execute("SELECT 1 + 1")
                print([result_set.rows[0] async for result_set in cursor])
            tasks = []
            for _ in range(1):
                tasks.append(asyncio.create_task(make_query(pool)))
            await asyncio.gather(*tasks)
            print(format(span.context.trace_id, "032x"))


if __name__ == "__main__":
    asyncio.run(main())
