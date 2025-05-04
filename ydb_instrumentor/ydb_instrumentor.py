import contextlib
from collections.abc import AsyncIterator
from collections.abc import Collection
from functools import wraps
from typing import Any

import ydb
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor

TRACE = trace.get_tracer(__name__)


class YDBInstrumentor(BaseInstrumentor):
    """
    Instruments YDB SDK queries adding query information to OpenTelemetry traces. Span attributes:
     * ydb.query.text - YQL text (without parameters) of the query.
     * ydb.session.id - Session UD used to execute the query.
     * ydb.tx.id - Transaction ID used to execute the query.
     * ydb.tx.mode - Isolation level of the transaction.
     * ydb.query.stats.total_duration - Total query duration (in microseconds).
     * ydb.query.stats.total_cpu_time - Total query CPU time (in microseconds).
     * ydb.query.stats.process_cpu_time - Process CPU time (in microseconds).

    :param trace_query_stats: add query statistics to the span attributes,
    sets YDB stats_mode to BASIC, which may affect latency.
    """

    def __init__(self, trace_query_text: bool = False, trace_query_stats: bool = False) -> None:
        self._original_aio_session_execute = ydb.aio.QuerySession.execute
        self._original_aio_tx_execute = ydb.aio.QueryTxContext.execute
        self._trace_query_text = trace_query_text
        self._trace_query_stats = trace_query_stats

    def instrumentation_dependencies(self) -> Collection[str]:
        return ("ydb >= 3.21.1",)

    def _instrument(self, **kwargs: Any) -> None:  # noqa: ARG002
        # TODO: Add support of TableClient and sync clients
        # TODO: Asynccontext manager must be optional to enter

        async def _async_iterator_wrapper(
            async_iterator: AsyncIterator[ydb.convert.ResultSet],
        ) -> AsyncIterator[ydb.convert.ResultSet]:
            async for result_set in async_iterator:
                yield result_set

        @wraps(self._original_aio_tx_execute)
        async def _trace_aio_query_tx_execute(
            *func_args: Any, **func_kwargs: Any
        ) -> contextlib.AbstractAsyncContextManager[ydb.convert.ResultSet]:
            @contextlib.asynccontextmanager
            async def __trace_aio_query_tx_execute(
                tx: ydb.aio.QueryTxContext,
                query: str,
                *inner_args: Any,
                stats_mode: ydb.QueryStatsMode | None = None,
                **inner_kwargs: Any,
            ) -> AsyncIterator[AsyncIterator[ydb.convert.ResultSet]]:
                with TRACE.start_as_current_span("YdbQuery") as span:
                    self._maybe_set_query_attribute(span, query)
                    self._set_session_id_attribute(span, tx.session_id)

                    stats_mode = self._maybe_set_stats_to_basic(stats_mode)
                    async with await self._original_aio_tx_execute(
                        tx, query, *inner_args, stats_mode=stats_mode, **inner_kwargs
                    ) as result_iterator:
                        yield _async_iterator_wrapper(result_iterator)

                    self._set_tx_attributes(span, tx)
                    self._maybe_set_query_stats_attributes(span, tx.last_query_stats)

            return __trace_aio_query_tx_execute(*func_args, **func_kwargs)

        @wraps(self._original_aio_session_execute)
        async def _trace_aio_query_session_execute(
            *func_args: Any, **func_kwargs: Any
        ) -> contextlib.AbstractAsyncContextManager[ydb.convert.ResultSet]:
            @contextlib.asynccontextmanager
            async def __trace_aio_query_session_execute(
                session: ydb.aio.QuerySession,
                query: str,
                *inner_args: Any,
                stats_mode: ydb.QueryStatsMode | None = None,
                **inner_kwargs: Any,
            ) -> AsyncIterator[AsyncIterator[ydb.convert.ResultSet]]:
                with TRACE.start_as_current_span("YdbQuery") as span:
                    self._maybe_set_query_attribute(span, query)
                    self._set_session_id_attribute(span, session._state.session_id)  # noqa: SLF001

                    stats_mode = self._maybe_set_stats_to_basic(stats_mode)
                    async with await self._original_aio_session_execute(
                        session, query, *inner_args, stats_mode=stats_mode, **inner_kwargs
                    ) as result_iterator:
                        yield _async_iterator_wrapper(result_iterator)

                    self._maybe_set_query_stats_attributes(span, session.last_query_stats)

            return __trace_aio_query_session_execute(*func_args, **func_kwargs)

        ydb.aio.QueryTxContext.execute = _trace_aio_query_tx_execute
        ydb.aio.QuerySession.execute = _trace_aio_query_session_execute

    def _uninstrument(self, **kwargs: Any) -> None:  # noqa: ARG002
        ydb.aio.QuerySession.execute = self._original_aio_session_execute
        ydb.aio.QueryTxContext.execute = self._original_aio_tx_execute

    def _maybe_set_stats_to_basic(self, stats_mode: ydb.QueryStatsMode | None) -> ydb.QueryStatsMode:
        if not self._trace_query_stats:
            return stats_mode

        if stats_mode is None or stats_mode in (ydb.QueryStatsMode.UNSPECIFIED, ydb.QueryStatsMode.NONE):
            return ydb.QueryStatsMode.BASIC

        return stats_mode

    def _maybe_set_query_attribute(self, span: trace.Span, query: str) -> None:
        if self._trace_query_text:
            span.set_attribute("ydb.query.text", query)

    def _set_session_id_attribute(self, span: trace.Span, session_id: str) -> None:
        span.set_attribute("ydb.session.id", session_id)

    def _set_tx_attributes(self, span: trace.Span, tx_context: ydb.QueryTxContext) -> None:
        span.set_attribute("ydb.tx.mode", tx_context._tx_state.tx_mode.name)  # noqa: SLF001
        span.set_attribute("ydb.tx.id", tx_context._tx_state.tx_id)  # noqa: SLF001

    def _maybe_set_query_stats_attributes(self, span: trace.Span, last_query_stats: Any | None) -> None:
        if last_query_stats is not None:
            span.set_attribute("ydb.query.stats.process_cpu_time", f"{last_query_stats.process_cpu_time_us}us")
            span.set_attribute("ydb.query.stats.total_cpu_time", f"{last_query_stats.total_cpu_time_us}us")
            span.set_attribute("ydb.query.stats.total_duration", f"{last_query_stats.total_duration_us}us")
