import opentelemetry
from opentelemetry.exporter.otlp.proto.grpc import trace_exporter
from opentelemetry.instrumentation.grpc import GrpcAioInstrumentorClient
from opentelemetry.sdk import trace
from opentelemetry.sdk.trace import export

from ydb_instrumentor import YDBInstrumentor

PROVIDER = None
EXPORTER = None


def init_o11y() -> None:
    global PROVIDER, EXPORTER
    PROVIDER = trace.TracerProvider()
    EXPORTER = trace_exporter.OTLPSpanExporter(endpoint="localhost:4317", insecure=True)
    PROVIDER.add_span_processor(export.BatchSpanProcessor(EXPORTER))
    opentelemetry.trace.set_tracer_provider(PROVIDER)

    instrumentor = GrpcAioInstrumentorClient()
    instrumentor.instrument()

    instrumentor = YDBInstrumentor(True, True)
    instrumentor.instrument()
