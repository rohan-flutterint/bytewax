from bytewax.dataflow import Dataflow
from bytewax.execution import cluster_main
from bytewax.inputs import WebServerInputConfig
from bytewax.outputs import StdOutputConfig
from bytewax.tracing import setup_tracing

# tracer = setup_tracing(
#     log_level="TRACE",
# )


def input_builder(_worker_index, _worker_count, _resume_state):
    def handler(headers, request):
        return (b"whatever", request.decode("utf-8"))

    return handler


flow = Dataflow()
flow.input("input", WebServerInputConfig(input_builder))
flow.capture(StdOutputConfig())


if __name__ == "__main__":
    cluster_main(flow, [], 0)
