from bytewax.dataflow import Dataflow
from bytewax.execution import cluster_main
from bytewax.inputs import WebServerInputConfig
from bytewax.outputs import StdOutputConfig
from bytewax.tracing import setup_tracing

tracer = setup_tracing(
    log_level="TRACE",
)


def input_builder(worker_index, worker_count, resume_state):
    def handler(request):
        return request

    return handler


flow = Dataflow()
flow.input("input", WebServerInputConfig(input_builder))
flow.map(lambda x: f"<dance>{x}</dance>")
flow.capture(StdOutputConfig())


if __name__ == "__main__":
    cluster_main(flow, [], 0)
