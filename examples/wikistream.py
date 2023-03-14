import json
import operator
from datetime import timedelta

# pip install sseclient-py urllib3
import sseclient
import urllib3

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import DynamicInput, StatelessSource
from bytewax.connectors.stdio import StdOutput
from bytewax.window import SystemClockConfig, SessionWindow

# import os
# from bytewax.tracing import setup_tracing, OtlpTracingConfig
# tracer = setup_tracing(
#     # log_level="INFO",
#     # tracing_config=OtlpTracingConfig(
#     #     url=os.getenv("BYTEWAX_OTLP_URL", "grpc://127.0.0.1:4317"),
#     #     service_name="Tracing-example",
#     # ),
# )


class WikiSource(StatelessSource):
    def __init__(self, client, events):
        self.client = client
        self.events = events

    def next(self):
        raise Exception("BOOM")
        next(self.events)

    def close(self):
        self.client.close()


class WikiStreamInput(DynamicInput):
    def __init__(self):
        self.pool = urllib3.PoolManager()
        self.resp = self.pool.request(
            "GET",
            "https://stream.wikimedia.org/v2/stream/recentchange/",
            preload_content=False,
            headers={"Accept": "text/event-stream"},
        )
        self.client = sseclient.SSEClient(self.resp)
        self.events = self.client.events()

    def build(self, worker_index, worker_count):
        return WikiSource(self.client, self.events)


def initial_count(data_dict):
    return data_dict["server_name"], 1


def keep_max(max_count, new_count):
    new_max = max(max_count, new_count)
    return new_max, new_max


flow = Dataflow()
flow.input("inp", WikiStreamInput())
# "event_json"
flow.map(json.loads)
# flow.map(lambda x: f"{x['server_name']}")
# {"server_name": "server.name", ...}
flow.map(initial_count)
# ("server.name", 1)
flow.reduce_window(
    "sum",
    SystemClockConfig(),
    SessionWindow(gap=timedelta(milliseconds=100)),
    operator.add,
)
# ("server.name", sum_per_window)
flow.stateful_map(
    "keep_max",
    lambda: 0,
    keep_max,
)
# ("server.name", max_per_window)
# flow.output("out", ServerOutput("http://localhost:8000"))
flow.output("out", StdOutput())


if __name__ == "__main__":
    run_main(flow)
