from transformers import AutoModelForSeq2SeqLM, AutoTokenizer, pipeline

from bytewax.dataflow import Dataflow
from bytewax.execution import cluster_main
from bytewax.inputs import WebServerInputConfig
from bytewax.outputs import StdOutputConfig
from bytewax.tracing import setup_tracing

classifier = pipeline("summarization")

# tracer = setup_tracing(
#     log_level="TRACE",
# )


def input_builder(worker_index, worker_count, resume_state):
    def handler(request):
        return request

    return handler


def summarize(text):
    return classifier(text)


flow = Dataflow()
flow.input("input", WebServerInputConfig(input_builder))
flow.map(summarize)
flow.map(lambda x: x[0]["summary_text"])
flow.capture(StdOutputConfig())


if __name__ == "__main__":
    cluster_main(flow, [], 0)
