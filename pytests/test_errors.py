import pytest

from bytewax.dataflow import Dataflow
from bytewax.testing import TestingOutput
from bytewax.inputs import DynamicInput, StatelessSource
from bytewax.execution import run_main


class ErrorInput(DynamicInput):
    def build(self, wi, wc):

        class Source(StatelessSource):
            def next(self):
                raise Exception("BOOM")

            def close(self):
                pass

        return Source()


def test_error():
    flow = Dataflow()
    flow.input("inp", ErrorInput())
    out = []
    flow.output("out", TestingOutput(out))

    with pytest.raises(RuntimeError, match="error getting input"):
        run_main(flow)
