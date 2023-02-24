from bytewax.dataflow import Dataflow
from bytewax.testing import TestingInput
# from bytewax.outputs import StdOutputConfig
from bytewax.connectors.files import FileOutput


def flow1(path):
    flow = Dataflow()
    flow.input("inp", TestingInput(list(range(3))))
    flow.map(lambda x: ("ALL", f"{x}"))
    flow.output("out", FileOutput(path))
    return flow


# # Flow that raises an exception
# flow_raises = Dataflow()
#
#
# def boom(item):
#     if item == 0:
#         raise RuntimeError("BOOM")
#     else:
#         return item
#
#
# flow_raises.input("inp", TestingInput(range(3)))
# flow_raises.map(boom)
# flow_raises.capture(StdOutputConfig())
