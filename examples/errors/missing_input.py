"""
This dataflow crashes because we never add an input operator.
"""

from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput


def stringify(x):
    return f"{x}"


def get_flow():
    flow = Dataflow()
    # XXX: Error here
    # flow.input("inp", TestingInput(10))
    flow.map(stringify)
    flow.output("out", StdOutput())
    return flow
