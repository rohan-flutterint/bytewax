"""
This dataflow crashes because we never add an input operator.
"""

from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput


def stringify(x):
    return f"{x}"


flow = Dataflow()
# XXX: Error here
# flow.input("inp", TestingInput(10))
flow.map(stringify)
flow.output("out", StdOutput())

if __name__ == "__main__":
    from bytewax.execution import run_main

    run_main(flow)
