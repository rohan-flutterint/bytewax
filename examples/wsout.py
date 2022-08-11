from time import sleep

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import WebSocketOutputConfig


def ib(i, n):
    for x in range(10000):
        yield x
        sleep(1)


flow = Dataflow(ManualInputConfig(ib))
flow.capture(WebSocketOutputConfig("0.0.0.0:3000"))

if __name__ == "__main__":
    run_main(flow)
