import os
import signal
import tempfile
from sys import exit

from pytest import mark, raises

from bytewax.dataflow import Dataflow
from bytewax.testing import TestingInput, TestingOutput
from bytewax.run import run


def test_run():
    # f = open("/tmp/output", "w+b")
    f = tempfile.NamedTemporaryFile()
    run(
        path="pytests/test_flows/__init__.py",
        dataflow_name="flow1",
        dataflow_args=[f.name,],
        processes=1,
        workers_per_process=1,
        snapshot_every=0,
        recovery_engine=None,
        kafka_topic=None,
        kafka_brokers=None,
        sqlite_directory=None,
    )
    f.seek(0)
    out = [int(i) for i in f.read().split()]
    f.close()
    assert sorted(out) == sorted([0, 1, 2])


def test_reraises_exception():
    f = open("/tmp/output", "w+b")

    with raises(RuntimeError):
        run(
            path="pytests/test_flows/__init__.py",
            dataflow_name="flow_raises",
            # Using more than one process makes the dataflow panic
            # while panicking
            processes=None,
            workers_per_process=None,
            out=f
        )

    f.seek(0)
    out = [int(i) for i in f.read().split()]
    f.close()
    assert len(out) == 3


@mark.skipif(
    os.name == "nt",
    reason=(
        "Sending os.kill(test_proc.pid, signal.CTRL_C_EVENT) sends event to all"
        " processes on this console so interrupts pytest itself"
    ),
)
def test_can_be_ctrl_c(mp_ctx, entry_point):
    with mp_ctx.Manager() as man:
        is_running = man.Event()
        out = man.list()

        def proc_main():
            flow = Dataflow()
            inp = range(1000)
            flow.input("inp", TestingInput(inp))

            def mapper(item):
                is_running.set()
                return item

            flow.map(mapper)
            flow.output("out", TestingOutput(out))

            try:
                entry_point(flow)
            except KeyboardInterrupt:
                exit(99)

        test_proc = mp_ctx.Process(target=proc_main)
        test_proc.start()

        assert is_running.wait(timeout=5.0), "Timeout waiting for test proc to start"
        os.kill(test_proc.pid, signal.SIGINT)
        test_proc.join(timeout=10.0)

        assert test_proc.exitcode == 99
        assert len(out) < 1000


@mark.skip(
    "We should have a way to access worker_index in whatever replaces spawn_cluster"
)
def test_input_parts_hashed_to_workers(mp_ctx):
    pass


def test_parts_hashed_to_workers(mp_ctx):
    with mp_ctx.Manager() as man:
        proc_count = 3
        worker_count_per_proc = 2
        worker_count = proc_count * worker_count_per_proc


@mark.skip(
    "We should have a way to access worker_index in whatever replaces spawn_cluster"
)
def test_output_parts_hashed_to_workers(mp_ctx):
    pass
