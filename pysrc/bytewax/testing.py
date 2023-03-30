"""Helper tools for testing dataflows.
"""
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any, Iterable

from bytewax.inputs import PartitionedInput, StatefulSource
from bytewax.outputs import DynamicOutput, StatelessSink

from .bytewax import run_main

__all__ = [
    "run_main",
    "TestingInput",
    "TestingOutput",
]


class _IterSource(StatefulSource):
    def __init__(self, it, resume_state):
        self._idx = resume_state or -1
        self._it = enumerate(it)
        # Resume to one after the last completed read.
        for i in range(self._idx + 1):
            next(self._it)

    def next(self):
        # next will raise StopIteration on its own.
        self._idx, item = next(self._it)
        return item

    def snapshot(self):
        return self._idx


class TestingInput(PartitionedInput):
    """Produce input from a Python iterable. You only want to use this
    for unit testing.

    The iterable must be identical on all workers; this will
    automatically distribute the items across workers and handle
    recovery.

    Be careful using a generator as the iterable; if you fail and
    attempt to resume the dataflow without rebuilding it, the
    half-consumed generator will be re-used on recovery and early
    input will be lost so resume will see the correct data.

    Args:

        it: Iterable for input.

    """

    __test__ = False

    def __init__(self, it: Iterable[Any]):
        self._it = it

    def list_parts(self):
        return {"iter"}

    def build_part(self, for_key, resume_state):
        assert for_key == "iter"
        return _IterSource(self._it, resume_state)


class _ListSink(StatelessSink):
    def __init__(self, ls):
        self._ls = ls

    def write(self, item):
        self._ls.append(item)


class TestingOutput(DynamicOutput):
    """Append each output item to a list. You only want to use this
    for unit testing.

    Can support at-least-once processing. The list is not cleared
    between executions.

    Args:

        ls: List to append to.

    """

    __test__ = False

    def __init__(self, ls):
        self._ls = ls

    def build(self, worker_index, worker_count):
        return _ListSink(self._ls)


def poll_next(source, timeout=timedelta(seconds=5)):
    """Repeatedly poll an input source until it returns a value.

    You'll want to use this in unit tests of sources when there's some
    non-determinism in how items are read.

    Args:

        source: To call `source.next()` on.

    Returns:

        The next item found.

    Raises:

        TimeoutError: If no item was returned within the timeout.

    """
    item = None
    start = datetime.now(timezone.utc)
    while item is None:
        if datetime.now(timezone.utc) - start > timeout:
            raise TimeoutError()
        item = source.next()
    return item


_print_lock = Lock()


def test_print(*args, **kwargs):
    """A version of `print()` which takes an in-process lock to prevent
    multiple worker threads from writing simultaneously which results
    in interleaved output.

    You'd use this if you're integration testing a dataflow and want
    more deterministic output. Remember that even with this, the items
    from multi-worker output might be "out-of-order" because each
    worker is racing each other. You probably want to sort your output
    in some way.

    Arguments are passed through to `print()` unmodified.

    """
    with _print_lock:
        print(*args, flush=True, **kwargs)
