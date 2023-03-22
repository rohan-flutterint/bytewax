from pytest import fixture

from bytewax.execution import cluster_main, run_main
from bytewax.recovery import SqliteRecoveryConfig


@fixture(params=["run_main", "cluster_main-2thread"])
def entry_point_name(request):
    return request.param


def _wrapped_cluster_main1x2(*args, **kwargs):
    return cluster_main(*args, [], 0, worker_count_per_proc=2, **kwargs)


@fixture
def entry_point(entry_point_name):
    if entry_point_name == "run_main":
        return run_main
    elif entry_point_name == "cluster_main-2thread":
        return _wrapped_cluster_main1x2
    else:
        raise ValueError("unknown entry point name: {request.param!r}")


@fixture
def out():
    yield []


@fixture
def inp():
    yield []


@fixture
def recovery_config(tmp_path):
    yield SqliteRecoveryConfig(str(tmp_path))
