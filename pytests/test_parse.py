from bytewax.parse import proc_args, proc_env


def test_proc_args():
    args = ["-w2", "-p0", "-a", "localhost:1234", "-a", "localhost:5678"]

    found = proc_args(args)

    assert found == {
        "worker_count_per_proc": 2,
        "proc_id": 0,
        "addresses": ["localhost:1234", "localhost:5678"],
    }


def test_proc_env(tmpdir):
    hostpath = tmpdir / "hosts.txt"
    with open(hostpath, "w") as hostfile:
        hostfile.write("localhost:1234\n")
        hostfile.write("localhost:5678\n")
        hostfile.write("\n")

    env = {
        "BYTEWAX_WORKERS_PER_PROCESS": "2",
        "BYTEWAX_HOSTFILE_PATH": str(hostpath),
        "BYTEWAX_POD_NAME": "stateful_set-0",
        "BYTEWAX_STATEFULSET_NAME": "stateful_set",
    }

    found = proc_env(env)

    assert found == {
        "worker_count_per_proc": 2,
        "proc_id": 0,
        "addresses": ["localhost:1234", "localhost:5678"],
    }
