import os
import sys
import subprocess
import argparse
import pathlib

from datetime import timedelta
from importlib.util import spec_from_file_location, module_from_spec

from bytewax.execution import run_main, cluster_main
from bytewax.dataflow import Dataflow
from bytewax.recovery import SqliteRecoveryConfig, KafkaRecoveryConfig


class ExecutionError(Exception):
    """An exception for errors related to loading the dataflow module."""

    pass


def _get_flow(path, name, dataflow_args=[]):
    """Get a Dataflow object from a given python file using importlib."""
    # print(dataflow_args)
    try:
        spec = spec_from_file_location("dataflow", path)
        if spec is None:
            raise ExecutionError(f"Error getting {path}, check the path is valid")
        module = module_from_spec(spec)
        spec.loader.exec_module(module)
    except Exception as e:
        raise ExecutionError(f"Error loading {path}") from e

    try:
        flow = module.__getattribute__(name)
        if callable(flow):
            flow = flow(*dataflow_args)
        assert type(flow) == Dataflow
    except (AttributeError, AssertionError) as e:
        raise ExecutionError(
            f"The python file should contain a Dataflow named '{name}'"
        ) from e
    return flow


def _get_args():
    parser = argparse.ArgumentParser(
        prog="python -m bytewax.run", description="Run a bytewax dataflow"
    )
    parser.add_argument("file_path", metavar="FILE_PATH", type=pathlib.Path)
    parser.add_argument(
        "-d",
        "--dataflow-name",
        type=str,
        default="flow",
        help="Name of the Dataflow variable",
    )
    parser.add_argument(
        "--dataflow-args",
        type=str,
        nargs="*",
        help="Args to pass to the dataflow getter",
    )
    scaling = parser.add_argument_group("Scaling")
    scaling.add_argument(
        "-p",
        "--processes",
        type=int,
        help="Number of separate processes to run",
    )
    scaling.add_argument(
        "-w",
        "--workers-per-process",
        type=int,
        help="Number of workers for each process",
    )
    # Config options for recovery
    recovery = parser.add_argument_group("Recovery")
    recovery.add_argument(
        "-r", "--recovery-engine", type=str, choices=["kafka", "sqlite"]
    )
    kafka_config = parser.add_argument_group("Kafka recovery config")
    kafka_config.add_argument(
        "--kafka-brokers", type=list[str], default=["localhost:9092"]
    )
    kafka_config.add_argument("--kafka-topic", type=str)
    sqlite_config = parser.add_argument_group("SQLite recovery config")
    sqlite_config.add_argument("--sqlite-directory", type=pathlib.Path)

    # Epoch configuration
    parser.add_argument("-s", "--snapshot-every", type=int, default=10)

    args = parser.parse_args()
    return args


def run(
    path,
    dataflow_name,
    dataflow_args,
    processes,
    workers_per_process,
    snapshot_every,
    recovery_engine,
    kafka_topic,
    kafka_brokers,
    sqlite_directory,
):
    proc_id = os.getenv("__BYTEWAX_PROC_ID", None)
    flow = _get_flow(path, dataflow_name, dataflow_args)

    epoch_interval = timedelta(seconds=snapshot_every)
    recovery_config = None
    if recovery_engine is not None:
        if recovery_engine == "kafka":
            recovery_config = KafkaRecoveryConfig(
                brokers=kafka_brokers, topic_prefix=kafka_topic
            )
        elif recovery_engine == "sqlite":
            recovery_config = SqliteRecoveryConfig(sqlite_directory or "./")

    if proc_id is None and processes is None and workers_per_process is None:
        run_main(flow, epoch_interval=epoch_interval, recovery_config=recovery_config)
    else:
        addresses = [f"localhost:{proc_id + 2101}" for proc_id in range(processes)]

        if proc_id is not None:
            cluster_main(
                flow,
                addresses,
                int(proc_id),
                epoch_interval=epoch_interval,
                recovery_config=recovery_config,
                worker_count_per_proc=workers_per_process,
            )
        else:
            ps = []
            for proc_id in range(processes):
                env = os.environ.copy()
                env["__BYTEWAX_PROC_ID"] = f"{proc_id}"
                args = [sys.executable, "-m", "bytewax.run", path]
                if workers_per_process is not None:
                    args.extend(["-w", f"{workers_per_process}"])
                if processes is not None:
                    args.extend(["-p", f"{processes}"])
                if dataflow_name is not None:
                    args.extend(["-d", f"{dataflow_name}"])
                if dataflow_args is not None:
                    args.extend(["--dataflow-args", *dataflow_args])
                if snapshot_every is not None:
                    args.extend(["--snapshot-every", f"{snapshot_every}"])
                if recovery_engine is not None:
                    args.extend(["--recovery-engine", f"{recovery_engine}"])
                if kafka_brokers is not None:
                    args.extend(["--kafka-brokers", f"{kafka_brokers}"])
                if kafka_topic is not None:
                    args.extend(["--kafka-topic", f"{kafka_topic}"])
                if sqlite_directory is not None:
                    args.extend(["--sqlite-directory", f"{sqlite_directory}"])

                ps.append(subprocess.Popen(args, env=env, preexec_fn=os.setpgrp))

            while True:
                try:
                    if all([process.poll() is not None for process in ps]):
                        # All processes have terminated, stop polling...
                        break
                except KeyboardInterrupt:
                    print(
                        f"\nKeyboard interrupt received, terminating {len(ps)} processes..."
                    )
                    for process in ps:
                        process.terminate()
                    print("Done")
                    sys.exit(0)
                except Exception as e:
                    for process in ps:
                        process.terminate()
                    raise e


def main():
    args = _get_args()
    run(
        args.file_path,
        args.dataflow_name,
        args.dataflow_args,
        args.processes,
        args.workers_per_process,
        args.snapshot_every,
        args.recovery_engine,
        args.kafka_topic,
        args.kafka_brokers,
        args.sqlite_directory,
    )


if __name__ == "__main__":
    main()