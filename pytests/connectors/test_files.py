from pathlib import Path

from pytest import raises

from bytewax.connectors.files import DirInput, DirOutput, FileInput, FileOutput
from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.testing import TestingInput, TestingOutput


def test_dir_input():
    flow = Dataflow()

    flow.input("inp", DirInput(Path("examples/sample_data/cluster")))

    out = []
    flow.output("out", TestingOutput(out))

    run_main(flow)

    assert "one1" in out
    assert "two1" in out
    assert "three1" in out
    assert "four1" in out
    assert "five1" in out


def test_dir_input_raises_on_non_exist():
    path = Path("examples/sample_data/bluster")

    with raises(ValueError) as exinfo:
        flow = Dataflow()

        flow.input("inp", DirInput(path))

        run_main(flow)

    assert str(exinfo.value) == f"input directory `{path}` does not exist"


def test_dir_input_raises_on_file():
    path = Path("examples/sample_data/cluster/partition-1.txt")

    with raises(ValueError) as exinfo:
        flow = Dataflow()

        flow.input("inp", DirInput(path))

        run_main(flow)

    assert str(exinfo.value) == f"input directory `{path}` is not a directory"


def test_file_input():
    file_path = Path("examples/sample_data/cluster/partition-1.txt")

    flow = Dataflow()

    flow.input("inp", FileInput(file_path))

    out = []
    flow.output("out", TestingOutput(out))

    run_main(flow)

    assert out == [
        "one1",
        "one2",
        "one3",
        "one4",
        "one5",
        "one6",
    ]


def test_file_output(tmp_path):
    file_path = tmp_path / "out.txt"

    flow = Dataflow()

    inp = [
        ("1", "1"),
        ("2", "2"),
        ("3", "3"),
    ]
    flow.input("inp", TestingInput(inp))

    flow.output("out", FileOutput(file_path))

    run_main(flow)

    with open(file_path, "r") as f:
        out = f.readlines()
        assert out == [
            "1\n",
            "2\n",
            "3\n",
        ]


def test_dir_output(tmp_path):
    flow = Dataflow()

    inp = [
        ("1", "1"),
        ("2", "2"),
        ("3", "3"),
    ]
    flow.input("inp", TestingInput(inp))

    # Route each item to the partition index that is int version of
    # the key (which must be a str).
    flow.output("out", DirOutput(tmp_path, 3, assign_file=int))

    run_main(flow)

    with open(tmp_path / "part_0", "r") as f:
        out = f.readlines()
        assert out == ["3\n"]

    with open(tmp_path / "part_1", "r") as f:
        out = f.readlines()
        assert out == ["1\n"]

    with open(tmp_path / "part_2", "r") as f:
        out = f.readlines()
        assert out == ["2\n"]