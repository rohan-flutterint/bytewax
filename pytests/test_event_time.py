from datetime import datetime, timedelta, timezone

from bytewax.dataflow import Dataflow
from bytewax.testing import TestingInput, TestingOutput, run_main
from bytewax.window import EventClockConfig, TumblingWindow


def test_event_time_processing():
    align_to = datetime(2022, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    window_length = timedelta(seconds=5)

    inp = [
        # This should be processed in the first window
        {"type": "temp", "time": align_to, "value": 1},
        # This too should be processed in the first window
        {"type": "temp", "time": align_to + timedelta(seconds=2), "value": 2},
        # This should be processed in the second window
        {"type": "temp", "time": align_to + timedelta(seconds=7), "value": 200},
        # This should be processed in the third window
        {"type": "temp", "time": align_to + timedelta(seconds=12), "value": 17},
        # This should be dropped, because the first window already closed.
        {"type": "temp", "time": align_to + timedelta(seconds=1), "value": 200},
    ]

    def extract_sensor_type(event):
        return event["type"], event

    def acc_values(acc, event):
        acc.append(event["value"])
        return acc

    cc = EventClockConfig(
        lambda event: event["time"], wait_for_system_duration=timedelta(seconds=0)
    )
    wc = TumblingWindow(align_to=align_to, length=window_length)

    flow = Dataflow()
    flow.input("inp", TestingInput(inp))
    flow.map(extract_sensor_type)
    flow.fold_window("running_average", cc, wc, list, acc_values)
    flow.map(lambda x: {f"{x[0]}_avg": sum(x[1]) / len(x[1])})
    out = []
    flow.output("out", TestingOutput(out))
    run_main(flow)

    expected = [
        {"temp_avg": 1.5},
        {"temp_avg": 200},
        {"temp_avg": 17},
    ]
    assert out == expected
