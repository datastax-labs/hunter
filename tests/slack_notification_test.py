import json

from hunter.series import Series, Metric
from hunter.slack import SlackNotification


def test_blocks_dispatch():
    series1 = [
        1.02,
        0.95,
        0.99,
        1.00,
        1.12,
        0.90,
        0.50,
        0.51,
        0.48,
        0.48,
        0.55,
        0.26,
        0.27,
        0.25,
        0.26,
        0.24,
    ]
    series2 = [
        2.02,
        2.03,
        2.01,
        2.04,
        1.82,
        1.85,
        1.79,
        1.81,
        1.80,
        1.76,
        1.78,
        1.59,
        1.51,
        1.50,
        1.56,
        1.58,
    ]
    time = list(range(len(series1)))
    test = Series(
        "test",
        time,
        metrics={"series1": Metric(), "series2": Metric()},
        data={"series1": series1, "series2": series2},
        attributes={},
    )
    changepoints = test.analyze().change_points_by_time
    dispatches = SlackNotification(change_point_groups={"test": changepoints}).create_dispatches()
    assert len(dispatches) == 1, "Unexpected number of Slack messages created"
    with open("tests/resources/expected-slack-blocks.json", "r") as f:
        assert dispatches[0] == json.loads(f.read())
