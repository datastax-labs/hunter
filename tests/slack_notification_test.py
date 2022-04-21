import json
from datetime import datetime
from typing import Dict, List

from dateutil import tz

from hunter.data_selector import DataSelector
from hunter.series import Metric, Series
from hunter.slack import NotificationError, SlackNotifier

NOTIFICATION_CHANNELS = ["a-channel", "b-channel"]


class DispatchTrackingMockClient:
    dispatches: Dict[str, List[List[object]]] = dict()

    def chat_postMessage(self, channel: str = None, blocks: List[object] = None):
        if not channel or not blocks:
            raise NotificationError(f"Invalid dispatch: {channel} {blocks}")
        if channel not in self.dispatches:
            self.dispatches[channel] = []
        self.dispatches[channel].append(blocks)


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
        branch=None,
        time=time,
        metrics={"series1": Metric(), "series2": Metric()},
        data={"series1": series1, "series2": series2},
        attributes={},
    )
    data_selector = DataSelector()
    since_time = datetime(1970, 1, 1, tzinfo=tz.UTC)
    data_selector.since_time = since_time
    data_selector.until_time = datetime(1970, 1, 1, hour=1, tzinfo=tz.UTC)
    analyzed_series = test.analyze()
    mock_client = DispatchTrackingMockClient()
    notifier = SlackNotifier(client=mock_client)
    notifier.notify(
        test_analyzed_series={"test": analyzed_series},
        selector=data_selector,
        channels=NOTIFICATION_CHANNELS,
        since=since_time,
    )
    dispatches = mock_client.dispatches
    assert list(dispatches.keys()) == NOTIFICATION_CHANNELS, "Wrong channels were notified"
    for channel in NOTIFICATION_CHANNELS:
        assert len(dispatches[channel]) == 1, "Unexpected number of Slack messages created"
        with open("tests/resources/expected-slack-blocks.json", "r") as f:
            assert dispatches[channel][0] == json.loads(f.read())
