import ast
import json
import urllib.request

from dataclasses import dataclass
from datetime import datetime, timedelta
from logging import info, warning
from typing import Dict, List, Optional

from hunter.data_selector import DataSelector


@dataclass
class GraphiteConfig:
    url: str
    suffixes: List[str]


@dataclass
class DataPoint:
    time: int
    value: float


@dataclass
class TimeSeries:
    name: str
    points: List[DataPoint]


def decode_graphite_datapoints(series: Dict[str, List[List[float]]]) -> List[DataPoint]:

    points = series["datapoints"]
    return [DataPoint(int(p[1]), p[0]) for p in points if p[0] is not None]


def to_graphite_time(time: datetime, default: str) -> str:
    if time is not None:
        return time.strftime("%H:%M_%Y%m%d")
    else:
        return default


@dataclass
class GraphiteError(IOError):
    message: str


@dataclass
class GraphiteEvent:
    test_owner: str
    test_name: str
    run_id: str
    status: str
    start_time: int
    pub_time: int
    end_time: int
    version: Optional[str]
    branch: Optional[str]
    commit: Optional[str]

    def __init__(
        self,
        pub_time: str,
        test_owner: str,
        test_name: str,
        run_id: str,
        status: str,
        start_time: str,
        end_time: str,
        version: Optional[str],
        branch: Optional[str],
        commit: Optional[str],
    ):
        self.test_owner = test_owner
        self.test_name = test_name
        self.run_id = run_id
        self.status = status
        self.start_time = int(start_time)
        self.pub_time = int(pub_time)
        self.end_time = int(end_time)
        if len(version) == 0 or version == "null":
            self.version = None
        else:
            self.version = version
        if len(branch) == 0 or branch == "null":
            self.branch = None
        else:
            self.branch = branch
        if len(commit) == 0 or commit == "null":
            self.commit = None
        else:
            self.commit = commit


class Graphite:
    __url: str

    def __init__(self, conf: GraphiteConfig):
        self.__url = conf.url

    def fetch_events(
        self, fallout_user: str, test_name: str, from_time: datetime, until_time: datetime
    ) -> List[GraphiteEvent]:
        """
        Returns 'Performance Test' events that match all of
        the following criteria:
        - the Fallout user of interest
        - the Fallout test of interest
        - published between given from_time and until_time (both bounds inclusive)

        References:
            - Graphite events REST API: https://graphite.readthedocs.io/en/stable/events.html
            - Haxx: https://github.com/riptano/haxx/pull/588
        """
        try:
            from_time = to_graphite_time(from_time, "-365d")
            until_time = to_graphite_time(until_time, "now")

            url = (
                f"{self.__url}events/get_data"
                f"?tags={test_name}+{fallout_user}"
                f"&from={from_time}"
                f"&until={until_time}"
                f"&set=intersection"
            )

            data_str = urllib.request.urlopen(url).read()
            data_as_json = json.loads(data_str)
            return [
                GraphiteEvent(event.get("when"), **ast.literal_eval(event.get("data")))
                for event in data_as_json
                if event.get("what") == "Performance Test"
            ]

        except IOError as e:
            raise GraphiteError(f"Failed to fetch Graphite events: {str(e)}")

    def fetch_event(
        self, fallout_user: str, test_name: str, timestamp: datetime
    ) -> Optional[GraphiteEvent]:
        """
        Queries the Graphite events API endpoint, returns at most one event that meets
        the following criteria:
        - the Fallout user of interest
        - the Fallout test of interest
        - the passed timestamp is within the bounds of the start and end timestamps of
          the Fallout run associated with the event

        If none of the events meet the criteria, a warning is printed and None is returned.
        If more than one event meets the criteria, a warning is printed and
        the first event is returned.

        Limitation:
        - The fallout test event must be published earliest 7 days before and latest
          7 days after the given timestamp; otherwise it will be missed

        References:
            - Graphite events REST API: https://graphite.readthedocs.io/en/stable/events.html
            - Haxx: https://github.com/riptano/haxx/pull/588
        """

        # We obviously don't want to fetch the entire database here.
        # We assume a single test does not run longer than a week.
        from_time = timestamp - timedelta(days=7)
        until_time = timestamp + timedelta(days=7)
        events = self.fetch_events(fallout_user, test_name, from_time, until_time)
        events = [e for e in events if e.start_time <= timestamp.timestamp() <= e.end_time]
        if not events:
            warning(
                f"No Graphite events for {fallout_user}'s test {test_name} "
                f"with metric timestamp {timestamp}"
            )
        if len(events) > 1:
            warning(
                f"Found multiple potential Graphite events for {fallout_user}'s test {test_name} "
                f"with metric timestamp: {timestamp}. Returning the first one."
            )
        return next(iter(events), None)

    def fetch_data(
        self, prefix: str, suffixes: List[str], selector: DataSelector
    ) -> List[TimeSeries]:
        """
        Connects to Graphite server and downloads interesting series with the
        given prefix. The series to be downloaded are picked from SUFFIXES list.
        """
        try:
            info("Fetching data from Graphite...")
            result = []
            if selector.metrics is not None:
                metrics = "{" + ",".join(selector.metrics) + "}"
            else:
                metrics = "*"
            from_time = to_graphite_time(selector.from_time, "-365d")
            until_time = to_graphite_time(selector.until_time, "now")
            targets = ""
            for suffix in suffixes:
                targets += f"target={prefix}.{suffix}.{metrics}&"
            targets.strip("&")

            url = (
                f"{self.__url}render"
                f"?{targets}"
                f"&format=json"
                f"&from={from_time}"
                f"&until={until_time}"
            )

            data_str = urllib.request.urlopen(url).read()
            data_as_json = json.loads(data_str)
            for s in data_as_json:
                series = TimeSeries(name=s["target"], points=decode_graphite_datapoints(s))
                if len(series.points) > 5:
                    result.append(series)
                else:
                    warning(
                        f"Not enough data points in series {series.name}. "
                        f"Required at least 5 points."
                    )

            return result

        except IOError as err:
            raise GraphiteError(f"Failed to fetch data from Graphite: {str(err)}")

    def fetch_metric_paths(self, prefix: str, paths: Optional[List[str]] = None) -> List[str]:
        """
        Provided a valid Graphite metric prefix, this method will retrieve all corresponding metric paths
        Reference:
        - https://graphite-api.readthedocs.io/en/latest/api.html
        """
        if paths is None:
            paths = []
        try:
            url = f"{self.__url}metrics/find?query={prefix}"
            data_str = urllib.request.urlopen(url).read()
            data_as_json = json.loads(data_str)
            for result in data_as_json:
                curr_path = result["id"]
                if result["leaf"]:
                    paths.append(curr_path)
                else:
                    paths = self.fetch_metric_paths(f"{curr_path}.*", paths)
            return sorted(paths)
        except IOError as err:
            raise GraphiteError(f"Failed to fetch metric path from Graphite: {str(err)}")
